// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "MDCache.h"
#include "MDStore.h"
#include "MDS.h"
#include "Server.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "AnchorClient.h"
#include "Migrator.h"
#include "Renamer.h"

#include "MDSMap.h"

#include "CInode.h"
#include "CDir.h"

#include "include/filepath.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "common/Logger.h"

#include "osdc/Filer.h"

#include "events/EInodeUpdate.h"
#include "events/EDirUpdate.h"
#include "events/EUnlink.h"

#include "messages/MGenericMessage.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"
#include "messages/MCacheExpire.h"

#include "messages/MInodeFileCaps.h"

#include "messages/MInodeLink.h"
#include "messages/MInodeLinkAck.h"
#include "messages/MInodeUnlink.h"
#include "messages/MInodeUnlinkAck.h"

#include "messages/MLock.h"
#include "messages/MDentryUnlink.h"

#include "messages/MClientRequest.h"
#include "messages/MClientFileCaps.h"

#include "IdAllocator.h"

#include "common/Timer.h"

#include <assert.h>
#include <errno.h>
#include <iostream>
#include <string>
#include <map>
using namespace std;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".cache "




MDCache::MDCache(MDS *m)
{
  mds = m;
  migrator = new Migrator(mds, this);
  renamer = new Renamer(mds, this);
  root = NULL;
  lru.lru_set_max(g_conf.mds_cache_size);
  lru.lru_set_midpoint(g_conf.mds_cache_mid);

  did_shutdown_exports = false;
  shutdown_commits = 0;
}

MDCache::~MDCache() 
{
}



void MDCache::log_stat(Logger *logger)
{
  if (get_root()) {
    logger->set("popanyd", (int)get_root()->popularity[MDS_POP_ANYDOM].meta_load());
    logger->set("popnest", (int)get_root()->popularity[MDS_POP_NESTED].meta_load());
  }
  logger->set("c", lru.lru_get_size());
  logger->set("cpin", lru.lru_get_num_pinned());
  logger->set("ctop", lru.lru_get_top());
  logger->set("cbot", lru.lru_get_bot());
  logger->set("cptail", lru.lru_get_pintail());
}


// 

bool MDCache::shutdown()
{
  if (lru.lru_get_size() > 0) {
    dout(7) << "WARNING: mdcache shutodwn with non-empty cache" << endl;
    //show_cache();
    show_imports();
    //dump();
  }
  return true;
}


// MDCache

CInode *MDCache::create_inode()
{
  CInode *in = new CInode;

  // zero
  memset(&in->inode, 0, sizeof(inode_t));
  
  // assign ino
  in->inode.ino = mds->idalloc->alloc_id();

  in->inode.nlink = 1;   // FIXME

  in->inode.layout = g_OSD_FileLayout;

  add_inode(in);  // add
  return in;
}

void MDCache::destroy_inode(CInode *in)
{
  mds->idalloc->reclaim_id(in->ino());
  remove_inode(in);
}


void MDCache::add_inode(CInode *in) 
{
  // add to lru, inode map
  assert(inode_map.size() == lru.lru_get_size());
  lru.lru_insert_mid(in);
  assert(inode_map.count(in->ino()) == 0);  // should be no dup inos!
  inode_map[ in->ino() ] = in;
  assert(inode_map.size() == lru.lru_get_size());
}

void MDCache::remove_inode(CInode *o) 
{ 
  dout(14) << "remove_inode " << *o << endl;
  if (o->get_parent_dn()) {
    // FIXME: multiple parents?
    CDentry *dn = o->get_parent_dn();
    assert(!dn->is_dirty());
    if (dn->is_sync())
      dn->dir->remove_dentry(dn);  // unlink inode AND hose dentry
    else
      dn->dir->unlink_inode(dn);   // leave dentry
  }
  inode_map.erase(o->ino());    // remove from map
  lru.lru_remove(o);           // remove from lru
}




void MDCache::rename_file(CDentry *srcdn, 
                          CDentry *destdn)
{
  CInode *in = srcdn->inode;

  // unlink src
  srcdn->dir->unlink_inode(srcdn);
  
  // unlink old inode?
  if (destdn->inode) destdn->dir->unlink_inode(destdn);
  
  // link inode w/ dentry
  destdn->dir->link_inode( destdn, in );
}






bool MDCache::trim(int max) 
{
  // empty?  short cut.
  if (lru.lru_get_size() == 0) return true;

  if (max < 0) {
    max = lru.lru_get_max();
    if (!max) return false;
  }

  map<int, MCacheExpire*> expiremap;

  dout(7) << "trim max=" << max << "  cur=" << lru.lru_get_size() << endl;
  assert(expiremap.empty());

  while (lru.lru_get_size() > (unsigned)max) {
    CInode *in = (CInode*)lru.lru_expire();
    if (!in) break; //return false;

    if (in->dir) {
      // notify dir authority?
      int auth = in->dir->authority();
      if (auth != mds->get_nodeid()) {
        dout(17) << "sending expire to mds" << auth << " on   " << *in->dir << endl;
        if (expiremap.count(auth) == 0) expiremap[auth] = new MCacheExpire(mds->get_nodeid());
        expiremap[auth]->add_dir(in->ino(), in->dir->replica_nonce);
      }
    }

    // notify inode authority?
    {
      int auth = in->authority();
      if (auth != mds->get_nodeid()) {
        assert(!in->is_auth());
        dout(17) << "sending expire to mds" << auth << " on " << *in << endl;
        if (expiremap.count(auth) == 0) expiremap[auth] = new MCacheExpire(mds->get_nodeid());
        expiremap[auth]->add_inode(in->ino(), in->replica_nonce);
      }    else {
        assert(in->is_auth());
      }
    }
    CInode *diri = NULL;
    if (in->parent)
      diri = in->parent->dir->inode;

    if (in->is_root()) {
      dout(7) << "just trimmed root, cache now empty." << endl;
      root = NULL;
    }


    // last link?
    if (in->inode.nlink == 0) {
      dout(17) << "last link, removing file content " << *in << endl;             // FIXME THIS IS WRONG PLACE FOR THIS!
      mds->filer->zero(in->inode, 
                       0, in->inode.size, 
                       NULL, NULL);   // FIXME
    }

    // remove it
    dout(15) << "trim removing " << *in << " " << in << endl;
    remove_inode(in);
    delete in;

    if (diri) {
      // dir incomplete!
      diri->dir->state_clear(CDIR_STATE_COMPLETE);

      // reexport?
      if (diri->dir->is_import() &&             // import
          diri->dir->get_size() == 0 &&         // no children
          !diri->is_root())                   // not root
        migrator->export_empty_import(diri->dir);
      
    } 

    mds->logger->inc("cex");
  }


  /* hack
  if (lru.lru_get_size() == max) {
    int i;
    dout(1) << "lru_top " << lru.lru_ntop << "/" << lru.lru_num << endl;
    CInode *cur = (CInode*)lru.lru_tophead;
    i = 1;
    while (cur) {
      dout(1) << " top " << i++ << "/" << lru.lru_ntop << " " << cur->lru_is_expireable() << "  " << *cur << endl;
      cur = (CInode*)cur->lru_next;
    }

    dout(1) << "lru_bot " << lru.lru_nbot << "/" << lru.lru_num << endl;
    cur = (CInode*)lru.lru_bothead;
    i = 1;
    while (cur) {
      dout(1) << " bot " << i++ << "/" << lru.lru_nbot << " " << cur->lru_is_expireable() << "  " << *cur << endl;
      cur = (CInode*)cur->lru_next;
    }

  }
  */

  // send expires
  for (map<int, MCacheExpire*>::iterator it = expiremap.begin();
       it != expiremap.end();
       it++) {
    dout(7) << "sending cache_expire to " << it->first << endl;
    mds->send_message_mds(it->second, it->first, MDS_PORT_CACHE);
  }


  return true;
}

class C_MDC_ShutdownCommit : public Context {
  MDCache *mdc;
public:
  C_MDC_ShutdownCommit(MDCache *mdc) {
    this->mdc = mdc;
  }
  void finish(int r) {
    mdc->shutdown_commits--;
  }
};

class C_MDC_ShutdownCheck : public Context {
  MDCache *mdc;
  Mutex *lock;
public:
  C_MDC_ShutdownCheck(MDCache *m, Mutex *l) : mdc(m), lock(l) {}
  void finish(int) {
    lock->Lock();
    mdc->shutdown_check();
    lock->Unlock();
  }
};

void MDCache::shutdown_check()
{
  dout(0) << "shutdown_check at " << g_clock.now() << endl;

  // cache
  int o = g_conf.debug_mds;
  g_conf.debug_mds = 10;
  show_cache();
  g_conf.debug_mds = o;
  g_timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this, &mds->mds_lock));

  // this
  dout(0) << "lru size now " << lru.lru_get_size() << endl;
  dout(0) << "log len " << mds->mdlog->get_num_events() << endl;


  if (exports.size()) 
    dout(0) << "still have " << exports.size() << " exports" << endl;

  if (mds->filer->is_active()) 
    dout(0) << "filer still active" << endl;
}

void MDCache::shutdown_start()
{
  dout(1) << "shutdown_start" << endl;

  if (g_conf.mds_shutdown_check)
    g_timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this, &mds->mds_lock));
}



bool MDCache::shutdown_pass()
{
  dout(7) << "shutdown_pass" << endl;
  //assert(mds->is_shutting_down());
  if (mds->is_stopped()) {
    dout(7) << " already shut down" << endl;
    show_cache();
    show_imports();
    return true;
  }

  // unhash dirs?
  if (!hashdirs.empty()) {
    // unhash any of my dirs?
    for (set<CDir*>::iterator it = hashdirs.begin();
         it != hashdirs.end();
         it++) {
      CDir *dir = *it;
      if (!dir->is_auth()) continue;
      if (dir->is_unhashing()) continue;
      migrator->unhash_dir(dir);
    }

    dout(7) << "waiting for dirs to unhash" << endl;
    return false;
  }

  // commit dirs?
  if (g_conf.mds_commit_on_shutdown) {
    
    if (shutdown_commits < 0) {
      dout(1) << "shutdown_pass committing all dirty dirs" << endl;
      shutdown_commits = 0;
      
      for (hash_map<inodeno_t, CInode*>::iterator it = inode_map.begin();
           it != inode_map.end();
           it++) {
        CInode *in = it->second;
        
        // commit any dirty dir that's ours
        if (in->is_dir() && in->dir && in->dir->is_auth() && in->dir->is_dirty()) {
          mds->mdstore->commit_dir(in->dir, new C_MDC_ShutdownCommit(this));
          shutdown_commits++;
        }
      }
    }

    // commits?
    if (shutdown_commits > 0) {
      dout(7) << "shutdown_commits still waiting for " << shutdown_commits << endl;
      return false;
    }
  }

  // flush anything we can from the cache
  trim(0);
  dout(5) << "cache size now " << lru.lru_get_size() << endl;


  // (wait for) flush log?
  if (g_conf.mds_log_flush_on_shutdown &&
      mds->mdlog->get_num_events()) {
    dout(7) << "waiting for log to flush .. " << mds->mdlog->get_num_events() << endl;
    return false;
  } 
  
  // send all imports back to 0.
  if (mds->get_nodeid() != 0 && !did_shutdown_exports) {
    // flush what i can from the cache first..
    trim(0);

    // export to root
    for (set<CDir*>::iterator it = imports.begin();
         it != imports.end();
         ) {
      CDir *im = *it;
      it++;
      if (im->inode->is_root()) continue;
      if (im->is_frozen() || im->is_freezing()) continue;
      
      dout(7) << "sending " << *im << " back to mds0" << endl;
      migrator->export_dir(im,0);
    }
    did_shutdown_exports = true;
  } 


  // waiting for imports?  (e.g. root?)
  if (exports.size()) {
    dout(7) << "still have " << exports.size() << " exports" << endl;
    //show_cache();
    return false;
  }

  // filer active?
  if (mds->filer->is_active()) {
    dout(7) << "filer still active" << endl;
    return false;
  }
  
  // close root?
  if (mds->get_nodeid() == 0 &&
      lru.lru_get_size() == 1 &&
      root && 
      root->dir && 
      root->dir->is_import() &&
      root->dir->get_ref() == 1) {  // 1 is the import!
    // un-import
    dout(7) << "removing root import" << endl;
    imports.erase(root->dir);
    root->dir->state_clear(CDIR_STATE_IMPORT);
    root->dir->put(CDIR_PIN_IMPORT);

    if (root->is_pinned_by(CINODE_PIN_DIRTY)) {
      dout(7) << "clearing root dirty flag" << endl;
      root->put(CINODE_PIN_DIRTY);
    }

    trim(0);
    assert(inode_map.size() == lru.lru_get_size());
  }
  
  // imports?
  if (!imports.empty()) {
    dout(7) << "still have " << imports.size() << " imports" << endl;
    show_cache();
    return false;
  }
  
  // done?
  if (lru.lru_get_size() > 0) {
    dout(7) << "there's still stuff in the cache: " << lru.lru_get_size() << endl;
    show_cache();
    //dump();
    return false;
  } 
  
  // done!
  dout(1) << "shutdown done." << endl;
  return true;
}







int MDCache::open_root(Context *c)
{
  int whoami = mds->get_nodeid();

  // open root inode
  if (whoami == 0) { 
    // i am root inode
    CInode *root = new CInode();
    memset(&root->inode, 0, sizeof(inode_t));
    root->inode.ino = 1;
    root->inode.hash_seed = 0;   // not hashed!

    // make it up (FIXME)
    root->inode.mode = 0755 | INODE_MODE_DIR;
    root->inode.size = 0;
    root->inode.ctime = 0;
    root->inode.mtime = g_clock.gettime();

    root->inode.nlink = 1;
    root->inode.layout = g_OSD_MDDirLayout;

    root->state_set(CINODE_STATE_ROOT);

    set_root( root );

    // root directory too
    assert(root->dir == NULL);
    root->set_dir( new CDir(root, mds, true) );
    root->dir->set_dir_auth( 0 );  // me!
    root->dir->dir_rep = CDIR_REP_ALL;   //NONE;

    // root is sort of technically an import (from a vacuum)
    imports.insert( root->dir );
    root->dir->state_set(CDIR_STATE_IMPORT);
    root->dir->get(CDIR_PIN_IMPORT);

    if (c) {
      c->finish(0);
      delete c;
    }
  } else {
    // request inode from root mds
    if (waiting_for_root.empty()) {
      dout(7) << "discovering root" << endl;

      filepath want;
      MDiscover *req = new MDiscover(whoami,
                                     0,
                                     want,
                                     false);  // there _is_ no base dir for the root inode
      mds->send_message_mds(req, 0, MDS_PORT_CACHE);
    } else {
      dout(7) << "waiting for root" << endl;
    }    

    // wait
    waiting_for_root.push_back(c);

  }

  return 0;
}








// ========= messaging ==============


void MDCache::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_DISCOVER:
    handle_discover((MDiscover*)m);
    break;
  case MSG_MDS_DISCOVERREPLY:
    handle_discover_reply((MDiscoverReply*)m);
    break;

    /*
  case MSG_MDS_INODEUPDATE:
    handle_inode_update((MInodeUpdate*)m);
    break;
    */

  case MSG_MDS_INODELINK:
    handle_inode_link((MInodeLink*)m);
    break;
  case MSG_MDS_INODELINKACK:
    handle_inode_link_ack((MInodeLinkAck*)m);
    break;

  case MSG_MDS_DIRUPDATE:
    handle_dir_update((MDirUpdate*)m);
    break;

  case MSG_MDS_CACHEEXPIRE:
    handle_cache_expire((MCacheExpire*)m);
    break;


    // locking
  case MSG_MDS_LOCK:
    handle_lock((MLock*)m);
    break;

    // cache fun
  case MSG_MDS_INODEFILECAPS:
    handle_inode_file_caps((MInodeFileCaps*)m);
    break;

  case MSG_CLIENT_FILECAPS:
    handle_client_file_caps((MClientFileCaps*)m);
    break;

  case MSG_MDS_DENTRYUNLINK:
    handle_dentry_unlink((MDentryUnlink*)m);
    break;


    

    
  default:
    dout(7) << "cache unknown message " << m->get_type() << endl;
    assert(0);
    break;
  }
}


/* path_traverse
 *
 * return values:
 *   <0 : traverse error (ENOTDIR, ENOENT)
 *    0 : success
 *   >0 : delayed or forwarded
 *
 * Notes:
 *   onfinish context is only needed if you specify MDS_TRAVERSE_DISCOVER _and_
 *   you aren't absolutely certain that the path actually exists.  If it doesn't,
 *   the context is needed to pass a (failure) result code.
 */

class C_MDC_TraverseDiscover : public Context {
  Context *onfinish, *ondelay;
 public:
  C_MDC_TraverseDiscover(Context *onfinish, Context *ondelay) {
    this->ondelay = ondelay;
    this->onfinish = onfinish;
  }
  void finish(int r) {
    //dout(10) << "TraverseDiscover r = " << r << endl;
    if (r < 0 && onfinish) {   // ENOENT on discover, pass back to caller.
      onfinish->finish(r);
    } else {
      ondelay->finish(r);      // retry as usual
    }
    delete onfinish;
    delete ondelay;
  }
};

int MDCache::path_traverse(filepath& origpath, 
                           vector<CDentry*>& trace, 
                           bool follow_trailing_symlink,
                           Message *req,
                           Context *ondelay,
                           int onfail,
                           Context *onfinish,
                           bool is_client_req)  // true if req is MClientRequest .. gross, FIXME
{
  int whoami = mds->get_nodeid();
  set< pair<CInode*, string> > symlinks_resolved; // keep a list of symlinks we touch to avoid loops

  bool noperm = false;
  if (onfail == MDS_TRAVERSE_DISCOVER ||
      onfail == MDS_TRAVERSE_DISCOVERXLOCK) noperm = true;

  // root
  CInode *cur = get_root();
  if (cur == NULL) {
    dout(7) << "traverse: i don't have root" << endl;
    open_root(ondelay);
    if (onfinish) delete onfinish;
    return 1;
  }

  // start trace
  trace.clear();

  // make our own copy, since we'll modify when we hit symlinks
  filepath path = origpath;  

  unsigned depth = 0;
  while (depth < path.depth()) {
    dout(12) << "traverse: path seg depth " << depth << " = " << path[depth] << endl;
    
    // ENOTDIR?
    if (!cur->is_dir()) {
      dout(7) << "traverse: " << *cur << " not a dir " << endl;
      delete ondelay;
      if (onfinish) {
        onfinish->finish(-ENOTDIR);
        delete onfinish;
      }
      return -ENOTDIR;
    }

    // open dir
    if (!cur->dir) {
      if (cur->dir_is_auth()) {
        // parent dir frozen_dir?
        if (cur->is_frozen_dir()) {
          dout(7) << "traverse: " << *cur->get_parent_dir() << " is frozen_dir, waiting" << endl;
          cur->get_parent_dir()->add_waiter(CDIR_WAIT_UNFREEZE, ondelay);
          if (onfinish) delete onfinish;
          return 1;
        }

        cur->get_or_open_dir(mds);
        assert(cur->dir);
      } else {
        // discover dir from/via inode auth
        assert(!cur->is_auth());
        if (cur->waiting_for(CINODE_WAIT_DIR)) {
          dout(10) << "traverse: need dir for " << *cur << ", already doing discover" << endl;
        } else {
          filepath want = path.postfixpath(depth);
          dout(10) << "traverse: need dir for " << *cur << ", doing discover, want " << want.get_path() << endl;
          mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					      cur->ino(),
					      want,
					      true),  // need this dir too
				cur->authority(), MDS_PORT_CACHE);
        }
        cur->add_waiter(CINODE_WAIT_DIR, ondelay);
        if (onfinish) delete onfinish;
        return 1;
      }
    }
    
    // frozen?
    /*
    if (cur->dir->is_frozen()) {
      // doh!
      // FIXME: traverse is allowed?
      dout(7) << "traverse: " << *cur->dir << " is frozen, waiting" << endl;
      cur->dir->add_waiter(CDIR_WAIT_UNFREEZE, ondelay);
      if (onfinish) delete onfinish;
      return 1;
    }
    */

    // must read directory hard data (permissions, x bit) to traverse
    if (!noperm && !inode_hard_read_try(cur, ondelay)) {
      if (onfinish) delete onfinish;
      return 1;
    }
    
    // check permissions?
    // XXX
    
    // ..?
    if (path[depth] == "..") {
      trace.pop_back();
      depth++;
      cur = cur->get_parent_inode();
      dout(10) << "traverse: following .. back to " << *cur << endl;
      continue;
    }


    // dentry
    CDentry *dn = cur->dir->lookup(path[depth]);

    // null and last_bit and xlocked by me?
    if (dn && dn->is_null() && 
        dn->is_xlockedbyme(req) &&
        depth == path.depth()-1) {
      dout(10) << "traverse: hit (my) xlocked dentry at tail of traverse, succeeding" << endl;
      trace.push_back(dn);
      break; // done!
    }

    if (dn && !dn->is_null()) {
      // dentry exists.  xlocked?
      if (!noperm && dn->is_xlockedbyother(req)) {
        dout(10) << "traverse: xlocked dentry at " << *dn << endl;
        cur->dir->add_waiter(CDIR_WAIT_DNREAD,
                             path[depth],
                             ondelay);
        if (onfinish) delete onfinish;
        return 1;
      }

      // do we have inode?
      if (!dn->inode) {
        assert(dn->is_remote());
        // do i have it?
        CInode *in = get_inode(dn->get_remote_ino());
        if (in) {
          dout(7) << "linking in remote in " << *in << endl;
          dn->link_remote(in);
        } else {
          dout(7) << "remote link to " << hex << dn->get_remote_ino() << dec << ", which i don't have" << endl;
          open_remote_ino(dn->get_remote_ino(), req,
                          ondelay);
          return 1;
        }        
      }

      // symlink?
      if (dn->inode->is_symlink() &&
          (follow_trailing_symlink || depth < path.depth()-1)) {
        // symlink, resolve!
        filepath sym = dn->inode->symlink;
        dout(10) << "traverse: hit symlink " << *dn->inode << " to " << sym << endl;

        // break up path components
        // /head/symlink/tail
        filepath head = path.prefixpath(depth);
        filepath tail = path.postfixpath(depth+1);
        dout(10) << "traverse: path head = " << head << endl;
        dout(10) << "traverse: path tail = " << tail << endl;
        
        if (symlinks_resolved.count(pair<CInode*,string>(dn->inode, tail.get_path()))) {
          dout(10) << "already hit this symlink, bailing to avoid the loop" << endl;
          return -ELOOP;
        }
        symlinks_resolved.insert(pair<CInode*,string>(dn->inode, tail.get_path()));

        // start at root?
        if (dn->inode->symlink[0] == '/') {
          // absolute
          trace.clear();
          depth = 0;
          path = tail;
          dout(10) << "traverse: absolute symlink, path now " << path << " depth " << depth << endl;
        } else {
          // relative
          path = head;
          path.append(sym);
          path.append(tail);
          dout(10) << "traverse: relative symlink, path now " << path << " depth " << depth << endl;
        }
        continue;        
      } else {
        // keep going.

        // forwarder wants replicas?
        if (is_client_req && ((MClientRequest*)req)->get_mds_wants_replica_in_dirino()) {
          dout(30) << "traverse: REP is here, " << hex << ((MClientRequest*)req)->get_mds_wants_replica_in_dirino() << " vs " << cur->dir->ino() << dec << endl;
          
          if (((MClientRequest*)req)->get_mds_wants_replica_in_dirino() == cur->dir->ino() &&
              cur->dir->is_auth() && 
              cur->dir->is_rep() &&
              cur->dir->is_open_by(req->get_source().num()) &&
              dn->get_inode()->is_auth()
              ) {
            assert(req->get_source().is_mds());
            int from = req->get_source().num();
            
            if (dn->get_inode()->is_cached_by(from)) {
              dout(15) << "traverse: REP would replicate to mds" << from << ", but already cached_by " 
                       << MSG_ADDR_NICE(req->get_source()) << " dn " << *dn << endl; 
            } else {
              dout(10) << "traverse: REP replicating to " << MSG_ADDR_NICE(req->get_source()) << " dn " << *dn << endl;
              MDiscoverReply *reply = new MDiscoverReply(cur->dir->ino());
              reply->add_dentry( dn->get_name(), !dn->can_read());
              reply->add_inode( dn->inode->replicate_to( from ) );
              mds->send_message_mds(reply, req->get_source().num(), MDS_PORT_CACHE);
            }
          }
        }
            
        trace.push_back(dn);
        cur = dn->inode;
        touch_inode(cur);
        depth++;
        continue;
      }
    }
    
    // MISS.  don't have it.

    int dauth = cur->dir->dentry_authority( path[depth] );
    dout(12) << "traverse: miss on dentry " << path[depth] << " dauth " << dauth << " in " << *cur->dir << endl;
    

    if (dauth == whoami) {
      // dentry is mine.
      if (cur->dir->is_complete()) {
        // file not found
        delete ondelay;
        if (onfinish) {
          onfinish->finish(-ENOENT);
          delete onfinish;
        }
        return -ENOENT;
      } else {
        
        //wrong?
        //if (onfail == MDS_TRAVERSE_DISCOVER) 
        //  return -1;
        
        // directory isn't complete; reload
        dout(7) << "traverse: incomplete dir contents for " << *cur << ", fetching" << endl;
        touch_inode(cur);
        mds->mdstore->fetch_dir(cur->dir, ondelay);
        
        mds->logger->inc("cmiss");

        if (onfinish) delete onfinish;
        return 1;
      }
    } else {
      // dentry is not mine.
      
      /* no, let's let auth handle the discovery/replication ..
      if (onfail == MDS_TRAVERSE_FORWARD && 
          onfinish == 0 &&   // no funnyness
          cur->dir->is_rep()) {
        dout(5) << "trying to discover in popular dir " << *cur->dir << endl;
        onfail = MDS_TRAVERSE_DISCOVER;
      }
      */

      if ((onfail == MDS_TRAVERSE_DISCOVER ||
           onfail == MDS_TRAVERSE_DISCOVERXLOCK)) {
        // discover

        filepath want = path.postfixpath(depth);
        if (cur->dir->waiting_for(CDIR_WAIT_DENTRY, path[depth])) {
          dout(7) << "traverse: already waiting for discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
        } else {
          dout(7) << "traverse: discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
          
          touch_inode(cur);
        
          mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					      cur->ino(),
					      want,
					      false),
				dauth, MDS_PORT_CACHE);
          mds->logger->inc("dis");
        }
        
        // delay processing of current request.
        //  delay finish vs ondelay until result of traverse, so that ENOENT can be 
        //  passed to onfinish if necessary
        cur->dir->add_waiter(CDIR_WAIT_DENTRY, 
                             path[depth], 
                             new C_MDC_TraverseDiscover(onfinish, ondelay));
        
        mds->logger->inc("cmiss");
        return 1;
      } 
      if (onfail == MDS_TRAVERSE_FORWARD) {
        // forward
        dout(7) << "traverse: not auth for " << path << " at " << path[depth] << ", fwd to mds" << dauth << endl;

        if (is_client_req && cur->dir->is_rep()) {
          dout(15) << "traverse: REP fw to mds" << dauth << ", requesting rep under " << *cur->dir << " req " << *(MClientRequest*)req << endl;
          ((MClientRequest*)req)->set_mds_wants_replica_in_dirino(cur->dir->ino());
          req->clear_payload();  // reencode!
        }

        mds->send_message_mds(req, dauth, req->get_dest_port());
        //show_imports();
        
        mds->logger->inc("cfw");
        if (onfinish) delete onfinish;
        delete ondelay;
        return 2;
      }    
      if (onfail == MDS_TRAVERSE_FAIL) {
        delete ondelay;
        if (onfinish) {
          onfinish->finish(-ENOENT);  // -ENOENT, but only because i'm not the authority!
          delete onfinish;
        }
        return -ENOENT;  // not necessarily exactly true....
      }
    }
    
    assert(0);  // i shouldn't get here
  }
  
  // success.
  delete ondelay;
  if (onfinish) {
    onfinish->finish(0);
    delete onfinish;
  }
  return 0;
}



void MDCache::open_remote_dir(CInode *diri,
                              Context *fin) 
{
  dout(10) << "open_remote_dir on " << *diri << endl;
  
  assert(diri->is_dir());
  assert(!diri->dir_is_auth());
  assert(!diri->is_auth());
  assert(diri->dir == 0);

  filepath want;  // no dentries, i just want the dir open
  mds->send_message_mds(new MDiscover(mds->get_nodeid(),
				      diri->ino(),
				      want,
				      true),  // need the dir open
			diri->authority(), MDS_PORT_CACHE);

  diri->add_waiter(CINODE_WAIT_DIR, fin);
}



class C_MDC_OpenRemoteInoLookup : public Context {
  MDCache *mdc;
  inodeno_t ino;
  Message *req;
  Context *onfinish;
public:
  vector<Anchor*> anchortrace;
  C_MDC_OpenRemoteInoLookup(MDCache *mdc, inodeno_t ino, Message *req, Context *onfinish) {
    this->mdc = mdc;
    this->ino = ino;
    this->req = req;
    this->onfinish = onfinish;
  }
  void finish(int r) {
    assert(r == 0);
    if (r == 0)
      mdc->open_remote_ino_2(ino, req, anchortrace, onfinish);
    else {
      onfinish->finish(r);
      delete onfinish;
    }
  }
};

void MDCache::open_remote_ino(inodeno_t ino,
                              Message *req,
                              Context *onfinish)
{
  dout(7) << "open_remote_ino on " << ino << endl;
  
  C_MDC_OpenRemoteInoLookup *c = new C_MDC_OpenRemoteInoLookup(this, ino, req, onfinish);
  mds->anchorclient->lookup(ino, c->anchortrace, c);
}

void MDCache::open_remote_ino_2(inodeno_t ino,
                                Message *req,
                                vector<Anchor*>& anchortrace,
                                Context *onfinish)
{
  dout(7) << "open_remote_ino_2 on " << ino << ", trace depth is " << anchortrace.size() << endl;
  
  // construct path
  filepath path;
  for (unsigned i=0; i<anchortrace.size(); i++) 
    path.add_dentry(anchortrace[i]->ref_dn);

  dout(7) << " path is " << path << endl;

  vector<CDentry*> trace;
  int r = path_traverse(path, trace, false,
                        req,
                        onfinish,  // delay actually
                        MDS_TRAVERSE_DISCOVER);
  if (r > 0) return;
  
  onfinish->finish(r);
  delete onfinish;
}




// path pins

bool MDCache::path_pin(vector<CDentry*>& trace,
                       Message *m,
                       Context *c)
{
  // verify everything is pinnable
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++) {
    CDentry *dn = *it;
    if (!dn->is_pinnable(m)) {
      // wait
      if (c) {
        dout(10) << "path_pin can't pin " << *dn << ", waiting" << endl;
        dn->dir->add_waiter(CDIR_WAIT_DNPINNABLE,   
                            dn->name,
                            c);
      } else {
        dout(10) << "path_pin can't pin, no waiter, failing." << endl;
      }
      return false;
    }
  }

  // pin!
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++) {
    (*it)->pin(m);
    dout(11) << "path_pinned " << *(*it) << endl;
  }

  delete c;
  return true;
}


void MDCache::path_unpin(vector<CDentry*>& trace,
                         Message *m)
{
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++) {
    CDentry *dn = *it;
    dn->unpin(m);
    dout(11) << "path_unpinned " << *dn << endl;

    // did we completely unpin a waiter?
    if (dn->lockstate == DN_LOCK_UNPINNING && !dn->is_pinned()) {
      // return state to sync, in case the unpinner flails
      dn->lockstate = DN_LOCK_SYNC;

      // run finisher right now to give them a fair shot.
      dn->dir->finish_waiting(CDIR_WAIT_DNUNPINNED, dn->name);
    }
  }
}


void MDCache::make_trace(vector<CDentry*>& trace, CInode *in)
{
  CInode *parent = in->get_parent_inode();
  if (parent) {
    make_trace(trace, parent);

    CDentry *dn = in->get_parent_dn();
    dout(15) << "make_trace adding " << *dn << endl;
    trace.push_back(dn);
  }
}


bool MDCache::request_start(Message *req,
                            CInode *ref,
                            vector<CDentry*>& trace)
{
  assert(active_requests.count(req) == 0);

  // pin path
  if (trace.size()) {
    if (!path_pin(trace, req, new C_MDS_RetryMessage(mds,req))) return false;
  }

  dout(7) << "request_start " << *req << endl;

  // add to map
  active_requests[req].ref = ref;
  if (trace.size()) active_requests[req].traces[trace[trace.size()-1]] = trace;

  // request pins
  request_pin_inode(req, ref);
  
  mds->logger->inc("req");

  return true;
}


void MDCache::request_pin_inode(Message *req, CInode *in) 
{
  if (active_requests[req].request_pins.count(in) == 0) {
    in->request_pin_get();
    active_requests[req].request_pins.insert(in);
  }
}

void MDCache::request_pin_dir(Message *req, CDir *dir) 
{
  if (active_requests[req].request_dir_pins.count(dir) == 0) {
    dir->request_pin_get();
    active_requests[req].request_dir_pins.insert(dir);
  }
}


void MDCache::request_cleanup(Message *req)
{
  assert(active_requests.count(req) == 1);

  // leftover xlocks?
  if (active_requests[req].xlocks.size()) {
    set<CDentry*> dns = active_requests[req].xlocks;

    for (set<CDentry*>::iterator it = dns.begin();
         it != dns.end();
         it++) {
      CDentry *dn = *it;
      
      dout(7) << "request_cleanup leftover xlock " << *dn << endl;
      
      dentry_xlock_finish(dn);
      
      // queue finishers
      dn->dir->take_waiting(CDIR_WAIT_ANY, dn->name, mds->finished_queue);

      // remove clean, null dentry?  (from a failed rename or whatever)
      if (dn->is_null() && dn->is_sync() && !dn->is_dirty()) {
        dn->dir->remove_dentry(dn);
      }
    }
    
    assert(active_requests[req].xlocks.empty());  // we just finished finished them
  }

  // foreign xlocks?
  if (active_requests[req].foreign_xlocks.size()) {
    set<CDentry*> dns = active_requests[req].foreign_xlocks;
    active_requests[req].foreign_xlocks.clear();
    
    for (set<CDentry*>::iterator it = dns.begin();
         it != dns.end();
         it++) {
      CDentry *dn = *it;
      
      dout(7) << "request_cleanup sending unxlock for foreign xlock on " << *dn << endl;
      assert(dn->is_xlocked());
      int dauth = dn->dir->dentry_authority(dn->name);
      MLock *m = new MLock(LOCK_AC_UNXLOCK, mds->get_nodeid());
      m->set_dn(dn->dir->ino(), dn->name);
      mds->send_message_mds(m, dauth, MDS_PORT_CACHE);
    }
  }

  // unpin paths
  for (map< CDentry*, vector<CDentry*> >::iterator it = active_requests[req].traces.begin();
       it != active_requests[req].traces.end();
       it++) {
    path_unpin(it->second, req);
  }
  
  // request pins
  for (set<CInode*>::iterator it = active_requests[req].request_pins.begin();
       it != active_requests[req].request_pins.end();
       it++) {
    (*it)->request_pin_put();
  }
  for (set<CDir*>::iterator it = active_requests[req].request_dir_pins.begin();
       it != active_requests[req].request_dir_pins.end();
       it++) {
    (*it)->request_pin_put();
  }

  // remove from map
  active_requests.erase(req);


  // log some stats *****
  mds->logger->set("c", lru.lru_get_size());
  mds->logger->set("cpin", lru.lru_get_num_pinned());
  mds->logger->set("ctop", lru.lru_get_top());
  mds->logger->set("cbot", lru.lru_get_bot());
  mds->logger->set("cptail", lru.lru_get_pintail());
  //mds->logger->set("buf",buffer_total_alloc);

  if (g_conf.log_pins) {
    // pin
    for (int i=0; i<CINODE_NUM_PINS; i++) {
      mds->logger2->set(cinode_pin_names[i],
                        cinode_pins[i]);
    }
    /*
      for (map<int,int>::iterator it = cdir_pins.begin();
      it != cdir_pins.end();
      it++) {
      //string s = "D";
      //s += cdir_pin_names[it->first];
      mds->logger2->set(//s, 
      cdir_pin_names[it->first],
      it->second);
      }
    */
  }

}

void MDCache::request_finish(Message *req)
{
  dout(7) << "request_finish " << *req << endl;
  request_cleanup(req);
  delete req;  // delete req
  
  mds->logger->inc("reply");


  //dump();
}


void MDCache::request_forward(Message *req, int who, int port)
{
  if (!port) port = MDS_PORT_SERVER;

  dout(7) << "request_forward to " << who << " req " << *req << endl;
  request_cleanup(req);
  mds->send_message_mds(req, who, port);

  mds->logger->inc("fw");
}



// ANCHORS

class C_MDC_AnchorInode : public Context {
  CInode *in;
  
public:
  C_MDC_AnchorInode(CInode *in) {
    this->in = in;
  }
  void finish(int r) {
    if (r == 0) {
      assert(in->inode.anchored == false);
      in->inode.anchored = true;

      in->state_clear(CINODE_STATE_ANCHORING);
      in->put(CINODE_PIN_ANCHORING);
      
      in->mark_dirty();
    }

    // trigger
    in->finish_waiting(CINODE_WAIT_ANCHORED, r);
  }
};

void MDCache::anchor_inode(CInode *in, Context *onfinish)
{
  assert(in->is_auth());

  // already anchoring?
  if (in->state_test(CINODE_STATE_ANCHORING)) {
    dout(7) << "anchor_inode already anchoring " << *in << endl;

    // wait
    in->add_waiter(CINODE_WAIT_ANCHORED,
                   onfinish);

  } else {
    dout(7) << "anchor_inode anchoring " << *in << endl;

    // auth: do it
    in->state_set(CINODE_STATE_ANCHORING);
    in->get(CINODE_PIN_ANCHORING);
    
    // wait
    in->add_waiter(CINODE_WAIT_ANCHORED,
                   onfinish);
    
    // make trace
    vector<Anchor*> trace;
    in->make_anchor_trace(trace);
    
    // do it
    mds->anchorclient->create(in->ino(), trace, 
                           new C_MDC_AnchorInode( in ));
  }
}


void MDCache::handle_inode_link(MInodeLink *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  if (!in->is_auth()) {
    assert(in->is_proxy());
    dout(7) << "handle_inode_link not auth for " << *in << ", fw to auth" << endl;
    mds->send_message_mds(m, in->authority(), MDS_PORT_CACHE);
    return;
  }

  dout(7) << "handle_inode_link on " << *in << endl;

  if (!in->is_anchored()) {
    assert(in->inode.nlink == 1);
    dout(7) << "needs anchor, nlink=" << in->inode.nlink << ", creating anchor" << endl;
    
    anchor_inode(in,
                 new C_MDS_RetryMessage(mds, m));
    return;
  }

  in->inode.nlink++;
  in->mark_dirty();

  // reply
  dout(7) << " nlink++, now " << in->inode.nlink++ << endl;

  mds->send_message_mds(new MInodeLinkAck(m->get_ino(), true), m->get_from(), MDS_PORT_CACHE);
  delete m;
}


void MDCache::handle_inode_link_ack(MInodeLinkAck *m) 
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_inode_link_ack success = " << m->is_success() << " on " << *in << endl;
  in->finish_waiting(CINODE_WAIT_LINK,
                     m->is_success() ? 1:-1);
}



// REPLICAS


void MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();

  // from me to me?
  if (dis->get_asker() == whoami) {
    dout(7) << "discover for " << dis->get_want().get_path() << " bounced back to me, dropping." << endl;
    delete dis;
    return;
  }

  CInode *cur = 0;
  MDiscoverReply *reply = 0;
  //filepath fullpath;

  // get started.
  if (dis->get_base_ino() == 0) {
    // wants root
    dout(7) << "discover from mds" << dis->get_asker() << " wants root + " << dis->get_want().get_path() << endl;

    assert(mds->get_nodeid() == 0);
    assert(root->is_auth());

    //fullpath = dis->get_want();


    // add root
    reply = new MDiscoverReply(0);
    reply->add_inode( root->replicate_to( dis->get_asker() ) );
    dout(10) << "added root " << *root << endl;

    cur = root;
    
  } else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino());
    assert(cur);

    if (dis->wants_base_dir()) {
      dout(7) << "discover from mds" << dis->get_asker() << " has " << *cur << " wants dir+" << dis->get_want().get_path() << endl;
    } else {
      dout(7) << "discover from mds" << dis->get_asker() << " has " << *cur->dir << " wants " << dis->get_want().get_path() << endl;
    }
    
    assert(cur->is_dir());
    
    // crazyness?
    if (!cur->dir && !cur->is_auth()) {
      int iauth = cur->authority();
      dout(7) << "no dir and not inode auth; fwd to auth " << iauth << endl;
      mds->send_message_mds( dis, iauth, MDS_PORT_CACHE);
      return;
    }

    // frozen_dir?
    if (!cur->dir && cur->is_frozen_dir()) {
      dout(7) << "is frozen_dir, waiting" << endl;
      cur->get_parent_dir()->add_waiter(CDIR_WAIT_UNFREEZE, 
                                        new C_MDS_RetryMessage(mds, dis));
      return;
    }

    if (!cur->dir) 
      cur->get_or_open_dir(mds);
    assert(cur->dir);

    dout(10) << "dir is " << *cur->dir << endl;
    
    // create reply
    reply = new MDiscoverReply(cur->ino());
  }

  assert(reply);
  assert(cur);
  
  /*
  // first traverse and make sure we won't have to do any waiting
  dout(10) << "traversing full discover path = " << fullpath << endl;
  vector<CInode*> trav;
  int r = path_traverse(fullpath, trav, dis, MDS_TRAVERSE_FAIL);
  if (r > 0) 
    return;  // fw or delay
  dout(10) << "traverse finish w/o blocking, continuing" << endl;
  // ok, now we know we won't block on dentry locks or readdir.
  */


  // add content
  // do some fidgeting to include a dir if they asked for the base dir, or just root.
  for (unsigned i = 0; i < dis->get_want().depth() || dis->get_want().depth() == 0; i++) {
    // add dir
    if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "they don't want the base dir" << endl;
    } else {
      // is it actaully a dir at all?
      if (!cur->is_dir()) {
        dout(7) << "not a dir " << *cur << endl;
        reply->set_flag_error_dir();
        break;
      }

      // add dir
      if (!cur->dir_is_auth()) {
        dout(7) << *cur << " dir auth is someone else, i'm done" << endl;
        break;
      }
      
      // did we hit a frozen_dir?
      if (!cur->dir && cur->is_frozen_dir()) {
        dout(7) << *cur << " is frozen_dir, stopping" << endl;
        break;
      }
      
      if (!cur->dir) cur->get_or_open_dir(mds);
      
      reply->add_dir( new CDirDiscover( cur->dir, 
                                        cur->dir->open_by_add( dis->get_asker() ) ) );
      dout(7) << "added dir " << *cur->dir << endl;
    }
    if (dis->get_want().depth() == 0) break;
    
    // lookup dentry
    int dentry_auth = cur->dir->dentry_authority( dis->get_dentry(i) );
    if (dentry_auth != mds->get_nodeid()) {
      dout(7) << *cur->dir << "dentry " << dis->get_dentry(i) << " auth " << dentry_auth << ", i'm done." << endl;
      break;      // that's it for us!
    }

    // get inode
    CDentry *dn = cur->dir->lookup( dis->get_dentry(i) );
    
    /*
    if (dn && !dn->can_read()) { // xlocked?
      dout(7) << "waiting on " << *dn << endl;
      cur->dir->add_waiter(CDIR_WAIT_DNREAD,
                           dn->name,
                           new C_MDS_RetryMessage(mds, dis));
      return;
    }
    */
    
    if (dn) {
      if (!dn->inode && dn->is_sync()) {
        dout(7) << "mds" << whoami << " dentry " << dis->get_dentry(i) << " null in " << *cur->dir << ", returning error" << endl;
        reply->set_flag_error_dn( dis->get_dentry(i) );
        break;   // don't replicate null but non-locked dentries.
      }
      
      reply->add_dentry( dis->get_dentry(i), !dn->can_read() );
      dout(7) << "added dentry " << *dn << endl;
      
      if (!dn->inode) break;  // we're done.
    }

    if (dn && dn->inode) {
        CInode *next = dn->inode;
        assert(next->is_auth());

        // add inode
        //int nonce = next->cached_by_add(dis->get_asker());
        reply->add_inode( next->replicate_to( dis->get_asker() ) );
        dout(7) << "added inode " << *next << endl;// " nonce=" << nonce<< endl;

        // descend
        cur = next;
    } else {
      // don't have inode?
      if (cur->dir->is_complete()) {
        // set error flag in reply
        dout(7) << "mds" << whoami << " dentry " << dis->get_dentry(i) << " not found in " << *cur->dir << ", returning error" << endl;
        reply->set_flag_error_dn( dis->get_dentry(i) );
        break;
      } else {
        // readdir
        dout(7) << "mds" << whoami << " incomplete dir contents for " << *cur->dir << ", fetching" << endl;

        //mds->mdstore->fetch_dir(cur->dir, NULL); //new C_MDS_RetryMessage(mds, dis));
        //break; // send what we have so far

        mds->mdstore->fetch_dir(cur->dir, new C_MDS_RetryMessage(mds, dis));
        return;
      }
    }
  }
       
  // how did we do.
  if (reply->is_empty()) {

    // discard empty reply
    delete reply;

    if ((cur->is_auth() || cur->is_proxy() || cur->dir->is_proxy()) &&
        !cur->dir->is_auth()) {
      // fwd to dir auth
      int dirauth = cur->dir->authority();
      if (dirauth == dis->get_asker()) {
        dout(7) << "from (new?) dir auth, dropping (obsolete) discover on floor." << endl;  // XXX FIXME is this right?
        //assert(dis->get_asker() == dis->get_source());  //might be a weird other loop.  either way, asker has it.
        delete dis;
      } else {
        dout(7) << "fwd to dir auth " << dirauth << endl;
        mds->send_message_mds( dis, dirauth, MDS_PORT_CACHE );
      }
      return;
    }
    
    dout(7) << "i'm not auth or proxy, dropping (this empty reply).  i bet i just exported." << endl;
    //assert(0);
    
  } else {
    // send back to asker
    dout(7) << "sending result back to asker mds" << dis->get_asker() << endl;
    mds->send_message_mds(reply, dis->get_asker(), MDS_PORT_CACHE);
  }

  // done.
  delete dis;
}


void MDCache::handle_discover_reply(MDiscoverReply *m) 
{
  // starting point
  CInode *cur;
  list<Context*> finished, error;
  
  if (m->has_root()) {
    // nowhere!
    dout(7) << "discover_reply root + " << m->get_path() << " " << m->get_num_inodes() << " inodes" << endl;
    assert(!root);
    assert(m->get_base_ino() == 0);
    assert(!m->has_base_dentry());
    assert(!m->has_base_dir());
    
    // add in root
    cur = new CInode(false);
      
    m->get_inode(0).update_inode(cur);
    
    // root
    cur->state_set(CINODE_STATE_ROOT);
    set_root( cur );
    dout(7) << " got root: " << *cur << endl;

    // take waiters
    finished.swap(waiting_for_root);
  } else {
    // grab inode
    cur = get_inode(m->get_base_ino());
    
    if (!cur) {
      dout(7) << "discover_reply don't have base ino " << hex << m->get_base_ino() << dec << ", dropping" << endl;
      delete m;
      return;
    }
    
    dout(7) << "discover_reply " << *cur << " + " << m->get_path() << ", have " << m->get_num_inodes() << " inodes" << endl;
  }

  // fyi
  if (m->is_flag_error_dir()) dout(7) << " flag error, dir" << endl;
  if (m->is_flag_error_dn()) dout(7) << " flag error, dentry = " << m->get_error_dentry() << endl;
  dout(10) << "depth is " << m->get_depth() << ", has_root = " << m->has_root() << endl;
  
  // loop over discover results.
  // indexese follow each ([[dir] dentry] inode) 
  // can start, end with any type.
  
  for (int i=m->has_root(); i<m->get_depth(); i++) {
    dout(10) << "discover_reply i=" << i << " cur " << *cur << endl;

    // dir
    if ((i >  0) ||
        (i == 0 && m->has_base_dir())) {
      if (cur->dir) {
        // had it
        /* this is strange, but it happens when:
           we discover multiple dentries under a dir.
           bc, no flag to indicate a dir discover is underway, (as there is w/ a dentry one).
           this is actually good, since (dir aside) they're asking for different information.
        */
        dout(7) << "had " << *cur->dir;
        m->get_dir(i).update_dir(cur->dir);
        dout2(7) << ", now " << *cur->dir << endl;
      } else {
        // add it (_replica_)
        cur->set_dir( new CDir(cur, mds, false) );
        m->get_dir(i).update_dir(cur->dir);
        dout(7) << "added " << *cur->dir << " nonce " << cur->dir->replica_nonce << endl;

        // get waiters
        cur->take_waiting(CINODE_WAIT_DIR, finished);
      }
    }    

    // dentry error?
    if (i == m->get_depth()-1 && 
        m->is_flag_error_dn()) {
      // error!
      assert(cur->is_dir());
      if (cur->dir) {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dentry?" << endl;
        cur->dir->take_waiting(CDIR_WAIT_DENTRY,
                               m->get_error_dentry(),
                               error);
      } else {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dir?" << endl;
        cur->take_waiting(CINODE_WAIT_DIR, error);
      }
      break;
    }

    if (i >= m->get_num_dentries()) break;
    
    // dentry
    dout(7) << "i = " << i << " dentry is " << m->get_dentry(i) << endl;

    CDentry *dn = 0;
    if (i > 0 || 
        m->has_base_dentry()) {
      dn = cur->dir->lookup( m->get_dentry(i) );
      
      if (dn) {
        dout(7) << "had " << *dn << endl;
      } else {
        dn = cur->dir->add_dentry( m->get_dentry(i) );
        if (m->get_dentry_xlock(i)) {
          dout(7) << " new dentry is xlock " << *dn << endl;
          dn->lockstate = DN_LOCK_XLOCK;
          dn->xlockedby = 0;
        }
        dout(7) << "added " << *dn << endl;
      }

      cur->dir->take_waiting(CDIR_WAIT_DENTRY,
                             m->get_dentry(i),
                             finished);
    }
    
    if (i >= m->get_num_inodes()) break;

    // inode
    dout(7) << "i = " << i << " ino is " << m->get_ino(i) << endl;
    CInode *in = get_inode( m->get_inode(i).get_ino() );
    assert(dn);
    
    if (in) {
      dout(7) << "had " << *in << endl;
      
      // fix nonce
      dout(7) << " my nonce is " << in->replica_nonce << ", taking from discover, which  has " << m->get_inode(i).get_replica_nonce() << endl;
      in->replica_nonce = m->get_inode(i).get_replica_nonce();
      
      if (dn && in != dn->inode) {
        dout(7) << " but it's not linked via dentry " << *dn << endl;
        // link
        if (dn->inode) {
          dout(7) << "dentry WAS linked to " << *dn->inode << endl;
          assert(0);  // WTF.
        }
        dn->dir->link_inode(dn, in);
      }
    }
    else {
      assert(dn->inode == 0);  // better not be something else linked to this dentry...

      // didn't have it.
      in = new CInode(false);
      
      m->get_inode(i).update_inode(in);
        
      // link in
      add_inode( in );
      dn->dir->link_inode(dn, in);
      
      dout(7) << "added " << *in << " nonce " << in->replica_nonce << endl;
    }
    
    // onward!
    cur = in;
  }

  // dir error at the end there?
  if (m->is_flag_error_dir()) {
    dout(7) << " flag_error on dir " << *cur << endl;
    assert(!cur->is_dir());
    cur->take_waiting(CINODE_WAIT_DIR, error);
  }

  // finish errors directly
  finish_contexts(error, -ENOENT);

  mds->queue_finished(finished);

  // done
  delete m;
}








/*
int MDCache::send_inode_updates(CInode *in)
{
  assert(in->is_auth());
  for (set<int>::iterator it = in->cached_by_begin(); 
       it != in->cached_by_end(); 
       it++) {
    dout(7) << "sending inode_update on " << *in << " to " << *it << endl;
    assert(*it != mds->get_nodeid());
    mds->send_message_mds(new MInodeUpdate(in, in->get_cached_by_nonce(*it)), *it, MDS_PORT_CACHE);
  }

  return 0;
}


void MDCache::handle_inode_update(MInodeUpdate *m)
{
  inodeno_t ino = m->get_ino();
  CInode *in = get_inode(m->get_ino());
  if (!in) {
    //dout(7) << "inode_update on " << m->get_ino() << ", don't have it, ignoring" << endl;
    dout(7) << "inode_update on " << hex << m->get_ino() << dec << ", don't have it, sending expire" << endl;
    MCacheExpire *expire = new MCacheExpire(mds->get_nodeid());
    expire->add_inode(m->get_ino(), m->get_nonce());
    mds->send_message_mds(expire, m->get_source().num(), MDS_PORT_CACHE);
    goto out;
  }

  if (in->is_auth()) {
    dout(7) << "inode_update on " << *in << ", but i'm the authority!" << endl;
    assert(0); // this should never happen
  }
  
  dout(7) << "inode_update on " << *in << endl;

  // update! NOTE dir_auth is unaffected by this.
  in->decode_basic_state(m->get_payload());

 out:
  // done
  delete m;
}
*/



void MDCache::handle_cache_expire(MCacheExpire *m)
{
  int from = m->get_from();
  int source = MSG_ADDR_NUM(m->get_source());
  map<int, MCacheExpire*> proxymap;
  
  if (m->get_from() == source) {
    dout(7) << "cache_expire from " << from << endl;
  } else {
    dout(7) << "cache_expire from " << from << " via " << source << endl;
  }

  // inodes
  for (map<inodeno_t,int>::iterator it = m->get_inodes().begin();
       it != m->get_inodes().end();
       it++) {
    CInode *in = get_inode(it->first);
    int nonce = it->second;
    
    if (!in) {
      dout(0) << "inode_expire on " << hex << it->first << dec << " from " << from << ", don't have it" << endl;
      assert(in);  // i should be authority, or proxy .. and pinned
    }  
    if (!in->is_auth()) {
      int newauth = in->authority();
      dout(7) << "proxy inode expire on " << *in << " to " << newauth << endl;
      assert(newauth >= 0);
      if (!in->state_test(CINODE_STATE_PROXY)) dout(0) << "missing proxy bit on " << *in << endl;
      assert(in->state_test(CINODE_STATE_PROXY));
      if (proxymap.count(newauth) == 0) proxymap[newauth] = new MCacheExpire(from);
      proxymap[newauth]->add_inode(it->first, it->second);
      continue;
    }
    
    // check nonce
    if (from == mds->get_nodeid()) {
      // my cache_expire, and the export_dir giving auth back to me crossed paths!  
      // we can ignore this.  no danger of confusion since the two parties are both me.
      dout(7) << "inode_expire on " << *in << " from mds" << from << " .. ME!  ignoring." << endl;
    } 
    else if (nonce == in->get_cached_by_nonce(from)) {
      // remove from our cached_by
      dout(7) << "inode_expire on " << *in << " from mds" << from << " cached_by was " << in->cached_by << endl;
      in->cached_by_remove(from);
      in->mds_caps_wanted.erase(from);
      
      // note: this code calls _eval more often than it needs to!
      // fix lock
      if (in->hardlock.is_gathering(from)) {
        in->hardlock.gather_set.erase(from);
        if (in->hardlock.gather_set.size() == 0)
          inode_hard_eval(in);
      }
      if (in->filelock.is_gathering(from)) {
        in->filelock.gather_set.erase(from);
        if (in->filelock.gather_set.size() == 0)
          inode_file_eval(in);
      }
      
      // alone now?
      if (!in->is_cached_by_anyone()) {
        inode_hard_eval(in);
        inode_file_eval(in);
      }

    } 
    else {
      // this is an old nonce, ignore expire.
      dout(7) << "inode_expire on " << *in << " from mds" << from << " with old nonce " << nonce << " (current " << in->get_cached_by_nonce(from) << "), dropping" << endl;
      assert(in->get_cached_by_nonce(from) > nonce);
    }
  }

  // dirs
  for (map<inodeno_t,int>::iterator it = m->get_dirs().begin();
       it != m->get_dirs().end();
       it++) {
    CInode *diri = get_inode(it->first);
    CDir *dir = diri->dir;
    int nonce = it->second;
    
    if (!dir) {
      dout(0) << "dir_expire on " << it->first << " from " << from << ", don't have it" << endl;
      assert(dir);  // i should be authority, or proxy ... and pinned
    }  
    if (!dir->is_auth()) {
      int newauth = dir->authority();
      dout(7) << "proxy dir expire on " << *dir << " to " << newauth << endl;
      if (!dir->is_proxy()) dout(0) << "nonproxy dir expire? " << *dir << " .. auth is " << newauth << " .. expire is from " << from << endl;
      assert(dir->is_proxy());
      assert(newauth >= 0);
      assert(dir->state_test(CDIR_STATE_PROXY));
      if (proxymap.count(newauth) == 0) proxymap[newauth] = new MCacheExpire(from);
      proxymap[newauth]->add_dir(it->first, it->second);
      continue;
    }
    
    // check nonce
    if (from == mds->get_nodeid()) {
      dout(7) << "dir_expire on " << *dir << " from mds" << from << " .. ME!  ignoring" << endl;
    } 
    else if (nonce == dir->get_open_by_nonce(from)) {
      // remove from our cached_by
      dout(7) << "dir_expire on " << *dir << " from mds" << from << " open_by was " << dir->open_by << endl;
      dir->open_by_remove(from);
    } 
    else {
      // this is an old nonce, ignore expire.
      dout(7) << "dir_expire on " << *dir << " from mds" << from << " with old nonce " << nonce << " (current " << dir->get_open_by_nonce(from) << "), dropping" << endl;
      assert(dir->get_open_by_nonce(from) > nonce);
    }
  }

  // send proxy forwards
  for (map<int, MCacheExpire*>::iterator it = proxymap.begin();
       it != proxymap.end();
       it++) {
    dout(7) << "sending proxy forward to " << it->first << endl;
    mds->send_message_mds(it->second, it->first, MDS_PORT_CACHE);
  }

  // done
  delete m;
}



int MDCache::send_dir_updates(CDir *dir, bool bcast)
{
  // this is an FYI, re: replication

  set<int> who = dir->open_by;
  if (bcast) 
    who = mds->get_mds_map()->get_mds();
  
  dout(7) << "sending dir_update on " << *dir << " bcast " << bcast << " to " << who << endl;

  string path;
  dir->inode->make_path(path);

  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = who.begin();
       it != who.end();
       it++) {
    if (*it == whoami) continue;
    //if (*it == except) continue;
    dout(7) << "sending dir_update on " << *dir << " to " << *it << endl;

    mds->send_message_mds(new MDirUpdate(dir->ino(),
					 dir->dir_rep,
					 dir->dir_rep_by,
					 path,
					 bcast),
			  *it, MDS_PORT_CACHE);
  }

  return 0;
}


void MDCache::handle_dir_update(MDirUpdate *m)
{
  CInode *in = get_inode(m->get_ino());
  if (!in || !in->dir) {
    dout(5) << "dir_update on " << hex << m->get_ino() << dec << ", don't have it" << endl;

    // discover it?
    if (m->should_discover()) {
      m->tried_discover();  // only once!
      vector<CDentry*> trace;
      filepath path = m->get_path();

      dout(5) << "trying discover on dir_update for " << path << endl;

      int r = path_traverse(path, trace, true,
                            m, new C_MDS_RetryMessage(mds, m),
                            MDS_TRAVERSE_DISCOVER);
      if (r > 0)
        return;
      if (r == 0) {
        assert(in);
        open_remote_dir(in, new C_MDS_RetryMessage(mds, m));
        return;
      }
      assert(0);
    }

    goto out;
  }

  // update
  dout(5) << "dir_update on " << *in->dir << endl;
  in->dir->dir_rep = m->get_dir_rep();
  in->dir->dir_rep_by = m->get_dir_rep_by();
  
  // done
 out:
  delete m;
}





class C_MDC_DentryUnlink : public Context {
public:
  MDCache *mdc;
  CDentry *dn;
  CDir *dir;
  Context *c;
  C_MDC_DentryUnlink(MDCache *mdc, CDentry *dn, CDir *dir, Context *c) {
    this->mdc = mdc;
    this->dn = dn;
    this->dir = dir;
    this->c = c;
  }
  void finish(int r) {
    assert(r == 0);
    mdc->dentry_unlink_finish(dn, dir, c);
  }
};


// NAMESPACE FUN

void MDCache::dentry_unlink(CDentry *dn, Context *c)
{
  CDir *dir = dn->dir;
  string dname = dn->name;

  assert(dn->lockstate == DN_LOCK_XLOCK);

  // i need the inode to do any of this properly
  assert(dn->inode);

  // log it
  if (dn->inode) dn->inode->mark_unsafe();   // XXX ??? FIXME
  mds->mdlog->submit_entry(new EUnlink(dir, dn),
                           NULL);    // FIXME FIXME FIXME

  // tell replicas
  if (dir->is_open_by_anyone()) {
    for (set<int>::iterator it = dir->open_by_begin();
         it != dir->open_by_end();
         it++) {
      dout(7) << "inode_unlink sending DentryUnlink to " << *it << endl;
      
      mds->send_message_mds(new MDentryUnlink(dir->ino(), dn->name), *it, MDS_PORT_CACHE);
    }

    // don't need ack.
  }


  // inode deleted?
  if (dn->is_primary()) {
    assert(dn->inode->is_auth());
    dn->inode->inode.nlink--;
    
    if (dn->inode->is_dir()) assert(dn->inode->inode.nlink == 0);  // no hard links on dirs

    // last link?
    if (dn->inode->inode.nlink == 0) {
      // truly dangling      
      if (dn->inode->dir) {
        // mark dir clean too, since it now dne!
        assert(dn->inode->dir->is_auth());
        dn->inode->dir->state_set(CDIR_STATE_DELETED);
        dn->inode->dir->remove_null_dentries();
        dn->inode->dir->mark_clean();
      }

      // mark it clean, it's dead
      if (dn->inode->is_dirty())
        dn->inode->mark_clean();
      
    } else {
      // migrate to inode file
      dout(7) << "removed primary, but there are remote links, moving to inode file: " << *dn->inode << endl;

      // dangling but still linked.  
      assert(dn->inode->is_anchored());

      // unlink locally
      CInode *in = dn->inode;
      dn->dir->unlink_inode( dn );
      dn->mark_dirty();

      // mark it dirty!
      in->mark_dirty();

      // update anchor to point to inode file+mds
      vector<Anchor*> atrace;
      in->make_anchor_trace(atrace);
      assert(atrace.size() == 1);   // it's dangling
      mds->anchorclient->update(in->ino(), atrace, 
                             new C_MDC_DentryUnlink(this, dn, dir, c));
      return;
    }
  }
  else if (dn->is_remote()) {
    // need to dec nlink on primary
    if (dn->inode->is_auth()) {
      // awesome, i can do it
      dout(7) << "remote target is local, nlink--" << endl;
      dn->inode->inode.nlink--;
      dn->inode->mark_dirty();

      if (( dn->inode->state_test(CINODE_STATE_DANGLING) && dn->inode->inode.nlink == 0) ||
          (!dn->inode->state_test(CINODE_STATE_DANGLING) && dn->inode->inode.nlink == 1)) {
        dout(7) << "nlink=1+primary or 0+dangling, removing anchor" << endl;

        // remove anchor (async)
        mds->anchorclient->destroy(dn->inode->ino(), NULL);
      }
    } else {
      int auth = dn->inode->authority();
      dout(7) << "remote target is remote, sending unlink request to " << auth << endl;

      mds->send_message_mds(new MInodeUnlink(dn->inode->ino(), mds->get_nodeid()),
			    auth, MDS_PORT_CACHE);

      // unlink locally
      CInode *in = dn->inode;
      dn->dir->unlink_inode( dn );
      dn->mark_dirty();

      // add waiter
      in->add_waiter(CINODE_WAIT_UNLINK, c);
      return;
    }
  }
  else 
    assert(0);   // unlink on null dentry??
 
  // unlink locally
  dn->dir->unlink_inode( dn );
  dn->mark_dirty();

  // finish!
  dentry_unlink_finish(dn, dir, c);
}


void MDCache::dentry_unlink_finish(CDentry *dn, CDir *dir, Context *c)
{
  dout(7) << "dentry_unlink_finish on " << *dn << endl;
  string dname = dn->name;

  // unpin dir / unxlock
  dentry_xlock_finish(dn, true); // quiet, no need to bother replicas since they're already unlinking
  
  // did i empty out an imported dir?
  if (dir->is_import() && !dir->inode->is_root() && dir->get_size() == 0) 
    migrator->export_empty_import(dir);

  // wake up any waiters
  dir->take_waiting(CDIR_WAIT_ANY, dname, mds->finished_queue);

  c->finish(0);
}




void MDCache::handle_dentry_unlink(MDentryUnlink *m)
{
  CInode *diri = get_inode(m->get_dirino());
  CDir *dir = 0;
  if (diri) dir = diri->dir;

  if (!diri || !dir) {
    dout(7) << "handle_dentry_unlink don't have dir " << hex << m->get_dirino() << dec << endl;
  }
  else {
    CDentry *dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << "handle_dentry_unlink don't have dentry " << *dir << " dn " << m->get_dn() << endl;
    } else {
      dout(7) << "handle_dentry_unlink on " << *dn << endl;
      
      // dir?
      if (dn->inode) {
        if (dn->inode->dir) {
          dn->inode->dir->state_set(CDIR_STATE_DELETED);
          dn->inode->dir->remove_null_dentries();
        }
      }
      
      string dname = dn->name;
      
      // unlink
      dn->dir->remove_dentry(dn);
      
      // wake up
      //dir->finish_waiting(CDIR_WAIT_DNREAD, dname);
      dir->take_waiting(CDIR_WAIT_DNREAD, dname, mds->finished_queue);
    }
  }

  delete m;
  return;
}


void MDCache::handle_inode_unlink(MInodeUnlink *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  // proxy?
  if (in->is_proxy()) {
    dout(7) << "handle_inode_unlink proxy on " << *in << endl;
    mds->send_message_mds(m, in->authority(), MDS_PORT_CACHE);
    return;
  }
  assert(in->is_auth());

  // do it.
  dout(7) << "handle_inode_unlink nlink=" << in->inode.nlink << " on " << *in << endl;
  assert(in->inode.nlink > 0);
  in->inode.nlink--;

  if (in->state_test(CINODE_STATE_DANGLING)) {
    // already dangling.
    // last link?
    if (in->inode.nlink == 0) {
      dout(7) << "last link, marking clean and removing anchor" << endl;
      
      in->mark_clean();       // mark it clean.
      
      // remove anchor (async)
      mds->anchorclient->destroy(in->ino(), NULL);
    }
    else {
      in->mark_dirty();
    }
  } else {
    // has primary link still.
    assert(in->inode.nlink >= 1);
    in->mark_dirty();

    if (in->inode.nlink == 1) {
      dout(7) << "nlink=1, removing anchor" << endl;
      
      // remove anchor (async)
      mds->anchorclient->destroy(in->ino(), NULL);
    }
  }

  // ack
  mds->send_message_mds(new MInodeUnlinkAck(m->get_ino()), m->get_from(), MDS_PORT_CACHE);
}

void MDCache::handle_inode_unlink_ack(MInodeUnlinkAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_inode_unlink_ack on " << *in << endl;
  in->finish_waiting(CINODE_WAIT_UNLINK, 0);
}








// file i/o -----------------------------------------

__uint64_t MDCache::issue_file_data_version(CInode *in)
{
  dout(7) << "issue_file_data_version on " << *in << endl;
  return in->inode.file_data_version;
}


Capability* MDCache::issue_new_caps(CInode *in,
                                    int mode,
                                    MClientRequest *req)
{
  dout(7) << "issue_new_caps for mode " << mode << " on " << *in << endl;
  
  // my needs
  int my_client = req->get_client();
  int my_want = 0;
  if (mode & FILE_MODE_R) my_want |= CAP_FILE_RDCACHE  | CAP_FILE_RD;
  if (mode & FILE_MODE_W) my_want |= CAP_FILE_WRBUFFER | CAP_FILE_WR;

  // register a capability
  Capability *cap = in->get_client_cap(my_client);
  if (!cap) {
    // new cap
    Capability c(my_want);
    in->add_client_cap(my_client, c);
    cap = in->get_client_cap(my_client);
    
    // note client addr
    mds->clientmap.add_open(my_client, req->get_client_inst());
    
  } else {
    // make sure it has sufficient caps
    if (cap->wanted() & ~my_want) {
      // augment wanted caps for this client
      cap->set_wanted( cap->wanted() | my_want );
    }
  }

  // suppress file cap messages for this guy for a few moments (we'll bundle with the open() reply)
  cap->set_suppress(true);
  int before = cap->pending();

  if (in->is_auth()) {
    // [auth] twiddle mode?
    inode_file_eval(in);
  } else {
    // [replica] tell auth about any new caps wanted
    request_inode_file_caps(in);
  }
    
  // issue caps (pot. incl new one)
  issue_caps(in);  // note: _eval above may have done this already...

  // re-issue whatever we can
  cap->issue(cap->pending());
  
  // ok, stop suppressing.
  cap->set_suppress(false);

  int now = cap->pending();
  if (before != now &&
      (before & CAP_FILE_WR) == 0 &&
      (now & CAP_FILE_WR)) {
    // FIXME FIXME FIXME
  }
  
  // twiddle file_data_version?
  if ((before & CAP_FILE_WRBUFFER) == 0 &&
      (now & CAP_FILE_WRBUFFER)) {
    in->inode.file_data_version++;
    dout(7) << " incrementing file_data_version, now " << in->inode.file_data_version << " for " << *in << endl;
  }

  return cap;
}



bool MDCache::issue_caps(CInode *in)
{
  // allowed caps are determined by the lock mode.
  int allowed = in->filelock.caps_allowed(in->is_auth());
  dout(7) << "issue_caps filelock allows=" << cap_string(allowed) 
          << " on " << *in << endl;

  // count conflicts with
  int nissued = 0;        

  // client caps
  for (map<int, Capability>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       it++) {
    if (it->second.issued() != (it->second.wanted() & allowed)) {
      // issue
      nissued++;

      int before = it->second.pending();
      long seq = it->second.issue(it->second.wanted() & allowed);
      int after = it->second.pending();

      // twiddle file_data_version?
      if (!(before & CAP_FILE_WRBUFFER) &&
          (after & CAP_FILE_WRBUFFER)) {
        dout(7) << "   incrementing file_data_version for " << *in << endl;
        in->inode.file_data_version++;
      }

      if (seq > 0 && 
          !it->second.is_suppress()) {
        dout(7) << "   sending MClientFileCaps to client" << it->first << " seq " << it->second.get_last_seq() << " new pending " << cap_string(it->second.pending()) << " was " << cap_string(before) << endl;
        mds->messenger->send_message(new MClientFileCaps(in->inode,
                                                         it->second.get_last_seq(),
                                                         it->second.pending(),
                                                         it->second.wanted()),
                                     MSG_ADDR_CLIENT(it->first), mds->clientmap.get_inst(it->first), 
				     0, MDS_PORT_CACHE);
      }
    }
  }

  return (nissued == 0);  // true if no re-issued, no callbacks
}



void MDCache::request_inode_file_caps(CInode *in)
{
  int wanted = in->get_caps_wanted();
  if (wanted != in->replica_caps_wanted) {

    if (wanted == 0) {
      if (in->replica_caps_wanted_keep_until > g_clock.recent_now()) {
        // ok, release them finally!
        in->replica_caps_wanted_keep_until.sec_ref() = 0;
        dout(7) << "request_inode_file_caps " << cap_string(wanted)
                 << " was " << cap_string(in->replica_caps_wanted) 
                 << " no keeping anymore " 
                 << " on " << *in 
                 << endl;
      }
      else if (in->replica_caps_wanted_keep_until.sec() == 0) {
        in->replica_caps_wanted_keep_until = g_clock.recent_now();
        in->replica_caps_wanted_keep_until.sec_ref() += 2;
        
        dout(7) << "request_inode_file_caps " << cap_string(wanted)
                 << " was " << cap_string(in->replica_caps_wanted) 
                 << " keeping until " << in->replica_caps_wanted_keep_until
                 << " on " << *in 
                 << endl;
        return;
      } else {
        // wait longer
        return;
      }
    } else {
      in->replica_caps_wanted_keep_until.sec_ref() = 0;
    }
    assert(!in->is_auth());

    int auth = in->authority();
    dout(7) << "request_inode_file_caps " << cap_string(wanted)
            << " was " << cap_string(in->replica_caps_wanted) 
            << " on " << *in << " to mds" << auth << endl;
    assert(!in->is_auth());

    in->replica_caps_wanted = wanted;
    mds->send_message_mds(new MInodeFileCaps(in->ino(), mds->get_nodeid(),
					     in->replica_caps_wanted),
			  auth, MDS_PORT_CACHE);
  } else {
    in->replica_caps_wanted_keep_until.sec_ref() = 0;
  }
}

void MDCache::handle_inode_file_caps(MInodeFileCaps *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  assert(in->is_auth() || in->is_proxy());
  
  dout(7) << "handle_inode_file_caps replica mds" << m->get_from() << " wants caps " << cap_string(m->get_caps()) << " on " << *in << endl;

  if (in->is_proxy()) {
    dout(7) << "proxy, fw" << endl;
    mds->send_message_mds(m, in->authority(), MDS_PORT_CACHE);
    return;
  }

  if (m->get_caps())
    in->mds_caps_wanted[m->get_from()] = m->get_caps();
  else
    in->mds_caps_wanted.erase(m->get_from());

  inode_file_eval(in);
  delete m;
}


/*
 * note: we only get these from the client if
 * - we are calling back previously issued caps (fewer than the client previously had)
 * - or if the client releases (any of) its caps on its own
 */
void MDCache::handle_client_file_caps(MClientFileCaps *m)
{
  int client = MSG_ADDR_NUM(m->get_source());
  CInode *in = get_inode(m->get_ino());
  Capability *cap = 0;
  if (in) 
    cap = in->get_client_cap(client);

  if (!in || !cap) {
    if (!in) {
      dout(7) << "handle_client_file_caps on unknown ino " << m->get_ino() << ", dropping" << endl;
    } else {
      dout(7) << "handle_client_file_caps no cap for client" << client << " on " << *in << endl;
    }
    delete m;
    return;
  } 
  
  assert(cap);

  // filter wanted based on what we could ever give out (given auth/replica status)
  int wanted = m->get_wanted() & in->filelock.caps_allowed_ever(in->is_auth());
  
  dout(7) << "handle_client_file_caps seq " << m->get_seq() 
          << " confirms caps " << cap_string(m->get_caps()) 
          << " wants " << cap_string(wanted)
          << " from client" << client
          << " on " << *in 
          << endl;  
  
  // update wanted
  if (cap->wanted() != wanted)
    cap->set_wanted(wanted);

  // confirm caps
  int had = cap->confirm_receipt(m->get_seq(), m->get_caps());
  int has = cap->confirmed();
  if (cap->is_null()) {
    dout(7) << " cap for client" << client << " is now null, removing from " << *in << endl;
    in->remove_client_cap(client);
    if (!in->is_auth())
      request_inode_file_caps(in);

    // dec client addr counter
    mds->clientmap.dec_open(client);

    // tell client.
    MClientFileCaps *r = new MClientFileCaps(in->inode, 
                                             0, 0, 0,
                                             MClientFileCaps::FILECAP_RELEASE);
    mds->messenger->send_message(r, m->get_source(), m->get_source_inst(), 0, MDS_PORT_CACHE);
  }

  // merge in atime?
  if (m->get_inode().atime > in->inode.atime) {
      dout(7) << "  taking atime " << m->get_inode().atime << " > " 
              << in->inode.atime << " for " << *in << endl;
    in->inode.atime = m->get_inode().atime;
  }
  
  if ((has|had) & CAP_FILE_WR) {
    bool dirty = false;

    // mtime
    if (m->get_inode().mtime > in->inode.mtime) {
      dout(7) << "  taking mtime " << m->get_inode().mtime << " > " 
              << in->inode.mtime << " for " << *in << endl;
      in->inode.mtime = m->get_inode().mtime;
      dirty = true;
    }
    // size
    if (m->get_inode().size > in->inode.size) {
      dout(7) << "  taking size " << m->get_inode().size << " > " 
              << in->inode.size << " for " << *in << endl;
      in->inode.size = m->get_inode().size;
      dirty = true;
    }

    if (dirty) 
      mds->mdlog->submit_entry(new EInodeUpdate(in));
  }  

  // reevaluate, waiters
  inode_file_eval(in);
  in->finish_waiting(CINODE_WAIT_CAPS, 0);

  delete m;
}










// locks ----------------------------------------------------------------

/*


INODES:

= two types of inode metadata:
   hard  - uid/gid, mode
   file  - mtime, size
 ? atime - atime  (*)       <-- we want a lazy update strategy?

= correspondingly, two types of inode locks:
   hardlock - hard metadata
   filelock - file metadata

   -> These locks are completely orthogonal! 

= metadata ops and how they affect inode metadata:
        sma=size mtime atime
   HARD FILE OP
  files:
    R   RRR stat
    RW      chmod/chown
    R    W  touch   ?ctime
    R       openr
          W read    atime
    R       openw
    Wc      openwc  ?ctime
        WW  write   size mtime
            close 

  dirs:
    R     W readdir atime 
        RRR  ( + implied stats on files)
    Rc  WW  mkdir         (ctime on new dir, size+mtime on parent dir)
    R   WW  link/unlink/rename/rmdir  (size+mtime on dir)

  

= relationship to client (writers):

  - ops in question are
    - stat ... need reasonable value for mtime (+ atime?)
      - maybe we want a "quicksync" type operation instead of full lock
    - truncate ... need to stop writers for the atomic truncate operation
      - need a full lock




= modes
  - SYNC
              Rauth  Rreplica  Wauth  Wreplica
        sync
        




ALSO:

  dirlock  - no dir changes (prior to unhashing)
  denlock  - dentry lock    (prior to unlink, rename)

     
*/


void MDCache::handle_lock(MLock *m)
{
  switch (m->get_otype()) {
  case LOCK_OTYPE_IHARD:
    handle_lock_inode_hard(m);
    break;
    
  case LOCK_OTYPE_IFILE:
    handle_lock_inode_file(m);
    break;
    
  case LOCK_OTYPE_DIR:
    handle_lock_dir(m);
    break;
    
  case LOCK_OTYPE_DN:
    handle_lock_dn(m);
    break;

  default:
    dout(7) << "handle_lock got otype " << m->get_otype() << endl;
    assert(0);
    break;
  }
}
 


// ===============================
// hard inode metadata

bool MDCache::inode_hard_read_try(CInode *in, Context *con)
{
  dout(7) << "inode_hard_read_try on " << *in << endl;  

  // can read?  grab ref.
  if (in->hardlock.can_read(in->is_auth())) 
    return true;
  
  assert(!in->is_auth());

  // wait!
  dout(7) << "inode_hard_read_try waiting on " << *in << endl;
  in->add_waiter(CINODE_WAIT_HARDR, con);
  return false;
}

bool MDCache::inode_hard_read_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_hard_read_start  on " << *in << endl;  

  // can read?  grab ref.
  if (in->hardlock.can_read(in->is_auth())) {
    in->hardlock.get_read();
    return true;
  }
  
  // can't read, and replicated.
  assert(!in->is_auth());

  // wait!
  dout(7) << "inode_hard_read_start waiting on " << *in << endl;
  in->add_waiter(CINODE_WAIT_HARDR, new C_MDS_RetryRequest(mds, m, in));
  return false;
}


void MDCache::inode_hard_read_finish(CInode *in)
{
  // drop ref
  assert(in->hardlock.can_read(in->is_auth()));
  in->hardlock.put_read();

  dout(7) << "inode_hard_read_finish on " << *in << endl;
  
  //if (in->hardlock.get_nread() == 0) in->finish_waiting(CINODE_WAIT_HARDNORD);
}


bool MDCache::inode_hard_write_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_hard_write_start  on " << *in << endl;

  // if not replicated, i can twiddle lock at will
  if (in->is_auth() &&
      !in->is_cached_by_anyone() &&
      in->hardlock.get_state() != LOCK_LOCK) 
    in->hardlock.set_state(LOCK_LOCK);
  
  // can write?  grab ref.
  if (in->hardlock.can_write(in->is_auth())) {
    assert(in->is_auth());
    if (!in->can_auth_pin()) {
      dout(7) << "inode_hard_write_start waiting for authpinnable on " << *in << endl;
      in->add_waiter(CINODE_WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mds, m, in));
      return false;
    }

    in->auth_pin();  // ugh, can't condition this on nwrite==0 bc we twiddle that in handle_lock_*
    in->hardlock.get_write();
    return true;
  }
  
  // can't write, replicated.
  if (in->is_auth()) {
    // auth
    if (in->hardlock.can_write_soon(in->is_auth())) {
      // just wait
    } else {
      // initiate lock
      inode_hard_lock(in);
    }
    
    dout(7) << "inode_hard_write_start waiting on " << *in << endl;
    in->add_waiter(CINODE_WAIT_HARDW, new C_MDS_RetryRequest(mds, m, in));

    return false;
  } else {
    // replica
    // fw to auth
    int auth = in->authority();
    dout(7) << "inode_hard_write_start " << *in << " on replica, fw to auth " << auth << endl;
    assert(auth != mds->get_nodeid());
    request_forward(m, auth);
    return false;
  }
}


void MDCache::inode_hard_write_finish(CInode *in)
{
  // drop ref
  assert(in->hardlock.can_write(in->is_auth()));
  in->hardlock.put_write();
  in->auth_unpin();
  dout(7) << "inode_hard_write_finish on " << *in << endl;
  
  // drop lock?
  if (in->hardlock.get_nwrite() == 0) {

    // auto-sync if alone.
    if (in->is_auth() &&
        !in->is_cached_by_anyone() &&
        in->hardlock.get_state() != LOCK_SYNC) 
      in->hardlock.set_state(LOCK_SYNC);
    
    inode_hard_eval(in);
  }
}


void MDCache::inode_hard_eval(CInode *in)
{
  // finished gather?
  if (in->is_auth() &&
      !in->hardlock.is_stable() &&
      in->hardlock.gather_set.empty()) {
    dout(7) << "inode_hard_eval finished gather on " << *in << endl;
    switch (in->hardlock.get_state()) {
    case LOCK_GLOCKR:
      in->hardlock.set_state(LOCK_LOCK);
      
      // waiters
      in->hardlock.get_write();
      in->finish_waiting(CINODE_WAIT_HARDRWB|CINODE_WAIT_HARDSTABLE);
      in->hardlock.put_write();
      break;
      
    default:
      assert(0);
    }
  }
  if (!in->hardlock.is_stable()) return;
  
  if (in->is_auth()) {

    // sync?
    if (in->is_cached_by_anyone() &&
        in->hardlock.get_nwrite() == 0 &&
        in->hardlock.get_state() != LOCK_SYNC) {
      dout(7) << "inode_hard_eval stable, syncing " << *in << endl;
      inode_hard_sync(in);
    }

  } else {
    // replica
  }
}


// mid

void MDCache::inode_hard_sync(CInode *in)
{
  dout(7) << "inode_hard_sync on " << *in << endl;
  assert(in->is_auth());
  
  // check state
  if (in->hardlock.get_state() == LOCK_SYNC)
    return; // already sync
  if (in->hardlock.get_state() == LOCK_GLOCKR) 
    assert(0); // um... hmm!
  assert(in->hardlock.get_state() == LOCK_LOCK);
  
  // hard data
  bufferlist harddata;
  in->encode_hard_state(harddata);
  
  // bcast to replicas
  for (set<int>::iterator it = in->cached_by_begin(); 
       it != in->cached_by_end(); 
       it++) {
    MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
    m->set_ino(in->ino(), LOCK_OTYPE_IHARD);
    m->set_data(harddata);
    mds->send_message_mds(m, *it, MDS_PORT_CACHE);
  }
  
  // change lock
  in->hardlock.set_state(LOCK_SYNC);
  
  // waiters?
  in->finish_waiting(CINODE_WAIT_HARDSTABLE);
}

void MDCache::inode_hard_lock(CInode *in)
{
  dout(7) << "inode_hard_lock on " << *in << " hardlock=" << in->hardlock << endl;  
  assert(in->is_auth());
  
  // check state
  if (in->hardlock.get_state() == LOCK_LOCK ||
      in->hardlock.get_state() == LOCK_GLOCKR) 
    return;  // already lock or locking
  assert(in->hardlock.get_state() == LOCK_SYNC);
  
  // bcast to replicas
  for (set<int>::iterator it = in->cached_by_begin(); 
       it != in->cached_by_end(); 
       it++) {
    MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
    m->set_ino(in->ino(), LOCK_OTYPE_IHARD);
    mds->send_message_mds(m, *it, MDS_PORT_CACHE);
  }
  
  // change lock
  in->hardlock.set_state(LOCK_GLOCKR);
  in->hardlock.init_gather(in->get_cached_by());
}





// messenger

void MDCache::handle_lock_inode_hard(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_IHARD);
  
  mds->logger->inc("lih");

  int from = m->get_asker();
  CInode *in = get_inode(m->get_ino());
  
  if (LOCK_AC_FOR_AUTH(m->get_action())) {
    // auth
    assert(in);
    assert(in->is_auth() || in->is_proxy());
    dout(7) << "handle_lock_inode_hard " << *in << " hardlock=" << in->hardlock << endl;  

    if (in->is_proxy()) {
      // fw
      int newauth = in->authority();
      assert(newauth >= 0);
      if (from == newauth) {
        dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, but from new auth, dropping" << endl;
        delete m;
      } else {
        dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
        mds->send_message_mds(m, newauth, MDS_PORT_CACHE);
      }
      return;
    }
  } else {
    // replica
    if (!in) {
      dout(7) << "handle_lock_inode_hard " << m->get_ino() << ": don't have it anymore" << endl;
      /* do NOT nak.. if we go that route we need to duplicate all the nonce funkiness
         to keep gather_set a proper/correct subset of cached_by.  better to use the existing
         cacheexpire mechanism instead!
      */
      delete m;
      return;
    }
    
    assert(!in->is_auth());
  }

  dout(7) << "handle_lock_inode_hard a=" << m->get_action() << " from " << from << " " << *in << " hardlock=" << in->hardlock << endl;  
 
  CLock *lock = &in->hardlock;
  
  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_LOCK);
    
    { // assim data
      int off = 0;
      in->decode_hard_state(m->get_data(), off);
    }
    
    // update lock
    lock->set_state(LOCK_SYNC);
    
    // no need to reply
    
    // waiters
    in->finish_waiting(CINODE_WAIT_HARDR|CINODE_WAIT_HARDSTABLE);
    break;
    
  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SYNC);
    //||           lock->get_state() == LOCK_GLOCKR);
    
    // wait for readers to finish?
    if (lock->get_nread() > 0) {
      dout(7) << "handle_lock_inode_hard readers, waiting before ack on " << *in << endl;
      lock->set_state(LOCK_GLOCKR);
      in->add_waiter(CINODE_WAIT_HARDNORD,
                     new C_MDS_RetryMessage(mds,m));
      assert(0);  // does this ever happen?  (if so, fix hard_read_finish, and CInodeExport.update_inode!)
      return;
     } else {

      // update lock and reply
      lock->set_state(LOCK_LOCK);
      
      {
        MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
        reply->set_ino(in->ino(), LOCK_OTYPE_IHARD);
        mds->send_message_mds(reply, from, MDS_PORT_CACHE);
      }
    }
    break;
    
    
    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->state == LOCK_GLOCKR);
    assert(lock->gather_set.count(from));
    lock->gather_set.erase(from);

    if (lock->gather_set.size()) {
      dout(7) << "handle_lock_inode_hard " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
    } else {
      dout(7) << "handle_lock_inode_hard " << *in << " from " << from << ", last one" << endl;
      inode_hard_eval(in);
    }
  }  
  delete m;
}




// =====================
// soft inode metadata


bool MDCache::inode_file_read_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_file_read_start " << *in << " filelock=" << in->filelock << endl;  

  // can read?  grab ref.
  if (in->filelock.can_read(in->is_auth())) {
    in->filelock.get_read();
    return true;
  }
  
  // can't read, and replicated.
  if (in->filelock.can_read_soon(in->is_auth())) {
    // wait
    dout(7) << "inode_file_read_start can_read_soon " << *in << endl;
  } else {    
    if (in->is_auth()) {
      // auth

      // FIXME or qsync?

      if (in->filelock.is_stable()) {
        inode_file_lock(in);     // lock, bc easiest to back off

        if (in->filelock.can_read(in->is_auth())) {
          in->filelock.get_read();
          
          in->filelock.get_write();
          in->finish_waiting(CINODE_WAIT_FILERWB|CINODE_WAIT_FILESTABLE);
          in->filelock.put_write();
          return true;
        }
      } else {
        dout(7) << "inode_file_read_start waiting until stable on " << *in << ", filelock=" << in->filelock << endl;
        in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
        return false;
      }
    } else {
      // replica
      if (in->filelock.is_stable()) {

        // fw to auth
        int auth = in->authority();
        dout(7) << "inode_file_read_start " << *in << " on replica and async, fw to auth " << auth << endl;
        assert(auth != mds->get_nodeid());
        request_forward(m, auth);
        return false;
        
      } else {
        // wait until stable
        dout(7) << "inode_file_read_start waiting until stable on " << *in << ", filelock=" << in->filelock << endl;
        in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
        return false;
      }
    }
  }

  // wait
  dout(7) << "inode_file_read_start waiting on " << *in << ", filelock=" << in->filelock << endl;
  in->add_waiter(CINODE_WAIT_FILER, new C_MDS_RetryRequest(mds, m, in));
        
  return false;
}


void MDCache::inode_file_read_finish(CInode *in)
{
  // drop ref
  assert(in->filelock.can_read(in->is_auth()));
  in->filelock.put_read();

  dout(7) << "inode_file_read_finish on " << *in << ", filelock=" << in->filelock << endl;

  if (in->filelock.get_nread() == 0) {
    in->finish_waiting(CINODE_WAIT_FILENORD);
    inode_file_eval(in);
  }
}


bool MDCache::inode_file_write_start(CInode *in, MClientRequest *m)
{
  // can write?  grab ref.
  if (in->filelock.can_write(in->is_auth())) {
    in->filelock.get_write();
    return true;
  }
  
  // can't write, replicated.
  if (in->is_auth()) {
    // auth
    if (in->filelock.can_write_soon(in->is_auth())) {
      // just wait
    } else {
      if (!in->filelock.is_stable()) {
        dout(7) << "inode_file_write_start on auth, waiting for stable on " << *in << endl;
        in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
        return false;
      }
      
      // initiate lock 
      inode_file_lock(in);

      if (in->filelock.can_write(in->is_auth())) {
        in->filelock.get_write();
        
        in->filelock.get_read();
        in->finish_waiting(CINODE_WAIT_FILERWB|CINODE_WAIT_FILESTABLE);
        in->filelock.put_read();
        return true;
      }
    }
    
    dout(7) << "inode_file_write_start on auth, waiting for write on " << *in << endl;
    in->add_waiter(CINODE_WAIT_FILEW, new C_MDS_RetryRequest(mds, m, in));
    return false;
  } else {
    // replica
    // fw to auth
    int auth = in->authority();
    dout(7) << "inode_file_write_start " << *in << " on replica, fw to auth " << auth << endl;
    assert(auth != mds->get_nodeid());
    request_forward(m, auth);
    return false;
  }
}


void MDCache::inode_file_write_finish(CInode *in)
{
  // drop ref
  assert(in->filelock.can_write(in->is_auth()));
  in->filelock.put_write();
  dout(7) << "inode_file_write_finish on " << *in << ", filelock=" << in->filelock << endl;
  
  // drop lock?
  if (in->filelock.get_nwrite() == 0) {
    in->finish_waiting(CINODE_WAIT_FILENOWR);
    inode_file_eval(in);
  }
}


/*
 * ...
 *
 * also called after client caps are acked to us
 * - checks if we're in unstable sfot state and can now move on to next state
 * - checks if soft state should change (eg bc last writer closed)
 */

void MDCache::inode_file_eval(CInode *in)
{
  int issued = in->get_caps_issued();

  // [auth] finished gather?
  if (in->is_auth() &&
      !in->filelock.is_stable() &&
      in->filelock.gather_set.size() == 0) {
    dout(7) << "inode_file_eval finished mds gather on " << *in << endl;

    switch (in->filelock.get_state()) {
      // to lock
    case LOCK_GLOCKR:
    case LOCK_GLOCKM:
    case LOCK_GLOCKL:
      if (issued == 0) {
        in->filelock.set_state(LOCK_LOCK);
        
        // waiters
        in->filelock.get_read();
        in->filelock.get_write();
        in->finish_waiting(CINODE_WAIT_FILERWB|CINODE_WAIT_FILESTABLE);
        in->filelock.put_read();
        in->filelock.put_write();
      }
      break;
      
      // to mixed
    case LOCK_GMIXEDR:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        in->filelock.set_state(LOCK_MIXED);
        in->finish_waiting(CINODE_WAIT_FILESTABLE);
      }
      break;

    case LOCK_GMIXEDL:
      if ((issued & ~(CAP_FILE_WR)) == 0) {
        in->filelock.set_state(LOCK_MIXED);

        if (in->is_cached_by_anyone()) {
          // data
          bufferlist softdata;
          in->encode_file_state(softdata);
          
          // bcast to replicas
          for (set<int>::iterator it = in->cached_by_begin(); 
               it != in->cached_by_end(); 
               it++) {
            MLock *m = new MLock(LOCK_AC_MIXED, mds->get_nodeid());
            m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
            m->set_data(softdata);
            mds->send_message_mds(m, *it, MDS_PORT_CACHE);
          }
        }

        in->finish_waiting(CINODE_WAIT_FILESTABLE);
      }
      break;

      // to loner
    case LOCK_GLONERR:
      if (issued == 0) {
        in->filelock.set_state(LOCK_LONER);
        in->finish_waiting(CINODE_WAIT_FILESTABLE);
      }
      break;

    case LOCK_GLONERM:
      if ((issued & ~CAP_FILE_WR) == 0) {
        in->filelock.set_state(LOCK_LONER);
        in->finish_waiting(CINODE_WAIT_FILESTABLE);
      }
      break;
      
      // to sync
    case LOCK_GSYNCL:
    case LOCK_GSYNCM:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        in->filelock.set_state(LOCK_SYNC);
        
        { // bcast data to replicas
          bufferlist softdata;
          in->encode_file_state(softdata);
          
          for (set<int>::iterator it = in->cached_by_begin(); 
               it != in->cached_by_end(); 
               it++) {
            MLock *reply = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
            reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
            reply->set_data(softdata);
            mds->send_message_mds(reply, *it, MDS_PORT_CACHE);
          }
        }
        
        // waiters
        in->filelock.get_read();
        in->finish_waiting(CINODE_WAIT_FILER|CINODE_WAIT_FILESTABLE);
        in->filelock.put_read();
      }
      break;
      
    default: 
      assert(0);
    }

    issue_caps(in);
  }
  
  // [replica] finished caps gather?
  if (!in->is_auth() &&
      !in->filelock.is_stable()) {
    switch (in->filelock.get_state()) {
    case LOCK_GMIXEDR:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        in->filelock.set_state(LOCK_MIXED);
        
        // ack
        MLock *reply = new MLock(LOCK_AC_MIXEDACK, mds->get_nodeid());
        reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(reply, in->authority(), MDS_PORT_CACHE);
      }
      break;

    case LOCK_GLOCKR:
      if (issued == 0) {
        in->filelock.set_state(LOCK_LOCK);
        
        // ack
        MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
        reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(reply, in->authority(), MDS_PORT_CACHE);
      }
      break;

    default:
      assert(0);
    }
  }

  // !stable -> do nothing.
  if (!in->filelock.is_stable()) return; 


  // stable.
  assert(in->filelock.is_stable());

  if (in->is_auth()) {
    // [auth]
    int wanted = in->get_caps_wanted();
    bool loner = (in->client_caps.size() == 1) && in->mds_caps_wanted.empty();
    dout(7) << "inode_file_eval wanted=" << cap_string(wanted)
            << "  filelock=" << in->filelock 
            << "  loner=" << loner
            << endl;

    // * -> loner?
    if (in->filelock.get_nread() == 0 &&
        in->filelock.get_nwrite() == 0 &&
        (wanted & CAP_FILE_WR) &&
        loner &&
        in->filelock.get_state() != LOCK_LONER) {
      dout(7) << "inode_file_eval stable, bump to loner " << *in << ", filelock=" << in->filelock << endl;
      inode_file_loner(in);
    }

    // * -> mixed?
    else if (in->filelock.get_nread() == 0 &&
             in->filelock.get_nwrite() == 0 &&
             (wanted & CAP_FILE_RD) &&
             (wanted & CAP_FILE_WR) &&
             !(loner && in->filelock.get_state() == LOCK_LONER) &&
             in->filelock.get_state() != LOCK_MIXED) {
      dout(7) << "inode_file_eval stable, bump to mixed " << *in << ", filelock=" << in->filelock << endl;
      inode_file_mixed(in);
    }

    // * -> sync?
    else if (in->filelock.get_nwrite() == 0 &&
             !(wanted & CAP_FILE_WR) &&
             ((wanted & CAP_FILE_RD) || 
              in->is_cached_by_anyone() || 
              (!loner && in->filelock.get_state() == LOCK_LONER)) &&
             in->filelock.get_state() != LOCK_SYNC) {
      dout(7) << "inode_file_eval stable, bump to sync " << *in << ", filelock=" << in->filelock << endl;
      inode_file_sync(in);
    }

    // * -> lock?  (if not replicated or open)
    else if (!in->is_cached_by_anyone() &&
             wanted == 0 &&
             in->filelock.get_state() != LOCK_LOCK) {
      inode_file_lock(in);
    }
    
  } else {
    // replica
    // recall? check wiaters?  XXX
  }
}


// mid

bool MDCache::inode_file_sync(CInode *in)
{
  dout(7) << "inode_file_sync " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());

  // check state
  if (in->filelock.get_state() == LOCK_SYNC ||
      in->filelock.get_state() == LOCK_GSYNCL ||
      in->filelock.get_state() == LOCK_GSYNCM)
    return true;

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  assert((in->get_caps_wanted() & CAP_FILE_WR) == 0);

  if (in->filelock.get_state() == LOCK_LOCK) {
    if (in->is_cached_by_anyone()) {
      // soft data
      bufferlist softdata;
      in->encode_file_state(softdata);
      
      // bcast to replicas
      for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
           it++) {
        MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
        m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
	m->set_data(softdata);
	mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
    }

    // change lock
    in->filelock.set_state(LOCK_SYNC);

    // reissue caps
    issue_caps(in);
    return true;
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
    // writers?
    if (issued & CAP_FILE_WR) {
      // gather client write caps
      in->filelock.set_state(LOCK_GSYNCM);
      issue_caps(in);
    } else {
      // no writers, go straight to sync

      if (in->is_cached_by_anyone()) {
        // bcast to replicas
        for (set<int>::iterator it = in->cached_by_begin(); 
             it != in->cached_by_end(); 
             it++) {
          MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
          m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
          mds->send_message_mds(m, *it, MDS_PORT_CACHE);
        }
      }
    
      // change lock
      in->filelock.set_state(LOCK_SYNC);
    }
    return false;
  }

  else if (in->filelock.get_state() == LOCK_LONER) {
    // writers?
    if (issued & CAP_FILE_WR) {
      // gather client write caps
      in->filelock.set_state(LOCK_GSYNCL);
      issue_caps(in);
    } else {
      // no writers, go straight to sync
      if (in->is_cached_by_anyone()) {
        // bcast to replicas
        for (set<int>::iterator it = in->cached_by_begin(); 
             it != in->cached_by_end(); 
             it++) {
          MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
          m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
          mds->send_message_mds(m, *it, MDS_PORT_CACHE);
        }
      }

      // change lock
      in->filelock.set_state(LOCK_SYNC);
    }
    return false;
  }
  else 
    assert(0); // wtf.

  return false;
}


void MDCache::inode_file_lock(CInode *in)
{
  dout(7) << "inode_file_lock " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());
  
  // check state
  if (in->filelock.get_state() == LOCK_LOCK ||
      in->filelock.get_state() == LOCK_GLOCKR ||
      in->filelock.get_state() == LOCK_GLOCKM ||
      in->filelock.get_state() == LOCK_GLOCKL) 
    return;  // lock or locking

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  if (in->filelock.get_state() == LOCK_SYNC) {
    if (in->is_cached_by_anyone()) {
      // bcast to replicas
      for (set<int>::iterator it = in->cached_by_begin(); 
           it != in->cached_by_end(); 
           it++) {
        MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
        m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
      in->filelock.init_gather(in->get_cached_by());
      
      // change lock
      in->filelock.set_state(LOCK_GLOCKR);

      // call back caps
      if (issued) 
        issue_caps(in);
    } else {
      if (issued) {
        // call back caps
        in->filelock.set_state(LOCK_GLOCKR);
        issue_caps(in);
      } else {
        in->filelock.set_state(LOCK_LOCK);
      }
    }
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
    if (in->is_cached_by_anyone()) {
      // bcast to replicas
      for (set<int>::iterator it = in->cached_by_begin(); 
           it != in->cached_by_end(); 
           it++) {
        MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
        m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
      in->filelock.init_gather(in->get_cached_by());

      // change lock
      in->filelock.set_state(LOCK_GLOCKM);
      
      // call back caps
      issue_caps(in);
    } else {
      //assert(issued);  // ??? -sage 2/19/06
      if (issued) {
        // change lock
        in->filelock.set_state(LOCK_GLOCKM);
        
        // call back caps
        issue_caps(in);
      } else {
        in->filelock.set_state(LOCK_LOCK);
      }
    }
      
  }
  else if (in->filelock.get_state() == LOCK_LONER) {
    if (issued & CAP_FILE_WR) {
      // change lock
      in->filelock.set_state(LOCK_GLOCKL);
  
      // call back caps
      issue_caps(in);
    } else {
      in->filelock.set_state(LOCK_LOCK);
    }
  }
  else 
    assert(0); // wtf.
}


void MDCache::inode_file_mixed(CInode *in)
{
  dout(7) << "inode_file_mixed " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());
  
  // check state
  if (in->filelock.get_state() == LOCK_GMIXEDR ||
      in->filelock.get_state() == LOCK_GMIXEDL)
    return;     // mixed or mixing

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  if (in->filelock.get_state() == LOCK_SYNC) {
    if (in->is_cached_by_anyone()) {
      // bcast to replicas
      for (set<int>::iterator it = in->cached_by_begin(); 
           it != in->cached_by_end(); 
           it++) {
        MLock *m = new MLock(LOCK_AC_MIXED, mds->get_nodeid());
        m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
      in->filelock.init_gather(in->get_cached_by());
    
      in->filelock.set_state(LOCK_GMIXEDR);
      issue_caps(in);
    } else {
      if (issued) {
        in->filelock.set_state(LOCK_GMIXEDR);
        issue_caps(in);
      } else {
        in->filelock.set_state(LOCK_MIXED);
      }
    }
  }

  else if (in->filelock.get_state() == LOCK_LOCK) {
    if (in->is_cached_by_anyone()) {
      // data
      bufferlist softdata;
      in->encode_file_state(softdata);
      
      // bcast to replicas
      for (set<int>::iterator it = in->cached_by_begin(); 
           it != in->cached_by_end(); 
           it++) {
        MLock *m = new MLock(LOCK_AC_MIXED, mds->get_nodeid());
        m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        m->set_data(softdata);
        mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
    }

    // change lock
    in->filelock.set_state(LOCK_MIXED);
    issue_caps(in);
  }

  else if (in->filelock.get_state() == LOCK_LONER) {
    if (issued & CAP_FILE_WRBUFFER) {
      // gather up WRBUFFER caps
      in->filelock.set_state(LOCK_GMIXEDL);
      issue_caps(in);
    }
    else if (in->is_cached_by_anyone()) {
      // bcast to replicas
      for (set<int>::iterator it = in->cached_by_begin(); 
           it != in->cached_by_end(); 
           it++) {
        MLock *m = new MLock(LOCK_AC_MIXED, mds->get_nodeid());
        m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
      in->filelock.set_state(LOCK_MIXED);
      issue_caps(in);
    } else {
      in->filelock.set_state(LOCK_MIXED);
      issue_caps(in);
    }
  }

  else 
    assert(0); // wtf.
}


void MDCache::inode_file_loner(CInode *in)
{
  dout(7) << "inode_file_loner " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());

  // check state
  if (in->filelock.get_state() == LOCK_LONER ||
      in->filelock.get_state() == LOCK_GLONERR ||
      in->filelock.get_state() == LOCK_GLONERM)
    return; 

  assert(in->filelock.is_stable());
  assert((in->client_caps.size() == 1) && in->mds_caps_wanted.empty());
  
  if (in->filelock.get_state() == LOCK_SYNC) {
    if (in->is_cached_by_anyone()) {
      // bcast to replicas
      for (set<int>::iterator it = in->cached_by_begin(); 
           it != in->cached_by_end(); 
           it++) {
        MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
        m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
      in->filelock.init_gather(in->get_cached_by());
      
      // change lock
      in->filelock.set_state(LOCK_GLONERR);
    } else {
      // only one guy with file open, who gets it all, so
      in->filelock.set_state(LOCK_LONER);
      issue_caps(in);
    }
  }

  else if (in->filelock.get_state() == LOCK_LOCK) {
    // change lock.  ignore replicas; they don't know about LONER.
    in->filelock.set_state(LOCK_LONER);
    issue_caps(in);
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
    if (in->is_cached_by_anyone()) {
      // bcast to replicas
      for (set<int>::iterator it = in->cached_by_begin(); 
           it != in->cached_by_end(); 
           it++) {
        MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
        m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
      in->filelock.init_gather(in->get_cached_by());
      
      // change lock
      in->filelock.set_state(LOCK_GLONERM);
    } else {
      in->filelock.set_state(LOCK_LONER);
      issue_caps(in);
    }
  }

  else 
    assert(0);
}

// messenger

void MDCache::handle_lock_inode_file(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_IFILE);
  
  mds->logger->inc("lif");

  CInode *in = get_inode(m->get_ino());
  int from = m->get_asker();

  if (LOCK_AC_FOR_AUTH(m->get_action())) {
    // auth
    assert(in);
    assert(in->is_auth() || in->is_proxy());
    dout(7) << "handle_lock_inode_file " << *in << " hardlock=" << in->hardlock << endl;  
        
    if (in->is_proxy()) {
      // fw
      int newauth = in->authority();
      assert(newauth >= 0);
      if (from == newauth) {
        dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, but from new auth, dropping" << endl;
        delete m;
      } else {
        dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
        mds->send_message_mds(m, newauth, MDS_PORT_CACHE);
      }
      return;
    }
  } else {
    // replica
    if (!in) {
      // drop it.  don't nak.
      dout(7) << "handle_lock " << m->get_ino() << ": don't have it anymore" << endl;
      delete m;
      return;
    }
    
    assert(!in->is_auth());
  }

  dout(7) << "handle_lock_inode_file a=" << m->get_action() << " from " << from << " " << *in << " filelock=" << in->filelock << endl;  
  
  CLock *lock = &in->filelock;
  int issued = in->get_caps_issued();

  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_LOCK ||
           lock->get_state() == LOCK_MIXED);
    
    { // assim data
      int off = 0;
      in->decode_file_state(m->get_data(), off);
    }
    
    // update lock
    lock->set_state(LOCK_SYNC);
    
    // no need to reply.
    
    // waiters
    in->filelock.get_read();
    in->finish_waiting(CINODE_WAIT_FILER|CINODE_WAIT_FILESTABLE);
    in->filelock.put_read();
    inode_file_eval(in);
    break;
    
  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_MIXED);
    
    // call back caps?
    if (issued & CAP_FILE_RD) {
      dout(7) << "handle_lock_inode_file client readers, gathering caps on " << *in << endl;
      issue_caps(in);
    }
    if (lock->get_nread() > 0) {
      dout(7) << "handle_lock_inode_file readers, waiting before ack on " << *in << endl;
      in->add_waiter(CINODE_WAIT_FILENORD,
                     new C_MDS_RetryMessage(mds,m));
      lock->set_state(LOCK_GLOCKR);
      assert(0);// i am broken.. why retry message when state captures all the info i need?
      return;
    } 
    if (issued & CAP_FILE_RD) {
      lock->set_state(LOCK_GLOCKR);
      break;
    }

    // nothing to wait for, lock and ack.
    {
      lock->set_state(LOCK_LOCK);

      MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
      reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
      mds->send_message_mds(reply, from, MDS_PORT_CACHE);
    }
    break;
    
  case LOCK_AC_MIXED:
    assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_LOCK);
    
    if (lock->get_state() == LOCK_SYNC) {
      // MIXED
      if (issued & CAP_FILE_RD) {
        // call back client caps
        lock->set_state(LOCK_GMIXEDR);
        issue_caps(in);
        break;
      } else {
        // no clients, go straight to mixed
        lock->set_state(LOCK_MIXED);

        // ack
        MLock *reply = new MLock(LOCK_AC_MIXEDACK, mds->get_nodeid());
        reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(reply, from, MDS_PORT_CACHE);
      }
    } else {
      // LOCK
      lock->set_state(LOCK_MIXED);
      
      // no ack needed.
    }

    issue_caps(in);
    
    // waiters
    in->filelock.get_write();
    in->finish_waiting(CINODE_WAIT_FILEW|CINODE_WAIT_FILESTABLE);
    in->filelock.put_write();
    inode_file_eval(in);
    break;

 
    

    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->state == LOCK_GLOCKR ||
           lock->state == LOCK_GLOCKM ||
           lock->state == LOCK_GLONERM ||
           lock->state == LOCK_GLONERR);
    assert(lock->gather_set.count(from));
    lock->gather_set.erase(from);

    if (lock->gather_set.size()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
      inode_file_eval(in);
    }
    break;
    
  case LOCK_AC_SYNCACK:
    assert(lock->state == LOCK_GSYNCM);
    assert(lock->gather_set.count(from));
    lock->gather_set.erase(from);
    
    /* not used currently
    {
      // merge data  (keep largest size, mtime, etc.)
      int off = 0;
      in->decode_merge_file_state(m->get_data(), off);
    }
    */

    if (lock->gather_set.size()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
      inode_file_eval(in);
    }
    break;

  case LOCK_AC_MIXEDACK:
    assert(lock->state == LOCK_GMIXEDR);
    assert(lock->gather_set.count(from));
    lock->gather_set.erase(from);
    
    if (lock->gather_set.size()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
      inode_file_eval(in);
    }
    break;


  default:
    assert(0);
  }  
  
  delete m;
}














void MDCache::handle_lock_dir(MLock *m) 
{

}



// DENTRY

bool MDCache::dentry_xlock_start(CDentry *dn, Message *m, CInode *ref)
{
  dout(7) << "dentry_xlock_start on " << *dn << endl;

  // locked?
  if (dn->lockstate == DN_LOCK_XLOCK) {
    if (dn->xlockedby == m) return true;  // locked by me!

    // not by me, wait
    dout(7) << "dentry " << *dn << " xlock by someone else" << endl;
    dn->dir->add_waiter(CDIR_WAIT_DNREAD, dn->name,
                        new C_MDS_RetryRequest(mds,m,ref));
    return false;
  }

  // prelock?
  if (dn->lockstate == DN_LOCK_PREXLOCK) {
    if (dn->xlockedby == m) {
      dout(7) << "dentry " << *dn << " prexlock by me" << endl;
      dn->dir->add_waiter(CDIR_WAIT_DNLOCK, dn->name,
                          new C_MDS_RetryRequest(mds,m,ref));
    } else {
      dout(7) << "dentry " << *dn << " prexlock by someone else" << endl;
      dn->dir->add_waiter(CDIR_WAIT_DNREAD, dn->name,
                          new C_MDS_RetryRequest(mds,m,ref));
    }
    return false;
  }


  // lockable!
  assert(dn->lockstate == DN_LOCK_SYNC ||
         dn->lockstate == DN_LOCK_UNPINNING);
  
  // dir auth pinnable?
  if (!dn->dir->can_auth_pin()) {
    dout(7) << "dentry " << *dn << " dir not pinnable, waiting" << endl;
    dn->dir->add_waiter(CDIR_WAIT_AUTHPINNABLE,
                        new C_MDS_RetryRequest(mds,m,ref));
    return false;
  }

  // is dentry path pinned?
  if (dn->is_pinned()) {
    dout(7) << "dentry " << *dn << " pinned, waiting" << endl;
    dn->lockstate = DN_LOCK_UNPINNING;
    dn->dir->add_waiter(CDIR_WAIT_DNUNPINNED,
                        dn->name,
                        new C_MDS_RetryRequest(mds,m,ref));
    return false;
  }

  // pin path up to dentry!            (if success, point of no return)
  CDentry *pdn = dn->dir->inode->get_parent_dn();
  if (pdn) {
    if (active_requests[m].traces.count(pdn)) {
      dout(7) << "already path pinned parent dentry " << *pdn << endl;
    } else {
      dout(7) << "pinning parent dentry " << *pdn << endl;
      vector<CDentry*> trace;
      make_trace(trace, pdn->inode);
      assert(trace.size());

      if (!path_pin(trace, m, new C_MDS_RetryRequest(mds, m, ref))) return false;
      
      active_requests[m].traces[trace[trace.size()-1]] = trace;
    }
  }

  // pin dir!
  dn->dir->auth_pin();
  
  // mine!
  dn->xlockedby = m;

  if (dn->dir->is_open_by_anyone()) {
    dn->lockstate = DN_LOCK_PREXLOCK;
    
    // xlock with whom?
    set<int> who = dn->dir->get_open_by();
    dn->gather_set = who;

    // make path
    string path;
    dn->make_path(path);
    dout(10) << "path is " << path << " for " << *dn << endl;

    for (set<int>::iterator it = who.begin();
         it != who.end();
         it++) {
      MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
      m->set_dn(dn->dir->ino(), dn->name);
      m->set_path(path);
      mds->send_message_mds(m, *it, MDS_PORT_CACHE);
    }

    // wait
    dout(7) << "dentry_xlock_start locking, waiting for replicas " << endl;
    dn->dir->add_waiter(CDIR_WAIT_DNLOCK, dn->name,
                        new C_MDS_RetryRequest(mds, m, ref));
    return false;
  } else {
    dn->lockstate = DN_LOCK_XLOCK;
    active_requests[dn->xlockedby].xlocks.insert(dn);
    return true;
  }
}

void MDCache::dentry_xlock_finish(CDentry *dn, bool quiet)
{
  dout(7) << "dentry_xlock_finish on " << *dn << endl;
  
  assert(dn->xlockedby);
  if (dn->xlockedby == DN_XLOCK_FOREIGN) {
    dout(7) << "this was a foreign xlock" << endl;
  } else {
    // remove from request record
    assert(active_requests[dn->xlockedby].xlocks.count(dn) == 1);
    active_requests[dn->xlockedby].xlocks.erase(dn);
  }

  dn->xlockedby = 0;
  dn->lockstate = DN_LOCK_SYNC;

  // unpin parent dir?
  // -> no?  because we might have xlocked 2 things in this dir.
  //         instead, we let request_finish clean up the mess.
    
  // tell replicas?
  if (!quiet) {
    // tell even if dn is null.
    if (dn->dir->is_open_by_anyone()) {
      for (set<int>::iterator it = dn->dir->open_by_begin();
           it != dn->dir->open_by_end();
           it++) {
        MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
        m->set_dn(dn->dir->ino(), dn->name);
        mds->send_message_mds(m, *it, MDS_PORT_CACHE);
      }
    }
  }
  
  // unpin dir
  dn->dir->auth_unpin();
}

/*
 * onfinish->finish() will be called with 
 * 0 on successful xlock,
 * -1 on failure
 */

class C_MDC_XlockRequest : public Context {
  MDCache *mdc;
  CDir *dir;
  string dname;
  Message *req;
  Context *finisher;
public:
  C_MDC_XlockRequest(MDCache *mdc, 
                     CDir *dir, string& dname, 
                     Message *req,
                     Context *finisher) {
    this->mdc = mdc;
    this->dir = dir;
    this->dname = dname;
    this->req = req;
    this->finisher = finisher;
  }

  void finish(int r) {
    mdc->dentry_xlock_request_finish(r, dir, dname, req, finisher);
  }
};

void MDCache::dentry_xlock_request_finish(int r, 
					  CDir *dir, string& dname, 
					  Message *req,
					  Context *finisher) 
{
  dout(10) << "dentry_xlock_request_finish r = " << r << endl;
  if (r == 1) {  // 1 for xlock request success
    CDentry *dn = dir->lookup(dname);
    if (dn && dn->xlockedby == 0) {
      // success
      dn->xlockedby = req;   // our request was the winner
      dout(10) << "xlock request success, now xlocked by req " << req << " dn " << *dn << endl;
      
      // remember!
      active_requests[req].foreign_xlocks.insert(dn);
    }        
  }
  
  // retry request (or whatever)
  finisher->finish(0);
  delete finisher;
}

void MDCache::dentry_xlock_request(CDir *dir, string& dname, bool create,
                                   Message *req, Context *onfinish)
{
  dout(10) << "dentry_xlock_request on dn " << dname << " create=" << create << " in " << *dir << endl; 
  // send request
  int dauth = dir->dentry_authority(dname);
  MLock *m = new MLock(create ? LOCK_AC_REQXLOCKC:LOCK_AC_REQXLOCK, mds->get_nodeid());
  m->set_dn(dir->ino(), dname);
  mds->send_message_mds(m, dauth, MDS_PORT_CACHE);
  
  // add waiter
  dir->add_waiter(CDIR_WAIT_DNREQXLOCK, dname, 
                  new C_MDC_XlockRequest(this, 
                                         dir, dname, req,
                                         onfinish));
}




void MDCache::handle_lock_dn(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_DN);
  
  CInode *diri = get_inode(m->get_ino());  // may be null 
  CDir *dir = 0;
  if (diri) dir = diri->dir;           // may be null
  string dname = m->get_dn();
  int from = m->get_asker();
  CDentry *dn = 0;

  if (LOCK_AC_FOR_AUTH(m->get_action())) {
    // auth

    // normally we have it always
    if (diri && dir) {
      int dauth = dir->dentry_authority(dname);
      assert(dauth == mds->get_nodeid() || dir->is_proxy() ||  // mine or proxy,
             m->get_action() == LOCK_AC_REQXLOCKACK ||         // or we did a REQXLOCK and this is our ack/nak
             m->get_action() == LOCK_AC_REQXLOCKNAK);
      
      if (dir->is_proxy()) {

        assert(dauth >= 0);

        if (dauth == m->get_asker() && 
            (m->get_action() == LOCK_AC_REQXLOCK ||
             m->get_action() == LOCK_AC_REQXLOCKC)) {
          dout(7) << "handle_lock_dn got reqxlock from " << dauth << " and they are auth.. dropping on floor (their import will have woken them up)" << endl;
          if (active_requests.count(m)) 
            request_finish(m);
          else
            delete m;
          return;
        }

        dout(7) << "handle_lock_dn " << m << " " << m->get_ino() << " dname " << dname << " from " << from << ": proxy, fw to " << dauth << endl;

        // forward
        if (active_requests.count(m)) {
          // xlock requests are requests, use request_* functions!
          assert(m->get_action() == LOCK_AC_REQXLOCK ||
                 m->get_action() == LOCK_AC_REQXLOCKC);
          // forward as a request
          request_forward(m, dauth, MDS_PORT_CACHE);
        } else {
          // not an xlock req, or it is and we just didn't register the request yet
          // forward normally
          mds->send_message_mds(m, dauth, MDS_PORT_CACHE);
        }
        return;
      }
      
      dn = dir->lookup(dname);
    }

    // except with.. an xlock request?
    if (!dn) {
      assert(dir);  // we should still have the dir, though!  the requester has the dir open.
      switch (m->get_action()) {

      case LOCK_AC_LOCK:
        dout(7) << "handle_lock_dn xlock on " << dname << ", adding (null)" << endl;
        dn = dir->add_dentry(dname);
        break;

      case LOCK_AC_REQXLOCK:
        // send nak
        if (dir->state_test(CDIR_STATE_DELETED)) {
          dout(7) << "handle_lock_dn reqxlock on deleted dir " << *dir << ", nak" << endl;
        } else {
          dout(7) << "handle_lock_dn reqxlock on " << dname << " in " << *dir << " dne, nak" << endl;
        }
        {
          MLock *reply = new MLock(LOCK_AC_REQXLOCKNAK, mds->get_nodeid());
          reply->set_dn(dir->ino(), dname);
          reply->set_path(m->get_path());
          mds->send_message_mds(reply, m->get_asker(), MDS_PORT_CACHE);
        }
         
        // finish request (if we got that far)
        if (active_requests.count(m)) request_finish(m);

        delete m;
        return;

      case LOCK_AC_REQXLOCKC:
        dout(7) << "handle_lock_dn reqxlockc on " << dname << " in " << *dir << " dne (yet!)" << endl;
        break;

      default:
        assert(0);
      }
    }
  } else {
    // replica
    if (dir) dn = dir->lookup(dname);
    if (!dn) {
      dout(7) << "handle_lock_dn " << m << " don't have " << m->get_ino() << " dname " << dname << endl;
      
      if (m->get_action() == LOCK_AC_REQXLOCKACK ||
          m->get_action() == LOCK_AC_REQXLOCKNAK) {
        dout(7) << "handle_lock_dn got reqxlockack/nak, but don't have dn " << m->get_path() << ", discovering" << endl;
        //assert(0);  // how can this happen?  tell me now!
        
        vector<CDentry*> trace;
        filepath path = m->get_path();
        int r = path_traverse(path, trace, true,
                              m, new C_MDS_RetryMessage(mds,m), 
                              MDS_TRAVERSE_DISCOVER);
        assert(r>0);
        return;
      } 

      if (m->get_action() == LOCK_AC_LOCK) {
        if (0) { // not anymore
          dout(7) << "handle_lock_dn don't have " << m->get_path() << ", discovering" << endl;
          
          vector<CDentry*> trace;
          filepath path = m->get_path();
          int r = path_traverse(path, trace, true,
                                m, new C_MDS_RetryMessage(mds,m), 
                                MDS_TRAVERSE_DISCOVER);
          assert(r>0);
        }
        if (1) {
          // NAK
          MLock *reply = new MLock(LOCK_AC_LOCKNAK, mds->get_nodeid());
          reply->set_dn(m->get_ino(), dname);
          mds->send_message_mds(reply, m->get_asker(), MDS_PORT_CACHE);
        }
      } else {
        dout(7) << "safely ignoring." << endl;
        delete m;
      }
      return;
    }

    assert(dn);
  }

  if (dn) {
    dout(7) << "handle_lock_dn a=" << m->get_action() << " from " << from << " " << *dn << endl;
  } else {
    dout(7) << "handle_lock_dn a=" << m->get_action() << " from " << from << " " << dname << " in " << *dir << endl;
  }
  
  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_LOCK:
    assert(dn->lockstate == DN_LOCK_SYNC ||
           dn->lockstate == DN_LOCK_UNPINNING ||
           dn->lockstate == DN_LOCK_XLOCK);   // <-- bc the handle_lock_dn did the discover!

    if (dn->is_pinned()) {
      dn->lockstate = DN_LOCK_UNPINNING;

      // wait
      dout(7) << "dn pinned, waiting " << *dn << endl;
      dn->dir->add_waiter(CDIR_WAIT_DNUNPINNED,
                          dn->name,
                          new C_MDS_RetryMessage(mds, m));
      return;
    } else {
      dn->lockstate = DN_LOCK_XLOCK;
      dn->xlockedby = 0;

      // ack now
      MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
      reply->set_dn(diri->ino(), dname);
      mds->send_message_mds(reply, from, MDS_PORT_CACHE);
    }

    // wake up waiters
    dir->finish_waiting(CDIR_WAIT_DNLOCK, dname);   // ? will this happen on replica ? 
    break;

  case LOCK_AC_SYNC:
    assert(dn->lockstate == DN_LOCK_XLOCK);
    dn->lockstate = DN_LOCK_SYNC;
    dn->xlockedby = 0;

    // null?  hose it.
    if (dn->is_null()) {
      dout(7) << "hosing null (and now sync) dentry " << *dn << endl;
      dir->remove_dentry(dn);
    }

    // wake up waiters
    dir->finish_waiting(CDIR_WAIT_DNREAD, dname);   // will this happen either?  YES: if a rename lock backs out
    break;

  case LOCK_AC_REQXLOCKACK:
  case LOCK_AC_REQXLOCKNAK:
    {
      dout(10) << "handle_lock_dn got ack/nak on a reqxlock for " << *dn << endl;
      list<Context*> finished;
      dir->take_waiting(CDIR_WAIT_DNREQXLOCK, m->get_dn(), finished, 1);  // TAKE ONE ONLY!
      finish_contexts(finished, 
                      (m->get_action() == LOCK_AC_REQXLOCKACK) ? 1:-1);
    }
    break;


    // -- auth --
  case LOCK_AC_LOCKACK:
  case LOCK_AC_LOCKNAK:
    assert(dn->gather_set.count(from) == 1);
    dn->gather_set.erase(from);
    if (dn->gather_set.size() == 0) {
      dout(7) << "handle_lock_dn finish gather, now xlock on " << *dn << endl;
      dn->lockstate = DN_LOCK_XLOCK;
      active_requests[dn->xlockedby].xlocks.insert(dn);
      dir->finish_waiting(CDIR_WAIT_DNLOCK, dname);
    }
    break;


  case LOCK_AC_REQXLOCKC:
    // make sure it's a _file_, if it exists.
    if (dn && dn->inode && dn->inode->is_dir()) {
      dout(7) << "handle_lock_dn failing, reqxlockc on dir " << *dn->inode << endl;
      
      // nak
      string path;
      dn->make_path(path);

      MLock *reply = new MLock(LOCK_AC_REQXLOCKNAK, mds->get_nodeid());
      reply->set_dn(dir->ino(), dname);
      reply->set_path(path);
      mds->send_message_mds(reply, m->get_asker(), MDS_PORT_CACHE);
      
      // done
      if (active_requests.count(m)) 
        request_finish(m);
      else
        delete m;
      return;
    }

  case LOCK_AC_REQXLOCK:
    if (dn) {
      dout(7) << "handle_lock_dn reqxlock on " << *dn << endl;
    } else {
      dout(7) << "handle_lock_dn reqxlock on " << dname << " in " << *dir << endl;      
    }
    

    // start request?
    if (!active_requests.count(m)) {
      vector<CDentry*> trace;
      if (!request_start(m, dir->inode, trace))
        return;  // waiting for pin
    }
    
    // try to xlock!
    if (!dn) {
      assert(m->get_action() == LOCK_AC_REQXLOCKC);
      dn = dir->add_dentry(dname);
    }

    if (dn->xlockedby != m) {
      if (!dentry_xlock_start(dn, m, dir->inode)) {
        // hose null dn if we're waiting on something
        if (dn->is_clean() && dn->is_null() && dn->is_sync()) dir->remove_dentry(dn);
        return;    // waiting for xlock
      }
    } else {
      // successfully xlocked!  on behalf of requestor.
      string path;
      dn->make_path(path);

      dout(7) << "handle_lock_dn reqxlock success for " << m->get_asker() << " on " << *dn << ", acking" << endl;
      
      // ACK xlock request
      MLock *reply = new MLock(LOCK_AC_REQXLOCKACK, mds->get_nodeid());
      reply->set_dn(dir->ino(), dname);
      reply->set_path(path);
      mds->send_message_mds(reply, m->get_asker(), MDS_PORT_CACHE);

      // note: keep request around in memory (to hold the xlock/pins on behalf of requester)
      return;
    }
    break;

  case LOCK_AC_UNXLOCK:
    dout(7) << "handle_lock_dn unxlock on " << *dn << endl;
    {
      string dname = dn->name;
      Message *m = dn->xlockedby;

      // finish request
      request_finish(m);  // this will drop the locks (and unpin paths!)
      return;
    }
    break;

  default:
    assert(0);
  }

  delete m;
}
















/*
 * some import/export helpers
 */

/** con = get_auth_container(dir)
 * Returns the directory in which authority is delegated for *dir.  
 * This may be because a directory is an import, or because it is hashed
 * and we are nested underneath an inode in that dir (that hashes to us).
 * Thus do not assume con->is_auth()!  It is_auth() || is_hashed().
 */
CDir *MDCache::get_auth_container(CDir *dir)
{
  CDir *imp = dir;  // might be *dir

  // find the underlying import or hash that delegates dir
  while (true) {
    if (imp->is_import()) break; // import
    imp = imp->get_parent_dir();
    assert(imp);
    if (imp->is_hashed()) break; // hash
  }

  return imp;
}


void MDCache::find_nested_exports(CDir *dir, set<CDir*>& s) 
{
  CDir *import = get_auth_container(dir);
  find_nested_exports_under(import, dir, s);
}

void MDCache::find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s)
{
  dout(10) << "find_nested_exports for " << *dir << endl;
  dout(10) << "find_nested_exports_under import " << *import << endl;

  if (import == dir) {
    // yay, my job is easy!
    for (set<CDir*>::iterator p = nested_exports[import].begin();
         p != nested_exports[import].end();
         p++) {
      CDir *nested = *p;
      s.insert(nested);
      dout(10) << "find_nested_exports " << *dir << " " << *nested << endl;
    }
    return;
  }

  // ok, my job is annoying.
  for (set<CDir*>::iterator p = nested_exports[import].begin();
       p != nested_exports[import].end();
       p++) {
    CDir *nested = *p;
    
    dout(12) << "find_nested_exports checking " << *nested << endl;

    // trace back to import, or dir
    CDir *cur = nested->get_parent_dir();
    while (!cur->is_import() || cur == dir) {
      if (cur == dir) {
        s.insert(nested);
        dout(10) << "find_nested_exports " << *dir << " " << *nested << endl;
        break;
      } else {
        cur = cur->get_parent_dir();
      }
    }
  }
}


















// ==============================================================
// debug crap


void MDCache::show_imports()
{
  mds->balancer->show_imports();
}


void MDCache::show_cache()
{
  dout(7) << "show_cache" << endl;
  for (hash_map<inodeno_t,CInode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it++) {
    dout(7) << *((*it).second) << endl;
    
    CDentry *dn = (*it).second->get_parent_dn();
    if (dn) 
      dout(7) << "       dn " << *dn << endl;
    if ((*it).second->dir) 
      dout(7) << "   subdir " << *(*it).second->dir << endl;
  }
}

