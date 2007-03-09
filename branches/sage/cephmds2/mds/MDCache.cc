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
#include "MDS.h"
#include "Server.h"
#include "Locker.h"
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

#include "events/EImportMap.h"
#include "events/EString.h"
#include "events/EUnlink.h"
#include "events/EPurgeFinish.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSImportMap.h"
#include "messages/MMDSCacheRejoin.h"
#include "messages/MMDSCacheRejoinAck.h"

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
#include "messages/MClientRequestForward.h"
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
  did_shutdown_log_cap = false;
  shutdown_commits = 0;
}

MDCache::~MDCache() 
{
  delete migrator;
  delete renamer;
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
    show_subtrees();
    //dump();
  }
  return true;
}


// ====================================================================
// some inode functions

CInode *MDCache::create_inode()
{
  CInode *in = new CInode(this);

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
  assert(inode_map.count(in->ino()) == 0);  // should be no dup inos!
  inode_map[ in->ino() ] = in;
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
}



CInode *MDCache::create_root_inode()
{
  CInode *root = new CInode(this);
  memset(&root->inode, 0, sizeof(inode_t));
  root->inode.ino = 1;
  root->inode.hash_seed = 0;   // not hashed!
  
  // make it up (FIXME)
  root->inode.mode = 0755 | INODE_MODE_DIR;
  root->inode.size = 0;
  root->inode.ctime = 
    root->inode.mtime = g_clock.gettime();
  
  root->inode.nlink = 1;
  root->inode.layout = g_OSD_MDDirLayout;
  
  set_root( root );
  add_inode( root );

  return root;
}


int MDCache::open_root(Context *c)
{
  int whoami = mds->get_nodeid();

  // open root inode
  if (whoami == 0) { 
    // i am root inode
    CInode *root = create_root_inode();

    // root directory too
    CDir *dir = root->get_or_open_dirfrag(this, frag_t());
    adjust_subtree_auth(dir, 0);   
    dir->dir_rep = CDir::REP_ALL;   //NONE;

    show_subtrees();

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






// ====================================================================
// subtree management

/*
 * adjust the dir_auth of a subtree.
 * merge with parent and/or child subtrees, if is it appropriate.
 * merge can ONLY happen if both parent and child have unambiguous auth.
 */
void MDCache::adjust_subtree_auth(CDir *dir, pair<int,int> auth)
{
  dout(7) << "adjust_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir << endl;

  show_subtrees();

  CDir *root;
  if (dir->ino() == 1) {
    root = dir;  // bootstrap hack.
    if (subtrees.count(root) == 0)
      subtrees[root].clear();
  } else {
    root = get_subtree_root(dir);  // subtree root
  }
  assert(root);
  assert(subtrees.count(root));
  dout(7) << " current root is " << *root << endl;

  if (root == dir) {
    // i am already a subtree.
    dir->set_dir_auth(auth);
  } else {
    // i am a new subtree.
    dout(10) << "  new subtree at " << *dir << endl;
    assert(subtrees.count(dir) == 0);
    subtrees[dir].clear();      // create empty subtree bounds list for me.

    // set dir_auth
    dir->set_dir_auth(auth);
    
    // move items nested beneath me, under me.
    set<CDir*>::iterator p = subtrees[root].begin();
    while (p != subtrees[root].end()) {
      set<CDir*>::iterator next = p;
      next++;
      if (get_subtree_root((*p)->get_parent_dir()) == dir) {
	// move under me
	dout(10) << "  claiming child bound " << **p << endl;
	subtrees[dir].insert(*p); 
	subtrees[root].erase(p);
      }
      p = next;
    }
    
    // i am a bound of the parent subtree.
    subtrees[root].insert(dir); 

    // i am now the subtree root.
    root = dir;
  }

  // adjust export pins
  adjust_export_state(dir);
  for (set<CDir*>::iterator p = subtrees[dir].begin();
       p != subtrees[dir].end();
       ++p) 
    adjust_export_state(*p);
  
  show_subtrees();
}


/*
 * any "export" point must be pinned in cache to ensure a proper
 * chain of delegation.  we do this by pinning when a dir is nonauth
 * but the inode is auth.
 *
 * import points don't need to be pinned the same way simply because the
 * exporter is pinned and thus always open.
 */
void MDCache::adjust_export_state(CDir *dir)
{
  //if (!dir->is_auth() && dir->inode->is_auth()) {
  
  // be auth bit agnostic, so that we work during recovery
  //  (before recalc_auth_bits)
  if (!dir->authority().first == mds->get_nodeid() &&
      dir->inode->authority().first != mds->get_nodeid()) {
    // export.
    if (!dir->state_test(CDir::STATE_EXPORT)) {
      dout(10) << "adjust_export_state pinning new export " << *dir << endl;
      dir->state_set(CDir::STATE_EXPORT);
      dir->get(CDir::PIN_EXPORT);
    }
  } 
  else {
    // not export.
    if (dir->state_test(CDir::STATE_EXPORT)) {
      dout(10) << "adjust_export_state unpinning old export " << *dir << endl;
      dir->state_clear(CDir::STATE_EXPORT);
      dir->put(CDir::PIN_EXPORT);
    }
  }
}

void MDCache::try_subtree_merge(CDir *dir)
{
  dout(7) << "try_subtree_merge " << *dir << endl;
  assert(subtrees.count(dir));
  set<CDir*> oldbounds = subtrees[dir];

  // try merge at my root
  try_subtree_merge_at(dir);

  // try merge at my old bounds
  for (set<CDir*>::iterator p = oldbounds.begin();
       p != oldbounds.end();
       ++p) 
    try_subtree_merge_at(*p);
  
}

void MDCache::try_subtree_merge_at(CDir *dir)
{
  dout(10) << "try_subtree_merge_at " << *dir << endl;
  assert(subtrees.count(dir));

  // merge with parent?
  CDir *parent = dir;  
  if (dir->ino() != 1)
    parent = get_subtree_root(dir->get_parent_dir());
  
  if (parent != dir &&                              // we have a parent,
      parent->dir_auth == dir->dir_auth &&          // auth matches,
      dir->dir_auth.second == CDIR_AUTH_UNKNOWN &&  // auth is unambiguous,
      !dir->state_test(CDir::STATE_EXPORTBOUND)) {  // not an exportbound,
    // merge with parent.
    dout(10) << "  subtree merge at " << *parent << endl;
    dir->set_dir_auth(CDIR_AUTH_DEFAULT);
    
    // move our bounds under the parent
    for (set<CDir*>::iterator p = subtrees[dir].begin();
	 p != subtrees[dir].end();
	 ++p) 
      subtrees[parent].insert(*p);
    
    // we are no longer a subtree or bound
    subtrees.erase(dir);
    subtrees[parent].erase(dir);
  } 

  show_subtrees(15);
}


void MDCache::adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, pair<int,int> auth)
{
  dout(7) << "adjust_bounded_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir << endl;

  show_subtrees();

  CDir *root;
  if (dir->ino() == 1) {
    root = dir;  // bootstrap hack.
    if (subtrees.count(root) == 0) 
      subtrees[root].clear();
  } else {
    root = get_subtree_root(dir);  // subtree root
  }
  assert(root);
  assert(subtrees.count(root));
  dout(7) << " current root is " << *root << endl;

  pair<int,int> oldauth = dir->authority();

  if (root == dir) {
    // i am already a subtree.
    dir->set_dir_auth(auth);
  } else {
    // i am a new subtree.
    dout(10) << "  new subtree at " << *dir << endl;
    assert(subtrees.count(dir) == 0);
    subtrees[dir].clear();      // create empty subtree bounds list for me.
    
    // set dir_auth
    dir->set_dir_auth(auth);
    
    // move items nested beneath me, under me.
    set<CDir*>::iterator p = subtrees[root].begin();
    while (p != subtrees[root].end()) {
      set<CDir*>::iterator next = p;
      next++;
      if (get_subtree_root((*p)->get_parent_dir()) == dir) {
	// move under me
	dout(10) << "  claiming child bound " << **p << endl;
	subtrees[dir].insert(*p); 
	subtrees[root].erase(p);
      }
      p = next;
    }
    
    // i am a bound of the parent subtree.
    subtrees[root].insert(dir); 

    // i am now the subtree root.
    root = dir;
  }

  // verify/adjust bounds.
  // - these may be new, or
  // - beneath existing ambiguous bounds (which will be collapsed),
  // - but NOT beneath unambiguous bounds.
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bound = *p;
    
    // new bound?
    if (subtrees[dir].count(bound) == 0) {
      if (get_subtree_root(bound) == dir) {
	dout(10) << "  new bound " << *bound << ", adjusting auth back to old " << oldauth << endl;
	adjust_subtree_auth(bound, oldauth);       // otherwise, adjust at bound.
      }
      else {
	dout(10) << "  want bound " << *bound << endl;
	// make sure it's nested beneath ambiguous subtree(s)
	while (1) {
	  CDir *t = get_subtree_root(bound->get_parent_dir());
	  if (t == dir) break;
	  while (subtrees[dir].count(t) == 0)
	    t = get_subtree_root(t->get_parent_dir());
	  dout(10) << "  swallowing intervening subtree at " << *t << endl;
	  adjust_subtree_auth(t, auth);
	  try_subtree_merge_at(t);
	}
      }
    }
    else {
      dout(10) << "  already have bound " << *bound << endl;
    }
  }
  // merge stray bounds?
  set<CDir*>::iterator p = subtrees[dir].begin();
  while (p != subtrees[dir].end()) {
    set<CDir*>::iterator n = p;
    n++;
    if (bounds.count(*p) == 0) {
      CDir *stray = *p;
      dout(10) << "  swallowing extra subtree at " << *stray << endl;
      adjust_subtree_auth(stray, auth);
      try_subtree_merge_at(stray);
    }
    p = n;
  }

  // bound should now match.
  verify_subtree_bounds(dir, bounds);

  show_subtrees();
}

void MDCache::adjust_bounded_subtree_auth(CDir *dir, list<dirfrag_t>& bound_dfs, pair<int,int> auth)
{
  dout(7) << "adjust_bounded_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir 
	  << " bound_dfs " << bound_dfs
	  << endl;
  
  // make bounds list
  set<CDir*> bounds;
  for (list<dirfrag_t>::iterator p = bound_dfs.begin();
       p != bound_dfs.end();
       ++p) {
    CDir *bd = get_dirfrag(*p);
    if (bd) 
      bounds.insert(bd);
  }
  
  adjust_bounded_subtree_auth(dir, bounds, auth);
}



CDir *MDCache::get_subtree_root(CDir *dir)
{
  // find the underlying dir that delegates (or is about to delegate) auth
  while (true) {
    if (dir->is_subtree_root()) 
      return dir;
    dir = dir->get_parent_dir();
    if (!dir) 
      return 0;             // none
  }
}

void MDCache::remove_subtree(CDir *dir)
{
  dout(10) << "remove_subtree " << *dir << endl;
  assert(subtrees.count(dir));
  assert(subtrees[dir].empty());
  subtrees.erase(dir);
  if (dir->get_parent_dir()) {
    CDir *p = get_subtree_root(dir->get_parent_dir());
    assert(subtrees[p].count(dir));
    subtrees[p].erase(dir);
  }
}

void MDCache::get_subtree_bounds(CDir *dir, set<CDir*>& bounds)
{
  assert(subtrees.count(dir));
  bounds = subtrees[dir];
}

void MDCache::get_wouldbe_subtree_bounds(CDir *dir, set<CDir*>& bounds)
{
  if (subtrees.count(dir)) {
    // just copy them, dir is a subtree.
    get_subtree_bounds(dir, bounds);
  } else {
    // find them
    CDir *root = get_subtree_root(dir);
    for (set<CDir*>::iterator p = subtrees[root].begin();
	 p != subtrees[root].end();
	 ++p) {
      CDir *t = *p;
      while (t != root) {
	t = t->get_parent_dir();
	assert(t);
	if (t == dir) {
	  bounds.insert(*p);
	  continue;
	}
      }
    }
  }
}

void MDCache::verify_subtree_bounds(CDir *dir, const set<CDir*>& bounds)
{
  // for debugging only.
  assert(subtrees.count(dir));
  if (bounds != subtrees[dir]) {
    dout(0) << "verify_subtree_bounds failed" << endl;
    set<CDir*> b = bounds;
    for (set<CDir*>::iterator p = subtrees[dir].begin();
	 p != subtrees[dir].end();
	 ++p) {
      if (bounds.count(*p)) {
	b.erase(*p);
	continue;
      }
      dout(0) << "  missing bound " << **p << endl;
    }
    for (set<CDir*>::iterator p = b.begin();
	 p != b.end();
	 ++p) 
      dout(0) << "    extra bound " << **p << endl;
  }
  assert(bounds == subtrees[dir]);
}

void MDCache::verify_subtree_bounds(CDir *dir, const list<dirfrag_t>& bounds)
{
  // for debugging only.
  assert(subtrees.count(dir));

  // make sure that any bounds i do have are properly noted as such.
  int failed = 0;
  for (list<dirfrag_t>::const_iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = get_dirfrag(*p);
    if (!bd) continue;
    if (subtrees[dir].count(bd) == 0) {
      dout(0) << "verify_subtree_bounds failed: extra bound " << *bd << endl;
      failed++;
    }
  }
  assert(failed == 0);
}


void MDCache::get_fullauth_subtrees(set<CDir*>& s)
{
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_fullauth())
      s.insert(root);
  }
}
void MDCache::get_auth_subtrees(set<CDir*>& s)
{
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_auth())
      s.insert(root);
  }
}


// count.

int MDCache::num_subtrees()
{
  return subtrees.size();
}

int MDCache::num_subtrees_fullauth()
{
  int n = 0;
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_fullauth())
      n++;
  }
  return n;
}

int MDCache::num_subtrees_fullnonauth()
{
  int n = 0;
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_fullnonauth())
      n++;
  }
  return n;
}







// ====================================================================
// import map, recovery

/*
 * take note of where we write import_maps in the log, as we need
 * to take care not to expire them until an updated map is safely flushed.
 */
class C_MDS_WroteImportMap : public Context {
  MDLog *mdlog;
  off_t end_off;
public:
  C_MDS_WroteImportMap(MDLog *ml, off_t eo) : mdlog(ml), end_off(eo) { }
  void finish(int r) {
    //    cout << "WroteImportMap at " << end_off << endl;
    if (r >= 0)
      mdlog->last_import_map = end_off;
    mdlog->writing_import_map = false;
  }
};


void MDCache::log_import_map(Context *onsync)
{
  dout(10) << "log_import_map " << num_subtrees() << " subtrees" 
	   << num_subtrees_fullauth() << " fullauth"
	   << endl;
  
  EImportMap *le = new EImportMap;
  
  // include all auth subtrees, and their bounds.
  // and a spanning tree to tie it to the root.
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    if (!dir->is_auth()) continue;

    le->imports.insert(dir->dirfrag());
    le->metablob.add_dir_context(dir, true);
    le->metablob.add_dir(dir, false);

    // bounds
    for (set<CDir*>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CDir *bound = *q;
      le->bounds[dir->dirfrag()].insert(bound->dirfrag());
      le->metablob.add_dir_context(bound);
      le->metablob.add_dir(bound, false);
    }
  }

  mds->mdlog->writing_import_map = true;
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_sync(new C_MDS_WroteImportMap(mds->mdlog, mds->mdlog->get_write_pos()));
  if (onsync)
    mds->mdlog->wait_for_sync(onsync);
}


void MDCache::send_import_map(int who)
{
  if (migrator->is_exporting())
    send_import_map_later(who);
  else
    send_import_map_now(who);
}

void MDCache::send_import_map_later(int who)
{
  dout(10) << "send_import_map_later to mds" << who << endl;
  wants_import_map.insert(who);
}

void MDCache::send_pending_import_maps()
{
  if (wants_import_map.empty())
    return;  // nothing to send.

  // only if it's appropriate!
  if (migrator->is_exporting() ||
      migrator->is_importing()) {
    dout(7) << "send_pending_import_maps waiting, imports/exports still in progress" << endl;
    return;  // not now
  }
  
  // ok, send them.
  for (set<int>::iterator p = wants_import_map.begin();
       p != wants_import_map.end();
       p++) 
    send_import_map_now(*p);
  wants_import_map.clear();
}

void MDCache::send_import_map_now(int who)
{
  dout(10) << "send_import_map_now to mds" << who << endl;
  MMDSImportMap *m = new MMDSImportMap;

  show_subtrees();

  // known
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       p++) {
    CDir *dir = p->first;

    // only our subtrees
    if (dir->authority().first != mds->get_nodeid()) 
      continue;
    
    if (migrator->is_importing(dir->dirfrag())) {
      // ambiguous (mid-import)
      m->add_ambiguous_import(dir->dirfrag(), 
			      migrator->get_import_bound_inos(dir->dirfrag()));
    } else {
      // not ambiguous.
      m->add_import(dir->dirfrag());
      
      // bounds too
      for (set<CDir*>::iterator q = subtrees[dir].begin();
	   q != subtrees[dir].end();
	   ++q) {
	CDir *bound = *q;
	m->add_import_export(dir->dirfrag(), bound->dirfrag());
      }
    }
  }

  // ambiguous
  for (map<dirfrag_t, list<dirfrag_t> >::iterator p = my_ambiguous_imports.begin();
       p != my_ambiguous_imports.end();
       ++p) 
    m->add_ambiguous_import(p->first, p->second);
  
  // send
  mds->send_message_mds(m, who, MDS_PORT_CACHE);
}


void MDCache::handle_mds_failure(int who)
{
  dout(7) << "handle_mds_failure mds" << who << endl;
  
  // make note of recovery set
  mds->mdsmap->get_recovery_mds_set(recovery_set);
  recovery_set.erase(mds->get_nodeid());
  dout(1) << "my recovery peers will be " << recovery_set << endl;

  // adjust my recovery lists
  wants_import_map.erase(who);   // MDS will ask again
  got_import_map.erase(who);     // i'll get another.
  rejoin_ack_gather.erase(who);  // i'll need/get another.
 
  // adjust subtree auth
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    // only if we are a _bystander_.
    if (dir->dir_auth.first == who &&
	dir->dir_auth.second >= 0 &&
	dir->dir_auth.second != mds->get_nodeid()) {
      dout(7) << "disambiguating auth for " << *dir << endl;
      adjust_subtree_auth(dir, dir->dir_auth.second);
      try_subtree_merge(dir);
    }
    else if (dir->dir_auth.second == who &&
	     dir->dir_auth.first != mds->get_nodeid()) {
      dout(7) << "disambiguating auth for " << *dir << endl;
      adjust_subtree_auth(dir, dir->dir_auth.first);
      try_subtree_merge(dir);
    }      
  }

  // tell the migrator too.
  migrator->handle_mds_failure(who);

  // kick any dir discovers that are waiting
  hash_map<inodeno_t,set<int> >::iterator p = dir_discovers.begin();
  while (p != dir_discovers.end()) {
    hash_map<inodeno_t,set<int> >::iterator n = p;
    n++;

    // waiting on this mds?
    if (p->second.count(who)) {
      CInode *in = get_inode(p->first);
      assert(in);
      
      // take waiters
      list<Context*> waiters;
      in->take_waiting(CInode::WAIT_DIR, waiters);
      mds->queue_finished(waiters);
      dout(10) << "kicking WAIT_DIR on " << *in << endl;
      
      // remove from mds list
      p->second.erase(who);
      if (p->second.empty())
	dir_discovers.erase(p);
    }
    p = n;
  }

  show_subtrees();  
}

/*
 * handle_mds_recovery - called on another node's transition 
 * from resolve -> active.
 */
void MDCache::handle_mds_recovery(int who)
{
  dout(7) << "handle_mds_recovery mds" << who << endl;

  list<Context*> waiters;

  // wake up any waiters in their subtrees
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;

    if (dir->authority().first != who) continue;
    assert(!dir->is_auth());
   
    // wake any waiters
    list<CDir*> q;
    q.push_back(dir);

    while (!q.empty()) {
      CDir *d = q.front();
      q.pop_front();
      d->take_waiting(CDir::WAIT_ANY, waiters);

      // inode waiters too
      for (CDir_map_t::iterator p = d->items.begin();
	   p != d->items.end();
	   ++p) {
	CDentry *dn = p->second;
	if (dn->is_primary()) {
	  dn->get_inode()->take_waiting(CInode::WAIT_ANY, waiters);
	  
	  // recurse?
	  list<CDir*> ls;
	  dn->get_inode()->get_dirfrags(ls);
	  for (list<CDir*>::iterator p = ls.begin();
	       p != ls.end();
	       ++p) {
	    CDir *subdir = *p;
	    if (!subdir->is_subtree_root())
	      q.push_back(subdir);
	  }
	}
      }
    }
  }

  // queue them up.
  mds->queue_finished(waiters);
}

void MDCache::set_recovery_set(set<int>& s) 
{
  dout(7) << "set_recovery_set " << s << endl;
  recovery_set = s;
}


/*
 * during resolve state, we share import_maps to determine who
 * is authoritative for which trees.  we expect to get an import_map
 * from _everyone_ in the recovery_set (the mds cluster at the time of
 * the first failure).
 */
void MDCache::handle_import_map(MMDSImportMap *m)
{
  dout(7) << "handle_import_map from " << m->get_source() << endl;
  int from = m->get_source().num();

  // update my dir_auth values
  for (map<dirfrag_t, list<dirfrag_t> >::iterator pi = m->imap.begin();
       pi != m->imap.end();
       ++pi) {
    CDir *im = get_dirfrag(pi->first);
    if (im) {
      adjust_bounded_subtree_auth(im, pi->second, from);
      try_subtree_merge(im);
    }
  }

  // am i a surviving ambiguous importer?
  if (mds->is_active() || mds->is_stopping()) {
    // check for any import success/failure (from this node)
    map<dirfrag_t, list<dirfrag_t> >::iterator p = my_ambiguous_imports.begin();
    while (p != my_ambiguous_imports.end()) {
      map<dirfrag_t, list<dirfrag_t> >::iterator n = p;
      n++;
      CDir *dir = get_dirfrag(p->first);
      assert(dir);
      dout(10) << "checking ambiguous import " << *dir << endl;
      assert(migrator->is_importing(dir->dirfrag()));
      assert(migrator->get_import_state(dir->dirfrag()) == Migrator::IMPORT_ACKING);
      if (migrator->get_import_peer(dir->dirfrag()) == from) {
	if (dir->auth_is_ambiguous()) {
	  dout(7) << "ambiguous import succeeded on " << *dir << endl;
	  migrator->import_finish(dir, true);	// don't wait for log flush
	} else {
	  dout(7) << "ambiguous import failed on " << *dir << endl;
	  migrator->import_reverse(dir, false);  // don't adjust dir_auth.
	}
	my_ambiguous_imports.erase(p);
      }
      p = n;
    }
  }

  show_subtrees();


  // recovering?
  if (!mds->is_rejoin() && !mds->is_active() && !mds->is_stopping()) {
    // note ambiguous imports too.. unless i'm already active
    for (map<dirfrag_t, list<dirfrag_t> >::iterator pi = m->ambiguous_imap.begin();
	 pi != m->ambiguous_imap.end();
	 ++pi) {
      dout(10) << "noting ambiguous import on " << pi->first << " bounds " << pi->second << endl;
      other_ambiguous_imports[from][pi->first].swap( pi->second );
    }

    // did i get them all?
    got_import_map.insert(from);
    
    if (got_import_map == recovery_set) {
      dout(10) << "got all import maps, ready to rejoin" << endl;
      disambiguate_imports();
      recalc_auth_bits();
      trim_non_auth(); 
      
      // move to rejoin state
      mds->set_want_state(MDSMap::STATE_REJOIN);
      
    } else {
      dout(10) << "still waiting for more importmaps, got " << got_import_map 
	       << ", need " << recovery_set << endl;
    }
  }

  delete m;
}


void MDCache::disambiguate_imports()
{
  dout(10) << "disambiguate_imports" << endl;

  // FIXME what about surviving bystanders

  // other nodes' ambiguous imports
  for (map<int, map<dirfrag_t, list<dirfrag_t> > >::iterator p = other_ambiguous_imports.begin();
       p != other_ambiguous_imports.end();
       ++p) {
    int who = p->first;
    dout(10) << "ambiguous imports for mds" << who << endl;

    for (map<dirfrag_t, list<dirfrag_t> >::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << " ambiguous import " << q->first << " bounds " << q->second << endl;
      CDir *dir = get_dirfrag(q->first);
      if (!dir) continue;
      
      if (dir->authority().first == CDIR_AUTH_UNKNOWN) {
	dout(10) << "mds" << who << " did import " << *dir << endl;
	adjust_bounded_subtree_auth(dir, q->second, who);
	try_subtree_merge(dir);
      } else {
	dout(10) << "mds" << who << " did not import " << *dir << endl;
      }
    }
  }
  other_ambiguous_imports.clear();

  // my ambiguous imports
  while (!my_ambiguous_imports.empty()) {
    map<dirfrag_t, list<dirfrag_t> >::iterator q = my_ambiguous_imports.begin();

    CDir *dir = get_dirfrag(q->first);
    if (!dir) continue;
    
    if (dir->authority().first != CDIR_AUTH_UNKNOWN) {
      dout(10) << "ambiguous import auth known, must not be me " << *dir << endl;
      cancel_ambiguous_import(q->first);
    } else {
      dout(10) << "ambiguous import auth unknown, must be me " << *dir << endl;
      finish_ambiguous_import(q->first);
    }
  }
  assert(my_ambiguous_imports.empty());

  show_subtrees();
}


void MDCache::add_ambiguous_import(dirfrag_t base, list<dirfrag_t>& bounds) 
{
  assert(my_ambiguous_imports.count(base) == 0);
  my_ambiguous_imports[base].swap( bounds );
}


void MDCache::add_ambiguous_import(CDir *base, const set<CDir*>& bounds)
{
  // make a list
  list<dirfrag_t> binos;
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) 
    binos.push_back((*p)->dirfrag());
  
  // note: this can get called twice if the exporter fails during recovery
  if (my_ambiguous_imports.count(base->dirfrag()))
    my_ambiguous_imports.erase(base->dirfrag());

  add_ambiguous_import(base->dirfrag(), binos);
}

void MDCache::cancel_ambiguous_import(dirfrag_t df)
{
  assert(my_ambiguous_imports.count(df));
  dout(10) << "cancel_ambiguous_import " << df
	   << " bounds " << my_ambiguous_imports[df]
	   << endl;
  my_ambiguous_imports.erase(df);
}

void MDCache::finish_ambiguous_import(dirfrag_t df)
{
  assert(my_ambiguous_imports.count(df));
  list<dirfrag_t> bound_inos;
  bound_inos.swap(my_ambiguous_imports[df]);
  my_ambiguous_imports.erase(df);
  
  dout(10) << "finish_ambiguous_import " << df
	   << " bounds " << bound_inos
	   << endl;
  CDir *dir = get_dirfrag(df);
  assert(dir);
  
  // adjust dir_auth, import maps
  adjust_bounded_subtree_auth(dir, bound_inos, mds->get_nodeid());
  try_subtree_merge(dir);
}


/*
 * once subtree auth is disambiguated, we need to adjust all the 
 * auth (and dirty) bits in our cache before moving on.
 */
void MDCache::recalc_auth_bits()
{
  dout(7) << "recalc_auth_bits" << endl;

  for (hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    CInode *in = p->second;
    if (in->authority().first == mds->get_nodeid())
      in->state_set(CInode::STATE_AUTH);
    else {
      in->state_clear(CInode::STATE_AUTH);
      if (in->is_dirty())
	in->mark_clean();
    }

    if (in->parent) {
      if (in->parent->authority().first == mds->get_nodeid())
	in->parent->state_set(CDentry::STATE_AUTH);
      else {
	in->parent->state_clear(CDentry::STATE_AUTH);
	if (in->parent->is_dirty()) 
	  in->parent->mark_clean();
      }
    }

    list<CDir*> ls;
    for (list<CDir*>::iterator p = ls.begin();
	 p != ls.end();
	 ++p) {
      CDir *dir = *p;
      if (dir->authority().first == mds->get_nodeid())
	dir->state_set(CDir::STATE_AUTH);
      else {
	dir->state_clear(CDir::STATE_AUTH);
	if (dir->is_dirty()) 
	  dir->mark_clean();
      }
    }
  }
  show_subtrees();
  show_cache();
}

/*
 * rejoin phase!
 * we start out by sending rejoins to everyone in the recovery set.
 *
 * if _were_ are rejoining, send for all regions in our cache.
 * if we are active|stopping, send only to nodes that are are rejoining.
 */
void MDCache::send_cache_rejoins()
{
  dout(10) << "send_cache_rejoins with recovery_set " << recovery_set << endl;

  map<int, MMDSCacheRejoin*> rejoins;

  // if i am rejoining, send a rejoin to everyone.
  // otherwise, just send to others who are rejoining.
  for (set<int>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) {
    if (*p == mds->get_nodeid())  continue;  // nothing to myself!
    if (mds->is_rejoin() ||
	mds->mdsmap->is_rejoin(*p))
      rejoins[*p] = new MMDSCacheRejoin;
  }	

  assert(!migrator->is_importing());
  assert(!migrator->is_exporting());

  // check all subtrees
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    assert(dir->is_subtree_root());
    assert(!dir->auth_is_ambiguous());

    int auth = dir->get_dir_auth().first;
    assert(auth >= 0);
    
    if (auth == mds->get_nodeid()) continue;  // skip my own regions!
    if (rejoins.count(auth) == 0) continue;   // don't care about this node's regions

    cache_rejoin_walk(dir, rejoins[auth]);
  }

  // send the messages
  assert(rejoin_ack_gather.empty());
  for (map<int,MMDSCacheRejoin*>::iterator p = rejoins.begin();
       p != rejoins.end();
       ++p) {
    mds->send_message_mds(p->second, p->first, MDS_PORT_CACHE);
    rejoin_ack_gather.insert(p->first);
  }

  // nothing?
  if (rejoins.empty()) {
    dout(10) << "nothing to rejoin, going active" << endl;
    mds->set_want_state(MDSMap::STATE_ACTIVE);
  }
}


void MDCache::cache_rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin)
{
  dout(10) << "cache_rejoin_walk " << *dir << endl;
  rejoin->add_dir(dir->ino());

  list<CDir*> nested;  // finish this dir, then do nested items
  
  // walk dentries
  for (map<string,CDentry*>::iterator p = dir->items.begin();
       p != dir->items.end();
       ++p) {
    // dentry
    rejoin->add_dentry(dir->ino(), p->first);
    
    // inode?
    if (p->second->is_primary() && p->second->get_inode()) {
      CInode *in = p->second->get_inode();
      rejoin->add_inode(in->ino(), 
			in->get_caps_wanted());
      
      // dir (in this subtree)?
      if (in->dir && !in->dir->is_subtree_root()) 
	nested.push_back(in->dir);
    }
  }

  // recurse into nested dirs
  for (list<CDir*>::iterator p = nested.begin(); 
       p != nested.end();
       ++p)
    cache_rejoin_walk(*p, rejoin);
}


/*
 * i got a rejoin.
 * 
 *  - reply with the lockstate
 *
 * if i am active|stopping, 
 *  - remove source from replica list for everything not referenced here.
 */
void MDCache::handle_cache_rejoin(MMDSCacheRejoin *m)
{
  dout(7) << "handle_cache_rejoin from " << m->get_source() << endl;
  int from = m->get_source().num();

  MMDSCacheRejoinAck *ack = new MMDSCacheRejoinAck;

  if (mds->is_active() || mds->is_stopping()) {
    dout(10) << "removing stale cache replicas" << endl;
    // first, scour cache of replica references
    for (hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
	 p != inode_map.end();
	 ++p) {
      // inode
      CInode *in = p->second;
      if (in->is_replica(from) && m->inodes.count(p->first) == 0) {
	inode_remove_replica(in, from);
	dout(10) << " rem " << *in << endl;
      }

      // dentry
      if (in->parent) {
	CDentry *dn = in->parent;
	if (dn->is_replica(from) &&
	    (m->dentries.count(dn->get_dir()->ino()) == 0 ||
	     m->dentries[dn->get_dir()->ino()].count(dn->get_name()) == 0)) {
	  dn->remove_replica(from);
	  dout(10) << " rem " << *dn << endl;
	}
      }

      // dir
      if (in->dir) {
	CDir *dir = in->dir;
	if (dir->is_replica(from) && m->dirs.count(p->first) == 0) {
	  dir->remove_replica(from);
	  dout(10) << " rem " << *dir << endl;
	}
      }
    }
  } else {
    assert(mds->is_rejoin());
  }

  // dirs
  for (set<inodeno_t>::iterator p = m->dirs.begin();
       p != m->dirs.end();
       ++p) {
    CDir *dir = get_dir(*p);
    assert(dir);
    int nonce = dir->add_replica(from);
    dout(10) << " has " << *dir << endl;
    ack->add_dir(*p, nonce);
    
    // dentries
    for (set<string>::iterator q = m->dentries[*p].begin();
	 q != m->dentries[*p].end();
	 ++q) {
      CDentry *dn = dir->lookup(*q);
      assert(dn);
      int nonce = dn->add_replica(from);
      dout(10) << " has " << *dn << endl;
      ack->add_dentry(*p, *q, dn->get_lockstate(), nonce);
    }
  }

  // inodes
  for (map<inodeno_t,int>::iterator p = m->inodes.begin();
       p != m->inodes.end();
       ++p) {
    CInode *in = get_inode(p->first);
    assert(in);
    int nonce = in->add_replica(from);
    if (p->second)
      in->mds_caps_wanted[from] = p->second;
    else
      in->mds_caps_wanted.erase(from);
    in->hardlock.gather_set.erase(from);  // just in case
    in->filelock.gather_set.erase(from);  // just in case
    dout(10) << " has " << *in << endl;
    ack->add_inode(p->first, 
		   in->hardlock.get_replica_state(), in->filelock.get_replica_state(), 
		   nonce);
  }

  // send ack
  mds->send_message_mds(ack, from, MDS_PORT_CACHE);
  
  delete m;
}


void MDCache::handle_cache_rejoin_ack(MMDSCacheRejoinAck *m)
{
  dout(7) << "handle_cache_rejoin_ack from " << m->get_source() << endl;
  int from = m->get_source().num();
  
  // dirs
  for (list<MMDSCacheRejoinAck::dirinfo>::iterator p = m->dirs.begin();
       p != m->dirs.end();
       ++p) {
    CDir *dir = get_dir(p->dirino);
    assert(dir);

    dir->set_replica_nonce(p->nonce);
    dout(10) << " got " << *dir << endl;

    // dentries
    for (map<string,MMDSCacheRejoinAck::dninfo>::iterator q = m->dentries[p->dirino].begin();
	 q != m->dentries[p->dirino].end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first);
      assert(dn);
      dn->set_replica_nonce(q->second.nonce);
      dn->set_lockstate(q->second.lock);
      dout(10) << " got " << *dn << endl;
    }
  }

  // inodes
  for (list<MMDSCacheRejoinAck::inodeinfo>::iterator p = m->inodes.begin();
       p != m->inodes.end();
       ++p) {
    CInode *in = get_inode(p->ino);
    assert(in);
    in->set_replica_nonce(p->nonce);
    in->hardlock.set_state(p->hardlock);
    in->filelock.set_state(p->filelock);
    dout(10) << " got " << *in << endl;
  }

  delete m;

  // done?
  rejoin_ack_gather.erase(from);
  if (rejoin_ack_gather.empty()) {
    dout(7) << "all done, going active!" << endl;
    show_subtrees();
    show_cache();
    mds->set_want_state(MDSMap::STATE_ACTIVE);
  } else {
    dout(7) << "still need rejoin_ack from " << rejoin_ack_gather << endl;
  }

}





// ===============================================================================

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



void MDCache::set_root(CInode *in)
{
  assert(root == 0);
  root = in;
  root->state_set(CInode::STATE_ROOT);
}






// **************
// Inode purging -- reliably removing deleted file's objects

class C_MDC_PurgeFinish : public Context {
  MDCache *mdc;
  inodeno_t ino;
public:
  C_MDC_PurgeFinish(MDCache *c, inodeno_t i) : mdc(c), ino(i) {}
  void finish(int r) {
    mdc->purge_inode_finish(ino);
  }
};
class C_MDC_PurgeFinish2 : public Context {
  MDCache *mdc;
  inodeno_t ino;
public:
  C_MDC_PurgeFinish2(MDCache *c, inodeno_t i) : mdc(c), ino(i) {}
  void finish(int r) {
    mdc->purge_inode_finish_2(ino);
  }
};

/* purge_inode in
 * will be called by on unlink or rmdir
 * caller responsible for journaling an appropriate EUnlink or ERmdir
 */
void MDCache::purge_inode(inode_t &inode)
{
  dout(10) << "purge_inode " << inode.ino << " size " << inode.size << endl;

  // take note
  assert(purging.count(inode.ino) == 0);
  purging[inode.ino] = inode;

  // remove
  mds->filer->remove(inode, 0, inode.size,
		     0, new C_MDC_PurgeFinish(this, inode.ino));
}

void MDCache::purge_inode_finish(inodeno_t ino)
{
  dout(10) << "purge_inode_finish " << ino << " - logging our completion" << endl;

  // log completion
  mds->mdlog->submit_entry(new EPurgeFinish(ino),
			   new C_MDC_PurgeFinish2(this, ino));
}

void MDCache::purge_inode_finish_2(inodeno_t ino)
{
  dout(10) << "purge_inode_finish_2 " << ino << endl;

  // remove from purging list
  purging.erase(ino);

  // tell anyone who cares (log flusher?)
  list<Context*> ls;
  ls.swap(waiting_for_purge[ino]);
  waiting_for_purge.erase(ino);
  finish_contexts(ls, 0);

  // reclaim ino?
  
}

void MDCache::start_recovered_purges()
{
  for (map<inodeno_t,inode_t>::iterator p = purging.begin();
       p != purging.end();
       ++p) {
    dout(10) << "start_recovered_purges " << p->first << " size " << p->second.size << endl;
    mds->filer->remove(p->second, 0, p->second.size,
		       0, new C_MDC_PurgeFinish(this, p->first));
  }
}



// ================================================================================
// cache trimming


bool MDCache::trim(int max) 
{
  // trim LRU
  if (max < 0) {
    max = lru.lru_get_max();
    if (!max) return false;
  }
  dout(7) << "trim max=" << max << "  cur=" << lru.lru_get_size() << endl;

  map<int, MCacheExpire*> expiremap;

  // DENTRIES from the LRU

  while (lru.lru_get_size() > (unsigned)max) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;
    
    CDir *dir = dn->get_dir();
    assert(dir);

    CDir *con = get_subtree_root(dir);
    assert(con);

    dout(12) << "trim removing " << *dn << endl;
    dout(12) << " in container " << *con << endl;
    
    // notify dentry authority?
    if (!dn->is_auth()) {
      pair<int,int> auth = dn->authority();

      for (int a=auth.first; 
	   a != auth.second && auth.second >= 0 && auth.second != mds->get_nodeid(); 
	   a=auth.second) {
	dout(12) << "  sending expire to mds" << a << " on " << *dn << endl;
	assert(a != mds->get_nodeid());
	if (expiremap.count(a) == 0) 
	  expiremap[a] = new MCacheExpire(mds->get_nodeid());
	expiremap[a]->add_dentry(con->dirfrag(), dir->dirfrag(), dn->get_name(), dn->get_replica_nonce());
      }
    }
    
    // unlink the dentry
    if (dn->is_remote()) {
      // just unlink.
      dir->unlink_inode(dn);
    } 
    else if (dn->is_primary()) {
      // expire the inode, too.
      CInode *in = dn->get_inode();
      assert(in);
      trim_inode(dn, in, con->dirfrag(), expiremap);
    } 
    else {
      assert(dn->is_null());
    }
    dir->remove_dentry(dn);

    // adjust the dir state
    dir->state_clear(CDir::STATE_COMPLETE);  // dir incomplete!

    // reexport?
    if (dir->get_size() == 0)
      migrator->export_empty_import(dir);
    
    if (mds->logger) mds->logger->inc("cex");
  }

  // troot inode+dir?
  if (max == 0 &&  // only if we're trimming everything!
      lru.lru_get_size() == 0 && 
      root) {
    // root dirfrags?
    list<CDir*> ls;
    root->get_dirfrags(ls);
    for (list<CDir*>::iterator p = ls.begin();
	 p != ls.end();
	 ++p) 
      if ((*p)->get_num_ref() == 0) 
	trim_dirfrag(*p, (*p)->dirfrag(), expiremap);
    
    // root inode?
    if (root->get_num_ref() == 0)
      trim_inode(0, root, dirfrag_t(1,frag_t()), expiremap);    // hrm, FIXME
  }

  // send expires
  for (map<int, MCacheExpire*>::iterator it = expiremap.begin();
       it != expiremap.end();
       it++) {
    dout(7) << "sending cache_expire to " << it->first << endl;
    mds->send_message_mds(it->second, it->first, MDS_PORT_CACHE);
  }

  return true;
}

void MDCache::trim_dirfrag(CDir *dir, dirfrag_t condf, map<int, MCacheExpire*>& expiremap)
{
  assert(dir->get_num_ref() == 0);

  CInode *in = dir->get_inode();

  if (!dir->is_auth()) {
    pair<int,int> dirauth = dir->authority();
    assert(dirauth.second < 100);   // hack die bug die
    
    // was this an auth delegation?  (if so, slightly modified container)
    dirfrag_t dcondf = condf;
    if (dir->is_subtree_root()) {
      dout(12) << "  this is a subtree, removing from map, container is " << *dir << endl;
      dcondf = dir->dirfrag();
    }
      
    for (int a=dirauth.first; 
	 a != dirauth.second && dirauth.second >= 0 && dirauth.second != mds->get_nodeid(); 
	 a=dirauth.second) {
      dout(12) << "  sending expire to mds" << a << " on   " << *in->dir << endl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_dir(dcondf, dir->dirfrag(), dir->replica_nonce);
    }
  }
  
  if (dir->is_subtree_root())
    remove_subtree(dir);	// remove from subtree map
  in->close_dirfrag(dir->dirfrag().frag);
}

void MDCache::trim_inode(CDentry *dn, CInode *in, dirfrag_t condf, map<int, MCacheExpire*>& expiremap)
{
  assert(in->get_num_ref() == 0);
    
  // DIR
  list<CDir*> dfls;
  in->get_dirfrags(dfls);
  for (list<CDir*>::iterator p = dfls.begin();
       p != dfls.end();
       ++p) 
    trim_dirfrag(*p, condf, expiremap);
  
  // INODE
  if (!in->is_auth()) {
    pair<int,int> auth = in->authority();
    
    for (int a=auth.first; 
	 a != auth.second && auth.second >= 0 && auth.second != mds->get_nodeid(); 
	 a=auth.second) {
      dout(12) << "  sending expire to mds" << a << " on " << *in << endl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_inode(condf, in->ino(), in->get_replica_nonce());
    }
  }
  
  dout(15) << "  trim removing " << *in << endl;
  
  // unlink
  if (dn)
    dn->get_dir()->unlink_inode(dn);
  remove_inode(in);
  if (in == root) root = 0;
}


void MDCache::trim_non_auth()
{
  dout(7) << "trim_non_auth" << endl;
  
  // note first auth item we see.  
  // when we see it the second time, stop.
  CDentry *first_auth = 0;
  
  // trim non-auth items from the lru
  while (lru.lru_get_size() > 0) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;

    if (dn->is_auth()) {
      // add back into lru (at the top)
      lru.lru_insert_top(dn);
      
      if (!first_auth) {
	first_auth = dn;
      } else {
	if (first_auth == dn) 
	  break;
      }
    } else {
      // non-auth.  expire.
      CDir *dir = dn->get_dir();
      assert(dir);

      // unlink the dentry
      dout(15) << "trim_non_auth removing " << *dn << endl;
      if (dn->is_remote()) {
	dir->unlink_inode(dn);
      } 
      else if (dn->is_primary()) {
	CInode *in = dn->get_inode();
	list<CDir*> ls;
	in->get_dirfrags(ls);
	for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
	  CDir *subdir = *p;
	  if (subdir->is_subtree_root()) 
	    remove_subtree(subdir);
	  in->close_dirfrag(subdir->dirfrag().frag);
	}
	dir->unlink_inode(dn);
	remove_inode(in);
	if (in == root) root = 0;
      } 
      else {
	assert(dn->is_null());
      }
      dir->remove_dentry(dn);
      
      // adjust the dir state
      dir->state_clear(CDir::STATE_COMPLETE);  // dir incomplete!
    }
  }

  if (lru.lru_get_size() == 0) {
    list<CDir*> ls;
    root->get_dirfrags(ls);
    for (list<CDir*>::iterator p = ls.begin();
	 p != ls.end();
	 ++p) {
      assert((*p)->get_num_ref() == 0);
      remove_subtree((*p));
      root->close_dirfrag((*p)->dirfrag().frag);
    }
    assert(root->get_num_ref() == 0);
    remove_inode(root);
    root = 0;
  }

  show_subtrees();
}

void MDCache::handle_cache_expire(MCacheExpire *m)
{
  int from = m->get_from();
  
  dout(7) << "cache_expire from mds" << from << endl;

  // loop over realms
  for (map<dirfrag_t,MCacheExpire::realm>::iterator p = m->realms.begin();
       p != m->realms.end();
       ++p) {
    // get container
    CDir *con = get_dirfrag(p->first);
    assert(con);  // we had better have this.

    if (!con->is_auth()) {
      // not auth.
      dout(7) << "delaying nonauth expires for " << *con << endl;
      assert(con->is_frozen_tree_root());

      // make a message container
      if (delayed_expire[con].count(from) == 0) 
	delayed_expire[con][from] = new MCacheExpire(from);

      // merge these expires into it
      delayed_expire[con][from]->add_realm(p->first, p->second);
      continue;
    }
    dout(7) << "expires for " << *con << endl;

    // INODES
    for (map<inodeno_t,int>::iterator it = p->second.inodes.begin();
	 it != p->second.inodes.end();
	 it++) {
      CInode *in = get_inode(it->first);
      int nonce = it->second;
      
      if (!in) {
	dout(0) << " inode expire on " << it->first << " from " << from << ", don't have it" << endl;
	assert(in);
      }        
      assert(in->is_auth());
      
      // check nonce
      if (nonce == in->get_replica_nonce(from)) {
	// remove from our cached_by
	dout(7) << " inode expire on " << *in << " from mds" << from << " cached_by was " << in->get_replicas() << endl;
	inode_remove_replica(in, from);
      } 
      else {
	// this is an old nonce, ignore expire.
	dout(7) << " inode expire on " << *in << " from mds" << from
		<< " with old nonce " << nonce << " (current " << in->get_replica_nonce(from) << "), dropping" 
		<< endl;
	assert(in->get_replica_nonce(from) > nonce);
      }
    }
    
    // DIRS
    for (map<dirfrag_t,int>::iterator it = p->second.dirs.begin();
	 it != p->second.dirs.end();
	 it++) {
      CDir *dir = get_dirfrag(it->first);
      int nonce = it->second;
      
      if (!dir) {
	dout(0) << " dir expire on " << it->first << " from " << from << ", don't have it" << endl;
	assert(dir);
      }  
      assert(dir->is_auth());
      
      // check nonce
      if (nonce == dir->get_replica_nonce(from)) {
	// remove from our cached_by
	dout(7) << " dir expire on " << *dir << " from mds" << from
		<< " replicas was " << dir->replicas << endl;
	dir->remove_replica(from);
      } 
      else {
	// this is an old nonce, ignore expire.
	dout(7) << " dir expire on " << *dir << " from mds" << from 
		<< " with old nonce " << nonce << " (current " << dir->get_replica_nonce(from)
		<< "), dropping" << endl;
	assert(dir->get_replica_nonce(from) > nonce);
      }
    }
    
    // DENTRIES
    for (map<dirfrag_t, map<string,int> >::iterator pd = p->second.dentries.begin();
	 pd != p->second.dentries.end();
	 ++pd) {
      dout(0) << " dn expires in dir " << pd->first << endl;
      CDir *dir = get_dirfrag(pd->first);
      
      if (!dir) {
	dout(0) << " dn expires on " << pd->first << " from " << from << ", don't have it" << endl;
	assert(dir);
      } 
      assert(dir->is_auth());
      
      for (map<string,int>::iterator p = pd->second.begin();
	   p != pd->second.end();
	   ++p) {
	int nonce = p->second;
	
	CDentry *dn = dir->lookup(p->first);
	if (!dn) 
	  dout(0) << "  missing dentry for " << p->first << " in " << *dir << endl;
	assert(dn);
	
	if (nonce == dn->get_replica_nonce(from)) {
	  dout(7) << "  dentry_expire on " << *dn << " from mds" << from << endl;
	  dn->remove_replica(from);
	} 
	else {
	  dout(7) << "  dentry_expire on " << *dn << " from mds" << from
		  << " with old nonce " << nonce << " (current " << dn->get_replica_nonce(from)
		  << "), dropping" << endl;
	  assert(dn->get_replica_nonce(from) > nonce);
	}
      }
    }
  }


  // done
  delete m;
}

void MDCache::process_delayed_expire(CDir *dir)
{
  dout(7) << "process_delayed_expire on " << *dir << endl;
  for (map<int,MCacheExpire*>::iterator p = delayed_expire[dir].begin();
       p != delayed_expire[dir].end();
       ++p) 
    handle_cache_expire(p->second);
  delayed_expire.erase(dir);  
}

void MDCache::discard_delayed_expire(CDir *dir)
{
  dout(7) << "discard_delayed_expire on " << *dir << endl;
  for (map<int,MCacheExpire*>::iterator p = delayed_expire[dir].begin();
       p != delayed_expire[dir].end();
       ++p) 
    delete p->second;
  delayed_expire.erase(dir);  
}

void MDCache::inode_remove_replica(CInode *in, int from)
{
  in->remove_replica(from);
  in->mds_caps_wanted.erase(from);
  
  // note: this code calls _eval more often than it needs to!
  // fix lock
  if (in->hardlock.is_gathering(from)) {
    in->hardlock.gather_set.erase(from);
    if (in->hardlock.gather_set.size() == 0)
      mds->locker->inode_hard_eval(in);
  }
  if (in->filelock.is_gathering(from)) {
    in->filelock.gather_set.erase(from);
    if (in->filelock.gather_set.size() == 0)
      mds->locker->inode_file_eval(in);
  }
  
  // alone now?
  if (!in->is_replicated()) {
    mds->locker->inode_hard_eval(in);
    mds->locker->inode_file_eval(in);
  }
}



// =========================================================================================
// shutdown

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
public:
  C_MDC_ShutdownCheck(MDCache *m) : mdc(m) {}
  void finish(int) {
    mdc->shutdown_check();
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
  mds->timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  // this
  dout(0) << "lru size now " << lru.lru_get_size() << endl;
  dout(0) << "log len " << mds->mdlog->get_num_events() << endl;


  if (mds->filer->is_active()) 
    dout(0) << "filer still active" << endl;
}

void MDCache::shutdown_start()
{
  dout(1) << "shutdown_start" << endl;

  if (g_conf.mds_shutdown_check)
    mds->timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));
}



bool MDCache::shutdown_pass()
{
  dout(7) << "shutdown_pass" << endl;
  //assert(mds->is_shutting_down());
  if (mds->is_out()) {
    dout(7) << " already shut down" << endl;
    show_cache();
    show_subtrees();
    return true;
  }

  // unhash dirs?
  /*
  if (!hashdirs.empty()) {
    // unhash any of my dirs?
    for (set<CDir*>::iterator it = hashdirs.begin();
         it != hashdirs.end();
         it++) {
      CDir *dir = *it;
      if (!dir->is_auth()) continue;
      if (dir->is_unhashing()) continue;
      //migrator->unhash_dir(dir);
    }

    dout(7) << "waiting for dirs to unhash" << endl;
    return false;
  }
  */

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
          in->dir->commit(0, new C_MDC_ShutdownCommit(this));
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
  dout(5) << "lru size now " << lru.lru_get_size() << endl;

  mds->mdlog->trim(0);

  // (wait for) flush log?
  if (g_conf.mds_log_flush_on_shutdown) {
    if (mds->mdlog->get_non_importmap_events()) {
      dout(7) << "waiting for log to flush .. " << mds->mdlog->get_num_events() 
	      << " (" << mds->mdlog->get_non_importmap_events() << ")" << endl;
      return false;
    } 
  }


  // send all imports back to 0.
  if (mds->get_nodeid() != 0 && 
      !migrator->is_exporting() &&
      !migrator->is_importing()) {
    // flush what i can from the cache first..
    trim(0);
    
    // export to root
    for (map<CDir*, set<CDir*> >::iterator it = subtrees.begin();
         it != subtrees.end();
         it++) {
      CDir *dir = it->first;
      if (dir->inode->is_root()) continue;
      if (dir->is_frozen() || dir->is_freezing()) continue;
      if (!dir->is_fullauth()) continue;
      
      dout(7) << "sending " << *dir << " back to mds0" << endl;
      migrator->export_dir(dir, 0);
    }
    did_shutdown_exports = true;
  } 
  
  // close root?
  if (lru.lru_get_size() == 0 &&
      root &&
      root->is_pinned_by(CInode::PIN_DIRTY)) {
    dout(7) << "clearing root inode dirty flag" << endl;
    root->put(CInode::PIN_DIRTY);
  }
  
  // subtrees map not empty yet?
  if (!subtrees.empty()) {
    dout(7) << "still have " << num_subtrees() << " subtrees" << endl;
    show_cache();
    return false;
  }
  assert(subtrees.empty());
  assert(!migrator->is_exporting());
  assert(!migrator->is_importing());
  
  // cap log?
  if (g_conf.mds_log_flush_on_shutdown) {

    // (only do this once!)
    if (!mds->mdlog->is_capped()) {
      dout(7) << "capping the log" << endl;
      mds->mdlog->cap();
      // note that this won't flush right away, so we'll make at least one more pass
    }
    
    if (mds->mdlog->get_num_events()) {
      dout(7) << "waiting for log to flush (including import_map, now) .. " << mds->mdlog->get_num_events() 
	      << " (" << mds->mdlog->get_non_importmap_events() << ")" << endl;
      return false;
    }
    
    if (!did_shutdown_log_cap) {
      // flush journal header
      dout(7) << "writing header for (now-empty) journal" << endl;
      assert(mds->mdlog->empty());
      mds->mdlog->write_head(0);  
      // NOTE: filer active checker below will block us until this completes.
      did_shutdown_log_cap = true;
      return false;
    }
  }

  // filer active?
  if (mds->filer->is_active()) {
    dout(7) << "filer still active" << endl;
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









// ========= messaging ==============


void MDCache::dispatch(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_IMPORTMAP:
    handle_import_map((MMDSImportMap*)m);
    break;

  case MSG_MDS_CACHEREJOIN:
    handle_cache_rejoin((MMDSCacheRejoin*)m);
    break;
  case MSG_MDS_CACHEREJOINACK:
    handle_cache_rejoin_ack((MMDSCacheRejoinAck*)m);
    break;


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
          cur->get_parent_dir()->add_waiter(CDir::WAIT_UNFREEZE, ondelay);
          if (onfinish) delete onfinish;
          return 1;
        }

	frag_t fg = cur->pick_dirfrag(path[depth]);
        cur->get_or_open_dirfrag(this, fg);
        assert(cur->dir);
      } else {
        // discover dir from/via inode auth
        assert(!cur->is_auth());
        if (cur->waiting_for(CInode::WAIT_DIR)) {
          dout(10) << "traverse: need dir for " << *cur << ", already doing discover" << endl;
        } else {
          filepath want = path.postfixpath(depth);
          dout(10) << "traverse: need dir for " << *cur << ", doing discover, want " << want.get_path() << endl;
          mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					      cur->ino(),
					      want,
					      true),  // need this dir too
				cur->authority().first, MDS_PORT_CACHE);
	  dir_discovers[cur->ino()].insert(cur->authority().first);
        }
        cur->add_waiter(CInode::WAIT_DIR, ondelay);
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
      cur->dir->add_waiter(CDir::WAIT_UNFREEZE, ondelay);
      if (onfinish) delete onfinish;
      return 1;
    }
    */

    // must read directory hard data (permissions, x bit) to traverse
    if (!noperm && !mds->locker->inode_hard_read_try(cur, ondelay)) {
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
        cur->dir->add_waiter(CDir::WAIT_DNREAD,
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
          dout(7) << "remote link to " << dn->get_remote_ino() << ", which i don't have" << endl;
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
          dout(30) << "traverse: REP is here, " << ((MClientRequest*)req)->get_mds_wants_replica_in_dirino() << " vs " << cur->dir->ino() << endl;
          
          if (((MClientRequest*)req)->get_mds_wants_replica_in_dirino() == cur->dir->ino() &&
              cur->dir->is_auth() && 
              cur->dir->is_rep() &&
              cur->dir->is_replica(req->get_source().num()) &&
              dn->get_inode()->is_auth()
              ) {
            assert(req->get_source().is_mds());
            int from = req->get_source().num();
            
            if (dn->get_inode()->is_replica(from)) {
              dout(15) << "traverse: REP would replicate to mds" << from << ", but already cached_by " 
                       << req->get_source() << " dn " << *dn << endl; 
            } else {
              dout(10) << "traverse: REP replicating to " << req->get_source() << " dn " << *dn << endl;
              MDiscoverReply *reply = new MDiscoverReply(cur->dir->ino());
              reply->add_dentry( dn->replicate_to( from ) );
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

    pair<int,int> dauth = cur->dir->dentry_authority( path[depth] );
    dout(12) << "traverse: miss on dentry " << path[depth] << " dauth " << dauth << " in " << *cur->dir << endl;
    

    if (dauth.first == whoami) {
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
        cur->dir->fetch(ondelay);
        
        if (mds->logger) mds->logger->inc("cmiss");

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
        if (cur->dir->waiting_for(CDir::WAIT_DENTRY, path[depth])) {
          dout(7) << "traverse: already waiting for discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
        } else {
          dout(7) << "traverse: discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
          
          touch_inode(cur);
        
          mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					      cur->ino(),
					      want,
					      false),
				dauth.first, MDS_PORT_CACHE);
	  if (dauth.second >= 0) 
	    mds->send_message_mds(new MDiscover(mds->get_nodeid(),
						cur->ino(),
						want,
						false),
				  dauth.second, MDS_PORT_CACHE);

          if (mds->logger) mds->logger->inc("dis");
        }
        
        // delay processing of current request.
        //  delay finish vs ondelay until result of traverse, so that ENOENT can be 
        //  passed to onfinish if necessary
        cur->dir->add_waiter(CDir::WAIT_DENTRY, 
                             path[depth], 
                             new C_MDC_TraverseDiscover(onfinish, ondelay));
        
        if (mds->logger) mds->logger->inc("cmiss");
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

        mds->send_message_mds(req, dauth.first, req->get_dest_port());
        //show_subtrees();
        
        if (mds->logger) mds->logger->inc("cfw");
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
			diri->authority().first, MDS_PORT_CACHE);
  dir_discovers[diri->ino()].insert(diri->authority().first);
  diri->add_waiter(CInode::WAIT_DIR, fin);
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
        dn->dir->add_waiter(CDir::WAIT_DNPINNABLE,   
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
    if (dn->lockstate == DN_LOCK_UNPINNING && !dn->get_num_ref()) {
      // return state to sync, in case the unpinner flails
      dn->lockstate = DN_LOCK_SYNC;

      // run finisher right now to give them a fair shot.
      dn->dir->finish_waiting(CDir::WAIT_DNUNPINNED, dn->name);
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
  
  if (mds->logger) mds->logger->inc("req");

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
      
      mds->locker->dentry_xlock_finish(dn);
      
      // queue finishers
      dn->dir->take_waiting(CDir::WAIT_ANY, dn->name, mds->finished_queue);

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
      int dauth = dn->dir->dentry_authority(dn->name).first;
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
  if (mds->logger) {
    mds->logger->set("c", lru.lru_get_size());
    mds->logger->set("cpin", lru.lru_get_num_pinned());
    mds->logger->set("ctop", lru.lru_get_top());
    mds->logger->set("cbot", lru.lru_get_bot());
    mds->logger->set("cptail", lru.lru_get_pintail());
    //mds->logger->set("buf",buffer_total_alloc);
  }

  if (g_conf.log_pins) {
    // pin
    /*
for (int i=0; i<CInode::NUM_PINS; i++) {
      if (mds->logger2) mds->logger2->set(cinode_pin_names[i],
					  cinode_pins[i]);
    }
    */
    /*
      for (map<int,int>::iterator it = cdir_pins.begin();
      it != cdir_pins.end();
      it++) {
      //string s = "D";
      //s += cdir_pin_names[it->first];
      if (mds->logger2) mds->logger2->set(//s, 
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
  
  if (mds->logger) mds->logger->inc("reply");


  //dump();
}


void MDCache::request_forward(Message *req, int who, int port)
{
  if (!port) port = MDS_PORT_SERVER;

  dout(7) << "request_forward to " << who << " req " << *req << endl;

  // clean up my state
  request_cleanup(req);

  // client request?
  if (req->get_type() == MSG_CLIENT_REQUEST) {
    MClientRequest *creq = (MClientRequest*)req;

    // inc forward counter
    creq->inc_num_fwd();
    
    // tell the client
    mds->messenger->send_message(new MClientRequestForward(creq->get_tid(), who, creq->get_num_fwd()),
				 creq->get_client_inst());
  }

  // forward
  mds->send_message_mds(req, who, port);

  if (mds->logger) mds->logger->inc("fw");
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

      in->state_clear(CInode::STATE_ANCHORING);
      in->put(CInode::PIN_ANCHORING);
      
      in->_mark_dirty(); // fixme
    }

    // trigger
    in->finish_waiting(CInode::WAIT_ANCHORED, r);
  }
};

void MDCache::anchor_inode(CInode *in, Context *onfinish)
{
  assert(in->is_auth());

  // already anchoring?
  if (in->state_test(CInode::STATE_ANCHORING)) {
    dout(7) << "anchor_inode already anchoring " << *in << endl;

    // wait
    in->add_waiter(CInode::WAIT_ANCHORED,
                   onfinish);

  } else {
    dout(7) << "anchor_inode anchoring " << *in << endl;

    // auth: do it
    in->state_set(CInode::STATE_ANCHORING);
    in->get(CInode::PIN_ANCHORING);
    
    // wait
    in->add_waiter(CInode::WAIT_ANCHORED,
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
    dout(7) << "handle_inode_link not auth for " << *in << ", fw to auth" << endl;
    mds->send_message_mds(m, in->authority().first, MDS_PORT_CACHE);
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
  in->_mark_dirty(); // fixme

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
  in->finish_waiting(CInode::WAIT_LINK,
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
      int iauth = cur->authority().first;
      dout(7) << "no dir and not inode auth; fwd to auth " << iauth << endl;
      mds->send_message_mds( dis, iauth, MDS_PORT_CACHE);
      return;
    }

    // frozen_dir?
    if (!cur->dir && cur->is_frozen_dir()) {
      dout(7) << "is frozen_dir, waiting" << endl;
      cur->get_parent_dir()->add_waiter(CDir::WAIT_UNFREEZE, 
                                        new C_MDS_RetryMessage(mds, dis));
      return;
    }

    if (!cur->dir) 
      cur->get_or_open_dir(this);
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
      
      if (!cur->dir) cur->get_or_open_dir(this);
      
      reply->add_dir( new CDirDiscover( cur->dir, 
                                        cur->dir->add_replica( dis->get_asker() ) ) );
      dout(7) << "added dir " << *cur->dir << endl;
    }
    if (dis->get_want().depth() == 0) break;
    
    // lookup dentry
    int dentry_auth = cur->dir->dentry_authority( dis->get_dentry(i) ).first;
    if (dentry_auth != mds->get_nodeid()) {
      dout(7) << *cur->dir << "dentry " << dis->get_dentry(i) << " auth " << dentry_auth << ", i'm done." << endl;
      break;      // that's it for us!
    }

    // get inode
    CDentry *dn = cur->dir->lookup( dis->get_dentry(i) );
    
    /*
    if (dn && !dn->can_read()) { // xlocked?
      dout(7) << "waiting on " << *dn << endl;
      cur->dir->add_waiter(CDir::WAIT_DNREAD,
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
      
      reply->add_dentry( dn->replicate_to( dis->get_asker() ) );
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

        cur->dir->fetch(new C_MDS_RetryMessage(mds, dis));
        return;
      }
    }
  }

  // set dir_auth hint?
  if (cur->is_dir() && cur->dir && 
      cur->is_auth() && !cur->dir->is_auth()) {
    dout(7) << "setting dir_auth_hint for " << *cur->dir << endl;
    reply->set_dir_auth_hint(cur->dir->authority().first);
  }
       
  // how did we do.
  if (reply->is_empty()) {

    // discard empty reply
    delete reply;

    /*
    if (cur->is_auth() && !cur->dir->is_auth()) {
      // set hint
      // fwd to dir auth
      int dirauth = cur->dir->authority().first;
      if (dirauth == dis->get_asker()) {
        dout(7) << "from (new?) dir auth, dropping (obsolete) discover on floor." << endl;  // XXX FIXME is this right?
        //assert(dis->get_asker() == dis->get_source());  //might be a weird other loop.  either way, asker has it.
        delete dis;
      } else {
        dout(7) << "fwd to dir auth " << dirauth << endl;
        mds->send_message_mds( dis, dirauth, MDS_PORT_CACHE );
      }
      return;
      }*/
    
    // wait for frozen dir?
    if (cur->dir->is_frozen()) {
      dout(7) << "waiting for frozen " << *cur->dir << endl;
      cur->dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
      return;
    } else {
      dout(7) << "i'm not auth, dropping request (+this empty reply)." << endl;
    }
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
    cur = new CInode(this, false);
      
    m->get_inode(0).update_inode(cur);
    
    // root
    set_root( cur );
    add_inode( cur );
    dout(7) << " got root: " << *cur << endl;

    // take waiters
    finished.swap(waiting_for_root);
  } else {
    // grab inode
    cur = get_inode(m->get_base_ino());
    
    if (!cur) {
      dout(7) << "discover_reply don't have base ino " << m->get_base_ino() << ", dropping" << endl;
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
	CDir *ndir = cur->add_dirfrag( new CDir(cur, frag_t(), this, false) );  // FIXME dirfrag_t
        m->get_dir(i).update_dir(ndir);

	// is this a dir_auth delegation boundary?
	if (m->get_source().num() != cur->authority().first)
	  adjust_subtree_auth(ndir, m->get_source().num());
	
        dout(7) << "added " << *ndir << " nonce " << ndir->replica_nonce << endl;

        // get waiters
        cur->take_waiting(CInode::WAIT_DIR, finished);
	dir_discovers.erase(cur->ino());
      }
    }    

    // dentry error?
    if (i == m->get_depth()-1 && 
        m->is_flag_error_dn()) {
      // error!
      assert(cur->is_dir());
      if (cur->dir) {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dentry?" << endl;
        cur->dir->take_waiting(CDir::WAIT_DENTRY,
                               m->get_error_dentry(),
                               error);
      } else {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dir?" << endl;
        cur->take_waiting(CInode::WAIT_DIR, error);
	dir_discovers.erase(cur->ino());
      }
      break;
    }

    if (i >= m->get_num_dentries()) break;
    
    // dentry
    dout(7) << "i = " << i << " dentry is " << m->get_dentry(i).get_dname() << endl;

    CDentry *dn = 0;
    if (i > 0 || 
        m->has_base_dentry()) {
      dn = cur->dir->lookup( m->get_dentry(i).get_dname() );
      
      if (dn) {
        dout(7) << "had " << *dn << endl;
	dn->replica_nonce = m->get_dentry(i).get_nonce();  // fix nonce.
      } else {
        dn = cur->dir->add_dentry( m->get_dentry(i).get_dname(), 0, false );
	m->get_dentry(i).update_dentry(dn);
        dout(7) << "added " << *dn << endl;
      }

      cur->dir->take_waiting(CDir::WAIT_DENTRY,
                             m->get_dentry(i).get_dname(),
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
      in = new CInode(this, false);
      
      m->get_inode(i).update_inode(in);
        
      // link in
      add_inode( in );
      dn->dir->link_inode(dn, in);
      
      dout(7) << "added " << *in << " nonce " << in->replica_nonce << endl;
    }
    
    // onward!
    cur = in;
  }

  // dir_auth hint?
  if (m->get_dir_auth_hint() != CDIR_AUTH_UNKNOWN) {
    dout(7) << " dir_auth_hint is " << m->get_dir_auth_hint() << endl;
    // let's just open it.
    int hint = m->get_dir_auth_hint();
    filepath want;  // no dentries, i just want the dir open
    mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					cur->ino(),
					want,
					true),  // need the dir open
			  hint, MDS_PORT_CACHE);
    
    // note the dangling discover
    dir_discovers[cur->ino()].insert(hint);
  }
  else if (m->is_flag_error_dir()) {
    // dir error at the end there?
    dout(7) << " flag_error on dir " << *cur << endl;
    assert(!cur->is_dir());
    cur->take_waiting(CInode::WAIT_DIR, error);
    dir_discovers.erase(cur->ino());
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
    dout(7) << "inode_update on " << m->get_ino() << ", don't have it, sending expire" << endl;
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






int MDCache::send_dir_updates(CDir *dir, bool bcast)
{
  // this is an FYI, re: replication

  set<int> who;
  if (bcast) {
    mds->get_mds_map()->get_active_mds_set(who);
  } else {
    for (map<int,int>::iterator p = dir->replicas_begin();
	 p != dir->replicas_end();
	 ++p)
      who.insert(p->first);
  }
  
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
    dout(5) << "dir_update on " << m->get_ino() << ", don't have it" << endl;

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
  mds->mdlog->submit_entry(new EString("unlink fixme fixme"),//EUnlink(dir, dn, dn->inode),
                           NULL);    // FIXME FIXME FIXME

  // tell replicas
  if (dir->is_replicated()) {
    for (map<int,int>::iterator it = dir->replicas_begin();
         it != dir->replicas_end();
         it++) {
      dout(7) << "inode_unlink sending DentryUnlink to mds" << it->first << endl;
      
      mds->send_message_mds(new MDentryUnlink(dir->ino(), dn->name), it->first, MDS_PORT_CACHE);
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
        dn->inode->dir->state_set(CDir::STATE_DELETED);
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
      dn->_mark_dirty(); // fixme

      // mark it dirty!
      in->_mark_dirty(); // fixme

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
      dn->inode->_mark_dirty(); // fixme

      if (( dn->inode->state_test(CInode::STATE_DANGLING) && dn->inode->inode.nlink == 0) ||
          (!dn->inode->state_test(CInode::STATE_DANGLING) && dn->inode->inode.nlink == 1)) {
        dout(7) << "nlink=1+primary or 0+dangling, removing anchor" << endl;

        // remove anchor (async)
        mds->anchorclient->destroy(dn->inode->ino(), NULL);
      }
    } else {
      int auth = dn->inode->authority().first;
      dout(7) << "remote target is remote, sending unlink request to " << auth << endl;

      mds->send_message_mds(new MInodeUnlink(dn->inode->ino(), mds->get_nodeid()),
			    auth, MDS_PORT_CACHE);

      // unlink locally
      CInode *in = dn->inode;
      dn->dir->unlink_inode( dn );
      dn->_mark_dirty(); // fixme

      // add waiter
      in->add_waiter(CInode::WAIT_UNLINK, c);
      return;
    }
  }
  else 
    assert(0);   // unlink on null dentry??
 
  // unlink locally
  dn->dir->unlink_inode( dn );
  dn->_mark_dirty(); // fixme

  // finish!
  dentry_unlink_finish(dn, dir, c);
}


void MDCache::dentry_unlink_finish(CDentry *dn, CDir *dir, Context *c)
{
  dout(7) << "dentry_unlink_finish on " << *dn << endl;
  string dname = dn->name;

  // unpin dir / unxlock
  mds->locker->dentry_xlock_finish(dn, true); // quiet, no need to bother replicas since they're already unlinking
  
  // did i empty out an imported dir?
  if (dir->get_size() == 0) 
    migrator->export_empty_import(dir);

  // wake up any waiters
  dir->take_waiting(CDir::WAIT_ANY, dname, mds->finished_queue);

  c->finish(0);
}




void MDCache::handle_dentry_unlink(MDentryUnlink *m)
{
  CDir *dir = get_dir(m->get_dirino());

  if (!dir) {
    dout(7) << "handle_dentry_unlink don't have dir " << m->get_dirino() << endl;
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
          dn->inode->dir->state_set(CDir::STATE_DELETED);
          dn->inode->dir->remove_null_dentries();
        }
      }
      
      string dname = dn->name;
      
      // unlink
      dn->dir->remove_dentry(dn);
      
      // wake up
      //dir->finish_waiting(CDir::WAIT_DNREAD, dname);
      dir->take_waiting(CDir::WAIT_DNREAD, dname, mds->finished_queue);
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
    mds->send_message_mds(m, in->authority().first, MDS_PORT_CACHE);
    return;
  }
  assert(in->is_auth());

  // do it.
  dout(7) << "handle_inode_unlink nlink=" << in->inode.nlink << " on " << *in << endl;
  assert(in->inode.nlink > 0);
  in->inode.nlink--;

  if (in->state_test(CInode::STATE_DANGLING)) {
    // already dangling.
    // last link?
    if (in->inode.nlink == 0) {
      dout(7) << "last link, marking clean and removing anchor" << endl;
      
      in->mark_clean();       // mark it clean.
      
      // remove anchor (async)
      mds->anchorclient->destroy(in->ino(), NULL);
    }
    else {
      in->_mark_dirty(); // fixme
    }
  } else {
    // has primary link still.
    assert(in->inode.nlink >= 1);
    in->_mark_dirty(); // fixme

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
  in->finish_waiting(CInode::WAIT_UNLINK, 0);
}










// ==============================================================
// debug crap

void MDCache::show_subtrees(int dbl)
{
  //dout(10) << "show_subtrees" << endl;
  
  list<pair<CDir*,int> > q;
  string indent;

  if (root && root->dir) 
    q.push_back(pair<CDir*,int>(root->dir, 0));

  set<CDir*> seen;

  while (!q.empty()) {
    CDir *dir = q.front().first;
    int d = q.front().second;
    q.pop_front();

    // sanity check
    if (seen.count(dir)) dout(0) << "aah, already seen " << *dir << endl;
    assert(seen.count(dir) == 0);
    seen.insert(dir);

    // adjust indenter
    while ((unsigned)d < indent.size()) 
      indent.resize(d);
    
    // pad
    string pad = "__________________________________";
    pad.resize(12-indent.size());

    string auth;
    if (dir->is_auth())
      auth = "auth ";
    else
      auth = " rep ";

    char s[10];
    if (dir->get_dir_auth().second == CDIR_AUTH_UNKNOWN)
      sprintf(s, "%2d   ", dir->get_dir_auth().first);
    else
      sprintf(s, "%2d,%2d", dir->get_dir_auth().first, dir->get_dir_auth().second);
    
    // print
    dout(dbl) << indent << "|_" << pad << s << " " << auth << *dir << endl;

    // nested items?
    if (!subtrees[dir].empty()) {
      // more at my level?
      if (!q.empty() && q.front().second == d)
	indent += "| ";
      else
	indent += "  ";

      for (set<CDir*>::iterator p = subtrees[dir].begin();
	   p != subtrees[dir].end();
	   ++p) 
	q.push_front(pair<CDir*,int>(*p, d+2));
    }
  }
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


void MDCache::dump_cache()
{
  char fn[20];
  sprintf(fn, "cachedump.%d.mds%d", mds->mdsmap->get_epoch(), mds->get_nodeid());

  dout(1) << "dump_cache to " << fn << endl;

  ofstream myfile;
  myfile.open(fn);
  
  for (hash_map<inodeno_t,CInode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it++) {
    /*
    myfile << *((*it).second) << endl;
    CDentry *dn = (*it).second->get_parent_dn();
    if (dn) 
      myfile << *dn << endl;
    */
    
    if ((*it).second->dir) {
      CDir *dir = (*it).second->dir;
      myfile << *dir->inode << endl;
      myfile << *dir << endl;
      
      for (CDir_map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	myfile << *dn << endl;
      }
    }
  }

  myfile.close();
}
