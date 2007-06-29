// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "MDSMap.h"

#include "CInode.h"
#include "CDir.h"

#include "include/filepath.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "common/Logger.h"

#include "osdc/Filer.h"

#include "events/EImportMap.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EString.h"
#include "events/EPurgeFinish.h"
#include "events/EImportFinish.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSImportMap.h"
#include "messages/MMDSResolveAck.h"
#include "messages/MMDSCacheRejoin.h"

#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"
#include "messages/MCacheExpire.h"

#include "messages/MInodeFileCaps.h"

#include "messages/MLock.h"
#include "messages/MDentryUnlink.h"

#include "messages/MClientRequest.h"
#include "messages/MClientFileCaps.h"

#include "messages/MMDSSlaveRequest.h"

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
  //  renamer = new Renamer(mds, this);
  root = NULL;
  stray = NULL;
  lru.lru_set_max(g_conf.mds_cache_size);
  lru.lru_set_midpoint(g_conf.mds_cache_mid);

  did_shutdown_log_cap = false;
  shutdown_commits = 0;
}

MDCache::~MDCache() 
{
  delete migrator;
  //delete renamer;
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
    dn->dir->unlink_inode(dn);   // leave dentry ... FIXME?
  }

  // remove from inode map
  inode_map.erase(o->ino());    

  // delete it
  delete o; 

  if (o == root) root = 0;
  if (o == stray) stray = 0;
}



CInode *MDCache::create_root_inode()
{
  CInode *root = new CInode(this);
  memset(&root->inode, 0, sizeof(inode_t));
  root->inode.ino = MDS_INO_ROOT;
  
  // make it up (FIXME)
  root->inode.mode = 0755 | INODE_MODE_DIR;
  root->inode.size = 0;
  root->inode.ctime = 
    root->inode.mtime = g_clock.now();
  
  root->inode.nlink = 1;
  root->inode.layout = g_OSD_MDDirLayout;
  
  set_root( root );
  add_inode( root );

  return root;
}


void MDCache::open_root(Context *c)
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
                                     MDS_INO_ROOT,
                                     want,
                                     false);  // there _is_ no base dir for the root inode
      mds->send_message_mds(req, 0, MDS_PORT_CACHE);
    } else {
      dout(7) << "waiting for root" << endl;
    }    

    // wait
    waiting_for_root.push_back(c);

  }
}

CInode *MDCache::create_stray_inode(int whose)
{
  if (whose < 0) whose = mds->get_nodeid();
  stray = new CInode(this, whose == mds->get_nodeid());
  memset(&stray->inode, 0, sizeof(inode_t));
  stray->inode.ino = MDS_INO_STRAY(whose);
  
  // make it up (FIXME)
  stray->inode.mode = 0755 | INODE_MODE_DIR;
  stray->inode.size = 0;
  stray->inode.ctime = 
    stray->inode.mtime = g_clock.now();
  
  stray->inode.nlink = 1;
  stray->inode.layout = g_OSD_MDDirLayout;
  
  add_inode( stray );

  return stray;
}

void MDCache::open_local_stray()
{
  create_stray_inode();
  CDir *dir = stray->get_or_open_dirfrag(this, frag_t());
  adjust_subtree_auth(dir, mds->get_nodeid());   
}

void MDCache::open_foreign_stray(int who, Context *c)
{
  inodeno_t ino = MDS_INO_STRAY(who);
  dout(10) << "open_foreign_stray mds" << who << " " << ino << endl;
  assert(!have_inode(ino));

  // discover
  filepath want;
  MDiscover *req = new MDiscover(mds->get_nodeid(),
				 ino,
				 want,
				 false);  // there _is_ no base dir for the stray inode
  mds->send_message_mds(req, who, MDS_PORT_CACHE);

  // wait
  waiting_for_stray[ino].push_back(c);
}


CDentry *MDCache::get_or_create_stray_dentry(CInode *in)
{
  string straydname;
  in->name_stray_dentry(straydname);
  frag_t fg = stray->pick_dirfrag(straydname);

  CDir *straydir = stray->get_or_open_dirfrag(this, fg);
  
  CDentry *straydn = straydir->lookup(straydname);
  if (!straydn) 
    straydn = straydir->add_dentry(straydname, 0);
  
  return straydn;
}



MDSCacheObject *MDCache::get_object(MDSCacheObjectInfo &info) 
{
  // inode?
  if (info.ino) 
    return get_inode(info.ino);

  // dir or dentry.
  CDir *dir = get_dirfrag(info.dirfrag);
  if (!dir) return 0;
    
  if (info.dname.length()) 
    return dir->lookup(info.dname);
  else
    return dir;
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
  if (dir->ino() < MDS_INO_BASE) {
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
  
  // evaluate subtree inode dirlock?
  //  (we should scatter the dirlock on subtree bounds)
  if (dir->inode->is_auth())
    mds->locker->scatter_eval(&dir->inode->dirlock);  // ** may or may not be auth_pinned **

  show_subtrees();
}


/*
 * any "export" point must be pinned in cache to ensure a proper
 * chain of delegation.  we do this by pinning when a dir is nonauth
 * but the inode is auth.
 *
 * import points don't need to be pinned the same way simply because the
 * exporting mds is pinning the exprot (as above) thus the dir is
 * always open on the importer.
 */
void MDCache::adjust_export_state(CDir *dir)
{
  // be auth bit agnostic, so that we work during recovery
  //  (before recalc_auth_bits)
  if (dir->authority().first        != mds->get_nodeid() &&
      dir->inode->authority().first == mds->get_nodeid()) {
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
  if (dir->ino() >= MDS_INO_BASE)
    parent = get_subtree_root(dir->get_parent_dir());
  
  if (parent != dir &&                              // we have a parent,
      parent->dir_auth == dir->dir_auth &&          // auth matches,
      dir->dir_auth.second == CDIR_AUTH_UNKNOWN &&  // auth is unambiguous,
      !dir->state_test(CDir::STATE_EXPORTBOUND)) {  // not an exportbound,
    // merge with parent.
    dout(10) << "  subtree merge at " << *dir << endl;
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
	  << " on " << *dir
	  << " bounds " << bounds
	  << endl;

  show_subtrees();

  CDir *root;
  if (dir->ino() < MDS_INO_BASE) {
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

void MDCache::adjust_subtree_after_rename(CInode *diri, CDir *olddir)
{
  dout(10) << "adjust_subtree_after_rename " << *diri << " from " << *olddir << endl;

  //show_subtrees();

  list<CDir*> dfls;
  diri->get_dirfrags(dfls);
  for (list<CDir*>::iterator p = dfls.begin(); p != dfls.end(); ++p) {
    CDir *dir = *p;
    dout(10) << "dirfrag " << *dir << endl;
    CDir *oldparent = get_subtree_root(olddir);
    dout(10) << " old parent " << *oldparent << endl;
    CDir *newparent = get_subtree_root(diri->get_parent_dir());
    dout(10) << " new parent " << *newparent << endl;

    if (oldparent == newparent) {
      dout(10) << "parent unchanged for " << *dir << " at " << *oldparent << endl;
      continue;
    }

    if (dir->is_subtree_root()) {
      // children are fine.  change parent.
      dout(10) << "moving " << *dir << " from " << *oldparent << " to " << *newparent << endl;
      assert(subtrees[oldparent].count(dir));
      subtrees[oldparent].erase(dir);
      assert(subtrees.count(newparent));
      subtrees[newparent].insert(dir);
    } else {
      // mid-subtree.

      // see if any old bounds move to the new parent.
      list<CDir*> tomove;
      for (set<CDir*>::iterator p = subtrees[oldparent].begin();
	   p != subtrees[oldparent].end();
	   ++p) {
	CDir *bound = *p;
	CDir *broot = get_subtree_root(bound->get_parent_dir());
	if (broot != oldparent) {
	  assert(broot == newparent);
	  tomove.push_back(bound);
	}
      }
      for (list<CDir*>::iterator p = tomove.begin(); p != tomove.end(); ++p) {
	CDir *bound = *p;
	dout(10) << "moving bound " << *bound << " from " << *oldparent << " to " << *newparent << endl;
	subtrees[oldparent].erase(bound);
	subtrees[newparent].insert(bound);
      }	   

      // did auth change?
      if (oldparent->authority() != newparent->authority()) 
	adjust_subtree_auth(dir, oldparent->authority());  // caller is responsible for *diri.
    }
  }

  show_subtrees();
}


void MDCache::get_fullauth_subtrees(set<CDir*>& s)
{
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_full_dir_auth())
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
    if (root->is_full_dir_auth())
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
    if (root->is_full_dir_nonauth())
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


class C_MDC_SendImportMap : public Context {
  MDCache *mdc;
  int who;
public:
  C_MDC_SendImportMap(MDCache *c, int w) : mdc(c), who(w) { }
  void finish(int r) {
    mdc->send_import_map_now(who);
  }
};

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
  

  // list prepare requests lacking a commit
  // [active survivor]
  for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
       p != active_requests.end();
       ++p) {
    if (p->second->is_slave() && p->second->slave_to_mds == who) {
      dout(10) << " including uncommitted " << *p->second << endl;
      m->add_slave_request(p->first);
    }
  }
  // [resolving]
  if (uncommitted_slave_updates.count(who)) {
    for (map<metareqid_t, EMetaBlob>::iterator p = uncommitted_slave_updates[who].begin();
	 p != uncommitted_slave_updates[who].end();
	 ++p) {
      dout(10) << " including uncommitted " << p->first << endl;
      m->add_slave_request(p->first);
    }
    need_resolve_ack.insert(who);
  }


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
  migrator->handle_mds_failure_or_stop(who);

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
      mds->queue_waiters(waiters);
      dout(10) << "kicking WAIT_DIR on " << *in << endl;
      
      // remove from mds list
      p->second.erase(who);
      if (p->second.empty())
	dir_discovers.erase(p);
    }
    p = n;
  }

  // clean up any requests slave to/from this node
  list<MDRequest*> finish;
  for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
       p != active_requests.end();
       ++p) {
    // slave to the failed node?
    if (p->second->slave_to_mds == who) {
      if (p->second->slave_did_prepare()) {
	dout(10) << " slave request " << *p->second << " uncommitted, will resolve shortly" << endl;
      } else {
	dout(10) << " slave request " << *p->second << " has no prepare, finishing up" << endl;
	if (p->second->slave_request)
	  p->second->aborted = true;
	else
	  finish.push_back(p->second);
      }
    }
    
    // failed node is slave?
    if (!p->second->committing) {
      if (p->second->witnessed.count(who)) {
	dout(10) << " master request " << *p->second << " no longer witnessed by slave mds" << who
		 << endl;
	// discard this peer's prepare (if any)
	p->second->witnessed.erase(who);
      }
      
      if (p->second->waiting_on_slave.count(who)) {
	dout(10) << " master request " << *p->second << " waiting for slave mds" << who
		 << " to recover" << endl;
	// retry request when peer recovers
	p->second->waiting_on_slave.erase(who);
	mds->wait_for_active_peer(who, new C_MDS_RetryRequest(this, p->second));
      }
    }
  }

  while (!finish.empty()) {
    dout(10) << "cleaning up slave request " << *finish.front() << endl;
    request_finish(finish.front());
    finish.pop_front();
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
  mds->queue_waiters(waiters);
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

  // ambiguous slave requests?
  if (!m->slave_requests.empty()) {
    MMDSResolveAck *ack = new MMDSResolveAck;

    for (list<metareqid_t>::iterator p = m->slave_requests.begin();
	 p != m->slave_requests.end();
	 ++p) {
      if (mds->clientmap.have_completed_request(*p)) {
	// COMMIT
	dout(10) << " ambiguous slave request " << *p << " will COMMIT" << endl;
	ack->add_commit(*p);
      } else {
	// ABORT
	dout(10) << " ambiguous slave request " << *p << " will ABORT" << endl;
	ack->add_abort(*p);      
      }
    }

    mds->send_message_mds(ack, from, MDS_PORT_CACHE);
  }

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
	if (dir->is_ambiguous_dir_auth()) {
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


  // resolving?
  if (mds->is_resolve()) {
    // note ambiguous imports too
    for (map<dirfrag_t, list<dirfrag_t> >::iterator pi = m->ambiguous_imap.begin();
	 pi != m->ambiguous_imap.end();
	 ++pi) {
      dout(10) << "noting ambiguous import on " << pi->first << " bounds " << pi->second << endl;
      other_ambiguous_imports[from][pi->first].swap( pi->second );
    }

    // did i get them all?
    got_import_map.insert(from);

    maybe_resolve_finish();
  }

  delete m;
}

void MDCache::maybe_resolve_finish()
{
  if (got_import_map != recovery_set) {
    dout(10) << "still waiting for more importmaps, got " << got_import_map 
	     << ", need " << recovery_set << endl;
  } 
  else if (!need_resolve_ack.empty()) {
    dout(10) << "still waiting for resolve_ack from " << need_resolve_ack << endl;
  } 
  else {
    dout(10) << "got all import maps, resolve_acks, done resolving subtrees" << endl;
    disambiguate_imports();
    recalc_auth_bits();
    trim_non_auth(); 
    
    // reconnect clients
    mds->set_want_state(MDSMap::STATE_RECONNECT);
  } 
}

void MDCache::handle_resolve_ack(MMDSResolveAck *ack)
{
  dout(10) << "handle_resolve_ack " << *ack << " from " << ack->get_source() << endl;
  int from = ack->get_source().num();

  for (list<metareqid_t>::iterator p = ack->commit.begin();
       p != ack->commit.end();
       ++p) {
    dout(10) << " commit on slave " << *p << endl;
    
    if (mds->is_resolve()) {
      // replay
      assert(uncommitted_slave_updates[from].count(*p));
      uncommitted_slave_updates[from][*p].replay(mds);
      uncommitted_slave_updates[from].erase(*p);
      // log commit
      mds->mdlog->submit_entry(new ESlaveUpdate("unknown", *p, from, ESlaveUpdate::OP_COMMIT));
    } else {
      MDRequest *mdr = request_get(*p);
      assert(mdr->slave_request == 0);  // shouldn't be doing anything!
      request_finish(mdr);
    }
  }

  for (list<metareqid_t>::iterator p = ack->abort.begin();
       p != ack->abort.end();
       ++p) {
    dout(10) << " abort on slave " << *p << endl;

    if (mds->is_resolve()) {
      assert(uncommitted_slave_updates[from].count(*p));
      uncommitted_slave_updates[from].erase(*p);
      mds->mdlog->submit_entry(new ESlaveUpdate("unknown", *p, from, ESlaveUpdate::OP_ABORT));
    } else {
      MDRequest *mdr = request_get(*p);
      if (mdr->slave_commit) {
	mdr->slave_commit->finish(-1);
	delete mdr->slave_commit;
	mdr->slave_commit = 0;
      }
      if (mdr->slave_request) 
	mdr->aborted = true;
      else
	request_finish(mdr);
    }
  }

  need_resolve_ack.erase(from);

  if (mds->is_resolve()) 
    maybe_resolve_finish();

  delete ack;
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
      mds->mdlog->submit_entry(new EImportFinish(dir, false));
    } else {
      dout(10) << "ambiguous import auth unknown, must be me " << *dir << endl;
      finish_ambiguous_import(q->first);
      mds->mdlog->submit_entry(new EImportFinish(dir, true));
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


/** recalc_auth_bits()
 * once subtree auth is disambiguated, we need to adjust all the 
 * auth and dirty bits in our cache before moving on.
 */
void MDCache::recalc_auth_bits()
{
  dout(7) << "recalc_auth_bits" << endl;

  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    list<CDir*> dfq;  // dirfrag queue
    dfq.push_back(p->first);

    bool auth = p->first->authority().first == mds->get_nodeid();
    dout(10) << " subtree auth=" << auth << " for " << *p->first << endl;

    while (!dfq.empty()) {
      CDir *dir = dfq.front();
      dfq.pop_front();

      // dir
      if (auth) 
	dir->state_set(CDir::STATE_AUTH);
      else {
	dir->state_set(CDir::STATE_REJOINING);
	dir->state_clear(CDir::STATE_AUTH);
	if (dir->is_dirty()) 
	  dir->mark_clean();
      }

      // dentries in this dir
      for (map<string,CDentry*>::iterator q = dir->items.begin();
	   q != dir->items.end();
	   ++q) {
	// dn
	CDentry *dn = q->second;
	if (auth)
	  dn->state_set(CDentry::STATE_AUTH);
	else {
	  dn->state_set(CDentry::STATE_REJOINING);
	  dn->state_clear(CDentry::STATE_AUTH);
	  if (dn->is_dirty()) 
	    dn->mark_clean();
	}

	if (dn->is_primary()) {
	  // inode
	  if (auth) 
	    dn->inode->state_set(CInode::STATE_AUTH);
	  else {
	    dn->inode->state_set(CInode::STATE_REJOINING);
	    dn->inode->state_clear(CInode::STATE_AUTH);
	    if (dn->inode->is_dirty())
	      dn->inode->mark_clean();
	  }

	  // recurse?
	  if (dn->inode->is_dir()) 
	    dn->inode->get_nested_dirfrags(dfq);
	}
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
 * if we are rejoin, send for all regions in our cache.
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
      rejoins[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_REJOIN);
  }	

  assert(!migrator->is_importing());
  assert(!migrator->is_exporting());
  
  // check all subtrees
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    assert(dir->is_subtree_root());
    assert(!dir->is_ambiguous_dir_auth());

    int auth = dir->get_dir_auth().first;
    assert(auth >= 0);
    
    if (auth == mds->get_nodeid()) continue;  // skip my own regions!
    if (rejoins.count(auth) == 0) continue;   // don't care about this node's regions

    cache_rejoin_walk(dir, rejoins[auth]);
  }

  // note request authpins, xlocks
  for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
       p != active_requests.end();
       ++p) {
    // auth pins
    for (set<MDSCacheObject*>::iterator q = p->second->auth_pins.begin();
	 q != p->second->auth_pins.end();
	 ++q) {
      if (!(*q)->is_auth()) {
	int who = (*q)->authority().first;
	if (rejoins.count(who) == 0) continue;
	MMDSCacheRejoin *rejoin = rejoins[who];

	dout(15) << " " << *p->second << " authpin on " << **q << endl;
	MDSCacheObjectInfo i;
	(*q)->set_object_info(i);
	if (i.ino)
	  rejoin->add_inode_authpin(i.ino, p->second->reqid);
	else
	  rejoin->add_dentry_authpin(i.dirfrag, i.dname, p->second->reqid);
      }
    }
    // xlocks
    for (set<SimpleLock*>::iterator q = p->second->xlocks.begin();
	 q != p->second->xlocks.end();
	 ++q) {
      if (!(*q)->get_parent()->is_auth()) {
	int who = (*q)->get_parent()->authority().first;
	if (rejoins.count(who) == 0) continue;
	MMDSCacheRejoin *rejoin = rejoins[who];

	dout(15) << " " << *p->second << " xlock on " << **q << " " << *(*q)->get_parent() << endl;
	MDSCacheObjectInfo i;
	(*q)->get_parent()->set_object_info(i);
	if (i.ino)
	  rejoin->add_inode_xlock(i.ino, (*q)->get_type(), p->second->reqid);
	else
	  rejoin->add_dentry_xlock(i.dirfrag, i.dname, p->second->reqid);
      }
    }
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
  if (mds->is_rejoin() && rejoins.empty()) {
    dout(10) << "nothing to rejoin, going active" << endl;
    mds->set_want_state(MDSMap::STATE_ACTIVE);
  }
}


void MDCache::cache_rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin)
{
  dout(10) << "cache_rejoin_walk " << *dir << endl;

  //if (mds->is_rejoin()) 
  rejoin->add_weak_dirfrag(dir->dirfrag());
  //else
  //rejoin->add_strong_dirfrag(dir->dirfrag(), dir->get_replica_nonce());
  
  list<CDir*> nested;  // finish this dir, then do nested items
  
  // walk dentries
  for (map<string,CDentry*>::iterator p = dir->items.begin();
       p != dir->items.end();
       ++p) {
    // dentry
    CDentry *dn = p->second;
    if (mds->is_rejoin())
      rejoin->add_weak_dentry(dir->dirfrag(), p->first);
    else {
      rejoin->add_strong_dentry(dir->dirfrag(), p->first,
				dn->get_replica_nonce(),
				dn->lock.get_state());
    }
    
    // inode?
    if (dn->is_primary() && dn->get_inode()) {
      CInode *in = dn->get_inode();
      if (mds->is_rejoin() && in->get_caps_wanted() == 0)
	rejoin->add_weak_inode(in->ino());
      else {
	rejoin->add_strong_inode(in->ino(), in->get_replica_nonce(),
				 in->get_caps_wanted(),
				 in->authlock.get_state(),
				 in->linklock.get_state(),
				 in->dirfragtreelock.get_state(),
				 in->filelock.get_state(),
				 in->dirlock.get_state());
      }
      
      // dirfrags in this subtree?
      list<CDir*> dfs;
      in->get_dirfrags(dfs);
      for (list<CDir*>::iterator p = dfs.begin();
	   p != dfs.end();
	   ++p) 
	if (!(*p)->is_subtree_root())
	  nested.push_back(*p);
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
 *  - reply with the lockstate
 *
 * if i am active|stopping, 
 *  - remove source from replica list for everything not referenced here.
 */
void MDCache::handle_cache_rejoin(MMDSCacheRejoin *m)
{
  dout(7) << "handle_cache_rejoin " << *m << " from " << m->get_source() << endl;

  switch (m->op) {
  case MMDSCacheRejoin::OP_REJOIN:
    handle_cache_rejoin_rejoin(m);
    break;

  case MMDSCacheRejoin::OP_ACK:
    handle_cache_rejoin_ack(m);
    break;

  case MMDSCacheRejoin::OP_MISSING:
    handle_cache_rejoin_missing(m);
    break;

  case MMDSCacheRejoin::OP_FULL:
    handle_cache_rejoin_full(m);
    break;

  default: 
    assert(0);
  }
  delete m;
}

void MDCache::handle_cache_rejoin_rejoin(MMDSCacheRejoin *m)
{
  int from = m->get_source().num();

  // do immediate ack?
  MMDSCacheRejoin *ack = 0;
  MMDSCacheRejoin *missing = 0;

  if (mds->is_active() || mds->is_stopping()) {
    dout(10) << "i am active.  removing stale cache replicas" << endl;
    
    // first, scour cache of unmentioned replica references
    for (hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
	 p != inode_map.end();
	 ++p) {
      // inode
      CInode *in = p->second;
      if (in->is_replica(from) && m->weak_inodes.count(p->first) == 0) {
	inode_remove_replica(in, from);
	dout(10) << " rem " << *in << endl;
      }

      // dentry
      if (in->parent) {
	CDentry *dn = in->parent;
	if (dn->is_replica(from) &&
	    (m->weak_dentries.count(dn->get_dir()->dirfrag()) == 0 ||
	     m->weak_dentries[dn->get_dir()->dirfrag()].count(dn->get_name()) == 0)) {
	  dentry_remove_replica(dn, from);
	  dout(10) << " rem " << *dn << endl;
	}
      }

      // dir
      list<CDir*> dfs;
      in->get_dirfrags(dfs);
      for (list<CDir*>::iterator p = dfs.begin();
	   p != dfs.end();
	   ++p) {
	CDir *dir = *p;
	if (dir->is_replica(from) && m->weak_dirfrags.count(dir->dirfrag()) == 0) {
	  dir->remove_replica(from);
	  dout(10) << " rem " << *dir << endl;
	}
      }
    }
    
    // do immediate ack.
    ack = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);
  }
  
  // dirs
  for (set<dirfrag_t>::iterator p = m->weak_dirfrags.begin();
       p != m->weak_dirfrags.end();
       ++p) {
    CDir *dir = get_dirfrag(*p);
    if (dir) {
      int nonce = dir->add_replica(from);
      dout(10) << " have " << *dir << endl;
      if (ack) 
	ack->add_strong_dirfrag(*p, nonce);
    
      // dentries
      for (set<string>::iterator q = m->weak_dentries[*p].begin();
	   q != m->weak_dentries[*p].end();
	   ++q) {
	CDentry *dn = dir->lookup(*q);
	if (dn) {
	  int nonce = dn->add_replica(from);
	  dout(10) << " have " << *dn << endl;
	  if (ack)
	    ack->add_strong_dentry(*p, *q, dn->lock.get_state(), nonce);
	} else {
	  dout(10) << " missing " << *p << " " << *q << endl;
	  if (!missing) missing = new MMDSCacheRejoin(MMDSCacheRejoin::OP_MISSING);
	  missing->add_weak_dentry(*p, *q);
	}
	if (ack)
	  ack->add_strong_dentry(*p, *q, nonce, dn->lock.get_state());
      }
    } else {
      dout(10) << " missing " << *p << endl;
      if (!missing) missing = new MMDSCacheRejoin(MMDSCacheRejoin::OP_MISSING);
      missing->add_weak_dirfrag(*p);
      
      // dentries
      for (set<string>::iterator q = m->weak_dentries[*p].begin();
	   q != m->weak_dentries[*p].end();
	   ++q) 
	missing->add_weak_dentry(*p, *q);
    }
  }

  // inodes
  for (set<inodeno_t>::iterator p = m->weak_inodes.begin();
       p != m->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    if (in) {
      int nonce = in->add_replica(from);
      in->mds_caps_wanted.erase(from);
      dout(10) << " have (weak) " << *in << endl;
      if (ack) {
	in->authlock.remove_gather(from);
	in->linklock.remove_gather(from);
	in->dirfragtreelock.remove_gather(from);
	in->filelock.remove_gather(from);
	in->dirlock.remove_gather(from);
	ack->add_strong_inode(in->ino(), 
			      nonce,
			      0,
			      in->authlock.get_replica_state(), 
			      in->linklock.get_replica_state(), 
			      in->dirfragtreelock.get_replica_state(), 
			      in->filelock.get_replica_state(),
			      in->dirlock.get_replica_state());
      }
    } else {
      dout(10) << " missing " << *p << endl;
      if (!missing) missing = new MMDSCacheRejoin(MMDSCacheRejoin::OP_MISSING);
      missing->add_weak_inode(*p);
    }
  }

  // strong inodes too?
  for (map<inodeno_t, MMDSCacheRejoin::inode_strong>::iterator p = m->strong_inodes.begin();
       p != m->strong_inodes.end();
       ++p) {
    CInode *in = get_inode(p->first);
    if (in) {
      int nonce = in->add_replica(from);
      if (p->second.caps_wanted)
	in->mds_caps_wanted[from] = p->second.caps_wanted;
      else
	in->mds_caps_wanted.erase(from);
      dout(10) << " have (strong) " << *in << endl;
      if (ack) {
	// i had inode, just tell replica the correct state
	in->authlock.remove_gather(from);
	in->linklock.remove_gather(from);
	in->dirfragtreelock.remove_gather(from);
	in->filelock.remove_gather(from);
	in->dirlock.remove_gather(from);
	ack->add_strong_inode(in->ino(), 
			      nonce,
			      0,
			      in->authlock.get_replica_state(), 
			      in->linklock.get_replica_state(), 
			      in->dirfragtreelock.get_replica_state(), 
			      in->filelock.get_replica_state(),
			      in->dirlock.get_replica_state());
      } else {
	// take note of replica state values.
	// SimpleLock -- 
	//  we can ignore; locked replicas can be safely changed to sync.
	// FileLock --
	//  we can also ignore.  
	//  replicas will at most issue RDCACHE|RD, which is covered by the default SYNC,
	//  so only _locally_ opened files are significant.
	// ScatterLock -- adjust accordingly
	if (p->second.dirlock == LOCK_SCATTER || 
	    p->second.dirlock == LOCK_GLOCKC)  // replica still has wrlocks
	  in->dirlock.set_state(LOCK_SCATTER);
      }
    } else {
      dout(10) << " missing " << p->first << endl;
      if (!missing) missing = new MMDSCacheRejoin(MMDSCacheRejoin::OP_MISSING);
      missing->add_weak_inode(p->first);
    }
  }
  
  // xlocks
  for (map<inodeno_t, map<int, metareqid_t> >::iterator p = m->xlocked_inodes.begin();
       p != m->xlocked_inodes.end();
       ++p) {
    for (map<int, metareqid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      CInode *in = get_inode(p->first);
      if (!in) continue;  // already missing, from strong_inodes list above.
      
      dout(10) << " inode xlock by " << q->second << " on " << *in << endl;
      
      // create slave mdrequest
      MDRequest *mdr = request_start_slave(q->second, from);
      
      // auth_pin
      mdr->auth_pin(in);
      
      // xlock
      SimpleLock *lock = in->get_lock(q->first);
      lock->set_state(LOCK_LOCK);
      lock->get_xlock(mdr);
      mdr->xlocks.insert(lock);
      mdr->locks.insert(lock);
    }
  }
  for (map<dirfrag_t, map<string, metareqid_t> >::iterator p = m->xlocked_dentries.begin();
       p != m->xlocked_dentries.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) continue;  // already missing, from above.
    for (map<string, metareqid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first);
      if (!dn) continue;  // already missing, from above.
      dout(10) << " dn authpin by " << q->second << " on " << *dn << endl;

      // get/create slave mdrequest
      MDRequest *mdr;
      if (have_request(q->second))
	mdr = request_get(q->second);
      else
	mdr = request_start_slave(q->second, from);

      // auth_pin
      mdr->auth_pin(dn);
    }
  }
  for (map<dirfrag_t, map<string, metareqid_t> >::iterator p = m->xlocked_dentries.begin();
       p != m->xlocked_dentries.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) continue;  // already missing, from above.
    for (map<string, metareqid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first);
      if (!dn) continue;  // already missing, from above.
      dout(10) << " dn xlock by " << q->second << " on " << *dn << endl;
   
      // get slave mdrequest (we should have it bc of authpin!)
      MDRequest *mdr = request_get(q->second);
      assert(mdr->is_auth_pinned(dn));

      // xlock
      dn->lock.set_state(LOCK_LOCK);
      dn->lock.get_xlock(mdr);
      mdr->xlocks.insert(&dn->lock);
      mdr->locks.insert(&dn->lock);
    }
  }
  
  // send ack?
  if (ack)
    mds->send_message_mds(ack, from, MDS_PORT_CACHE);
  else
    want_rejoin_ack.insert(from);

  // send missing?
  if (missing)
    mds->send_message_mds(missing, from, MDS_PORT_CACHE);
}


void MDCache::handle_cache_rejoin_ack(MMDSCacheRejoin *m)
{
  dout(7) << "handle_cache_rejoin_ack from " << m->get_source() << endl;
  int from = m->get_source().num();
  
  // dirs
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = m->strong_dirfrags.begin();
       p != m->strong_dirfrags.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    assert(dir);

    dir->set_replica_nonce(p->second.nonce);
    dir->state_clear(CDir::STATE_REJOINING);
    dout(10) << " got " << *dir << endl;

    // dentries
    for (map<string,MMDSCacheRejoin::dn_strong>::iterator q = m->strong_dentries[p->first].begin();
	 q != m->strong_dentries[p->first].end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first);
      assert(dn);
      dn->set_replica_nonce(q->second.nonce);
      dn->lock.set_state(q->second.lock);
      dn->state_clear(CDentry::STATE_REJOINING);
      dout(10) << " got " << *dn << endl;
    }
  }

  // inodes
  for (map<inodeno_t, MMDSCacheRejoin::inode_strong>::iterator p = m->strong_inodes.begin();
       p != m->strong_inodes.end();
       ++p) {
    CInode *in = get_inode(p->first);
    assert(in);
    in->set_replica_nonce(p->second.nonce);
    in->authlock.set_state(p->second.authlock);
    in->linklock.set_state(p->second.linklock);
    in->dirfragtreelock.set_state(p->second.dirfragtreelock);
    in->filelock.set_state(p->second.filelock);
    in->dirlock.set_state(p->second.dirlock);
    in->state_clear(CInode::STATE_REJOINING);
    dout(10) << " got " << *in << endl;
  }

  // done?
  rejoin_ack_gather.erase(from);
  if (mds->is_rejoin() && 
      rejoin_ack_gather.empty()) {
    dout(7) << "all done, going active!" << endl;
    send_cache_rejoin_acks();

    show_subtrees();
    show_cache();
    mds->set_want_state(MDSMap::STATE_ACTIVE);
  } else {
    dout(7) << "still need rejoin_ack from " << rejoin_ack_gather << endl;
  }

}


void MDCache::handle_cache_rejoin_missing(MMDSCacheRejoin *m)
{
  dout(7) << "handle_cache_rejoin_missing from " << m->get_source() << endl;

  MMDSCacheRejoin *full = new MMDSCacheRejoin(MMDSCacheRejoin::OP_FULL);

  // dirs
  for (set<dirfrag_t>::iterator p = m->weak_dirfrags.begin();
       p != m->weak_dirfrags.end();
       ++p) {
    CDir *dir = get_dirfrag(*p);
    if (!dir) {
      dout(10) << " don't have dirfrag " << *p << endl;   
      continue;      // we must have trimmed it after the original rejoin
    }

    dout(10) << " sending " << *dir << endl;
    
    // dentries
    for (set<string>::iterator q = m->weak_dentries[*p].begin();
	 q != m->weak_dentries[*p].end();
	 ++q) {
      CDentry *dn = dir->lookup(*q);
      if (!dn) {
	dout(10) << " don't have dentry " << *q << " in " << *dir << endl;
	continue; // we must have trimmed it after our original rejoin
      }
      dout(10) << " sending " << *dn << endl;
      if (mds->is_rejoin())
	full->add_weak_dentry(*p, *q);
      else
	full->add_strong_dentry(*p, *q, dn->get_replica_nonce(), dn->lock.get_state());
    }
  }
    
  // inodes
  for (set<inodeno_t>::iterator p = m->weak_inodes.begin();
       p != m->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    if (!in) {
      dout(10) << " don't have inode " << *p << endl;
      continue; // we must have trimmed it after the originalo rejoin
    }
    
    dout(10) << " sending " << *in << endl;
    full->add_full_inode(in->inode, in->symlink, in->dirfragtree);
    if (mds->is_rejoin())
      full->add_weak_inode(in->ino());
    else
      full->add_strong_inode(in->ino(),
			     in->get_replica_nonce(),
			     in->get_caps_wanted(),
			     in->authlock.get_replica_state(), 
			     in->linklock.get_replica_state(), 
			     in->dirfragtreelock.get_replica_state(), 
			     in->filelock.get_replica_state(),
			     in->dirlock.get_replica_state());
  }

  mds->send_message_mds(full, m->get_source().num(), MDS_PORT_CACHE);
}

void MDCache::handle_cache_rejoin_full(MMDSCacheRejoin *m)
{
  dout(7) << "handle_cache_rejoin_full from " << m->get_source() << endl;

  
  assert(0); // write me


  delete m;
}

void MDCache::send_cache_rejoin_acks()
{
  dout(7) << "send_cache_rejoin_acks to " << want_rejoin_ack << endl;
  
  assert(mds->is_rejoin());

  /* nope, not necessary, we adjust lock state gradually.
     after we've processed all rejoins, lockstate is legal.
     we just have to do a final _eval-ish thing at the end...

  // calculate proper filelock states
  for (set<CInode*>::iterator p = filelock_replica_readers.begin();
       p != filelock_replica_readers.end();
       ++p) {
    dout(10) << "replica(s) have RD caps on " << *p->first << endl;

    for (set<int>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      if (*q == LOCK_
    }
  }
  */

  // send acks
  map<int,MMDSCacheRejoin*> ack;
  
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin(); 
       p != subtrees.end();
       p++) {
    CDir *dir = p->first;
    if (!dir->is_auth()) continue;
    dout(10) << "subtree " << *dir << endl;
    
    // auth items in this subtree
    list<CDir*> dq;
    dq.push_back(dir);

    while (!dq.empty()) {
      CDir *dir = dq.front();
      dq.pop_front();
      
      // dir
      for (map<int,int>::iterator r = dir->replicas_begin();
	   r != dir->replicas_end();
	   ++r) {
	if (!ack[r->first]) ack[r->first] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);
	ack[r->first]->add_strong_dirfrag(dir->dirfrag(), r->second);
      }
	   
      for (map<string,CDentry*>::iterator q = dir->items.begin();
	   q != dir->items.end();
	   ++q) {
	CDentry *dn = q->second;

	// dentry
	for (map<int,int>::iterator r = dn->replicas_begin();
	     r != dn->replicas_end();
	     ++r) {
	  //if (!ack[r->first]) ack[r->first] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);
	  ack[r->first]->add_strong_dentry(dir->dirfrag(), dn->name, r->second,
					   dn->lock.get_replica_state());
	}
	
	if (!dn->is_primary()) continue;

	// inode
	CInode *in = dn->inode;
	
	// twiddle filelock at all?
	// hmm.

	for (map<int,int>::iterator r = in->replicas_begin();
	     r != in->replicas_end();
	     ++r) {
	  //if (!ack[r->first]) ack[r->first] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);
	  ack[r->first]->add_strong_inode(in->ino(), r->second, 0,
					  in->authlock.get_replica_state(),
					  in->linklock.get_replica_state(),
					  in->dirfragtreelock.get_replica_state(),
					  in->filelock.get_replica_state(),
					  in->dirlock.get_replica_state());
	}
	
	// subdirs in this subtree?
	in->get_nested_dirfrags(dq);
      }
    }
  }

  // send acks
  for (map<int,MMDSCacheRejoin*>::iterator p = ack.begin();
       p != ack.end();
       ++p) 
    mds->send_message_mds(p->second, p->first, MDS_PORT_CACHE);
  
}



// ===============================================================================

/*
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
*/


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
  off_t newsize;
public:
  C_MDC_PurgeFinish(MDCache *c, inodeno_t i, off_t s) : mdc(c), ino(i), newsize(s) {}
  void finish(int r) {
    mdc->purge_inode_finish(ino, newsize);
  }
};
class C_MDC_PurgeFinish2 : public Context {
  MDCache *mdc;
  inodeno_t ino;
  off_t newsize;
public:
  C_MDC_PurgeFinish2(MDCache *c, inodeno_t i, off_t s) : mdc(c), ino(i), newsize(s) {}
  void finish(int r) {
    mdc->purge_inode_finish_2(ino, newsize);
  }
};

/* purge_inode in
 * will be called by on unlink or rmdir or truncate
 * caller responsible for journaling an appropriate EUpdate
 */
void MDCache::purge_inode(inode_t *inode, off_t newsize)
{
  dout(10) << "purge_inode " << inode->ino << " size " << inode->size 
	   << " -> " << newsize
	   << endl;

  // take note
  assert(purging[inode->ino].count(newsize) == 0);
  purging[inode->ino][newsize] = *inode;

  assert(inode->size > newsize);

  // remove
  mds->filer->remove(*inode, newsize, inode->size,
		     0, new C_MDC_PurgeFinish(this, inode->ino, newsize));

  /*} else {
    // no need, empty file, just log it
    purge_inode_finish(inode->ino, newsize);
  }
  */
}

void MDCache::purge_inode_finish(inodeno_t ino, off_t newsize)
{
  dout(10) << "purge_inode_finish " << ino << " to " << newsize
	   << " - logging our completion" << endl;
  
  // log completion
  mds->mdlog->submit_entry(new EPurgeFinish(ino, newsize),
			   new C_MDC_PurgeFinish2(this, ino, newsize));
}

void MDCache::purge_inode_finish_2(inodeno_t ino, off_t newsize)
{
  dout(10) << "purge_inode_finish_2 " << ino << " to " << newsize << endl;

  // remove from purging list
  purging[ino].erase(newsize);
  if (purging[ino].empty())
    purging.erase(ino);
  
  // tell anyone who cares (log flusher?)
  list<Context*> ls;
  ls.swap(waiting_for_purge[ino][newsize]);
  waiting_for_purge[ino].erase(newsize);
  if (waiting_for_purge[ino].empty())
    waiting_for_purge.erase(ino);
  finish_contexts(ls, 0);
}

void MDCache::add_recovered_purge(const inode_t& inode, off_t newsize)
{
  assert(purging[inode.ino].count(newsize) == 0);
  purging[inode.ino][newsize] = inode;
}

void MDCache::remove_recovered_purge(inodeno_t ino, off_t newsize)
{
  purging[ino].erase(newsize);
}

void MDCache::start_recovered_purges()
{
  dout(10) << "start_recovered_purges (" << purging.size() << " purges)" << endl;

  for (map<inodeno_t, map<off_t,inode_t> >::iterator p = purging.begin();
       p != purging.end();
       ++p) {
    for (map<off_t,inode_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << "start_recovered_purges " << p->first
	       << " size " << q->second.size
	       << " to " << q->first << endl;
      mds->filer->remove(q->second, q->first, q->second.size,
			 0, new C_MDC_PurgeFinish(this, p->first, q->first));
    }
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
    trim_dentry(dn, expiremap);
  }

  // trim root inode+dir?
  if (max == 0 &&  // only if we're trimming everything!
      lru.lru_get_size() == 0) {
    hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
    while (p != inode_map.end()) {
      hash_map<inodeno_t,CInode*>::iterator n = p;
      n++;
      
      CInode *in = p->second;

      list<CDir*> ls;
      in->get_dirfrags(ls);
      for (list<CDir*>::iterator q = ls.begin();
	   q != ls.end();
	   ++q) 
	if ((*q)->get_num_ref() == 0) 
	  trim_dirfrag(*q, *q, expiremap);
      
      // root inode?
      if (in->get_num_ref() == 0)
	trim_inode(0, in, 0, expiremap);    // hrm, FIXME
      
      p = n;
    } 
  }

  // send!
  send_expire_messages(expiremap);

  return true;
}

void MDCache::send_expire_messages(map<int, MCacheExpire*>& expiremap)
{
  // send expires
  for (map<int, MCacheExpire*>::iterator it = expiremap.begin();
       it != expiremap.end();
       it++) {
    dout(7) << "sending cache_expire to " << it->first << endl;
    mds->send_message_mds(it->second, it->first, MDS_PORT_CACHE);
  }
}


void MDCache::trim_dentry(CDentry *dn, map<int, MCacheExpire*>& expiremap)
{
  dout(12) << "trim_dentry " << *dn << endl;

  CDir *dir = dn->get_dir();
  assert(dir);
  
  CDir *con = get_subtree_root(dir);
  assert(con);
  
  dout(12) << " in container " << *con << endl;

  // notify dentry authority?
  if (!dn->is_auth()) {
    pair<int,int> auth = dn->authority();
    
    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.
      
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
    trim_inode(dn, in, con, expiremap);
  } 
  else {
    assert(dn->is_null());
  }
  
  // adjust the dir state
    // NOTE: we can safely remove a clean, null dentry without effecting
    //       directory completeness.
    if (!(dn->is_null() && dn->is_clean())) 
      dir->state_clear(CDir::STATE_COMPLETE); 

    // remove dentry
    dir->remove_dentry(dn);

    // reexport?
    if (dir->get_size() == 0 && dir->is_subtree_root())
      migrator->export_empty_import(dir);
    
    if (mds->logger) mds->logger->inc("cex");
}


void MDCache::trim_dirfrag(CDir *dir, CDir *con, map<int, MCacheExpire*>& expiremap)
{
  assert(dir->get_num_ref() == 0);

  dout(15) << "trim_dirfrag " << *dir << endl;

  CInode *in = dir->get_inode();

  if (!dir->is_auth()) {
    pair<int,int> auth = dir->authority();
    
    // was this an auth delegation?  (if so, slightly modified container)
    dirfrag_t condf;
    if (dir->is_subtree_root()) {
      dout(12) << " subtree root, container is " << *dir << endl;
      con = dir;
      condf = dir->dirfrag();
    } else {
      condf = con->dirfrag();
    }
      
    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds" << a << " on   " << *dir << endl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_dir(condf, dir->dirfrag(), dir->replica_nonce);
    }
  }
  
  if (dir->is_subtree_root())
    remove_subtree(dir);	// remove from subtree map
  in->close_dirfrag(dir->dirfrag().frag);
}

void MDCache::trim_inode(CDentry *dn, CInode *in, CDir *con, map<int, MCacheExpire*>& expiremap)
{
  dout(15) << "trim_inode " << *in << endl;
  assert(in->get_num_ref() == 0);
    
  // DIR
  list<CDir*> dfls;
  in->get_dirfrags(dfls);
  for (list<CDir*>::iterator p = dfls.begin();
       p != dfls.end();
       ++p) 
    trim_dirfrag(*p, con ? con:*p, expiremap);  // if no container (e.g. root dirfrag), use *p
  
  // INODE
  if (!in->is_auth()) {
    pair<int,int> auth = in->authority();
    
    dirfrag_t df;
    if (con)
      df = con->dirfrag();
    else
      df = dirfrag_t(1,frag_t());

    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (con && mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds" << a << " on " << *in << endl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_inode(df, in->ino(), in->get_replica_nonce());
    }
  }
    
  // unlink
  if (dn)
    dn->get_dir()->unlink_inode(dn);
  remove_inode(in);
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
    // root, stray, etc.?
    hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
    while (p != inode_map.end()) {
      hash_map<inodeno_t,CInode*>::iterator next = p;
      ++next;
      CInode *in = p->second;
      if (!in->is_auth()) {
	list<CDir*> ls;
	in->get_dirfrags(ls);
	for (list<CDir*>::iterator p = ls.begin();
	     p != ls.end();
	     ++p) {
	  assert((*p)->get_num_ref() == 0);
	  remove_subtree((*p));
	  in->close_dirfrag((*p)->dirfrag().frag);
	}
	assert(in->get_num_ref() == 0);
	remove_inode(in);
      }
      p = next;
    }
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

    if (!con->is_auth() ||
	(con->is_auth() && con->is_exporting() &&
	 migrator->get_export_state(con) == Migrator::EXPORT_WARNING &&
	 migrator->export_has_warned(con,from))) {
      // not auth.
      dout(7) << "delaying nonauth|warned expires for " << *con << endl;
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
	  dentry_remove_replica(dn, from);
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
  if (in->authlock.remove_replica(from))
    mds->locker->simple_eval(&in->authlock);
  if (in->linklock.remove_replica(from))
    mds->locker->simple_eval(&in->linklock);
  if (in->dirfragtreelock.remove_replica(from))
    mds->locker->simple_eval(&in->dirfragtreelock);
  if (in->filelock.remove_replica(from))
    mds->locker->simple_eval(&in->filelock);
  
  // alone now?
  if (!in->is_replicated()) {
    mds->locker->simple_eval(&in->authlock);
    mds->locker->simple_eval(&in->linklock);
    mds->locker->simple_eval(&in->dirfragtreelock);
    mds->locker->file_eval(&in->filelock);
    mds->locker->scatter_eval(&in->dirlock);
  }
}

void MDCache::dentry_remove_replica(CDentry *dn, int from)
{
  dn->remove_replica(from);

  // fix lock
  if (dn->lock.remove_replica(from) ||
      !dn->is_replicated())
    mds->locker->simple_eval(&dn->lock);
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

  if (mds->is_out()) {
    dout(7) << " already shut down" << endl;
    show_cache();
    show_subtrees();
    return true;
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
	if (!in->is_dir()) continue;
        
        // commit any dirty dirfrag that's ours
	list<CDir*> dfs;
	in->get_dirfrags(dfs);
	for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
	  CDir *dir = *p;
	  if (dir->is_auth() && dir->is_dirty()) {
	    dir->commit(0, new C_MDC_ShutdownCommit(this));
	    shutdown_commits++;
	  }
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

  // flush batching eopens, so that we can properly expire them.
  mds->server->journal_opens();    // hrm, this is sort of a hack.

  // flush what we can from the log
  mds->mdlog->trim(0);

  // SUBTREES
  // send all imports back to 0.
  if (!subtrees.empty() &&
      mds->get_nodeid() != 0 && 
      !migrator->is_exporting() //&&
      //!migrator->is_importing()
      ) {
    // export to root
    dout(7) << "looking for subtrees to export to mds0" << endl;
    list<CDir*> ls;
    for (map<CDir*, set<CDir*> >::iterator it = subtrees.begin();
         it != subtrees.end();
         it++) {
      CDir *dir = it->first;
      if (dir->get_inode()->is_stray()) continue;
      if (dir->is_frozen() || dir->is_freezing()) continue;
      if (!dir->is_full_dir_auth()) continue;
      ls.push_back(dir);
    }
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
      CDir *dir = *p;
      dout(7) << "sending " << *dir << " back to mds0" << endl;
      migrator->export_dir(dir, 0);
    }
  }

  // subtrees map not empty yet?
  if (!subtrees.empty()) {
    dout(7) << "still have " << num_subtrees() << " subtrees" << endl;
    show_subtrees();
    migrator->show_importing();
    migrator->show_exporting();
    //show_cache();
    return false;
  }
  assert(subtrees.empty());
  assert(!migrator->is_exporting());
  assert(!migrator->is_importing());


  // empty out stray contents
  // FIXME
  dout(7) << "FIXME: i need to empty out stray dir contents..." << endl;

  // (wait for) flush log?
  if (g_conf.mds_log_flush_on_shutdown) {
    if (mds->mdlog->get_non_importmap_events()) {
      dout(7) << "waiting for log to flush .. " << mds->mdlog->get_num_events() 
	      << " (" << mds->mdlog->get_non_importmap_events() << ")" << endl;
      return false;
    } 
  }

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

  case MSG_MDS_RESOLVEACK:
    handle_resolve_ack((MMDSResolveAck*)m);
    break;

  case MSG_MDS_CACHEREJOIN:
    handle_cache_rejoin((MMDSCacheRejoin*)m);
    break;
    /*
  case MSG_MDS_CACHEREJOINACK:
    handle_cache_rejoin_ack((MMDSCacheRejoinAck*)m);
    break;
    */


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
 *   <0 : traverse error (ENOTDIR, ENOENT, etc.)
 *    0 : success
 *   >0 : delayed or forwarded
 *
 * onfail values:
 *
 *  MDS_TRAVERSE_FORWARD       - forward to auth (or best guess)
 *  MDS_TRAVERSE_DISCOVER      - discover missing items.  skip permission checks.
 *  MDS_TRAVERSE_DISCOVERXLOCK - discover XLOCKED items too (be careful!).
 *  MDS_TRAVERSE_FAIL          - return an error
 */

Context *MDCache::_get_waiter(MDRequest *mdr, Message *req)
{
  if (mdr)
    return new C_MDS_RetryRequest(this, mdr);
  else
    return new C_MDS_RetryMessage(mds, req);
}

int MDCache::path_traverse(MDRequest *mdr, Message *req,     // who
			   CInode *base, filepath& origpath, // what
                           vector<CDentry*>& trace,          // result
                           bool follow_trailing_symlink,     // how
                           int onfail)
{
  assert(mdr || req);
  bool null_okay = onfail == MDS_TRAVERSE_DISCOVERXLOCK;
  bool noperm = false;
  if (onfail == MDS_TRAVERSE_DISCOVER ||
      onfail == MDS_TRAVERSE_DISCOVERXLOCK) 
    noperm = true;

  // keep a list of symlinks we touch to avoid loops
  set< pair<CInode*, string> > symlinks_resolved; 

  // root
  CInode *cur = base;
  if (!cur) cur = get_root();
  if (cur == NULL) {
    dout(7) << "traverse: i don't have root" << endl;
    open_root(_get_waiter(mdr, req));
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
      return -ENOTDIR;
    }

    // open dir
    frag_t fg = cur->pick_dirfrag(path[depth]);
    CDir *curdir = cur->get_dirfrag(fg);
    if (!curdir) {
      if (cur->is_auth()) {
        // parent dir frozen_dir?
        if (cur->is_frozen_dir()) {
          dout(7) << "traverse: " << *cur->get_parent_dir() << " is frozen_dir, waiting" << endl;
          cur->get_parent_dir()->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req));
          return 1;
        }

        curdir = cur->get_or_open_dirfrag(this, fg);
      } else {
        // discover?
        assert(!cur->is_auth());
        if (cur->is_ambiguous_auth()) {
	  dout(10) << "traverse: need dir, waiting for single auth on " << *cur << endl;
	  cur->add_waiter(CInode::WAIT_SINGLEAUTH, _get_waiter(mdr, req));
	  return 1;
	} else if (dir_discovers.count(cur->ino())) {
          dout(10) << "traverse: need dir, already doing discover for " << *cur << endl;
	  assert(cur->is_waiter_for(CInode::WAIT_DIR));
	} else {
	  filepath want = path.postfixpath(depth);
	  dout(10) << "traverse: need dir, doing discover, want " << want.get_path() 
		   << " from " << *cur << endl;
	  mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					      cur->ino(),
					      want,
					      true,  // need this dir!
					      onfail == MDS_TRAVERSE_DISCOVERXLOCK),
				cur->authority().first, MDS_PORT_CACHE);
	  dir_discovers[cur->ino()].insert(cur->authority().first);
	}
	cur->add_waiter(CInode::WAIT_DIR, _get_waiter(mdr, req));
        return 1;
      }
    }
    assert(curdir);

    // frozen?
    /*
    if (curdir->is_frozen()) {
    // doh!
      // FIXME: traverse is allowed?
      dout(7) << "traverse: " << *curdir << " is frozen, waiting" << endl;
      curdir->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req));
      if (onfinish) delete onfinish;
      return 1;
    }
    */

    // must read directory hard data (permissions, x bit) to traverse
    if (!noperm && 
	!mds->locker->simple_rdlock_try(&cur->authlock, _get_waiter(mdr, req))) 
      return 1;
    
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
    CDentry *dn = curdir->lookup(path[depth]);

    // null and last_bit and xlocked by me?
    if (dn && dn->is_null() && null_okay) {
      dout(10) << "traverse: hit null dentry at tail of traverse, succeeding" << endl;
      trace.push_back(dn);
      break; // done!
    }

    if (dn && !dn->is_null()) {
      // dentry exists.  xlocked?
      if (!noperm && dn->lock.is_xlocked() && dn->lock.get_xlocked_by() != mdr) {
        dout(10) << "traverse: xlocked dentry at " << *dn << endl;
        dn->lock.add_waiter(SimpleLock::WAIT_RD, _get_waiter(mdr, req));
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
	  assert(mdr);  // we shouldn't hit non-primary dentries doing a non-mdr traversal!
          open_remote_ino(dn->get_remote_ino(), mdr, _get_waiter(mdr, req));
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
      }

      // forwarder wants replicas?
      if (mdr && mdr->client_request && 
	  mdr->client_request->get_mds_wants_replica_in_dirino()) {
	dout(30) << "traverse: REP is here, " 
		 << mdr->client_request->get_mds_wants_replica_in_dirino() 
		 << " vs " << curdir->dirfrag() << endl;
	
	if (mdr->client_request->get_mds_wants_replica_in_dirino() == curdir->ino() &&
	    curdir->is_auth() && 
	    curdir->is_rep() &&
	    curdir->is_replica(req->get_source().num()) &&
	    dn->is_auth()
	    ) {
	  assert(req->get_source().is_mds());
	  int from = req->get_source().num();
	  
	  if (dn->is_replica(from)) {
	    dout(15) << "traverse: REP would replicate to mds" << from << ", but already cached_by " 
		     << req->get_source() << " dn " << *dn << endl; 
	  } else {
	    dout(10) << "traverse: REP replicating to " << req->get_source() << " dn " << *dn << endl;
	    MDiscoverReply *reply = new MDiscoverReply(curdir->ino());
	    reply->add_dentry( dn->replicate_to( from ) );
	    if (dn->is_primary())
	      reply->add_inode( dn->inode->replicate_to( from ) );
	    mds->send_message_mds(reply, req->get_source().num(), MDS_PORT_CACHE);
	  }
	}
      }
      
      // add to trace, continue.
      trace.push_back(dn);
      cur = dn->inode;
      touch_inode(cur);
      depth++;
      continue;
    }
    
    // MISS.  dentry doesn't exist.
    dout(12) << "traverse: miss on dentry " << path[depth] << " in " << *curdir << endl;
    
    if (curdir->is_auth()) {
      // dentry is mine.
      if (curdir->is_complete()) {
        // file not found
        return -ENOENT;
      } else {
	// directory isn't complete; reload
        dout(7) << "traverse: incomplete dir contents for " << *cur << ", fetching" << endl;
        touch_inode(cur);
        curdir->fetch(_get_waiter(mdr, req));
	if (mds->logger) mds->logger->inc("cmiss");
        return 1;
      }
    } else {
      // dirfrag/dentry is not mine.
      pair<int,int> dauth = curdir->authority();

      if ((onfail == MDS_TRAVERSE_DISCOVER ||
           onfail == MDS_TRAVERSE_DISCOVERXLOCK)) {
        // discover?
        filepath want = path.postfixpath(depth);

        if (curdir->is_waiting_for_dentry(path[depth])) {
          dout(7) << "traverse: already waiting for discover " << want.get_path()
		  << " from " << *curdir << endl;
        } 
	else if (curdir->is_ambiguous_auth()) {
	  dout(7) << "traverse: waiting for single auth on " << *curdir << endl;
	  curdir->add_waiter(CDir::WAIT_SINGLEAUTH, _get_waiter(mdr, req));
	  return 1;
	} 
	else {
          dout(7) << "traverse: discover " << want << " from " << *curdir << endl;
          touch_inode(cur);
          
          mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					      cur->ino(),
					      want,
					      false,
					      onfail == MDS_TRAVERSE_DISCOVERXLOCK),
				dauth.first, MDS_PORT_CACHE);
          if (mds->logger) mds->logger->inc("dis");
        }
        
        // delay processing of current request.
        curdir->add_dentry_waiter(path[depth], _get_waiter(mdr, req));
        if (mds->logger) mds->logger->inc("cmiss");
        return 1;
      } 
      if (onfail == MDS_TRAVERSE_FORWARD) {
        // forward
        dout(7) << "traverse: not auth for " << path << " in " << *curdir << endl;
	
	if (curdir->is_ambiguous_auth()) {
	  // wait
	  dout(7) << "traverse: waiting for single auth in " << *curdir << endl;
	  curdir->add_waiter(CDir::WAIT_SINGLEAUTH, _get_waiter(mdr, req));
	  return 1;
	} else {
	  dout(7) << "traverse: forwarding, not auth for " << *curdir << endl;

	  // request replication?
	  if (mdr && mdr->client_request && curdir->is_rep()) {
	    dout(15) << "traverse: REP fw to mds" << dauth << ", requesting rep under "
		     << *curdir << " req " << *(MClientRequest*)req << endl;
	    mdr->client_request->set_mds_wants_replica_in_dirino(curdir->ino());
	    req->clear_payload();  // reencode!
	  }
	  
	  if (mdr) 
	    request_forward(mdr, dauth.first, req->get_dest_port());
	  else
	    mds->forward_message_mds(req, dauth.first, req->get_dest_port());
	  
	  if (mds->logger) mds->logger->inc("cfw");
	  return 2;
	}
      }    
      if (onfail == MDS_TRAVERSE_FAIL) {
        return -ENOENT;  // not necessarily exactly true....
      }
    }
    
    assert(0);  // i shouldn't get here
  }
  
  // success.
  return 0;
}



void MDCache::open_remote_dir(CInode *diri, frag_t fg, Context *fin) 
{
  dout(10) << "open_remote_dir on " << *diri << endl;
  
  assert(diri->is_dir());
  assert(!diri->is_auth());
  assert(diri->get_dirfrag(fg) == 0);

  int auth = diri->authority().first;

  if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
    // discover it
    filepath want;  // no dentries, i just want the dir open
    MDiscover *dis = new MDiscover(mds->get_nodeid(),
				   diri->ino(),
				   want,
				   true);  // need the base dir open
    dis->set_base_dir_frag(fg);
    mds->send_message_mds(dis, auth, MDS_PORT_CACHE);
    dir_discovers[diri->ino()].insert(auth);
    diri->add_waiter(CInode::WAIT_DIR, fin);
  } else {
    // mds is down or recovering.  forge a replica!
    forge_replica_dir(diri, fg, auth);
  }
}


/** get_dentry_inode
 * will return inode for primary, or link up/open up remote link's inode as necessary.
 */
CInode *MDCache::get_dentry_inode(CDentry *dn, MDRequest *mdr)
{
  assert(!dn->is_null());
  
  if (dn->is_primary()) 
    return dn->inode;

  assert(dn->is_remote());
  CInode *in = get_inode(dn->get_remote_ino());
  if (in) {
    dout(7) << "get_dentry_inode linking in remote in " << *in << endl;
    dn->link_remote(in);
    return in;
  } else {
    dout(10) << "get_dentry_inode on remote dn, opening inode for " << *dn << endl;
    open_remote_ino(dn->get_remote_ino(), mdr, new C_MDS_RetryRequest(this, mdr));
    return 0;
  }
}


class C_MDC_OpenRemoteIno : public Context {
  MDCache *mdcache;
  inodeno_t ino;
  MDRequest *mdr;
  Context *onfinish;
public:
  vector<Anchor> anchortrace;

  C_MDC_OpenRemoteIno(MDCache *mdc, inodeno_t i, MDRequest *r, Context *c) :
    mdcache(mdc), ino(i), mdr(r), onfinish(c) {}
  C_MDC_OpenRemoteIno(MDCache *mdc, inodeno_t i, vector<Anchor>& at,
		      MDRequest *r, Context *c) :
    mdcache(mdc), ino(i), mdr(r), onfinish(c), anchortrace(at) {}

  void finish(int r) {
    assert(r == 0);
    if (r == 0)
      mdcache->open_remote_ino_2(ino, mdr, anchortrace, onfinish);
    else {
      onfinish->finish(r);
      delete onfinish;
    }
  }
};

void MDCache::open_remote_ino(inodeno_t ino,
                              MDRequest *mdr,
                              Context *onfinish)
{
  dout(7) << "open_remote_ino on " << ino << endl;
  
  C_MDC_OpenRemoteIno *c = new C_MDC_OpenRemoteIno(this, ino, mdr, onfinish);
  mds->anchorclient->lookup(ino, c->anchortrace, c);
}

void MDCache::open_remote_ino_2(inodeno_t ino,
                                MDRequest *mdr,
                                vector<Anchor>& anchortrace,
                                Context *onfinish)
{
  dout(7) << "open_remote_ino_2 on " << ino
	  << ", trace depth is " << anchortrace.size() << endl;
  
  // find deepest cached inode in prefix
  unsigned i = anchortrace.size();  // i := array index + 1
  CInode *in = 0;
  while (1) {
    // inode?
    dout(10) << " " << i << ": " << anchortrace[i-1] << endl;
    CInode *in = get_inode(anchortrace[i-1].ino);
    if (in) break;
    i--;
    if (!i) {
      CInode *in = get_inode(anchortrace[i].dirfrag.ino);
      assert(in);
      break;
    }
  }
  dout(10) << "deepest cached inode at " << i << " is " << *in << endl;

  if (in->ino() == ino) {
    // success
    dout(10) << "open_remote_ino_2 have " << *in << endl;
    onfinish->finish(0);
    delete onfinish;
    return;
  } 

  // open dirfrag beneath *in
  frag_t frag = anchortrace[i].dirfrag.frag;

  if (!in->dirfragtree.contains(frag)) {
    dout(10) << "frag " << frag << " not valid, requerying anchortable" << endl;
    open_remote_ino(ino, mdr, onfinish);
    return;
  }

  if (!in->is_auth()) {
    dout(10) << "opening remote dirfrag " << frag << " under " << *in << endl;
    open_remote_dir(in, frag,
		    new C_MDC_OpenRemoteIno(this, ino, anchortrace, mdr, onfinish));
    return;
  }

  CDir *dir = in->get_or_open_dirfrag(this, frag);
  assert(dir);
  if (dir->is_auth()) {
    if (dir->is_complete()) {
      // hrm.  requery anchor table.
      dout(10) << "expected ino " << anchortrace[i].ino
	       << " in complete dir " << *dir
	       << ", requerying anchortable"
	       << endl;
      open_remote_ino(ino, mdr, onfinish);
    } else {
      dout(10) << "need ino " << anchortrace[i].ino
	       << ", fetching incomplete dir " << *dir
	       << endl;
      dir->fetch(new C_MDC_OpenRemoteIno(this, ino, anchortrace, mdr, onfinish));
    }
  } else {
    // hmm, discover.
    dout(10) << "have remote dirfrag " << *dir << ", discovering " 
	     << anchortrace[i].ino << endl;
    
    MDiscover *dis = new MDiscover(mds->get_nodeid(),
				   dir->dirfrag(),
				   anchortrace[i].ino,
				   true);  // being conservative here.
    mds->send_message_mds(dis, dir->authority().first, MDS_PORT_CACHE);
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


MDRequest *MDCache::request_start(MClientRequest *req)
{
  // did we win a forward race against a slave?
  if (active_requests.count(req->get_reqid())) {
    MDRequest *mdr = active_requests[req->get_reqid()];
    dout(10) << "request_start already had " << *mdr << ", cleaning up" << endl;
    assert(mdr->is_slave());
    request_cleanup(mdr);
    delete mdr;
  }

  // register new client request
  MDRequest *mdr = new MDRequest(req->get_reqid(), req);
  active_requests[req->get_reqid()] = mdr;
  dout(7) << "request_start " << *mdr << endl;
  return mdr;
}

MDRequest *MDCache::request_start_slave(metareqid_t ri, int by)
{
  MDRequest *mdr = new MDRequest(ri, by);
  assert(active_requests.count(mdr->reqid) == 0);
  active_requests[mdr->reqid] = mdr;
  dout(7) << "request_start_slave " << *mdr << " by mds" << by << endl;
  return mdr;
}


MDRequest *MDCache::request_get(metareqid_t rid)
{
  assert(active_requests.count(rid));
  dout(7) << "request_get " << rid << " " << *active_requests[rid] << endl;
  return active_requests[rid];
}

void MDCache::request_finish(MDRequest *mdr)
{
  dout(7) << "request_finish " << *mdr << endl;

  // slave finisher?
  if (mdr->slave_commit) {
    mdr->slave_commit->finish(0);
    delete mdr->slave_commit;
    mdr->slave_commit = 0;
  }

  delete mdr->client_request;
  delete mdr->slave_request;
  request_cleanup(mdr);
  
  if (mds->logger) mds->logger->inc("reply");
}


void MDCache::request_forward(MDRequest *mdr, int who, int port)
{
  if (!port) port = MDS_PORT_SERVER;
  dout(7) << "request_forward " << *mdr << " to mds" << who << " req " << *mdr << endl;
  
  mds->forward_message_mds(mdr->client_request, who, port);  
  request_cleanup(mdr);

  if (mds->logger) mds->logger->inc("fw");
}


void MDCache::dispatch_request(MDRequest *mdr)
{
  if (mdr->client_request) {
    mds->server->dispatch_client_request(mdr);
  } else if (mdr->slave_request) {
    mds->server->dispatch_slave_request(mdr);
  } else
    assert(0);
}



void MDCache::request_forget_foreign_locks(MDRequest *mdr)
{
  // xlocks
  set<SimpleLock*>::iterator p = mdr->xlocks.begin();
  while (p != mdr->xlocks.end()) {
    if ((*p)->get_parent()->is_auth()) 
      p++;
    else {
      dout(10) << "request_forget_foreign_locks " << **p
	       << " on " << *(*p)->get_parent() << endl;
      (*p)->put_xlock();
      mdr->locks.erase(*p);
      mdr->xlocks.erase(p++);
    }
  }
}

void MDCache::request_cleanup(MDRequest *mdr)
{
  dout(15) << "request_cleanup " << *mdr << endl;
  metareqid_t ri = mdr->reqid;

  // clear ref, trace
  mdr->ref = 0;
  mdr->trace.clear();

  // clean up slaves
  //  (will implicitly drop remote dn pins)
  for (set<int>::iterator p = mdr->slaves.begin();
       p != mdr->slaves.end();
       ++p) {
    MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_FINISH);
    mds->send_message_mds(r, *p, MDS_PORT_SERVER);
  }
  // strip foreign xlocks out of lock lists, since the OP_FINISH drops them implicitly.
  request_forget_foreign_locks(mdr);


  // drop locks
  mds->locker->drop_locks(mdr);

  // drop (local) auth pins
  mdr->drop_local_auth_pins();

  // drop cache pins
  for (set<MDSCacheObject*>::iterator it = mdr->pins.begin();
       it != mdr->pins.end();
       it++) 
    (*it)->put(MDSCacheObject::PIN_REQUEST);
  mdr->pins.clear();

  // remove from map
  active_requests.erase(mdr->reqid);
  delete mdr;




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


// --------------------------------------------------------------------
// ANCHORS

// CREATE

class C_MDC_AnchorCreatePrepared : public Context {
  MDCache *cache;
  CInode *in;
public:
  version_t atid;
  C_MDC_AnchorCreatePrepared(MDCache *c, CInode *i) : cache(c), in(i) {}
  void finish(int r) {
    cache->_anchor_create_prepared(in, atid);
  }
};

void MDCache::anchor_create(MDRequest *mdr, CInode *in, Context *onfinish)
{
  assert(in->is_auth());

  // auth pin
  if (!in->can_auth_pin() &&
      !mdr->is_auth_pinned(in)) {
    dout(7) << "anchor_create not authpinnable, waiting on " << *in << endl;
    in->add_waiter(CInode::WAIT_AUTHPINNABLE, onfinish);
    return;
  }

  // wait
  in->add_waiter(CInode::WAIT_ANCHORED, onfinish);

  // already anchoring?
  if (in->state_test(CInode::STATE_ANCHORING)) {
    dout(7) << "anchor_create already anchoring " << *in << endl;
    return;
  }

  dout(7) << "anchor_create " << *in << endl;

  // auth: do it
  in->state_set(CInode::STATE_ANCHORING);
  in->get(CInode::PIN_ANCHORING);
  in->auth_pin();
  
  // make trace
  vector<Anchor> trace;
  in->make_anchor_trace(trace);
  
  // do it
  C_MDC_AnchorCreatePrepared *fin = new C_MDC_AnchorCreatePrepared(this, in);
  mds->anchorclient->prepare_create(in->ino(), trace, &fin->atid, fin);
}

class C_MDC_AnchorCreateLogged : public Context {
  MDCache *cache;
  CInode *in;
  version_t atid;
  version_t pdv;
public:
  C_MDC_AnchorCreateLogged(MDCache *c, CInode *i, version_t t, version_t v) : 
    cache(c), in(i), atid(t), pdv(v) {}
  void finish(int r) {
    cache->_anchor_create_logged(in, atid, pdv);
  }
};

void MDCache::_anchor_create_prepared(CInode *in, version_t atid)
{
  dout(10) << "_anchor_create_prepared " << *in << " atid " << atid << endl;
  assert(in->inode.anchored == false);

  // predirty, prepare log entry
  version_t pdv = in->pre_dirty();

  EUpdate *le = new EUpdate("anchor_create");
  le->metablob.add_dir_context(in->get_parent_dir());

  // update the logged inode copy
  inode_t *pi = le->metablob.add_dentry(in->parent, true);
  pi->anchored = true;
  pi->version = pdv;

  // note anchor transaction
  le->metablob.add_anchor_transaction(atid);

  // log + wait
  mds->mdlog->submit_entry(le, new C_MDC_AnchorCreateLogged(this, in, atid, pdv));
}


void MDCache::_anchor_create_logged(CInode *in, version_t atid, version_t pdv)
{
  dout(10) << "_anchor_create_logged pdv " << pdv << " on " << *in << endl;

  // unpin
  assert(in->state_test(CInode::STATE_ANCHORING));
  in->state_clear(CInode::STATE_ANCHORING);
  in->put(CInode::PIN_ANCHORING);
  in->auth_unpin();
  
  // apply update to cache
  in->inode.anchored = true;
  in->mark_dirty(pdv);
  
  // tell the anchortable we've committed
  mds->anchorclient->commit(atid);

  // trigger waiters
  in->finish_waiting(CInode::WAIT_ANCHORED, 0);
}


// DESTROY

class C_MDC_AnchorDestroyPrepared : public Context {
  MDCache *cache;
  CInode *in;
public:
  version_t atid;
  C_MDC_AnchorDestroyPrepared(MDCache *c, CInode *i) : cache(c), in(i) {}
  void finish(int r) {
    cache->_anchor_destroy_prepared(in, atid);
  }
};

void MDCache::anchor_destroy(CInode *in, Context *onfinish)
{
  assert(in->is_auth());

  // auth pin
  if (!in->can_auth_pin()/* &&
			    !mdr->is_auth_pinned(in)*/) {
    dout(7) << "anchor_destroy not authpinnable, waiting on " << *in << endl;
    in->add_waiter(CInode::WAIT_AUTHPINNABLE, onfinish);
    return;
  }

  // wait
  if (onfinish)
    in->add_waiter(CInode::WAIT_UNANCHORED, onfinish);

  // already anchoring?
  if (in->state_test(CInode::STATE_UNANCHORING)) {
    dout(7) << "anchor_destroy already unanchoring " << *in << endl;
    return;
  }

  dout(7) << "anchor_destroy " << *in << endl;

  // auth: do it
  in->state_set(CInode::STATE_UNANCHORING);
  in->get(CInode::PIN_UNANCHORING);
  in->auth_pin();
  
  // do it
  C_MDC_AnchorDestroyPrepared *fin = new C_MDC_AnchorDestroyPrepared(this, in);
  mds->anchorclient->prepare_destroy(in->ino(), &fin->atid, fin);
}

class C_MDC_AnchorDestroyLogged : public Context {
  MDCache *cache;
  CInode *in;
  version_t atid;
  version_t pdv;
public:
  C_MDC_AnchorDestroyLogged(MDCache *c, CInode *i, version_t t, version_t v) :
    cache(c), in(i), atid(t), pdv(v) {}
  void finish(int r) {
    cache->_anchor_destroy_logged(in, atid, pdv);
  }
};

void MDCache::_anchor_destroy_prepared(CInode *in, version_t atid)
{
  dout(10) << "_anchor_destroy_prepared " << *in << " atid " << atid << endl;

  assert(in->inode.anchored == true);

  // predirty, prepare log entry
  version_t pdv = in->pre_dirty();

  EUpdate *le = new EUpdate("anchor_destroy");
  le->metablob.add_dir_context(in->get_parent_dir());

  // update the logged inode copy
  inode_t *pi = le->metablob.add_dentry(in->parent, true);
  pi->anchored = true;
  pi->version = pdv;

  // note anchor transaction
  le->metablob.add_anchor_transaction(atid);

  // log + wait
  mds->mdlog->submit_entry(le, new C_MDC_AnchorDestroyLogged(this, in, atid, pdv));
}


void MDCache::_anchor_destroy_logged(CInode *in, version_t atid, version_t pdv)
{
  dout(10) << "_anchor_destroy_logged pdv " << pdv << " on " << *in << endl;
  
  // unpin
  assert(in->state_test(CInode::STATE_UNANCHORING));
  in->state_clear(CInode::STATE_UNANCHORING);
  in->put(CInode::PIN_UNANCHORING);
  in->auth_unpin();
  
  // apply update to cache
  in->inode.anchored = false;
  in->inode.version = pdv;
  
  // tell the anchortable we've committed
  mds->anchorclient->commit(atid);

  // trigger waiters
  in->finish_waiting(CInode::WAIT_UNANCHORED, 0);
}


// -------------------------------------------------------------------------------
// STRAYS

void MDCache::eval_stray(CDentry *dn)
{
  dout(10) << "eval_stray " << *dn << endl;
  assert(dn->is_primary());
  CInode *in = dn->inode;
  assert(in);

  // purge?
  if (in->inode.nlink == 0) {
    if (!dn->is_replicated() && !in->is_any_caps()) 
      _purge_stray(dn);
    return;
  }
  else if (in->inode.nlink == 1) {
    // trivial reintegrate?
    if (!in->remote_parents.empty()) {
      CDentry *rlink = *in->remote_parents.begin();
      if (rlink->is_auth() &&
	  rlink->dir->can_auth_pin())
	reintegrate_stray(dn, rlink);
      
      if (!rlink->is_auth() &&
	  !in->is_ambiguous_auth()) 
	migrate_stray(dn, rlink->authority().first);
    }
  } else {
    // wait for next use.
  }
}


class C_MDC_PurgeStray : public Context {
  MDCache *cache;
  CDentry *dn;
  version_t pdv;
public:
  C_MDC_PurgeStray(MDCache *c, CDentry *d, version_t v) : cache(c), dn(d), pdv(v) { }
  void finish(int r) {
    cache->_purge_stray_logged(dn, pdv);
  }
};

void MDCache::_purge_stray(CDentry *dn)
{
  dout(10) << "_purge_stray " << *dn << " " << *dn->inode << endl;
  assert(!dn->is_replicated());

  // log removal
  version_t pdv = dn->pre_dirty();

  EUpdate *le = new EUpdate;
  le->metablob.add_dir_context(dn->dir);
  le->metablob.add_null_dentry(dn, true);
  le->metablob.add_inode_truncate(dn->inode->inode, 0);
  mds->mdlog->submit_entry(le, new C_MDC_PurgeStray(this, dn, pdv));
}

void MDCache::_purge_stray_logged(CDentry *dn, version_t pdv)
{
  dout(10) << "_purge_stray_logged " << *dn << " " << *dn->inode << endl;
  CInode *in = dn->inode;
  
  // dirty+unlink dentry
  dn->dir->mark_dirty(pdv);
  dn->dir->unlink_inode(dn);
  dn->dir->remove_dentry(dn);

  // purge+remove inode
  if (in->inode.size > 0)
    purge_inode(&in->inode, 0);
  remove_inode(in);
}



void MDCache::reintegrate_stray(CDentry *dn, CDentry *rlink)
{
  dout(10) << "reintegrate_stray " << *dn << " into " << *rlink << endl;
  
}
 

void MDCache::migrate_stray(CDentry *dn, int dest)
{
  dout(10) << "migrate_stray to mds" << dest << " " << *dn << endl;

}




// REPLICAS


void MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();

  assert(dis->get_asker() != whoami);

  CInode *cur = 0;
  MDiscoverReply *reply = new MDiscoverReply(dis->get_base_ino());

  // get started.
  if (dis->get_base_ino() == MDS_INO_ROOT) {
    // wants root
    dout(7) << "handle_discover from mds" << dis->get_asker()
	    << " wants root + " << dis->get_want().get_path() << endl;

    assert(mds->get_nodeid() == 0);
    assert(root->is_auth());

    // add root
    reply->add_inode( root->replicate_to( dis->get_asker() ) );
    dout(10) << "added root " << *root << endl;

    cur = root;
  }
  else if (dis->get_base_ino() == MDS_INO_STRAY(whoami)) {
    // wants root
    dout(7) << "handle_discover from mds" << dis->get_asker()
	    << " wants stray + " << dis->get_want().get_path() << endl;
    
    reply->add_inode( stray->replicate_to( dis->get_asker() ) );
    dout(10) << "added stray " << *stray << endl;

    cur = stray;
  }
  else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino());
    
    if (!cur) {
      dout(7) << "handle_discover mds" << dis->get_asker() 
	      << " don't have base ino " << dis->get_base_ino() 
	      << ", dropping" << endl;
      delete reply;
      return;
    }

    if (dis->wants_base_dir()) {
      dout(7) << "handle_discover mds" << dis->get_asker() 
	      << " has " << *cur 
	      << " wants basedir+" << dis->get_want().get_path() 
	      << endl;
    } else {
      dout(7) << "handle_discover mds" << dis->get_asker() 
	      << " has " << *cur
	      << " wants " << dis->get_want().get_path()
	      << endl;
    }
  }

  assert(reply);
  assert(cur);
  
  // add content
  // do some fidgeting to include a dir if they asked for the base dir, or just root.
  for (unsigned i = 0; 
       i < dis->get_want().depth() || dis->get_want().depth() == 0; 
       i++) {

    // -- figure out the dir

    // is *cur even a dir at all?
    if (!cur->is_dir()) {
      dout(7) << *cur << " not a dir" << endl;
      reply->set_flag_error_dir();
      break;
    }

    // pick frag
    frag_t fg;
    if (dis->get_want().depth()) {
      // dentry specifies
      fg = cur->pick_dirfrag(dis->get_dentry(i));
    } else {
      // requester explicity specified the frag
      fg = dis->get_base_dir_frag();
      assert(dis->wants_base_dir() || dis->get_base_ino() < MDS_INO_BASE);
    }
    CDir *curdir = cur->get_dirfrag(fg);

    // am i dir auth (or if no dir, at least the inode auth)
    if ((!curdir && !cur->is_auth()) ||
	(curdir && !curdir->is_auth())) {
      if (curdir) {
	dout(7) << *curdir << " not dirfrag auth, setting dir_auth_hint" << endl;
	reply->set_dir_auth_hint(curdir->authority().first);
      } else {
	dout(7) << *cur << " dirfrag not open, not inode auth, setting dir_auth_hint" << endl;
	reply->set_dir_auth_hint(cur->authority().first);
      }
      reply->set_wanted_xlocks_hint(dis->wants_xlocked());
      
      // set hint (+ dentry, if there is one)
      if (dis->get_want().depth() > i)
	reply->set_error_dentry(dis->get_dentry(i));
      break;
    }

    // open dir?
    if (!curdir) 
      curdir = cur->get_or_open_dirfrag(this, fg);
    assert(curdir);
    assert(curdir->is_auth());
    
    // is dir frozen?
    if (curdir->is_frozen()) {
      if (reply->is_empty()) {
	dout(7) << *curdir << " is frozen, empty reply, waiting" << endl;
	curdir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	delete reply;
	return;
      } else {
	dout(7) << *curdir << " is frozen, non-empty reply, stopping" << endl;
	break;
      }
    }
    
    // add dir
    if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "not adding unwanted base dir " << *curdir << endl;
    } else {
      assert(!curdir->is_ambiguous_auth()); // would be frozen.
      reply->add_dir( curdir->replicate_to(dis->get_asker()) );
      dout(7) << "added dir " << *curdir << endl;
    }
    if (dis->get_want().depth() == 0) break;
    
    // lookup inode?
    CDentry *dn = 0;
    if (dis->get_want_ino()) {
      CInode *in = get_inode(dis->get_want_ino());
      if (in && in->is_auth() && in->get_parent_dn()->get_dir() == curdir)
	dn = in->get_parent_dn();
    } else {
      // lookup dentry
      dn = curdir->lookup( dis->get_dentry(i) );
    }
    
    // incomplete dir?
    if (!dn) {
      if (!curdir->is_complete()) {
	// readdir
	dout(7) << "incomplete dir contents for " << *curdir << ", fetching" << endl;
	if (reply->is_empty()) {
	  // fetch and wait
	  curdir->fetch(new C_MDS_RetryMessage(mds, dis));
	  return;
	} else {
	  // initiate fetch, but send what we have so far
	  curdir->fetch(0);
	  break;
	}
      }
      
      // don't have wanted ino in this dir?
      if (dis->get_want_ino()) {
	// set error flag in reply
	dout(7) << "ino " << dis->get_want_ino() << " in this dir, flagging error in "
		<< *curdir << endl;
	reply->set_flag_error_ino();
	break;
      }
      
      // send null dentry
      dout(7) << "dentry " << dis->get_dentry(i) << " dne, returning null in "
	      << *curdir << endl;
      dn = curdir->add_dentry(dis->get_dentry(i), 0);
    }
    assert(dn);

    // xlocked dentry?
    //  ...always block on non-tail items (they are unrelated)
    //  ...allow xlocked tail disocvery _only_ if explicitly requested
    if (dn->lock.is_xlocked()) {
      // is this the last (tail) item in the discover traversal?
      bool tailitem = (dis->get_want().depth() == 0) || (i == dis->get_want().depth() - 1);
      if (tailitem && dis->wants_xlocked()) {
	dout(7) << "allowing discovery of xlocked tail " << *dn << endl;
      } else {
	dout(7) << "blocking on xlocked " << *dn << endl;
	dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryMessage(mds, dis));
	delete reply;
	return;
      }
    }

    // add dentry
    reply->add_dentry( dn->replicate_to( dis->get_asker() ) );
    dout(7) << "added dentry " << *dn << endl;
    
    if (!dn->is_primary()) break;  // stop on null or remote link.
    
    // add inode
    CInode *next = dn->inode;
    assert(next->is_auth());
    
    reply->add_inode( next->replicate_to( dis->get_asker() ) );
    dout(7) << "added inode " << *next << endl;
    
    // descend, keep going.
    cur = next;
    continue;
  }

  // how did we do?
  if (reply->is_empty()) {
    dout(7) << "dropping this empty reply)." << endl;
    delete reply;
  } else {
    dout(7) << "sending result back to asker mds" << dis->get_asker() << endl;
    mds->send_message_mds(reply, dis->get_asker(), MDS_PORT_CACHE);
  }

  // done.
  delete dis;
}


void MDCache::handle_discover_reply(MDiscoverReply *m) 
{
  // starting point
  list<Context*> finished, error;
  
  // grab base inode
  CInode *cur = get_inode(m->get_base_ino());
    
  if (cur) {
    dout(7) << "discover_reply " << *cur << " + " << m->get_path() << ", have " << m->get_num_inodes() << " inodes" << endl;
  } 
  else if (m->get_base_ino() == MDS_INO_ROOT) {
    // it's the root inode.
    assert(!root);
    assert(!m->has_base_dentry());
    assert(!m->has_base_dir());
    
    dout(7) << "discover_reply root + " << m->get_path() << " " << m->get_num_inodes() << " inodes" << endl;
    
    // add in root
    cur = add_replica_inode(m->get_inode(0), NULL);
    set_root(cur);
    dout(7) << "discover_reply got root " << *cur << endl;
    
    // take root waiters
    finished.swap(waiting_for_root);
  }
  else if (MDS_INO_IS_STRAY(m->get_base_ino())) {
    dout(7) << "discover_reply stray + " << m->get_path() << " " << m->get_num_inodes() << " inodes" << endl;
    
    // add 
    cur = add_replica_inode(m->get_inode(0), NULL);
    set_root(cur);
    dout(7) << "discover_reply got stray " << *cur << endl;
    
    // take waiters
    finished.swap(waiting_for_stray[cur->ino()]);
    waiting_for_stray.erase(cur->ino());
  }

  // fyi
  if (m->is_flag_error_dir()) dout(7) << " flag error, dir" << endl;
  if (m->is_flag_error_dn()) dout(7) << " flag error, dentry = " << m->get_error_dentry() << endl;
  dout(10) << "depth = " << m->get_depth()
	   << ", has base_dir/base_dn/root = " 
	   << m->has_base_dir() << " / " << m->has_base_dentry() << " / " << m->has_base_inode()
	   << ", num dirs/dentries/inodes = " 
	   << m->get_num_dirs() << " / " << m->get_num_dentries() << " / " << m->get_num_inodes()
	   << endl;
  
  // loop over discover results.
  // indexese follow each ([[dir] dentry] inode) 
  // can start, end with any type.
  
  for (int i=m->has_base_inode(); i<m->get_depth(); i++) {
    dout(10) << "discover_reply i=" << i << " cur " << *cur << endl;

    // dir
    frag_t fg;
    CDir *curdir = 0;
    if (i > 0 || m->has_base_dir()) {
      assert(m->get_dir(i).get_dirfrag().ino == cur->ino());
      fg = m->get_dir(i).get_dirfrag().frag;
      
      // add/update the dir replica
      curdir = add_replica_dir(cur, fg, m->get_dir(i), 
			       m->get_source().num(),
			       finished);
    }
    if (!curdir) {
      fg = cur->pick_dirfrag(m->get_dentry(i).get_dname());
      curdir = cur->get_dirfrag(fg);
    }
    
    // dentry error?
    if (i == m->get_depth()-1 && 
        m->is_flag_error_dn()) {
      // error!
      assert(cur->is_dir());
      if (curdir) {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dentry?" << endl;
        curdir->take_dentry_waiting(m->get_error_dentry(),
				    error);
      } else {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dir?" << endl;
        cur->take_waiting(CInode::WAIT_DIR, error);
	dir_discovers.erase(cur->ino());
      }
      break;
    }

    assert(curdir);

    // dentry
    CDentry *dn = 0;
    if (i >= m->get_last_dentry()) break;
    if (i > 0 || m->has_base_dentry()) {
      dn = add_replica_dentry(curdir, m->get_dentry(i), finished);
    }

    // inode
    if (i >= m->get_last_inode()) break;
    cur = add_replica_inode(m->get_inode(i), dn);
  }

  // dir_auth hint?
  if (m->get_dir_auth_hint() != CDIR_AUTH_UNKNOWN &&
      m->get_dir_auth_hint() != mds->get_nodeid()) {
    dout(7) << " dir_auth_hint is " << m->get_dir_auth_hint() << endl;

    // try again.  include dentry _and_ dirfrag, just in case.
    int hint = m->get_dir_auth_hint();
    filepath want;
    want.push_dentry(m->get_error_dentry());
    MDiscover *dis = new MDiscover(mds->get_nodeid(),
				   cur->ino(),
				   want,
				   true,
				   m->get_wanted_xlocks_hint());
    frag_t fg = cur->pick_dirfrag(m->get_error_dentry());
    dis->set_base_dir_frag(fg);    
    mds->send_message_mds(dis, hint, MDS_PORT_CACHE);
    
    // note the dangling discover... but only if it's already noted in dir_discovers (i.e. someone is waiting)
    if (dir_discovers.count(cur->ino())) {
      dir_discovers[cur->ino()].insert(hint);
      assert(cur->is_waiter_for(CInode::WAIT_DIR));
    }
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
  mds->queue_waiters(finished);

  // done
  delete m;
}


CDir *MDCache::add_replica_dir(CInode *diri, 
			       frag_t fg, CDirDiscover &dis, int from,
			       list<Context*>& finished)
{
  // add it (_replica_)
  CDir *dir = diri->get_dirfrag(fg);

  if (dir) {
    // had replica. update w/ new nonce.
    dis.update_dir(dir);
    dout(7) << "add_replica_dir had " << *dir << " nonce " << dir->replica_nonce << endl;
  } else {
    // add replica.
    dir = diri->add_dirfrag( new CDir(diri, fg, this, false) );
    dis.update_dir(dir);

    // is this a dir_auth delegation boundary?
    if (from != diri->authority().first ||
	diri->is_ambiguous_auth() ||
	diri->ino() < MDS_INO_BASE)
      adjust_subtree_auth(dir, from);
    
    dout(7) << "add_replica_dir added " << *dir << " nonce " << dir->replica_nonce << endl;
    
    // get waiters
    diri->take_waiting(CInode::WAIT_DIR, finished);
    dir_discovers.erase(diri->ino());
  }

  return dir;
}

CDir *MDCache::forge_replica_dir(CInode *diri, frag_t fg, int from)
{
  assert(mds->mdsmap->get_state(from) < MDSMap::STATE_REJOIN);
  
  // forge a replica.
  CDir *dir = diri->add_dirfrag( new CDir(diri, fg, this, false) );
  
  // i'm assuming this is a subtree root. 
  adjust_subtree_auth(dir, from);

  dout(7) << "forge_replica_dir added " << *dir << " while mds" << from << " is down" << endl;

  return dir;
}
		
CDentry *MDCache::add_replica_dentry(CDir *dir, CDentryDiscover &dis, list<Context*>& finished)
{
  CDentry *dn = dir->lookup( dis.get_dname() );
  
  // have it?
  if (dn) {
    dis.update_dentry(dn);
    dout(7) << "add_replica_dentry had " << *dn << endl;
  } else {
    dn = dir->add_dentry( dis.get_dname(), 0 );
    dis.update_dentry(dn);
    dis.init_dentry_lock(dn);
    dout(7) << "add_replica_dentry added " << *dn << endl;
  }
  
  // remote_ino linkage?
  if (dis.get_remote_ino()) {
    if (dn->is_null()) 
      dir->link_inode(dn, dis.get_remote_ino());
    
    // hrm.  yeah.
    assert(dn->is_remote() && dn->get_remote_ino() == dis.get_remote_ino());
  }

  dir->take_dentry_waiting(dis.get_dname(), finished);

  return dn;
}

CInode *MDCache::add_replica_inode(CInodeDiscover& dis, CDentry *dn)
{
  CInode *in = get_inode(dis.get_ino());
  if (!in) {
    in = new CInode(this, false);
    dis.update_inode(in);
    dis.init_inode_locks(in);
    add_inode(in);
    dout(10) << "add_replica_inode had " << *in << endl;
    if (dn && dn->is_null()) 
      dn->dir->link_inode(dn, in);
  } else {
    dis.update_inode(in);
    dout(10) << "add_replica_inode added " << *in << endl;
  }

  if (dn) {
    assert(dn->is_primary());
    assert(dn->inode == in);
  }
  
  return in;
}

    
CDentry *MDCache::add_replica_stray(bufferlist &bl, CInode *in, int from)
{
  list<Context*> finished;
  int off = 0;
  
  // inode
  CInodeDiscover indis;
  indis._decode(bl, off);
  CInode *strayin = add_replica_inode(indis, NULL);
  dout(15) << "strayin " << *strayin << endl;
  
  // dir
  CDirDiscover dirdis;
  dirdis._decode(bl, off);
  CDir *straydir = add_replica_dir(strayin, dirdis.get_dirfrag().frag, dirdis,
					    from, finished);
  dout(15) << "straydir " << *straydir << endl;
  
  // dentry
  CDentryDiscover dndis;
  dndis._decode(bl, off);
  
  string straydname;
  in->name_stray_dentry(straydname);
  CDentry *straydn = add_replica_dentry(straydir, dndis, finished);

  mds->queue_waiters(finished);

  return straydn;
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

    mds->send_message_mds(new MDirUpdate(dir->dirfrag(),
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
  CDir *dir = get_dirfrag(m->get_dirfrag());
  if (!dir) {
    dout(5) << "dir_update on " << m->get_dirfrag() << ", don't have it" << endl;

    // discover it?
    if (m->should_discover()) {
      m->tried_discover();  // only once!
      vector<CDentry*> trace;
      filepath path = m->get_path();

      dout(5) << "trying discover on dir_update for " << path << endl;

      int r = path_traverse(0, m,
			    0, path, trace, true,
                            MDS_TRAVERSE_DISCOVER);
      if (r > 0)
        return;
      assert(r == 0);

      CInode *in = get_inode(m->get_dirfrag().ino);
      assert(in);
      open_remote_dir(in, m->get_dirfrag().frag, 
		      new C_MDS_RetryMessage(mds, m));
      return;
    }

    delete m;
    return;
  }

  // update
  dout(5) << "dir_update on " << *dir << endl;
  dir->dir_rep = m->get_dir_rep();
  dir->dir_rep_by = m->get_dir_rep_by();
  
  // done
  delete m;
}






// UNLINK

void MDCache::handle_dentry_unlink(MDentryUnlink *m)
{
  CDir *dir = get_dirfrag(m->get_dirfrag());

  if (!dir) {
    dout(7) << "handle_dentry_unlink don't have dirfrag " << m->get_dirfrag() << endl;
  }
  else {
    CDentry *dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << "handle_dentry_unlink don't have dentry " << *dir << " dn " << m->get_dn() << endl;
    } else {
      dout(7) << "handle_dentry_unlink on " << *dn << endl;
      
      // move to stray?
      CDentry *straydn = 0;
      if (m->strayin) {
	list<Context*> finished;
	CInode *in = add_replica_inode(*m->strayin, NULL);
	CDir *dir = add_replica_dir(in, m->straydir->get_dirfrag().frag, *m->straydir,
				    m->get_source().num(), finished);
	straydn = add_replica_dentry(dir, *m->straydn, finished);
	if (!finished.empty()) mds->queue_waiters(finished);
      }

      // open inode?
      if (dn->is_primary()) {
	CInode *in = dn->inode;
	dn->dir->unlink_inode(dn);
	assert(straydn);
	straydn->dir->link_inode(straydn, in);
      } else {
	assert(dn->is_remote());
	dn->dir->unlink_inode(dn);
      }
      assert(dn->is_null());
      
      // move to bottom of lru
      lru.lru_bottouch(dn);
    }
  }

  delete m;
  return;
}









// ==============================================================
// debug crap

void MDCache::show_subtrees(int dbl)
{
  //dout(10) << "show_subtrees" << endl;

  if (dbl > g_conf.debug && dbl > g_conf.debug_mds) 
    return;  // i won't print anything.

  if (subtrees.empty()) {
    dout(dbl) << "no subtrees" << endl;
    return;
  }

  // root frags
  list<CDir*> rootfrags;
  if (root) root->get_dirfrags(rootfrags);
  if (stray) stray->get_dirfrags(rootfrags);
  dout(15) << "rootfrags " << rootfrags << endl;

  // queue stuff
  list<pair<CDir*,int> > q;
  string indent;
  set<CDir*> seen;

  // calc max depth
  for (list<CDir*>::iterator p = rootfrags.begin(); p != rootfrags.end(); ++p) 
    q.push_back(pair<CDir*,int>(*p, 0));

  int depth = 0;
  while (!q.empty()) {
    CDir *dir = q.front().first;
    int d = q.front().second;
    q.pop_front();

    if (d > depth) depth = d;

    // sanity check
    //dout(25) << "saw depth " << d << " " << *dir << endl;
    if (seen.count(dir)) dout(0) << "aah, already seen " << *dir << endl;
    assert(seen.count(dir) == 0);
    seen.insert(dir);

    // nested items?
    if (!subtrees[dir].empty()) {
      for (set<CDir*>::iterator p = subtrees[dir].begin();
	   p != subtrees[dir].end();
	   ++p) {
	//dout(25) << " saw sub " << **p << endl;
	q.push_front(pair<CDir*,int>(*p, d+1));
      }
    }
  }


  // print tree
  for (list<CDir*>::iterator p = rootfrags.begin(); p != rootfrags.end(); ++p) 
    q.push_back(pair<CDir*,int>(*p, 0));

  while (!q.empty()) {
    CDir *dir = q.front().first;
    int d = q.front().second;
    q.pop_front();

    // adjust indenter
    while ((unsigned)d < indent.size()) 
      indent.resize(d);
    
    // pad
    string pad = "______________________________________";
    pad.resize(depth*2+1-indent.size());
    if (!subtrees[dir].empty()) 
      pad[0] = '.'; // parent


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

    if (dir->ino() == MDS_INO_ROOT)
      assert(dir->inode == root);
    if (dir->ino() == MDS_INO_STRAY(mds->get_nodeid()))
      assert(dir->inode == stray);

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
    // unlinked?
    if (!it->second->parent)
      dout(7) << " unlinked " << *it->second << endl;
    
    // dirfrags?
    list<CDir*> dfs;
    it->second->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
      CDir *dir = *p;
      dout(7) << "  dirfrag " << *dir << endl;
	    
      for (CDir_map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	dout(7) << "   dentry " << *dn << endl;
	if (dn->is_primary() && dn->inode) 
	  dout(7) << "    inode " << *dn->inode << endl;
      }
    }
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
    list<CDir*> dfs;
    it->second->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
      CDir *dir = *p;
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
