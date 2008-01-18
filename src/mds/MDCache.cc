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

#include "events/ESubtreeMap.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EString.h"
#include "events/EPurgeFinish.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSResolve.h"
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

#include "messages/MMDSFragmentNotify.h"


#include "IdAllocator.h"

#include "common/Timer.h"

#include <assert.h>
#include <errno.h>
#include <iostream>
#include <string>
#include <map>
using namespace std;

#include "config.h"

#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".cache "




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
}

MDCache::~MDCache() 
{
  delete migrator;
  //delete renamer;
}



void MDCache::log_stat(Logger *logger)
{
  if (get_root()) {
    utime_t now = g_clock.now();
    //logger->set("pop", (int)get_root()->pop_nested.meta_load(now));
    //logger->set("popauth", (int)get_root()->pop_auth_subtree_nested.meta_load(now));
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
    dout(7) << "WARNING: mdcache shutodwn with non-empty cache" << dendl;
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

  if (in->ino() < MDS_INO_BASE) {
    base_inodes.insert(in);
    if (in->ino() == MDS_INO_ROOT)
      set_root(in);
    if (in->ino() == MDS_INO_STRAY(mds->get_nodeid()))
      stray = in;
  }
}

void MDCache::remove_inode(CInode *o) 
{ 
  dout(14) << "remove_inode " << *o << dendl;

  if (o->get_parent_dn()) {
    // FIXME: multiple parents?
    CDentry *dn = o->get_parent_dn();
    assert(!dn->is_dirty());
    dn->dir->unlink_inode(dn);   // leave dentry ... FIXME?
  }

  // remove from inode map
  inode_map.erase(o->ino());    

  if (o->ino() < MDS_INO_BASE) {
    assert(base_inodes.count(o));
    base_inodes.erase(o);

    if (o == root) root = 0;
    if (o == stray) stray = 0;
  }

  // delete it
  delete o; 
}



CInode *MDCache::create_root_inode()
{
  CInode *root = new CInode(this);
  memset(&root->inode, 0, sizeof(inode_t));
  root->inode.ino = MDS_INO_ROOT;
  
  // make it up (FIXME)
  root->inode.mode = 0755 | S_IFDIR;
  root->inode.size = 0;
  root->inode.ctime = 
    root->inode.mtime = g_clock.now();
  
  root->inode.nlink = 1;
  root->inode.layout = g_OSD_MDDirLayout;
  
  root->inode_auth = pair<int,int>(0, CDIR_AUTH_UNKNOWN);

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
    discover_base_ino(MDS_INO_ROOT, c, 0);
  }
}

CInode *MDCache::create_stray_inode(int whose)
{
  if (whose < 0) whose = mds->get_nodeid();

  CInode *in = new CInode(this, whose == mds->get_nodeid());
  memset(&in->inode, 0, sizeof(inode_t));
  in->inode.ino = MDS_INO_STRAY(whose);
  
  // make it up (FIXME)
  in->inode.mode = 0755 | S_IFDIR;
  in->inode.size = 0;
  in->inode.ctime = 
    in->inode.mtime = g_clock.now();
  
  in->inode.nlink = 1;
  in->inode.layout = g_OSD_MDDirLayout;
  
  add_inode( in );

  return in;
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
  dout(10) << "open_foreign_stray mds" << who << " " << ino << dendl;
  assert(!have_inode(ino));

  discover_base_ino(ino, c, who);
}


CDentry *MDCache::get_or_create_stray_dentry(CInode *in)
{
  string straydname;
  in->name_stray_dentry(straydname);
  
  if (!stray) create_stray_inode(mds->get_nodeid());

  frag_t fg = stray->pick_dirfrag(straydname);

  CDir *straydir = stray->get_or_open_dirfrag(this, fg);
  
  CDentry *straydn = straydir->lookup(straydname);
  if (!straydn) 
    straydn = straydir->add_null_dentry(straydname);
  
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

void MDCache::list_subtrees(list<CDir*>& ls)
{
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p)
    ls.push_back(p->first);
}

/*
 * adjust the dir_auth of a subtree.
 * merge with parent and/or child subtrees, if is it appropriate.
 * merge can ONLY happen if both parent and child have unambiguous auth.
 */
void MDCache::adjust_subtree_auth(CDir *dir, pair<int,int> auth)
{
  dout(7) << "adjust_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir << dendl;

  show_subtrees();

  CDir *root;
  if (dir->ino() < MDS_INO_BASE) {
    root = dir;  // bootstrap hack.
    if (subtrees.count(root) == 0) {
      subtrees[root].clear();
      root->get(CDir::PIN_SUBTREE);
    }
  } else {
    root = get_subtree_root(dir);  // subtree root
  }
  assert(root);
  assert(subtrees.count(root));
  dout(7) << " current root is " << *root << dendl;

  if (root == dir) {
    // i am already a subtree.
    dir->set_dir_auth(auth);
  } else {
    // i am a new subtree.
    dout(10) << "  new subtree at " << *dir << dendl;
    assert(subtrees.count(dir) == 0);
    subtrees[dir].clear();      // create empty subtree bounds list for me.
    dir->get(CDir::PIN_SUBTREE);

    // set dir_auth
    dir->set_dir_auth(auth);
    
    // move items nested beneath me, under me.
    set<CDir*>::iterator p = subtrees[root].begin();
    while (p != subtrees[root].end()) {
      set<CDir*>::iterator next = p;
      next++;
      if (get_subtree_root((*p)->get_parent_dir()) == dir) {
	// move under me
	dout(10) << "  claiming child bound " << **p << dendl;
	subtrees[dir].insert(*p); 
	subtrees[root].erase(p);
      }
      p = next;
    }
    
    // i am a bound of the parent subtree.
    subtrees[root].insert(dir); 

    // i am now the subtree root.
    root = dir;

    // adjust recursive pop counters
    if (dir->is_auth()) {
      CDir *p = dir->get_parent_dir();
      while (p) {
	p->pop_auth_subtree -= dir->pop_auth_subtree;
	if (p->is_subtree_root()) break;
	p = p->inode->get_parent_dir();
      }
    }

    eval_subtree_root(dir);
  }

  show_subtrees();
}


void MDCache::try_subtree_merge(CDir *dir)
{
  dout(7) << "try_subtree_merge " << *dir << dendl;
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

class C_MDC_SubtreeMergeWB : public Context {
  MDCache *mdcache;
  CInode *in;
  LogSegment *ls;
public:
  C_MDC_SubtreeMergeWB(MDCache *mdc, CInode *i, LogSegment *s) : mdcache(mdc), in(i), ls(s) {}
  void finish(int r) { 
    mdcache->subtree_merge_writebehind_finish(in, ls);
  }
};

void MDCache::try_subtree_merge_at(CDir *dir)
{
  dout(10) << "try_subtree_merge_at " << *dir << dendl;
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
    dout(10) << "  subtree merge at " << *dir << dendl;
    dir->set_dir_auth(CDIR_AUTH_DEFAULT);
    
    // move our bounds under the parent
    for (set<CDir*>::iterator p = subtrees[dir].begin();
	 p != subtrees[dir].end();
	 ++p) 
      subtrees[parent].insert(*p);
    
    // we are no longer a subtree or bound
    dir->put(CDir::PIN_SUBTREE);
    subtrees.erase(dir);
    subtrees[parent].erase(dir);

    // adjust popularity?
    if (dir->is_auth()) {
      CDir *p = dir->get_parent_dir();
      while (p) {
	p->pop_auth_subtree += dir->pop_auth_subtree;
	if (p->is_subtree_root()) break;
	p = p->inode->get_parent_dir();
      }
    }

    eval_subtree_root(dir);

    // journal inode? 
    //  (this is a large hammer to ensure that dirfragtree updates will
    //   hit the disk before the relevant dirfrags ever close)
    if (dir->inode->is_auth() &&
	dir->inode->can_auth_pin() &&
	(mds->is_active() || mds->is_stopping())) {
      CInode *in = dir->inode;
      dout(10) << "try_subtree_merge_at journaling merged bound " << *in << dendl;
      
      in->auth_pin();

      // journal write-behind.
      inode_t *pi = in->project_inode();
      pi->version = in->pre_dirty();
      
      EUpdate *le = new EUpdate(mds->mdlog, "subtree merge writebehind");
      le->metablob.add_dir_context(in->get_parent_dn()->get_dir());
      le->metablob.add_primary_dentry(in->get_parent_dn(), true, 0, pi);
      
      mds->mdlog->submit_entry(le);
      mds->mdlog->wait_for_sync(new C_MDC_SubtreeMergeWB(this, in, 
							 mds->mdlog->get_current_segment()));
    }
  } 

  show_subtrees(15);
}

void MDCache::subtree_merge_writebehind_finish(CInode *in, LogSegment *ls)
{
  dout(10) << "subtree_merge_writebehind_finish on " << in << dendl;
  in->pop_and_dirty_projected_inode(ls);
  in->auth_unpin();
}

void MDCache::eval_subtree_root(CDir *dir)
{
  // evaluate subtree inode dirlock?
  //  (we should scatter the dirlock on subtree bounds)
  if (dir->inode->is_auth() &&
      dir->inode->dirlock.is_stable()) {
    // force the issue a bit
    if (!dir->inode->is_frozen())
      mds->locker->scatter_eval(&dir->inode->dirlock);
    else
      mds->locker->try_scatter_eval(&dir->inode->dirlock);  // ** may or may not be auth_pinned **
  }
  
}


void MDCache::adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, pair<int,int> auth)
{
  dout(7) << "adjust_bounded_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir
	  << " bounds " << bounds
	  << dendl;

  show_subtrees();

  CDir *root;
  if (dir->ino() < MDS_INO_BASE) {
    root = dir;  // bootstrap hack.
    if (subtrees.count(root) == 0) {
      subtrees[root].clear();
      root->get(CDir::PIN_SUBTREE);
    }
  } else {
    root = get_subtree_root(dir);  // subtree root
  }
  assert(root);
  assert(subtrees.count(root));
  dout(7) << " current root is " << *root << dendl;

  pair<int,int> oldauth = dir->authority();

  if (root == dir) {
    // i am already a subtree.
    dir->set_dir_auth(auth);
  } else {
    // i am a new subtree.
    dout(10) << "  new subtree at " << *dir << dendl;
    assert(subtrees.count(dir) == 0);
    subtrees[dir].clear();      // create empty subtree bounds list for me.
    dir->get(CDir::PIN_SUBTREE);
    
    // set dir_auth
    dir->set_dir_auth(auth);
    
    // move items nested beneath me, under me.
    set<CDir*>::iterator p = subtrees[root].begin();
    while (p != subtrees[root].end()) {
      set<CDir*>::iterator next = p;
      next++;
      if (get_subtree_root((*p)->get_parent_dir()) == dir) {
	// move under me
	dout(10) << "  claiming child bound " << **p << dendl;
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
	dout(10) << "  new bound " << *bound << ", adjusting auth back to old " << oldauth << dendl;
	adjust_subtree_auth(bound, oldauth);       // otherwise, adjust at bound.
      }
      else {
	dout(10) << "  want bound " << *bound << dendl;
	// make sure it's nested beneath ambiguous subtree(s)
	while (1) {
	  CDir *t = get_subtree_root(bound->get_parent_dir());
	  if (t == dir) break;
	  while (subtrees[dir].count(t) == 0)
	    t = get_subtree_root(t->get_parent_dir());
	  dout(10) << "  swallowing intervening subtree at " << *t << dendl;
	  adjust_subtree_auth(t, auth);
	  try_subtree_merge_at(t);
	}
      }
    }
    else {
      dout(10) << "  already have bound " << *bound << dendl;
    }
  }
  // merge stray bounds?
  set<CDir*>::iterator p = subtrees[dir].begin();
  while (p != subtrees[dir].end()) {
    set<CDir*>::iterator n = p;
    n++;
    if (bounds.count(*p) == 0) {
      CDir *stray = *p;
      dout(10) << "  swallowing extra subtree at " << *stray << dendl;
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
	  << dendl;
  
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

void MDCache::map_dirfrag_set(list<dirfrag_t>& dfs, set<CDir*>& result)
{
  // group by inode
  map<inodeno_t, fragset_t> ino_fragset;
  for (list<dirfrag_t>::iterator p = dfs.begin(); p != dfs.end(); ++p)
    ino_fragset[p->ino].insert(p->frag);

  // get frags
  for (map<inodeno_t, fragset_t>::iterator p = ino_fragset.begin();
       p != ino_fragset.end();
       ++p) {
    CInode *in = get_inode(p->first);
    if (!in) continue;

    list<frag_t> fglist;
    for (set<frag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q)
      in->dirfragtree.get_leaves_under(*q, fglist);

    dout(15) << "map_dirfrag_set " << p->second << " -> " << fglist
	     << " on " << *in << dendl;

    for (list<frag_t>::iterator q = fglist.begin(); q != fglist.end(); ++q) {
      CDir *dir = in->get_dirfrag(*q);
      if (dir) result.insert(dir);
    }
  }
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
  dout(10) << "remove_subtree " << *dir << dendl;
  assert(subtrees.count(dir));
  assert(subtrees[dir].empty());
  subtrees.erase(dir);
  dir->put(CDir::PIN_SUBTREE);
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
    dout(0) << "verify_subtree_bounds failed" << dendl;
    set<CDir*> b = bounds;
    for (set<CDir*>::iterator p = subtrees[dir].begin();
	 p != subtrees[dir].end();
	 ++p) {
      if (bounds.count(*p)) {
	b.erase(*p);
	continue;
      }
      dout(0) << "  missing bound " << **p << dendl;
    }
    for (set<CDir*>::iterator p = b.begin();
	 p != b.end();
	 ++p) 
      dout(0) << "    extra bound " << **p << dendl;
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
      dout(0) << "verify_subtree_bounds failed: extra bound " << *bd << dendl;
      failed++;
    }
  }
  assert(failed == 0);
}

void MDCache::adjust_subtree_after_rename(CInode *diri, CDir *olddir)
{
  dout(10) << "adjust_subtree_after_rename " << *diri << " from " << *olddir << dendl;

  //show_subtrees();

  list<CDir*> dfls;
  diri->get_dirfrags(dfls);
  for (list<CDir*>::iterator p = dfls.begin(); p != dfls.end(); ++p) {
    CDir *dir = *p;

    dout(10) << "dirfrag " << *dir << dendl;
    CDir *oldparent = get_subtree_root(olddir);
    dout(10) << " old parent " << *oldparent << dendl;
    CDir *newparent = get_subtree_root(diri->get_parent_dir());
    dout(10) << " new parent " << *newparent << dendl;

    if (oldparent == newparent) {
      dout(10) << "parent unchanged for " << *dir << " at " << *oldparent << dendl;
      continue;
    }

    if (dir->is_subtree_root()) {
      // children are fine.  change parent.
      dout(10) << "moving " << *dir << " from " << *oldparent << " to " << *newparent << dendl;
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
	dout(10) << "moving bound " << *bound << " from " << *oldparent << " to " << *newparent << dendl;
	subtrees[oldparent].erase(bound);
	subtrees[newparent].insert(bound);
      }	   

      // did auth change?
      if (oldparent->authority() != newparent->authority()) 
	adjust_subtree_auth(dir, oldparent->authority());  // caller is responsible for *diri.
    }
  }

  for (list<CDir*>::iterator p = dfls.begin(); p != dfls.end(); ++p) {
    CDir *dir = *p;

    // un-force dir to subtree root
    if (dir->dir_auth == pair<int,int>(dir->dir_auth.first, dir->dir_auth.first)) {
      adjust_subtree_auth(dir, dir->dir_auth.first);
      try_subtree_merge_at(dir);
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


ESubtreeMap *MDCache::create_subtree_map() 
{
  dout(10) << "create_subtree_map " << num_subtrees() << " subtrees, " 
	   << num_subtrees_fullauth() << " fullauth"
	   << dendl;
  
  ESubtreeMap *le = new ESubtreeMap();
  
  // include all auth subtrees, and their bounds.
  // and a spanning tree to tie it to the root.
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    if (!dir->is_auth()) continue;

    dout(15) << " subtree " << *dir << dendl;
    le->subtrees[dir->dirfrag()].clear();
    le->metablob.add_dir_context(dir, EMetaBlob::TO_ROOT);
    le->metablob.add_dir(dir, false);

    // bounds
    for (set<CDir*>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CDir *bound = *q;
      dout(15) << " subtree bound " << *bound << dendl;
      le->subtrees[dir->dirfrag()].push_back(bound->dirfrag());
      le->metablob.add_dir_context(bound, EMetaBlob::TO_ROOT);
      le->metablob.add_dir(bound, false);
    }
  }

  //le->metablob.print(cout);
  return le;
}


void MDCache::send_resolve(int who)
{
  if (migrator->is_importing() || 
      migrator->is_exporting())
    send_resolve_later(who);
  else
    send_resolve_now(who);
}

void MDCache::send_resolve_later(int who)
{
  dout(10) << "send_resolve_later to mds" << who << dendl;
  wants_resolve.insert(who);
}

void MDCache::maybe_send_pending_resolves()
{
  if (wants_resolve.empty())
    return;  // nothing to send.

  // only if it's appropriate!
  if (migrator->is_exporting() ||
      migrator->is_importing()) {
    dout(7) << "maybe_send_pending_resolves waiting, imports/exports still in progress" << dendl;
    migrator->show_importing();
    migrator->show_exporting();
    return;  // not now
  }
  
  // ok, send them.
  for (set<int>::iterator p = wants_resolve.begin();
       p != wants_resolve.end();
       p++) 
    send_resolve_now(*p);
  wants_resolve.clear();
}


class C_MDC_SendResolve : public Context {
  MDCache *mdc;
  int who;
public:
  C_MDC_SendResolve(MDCache *c, int w) : mdc(c), who(w) { }
  void finish(int r) {
    mdc->send_resolve_now(who);
  }
};

void MDCache::send_resolve_now(int who)
{
  dout(10) << "send_resolve_now to mds" << who << dendl;
  MMDSResolve *m = new MMDSResolve;

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
      //  NOTE: because we are first authority, import state is at least IMPORT_LOGGINSTART.
      assert(migrator->get_import_state(dir->dirfrag()) >= Migrator::IMPORT_LOGGINGSTART);
      set<CDir*> bounds;
      get_subtree_bounds(dir, bounds);
      list<dirfrag_t> dfls;
      for (set<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p)
	dfls.push_back((*p)->dirfrag());
      m->add_ambiguous_import(dir->dirfrag(), dfls);
    } else {
      // not ambiguous.
      m->add_subtree(dir->dirfrag());
      
      // bounds too
      for (set<CDir*>::iterator q = subtrees[dir].begin();
	   q != subtrees[dir].end();
	   ++q) {
	CDir *bound = *q;
	m->add_subtree_bound(dir->dirfrag(), bound->dirfrag());
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
      dout(10) << " including uncommitted " << *p->second << dendl;
      m->add_slave_request(p->first);
    }
  }
  // [resolving]
  if (uncommitted_slave_updates.count(who)) {
    for (map<metareqid_t, MDSlaveUpdate*>::iterator p = uncommitted_slave_updates[who].begin();
	 p != uncommitted_slave_updates[who].end();
	 ++p) {
      dout(10) << " including uncommitted " << p->first << dendl;
      m->add_slave_request(p->first);
    }
    need_resolve_ack.insert(who);
  }


  // send
  mds->send_message_mds(m, who);
}


void MDCache::handle_mds_failure(int who)
{
  dout(7) << "handle_mds_failure mds" << who << dendl;
  
  // make note of recovery set
  mds->mdsmap->get_recovery_mds_set(recovery_set);
  recovery_set.erase(mds->get_nodeid());
  dout(1) << "handle_mds_failure mds" << who << " : recovery peers are " << recovery_set << dendl;

  // adjust my recovery lists
  wants_resolve.erase(who);   // MDS will ask again
  got_resolve.erase(who);     // i'll get another.

  rejoin_sent.erase(who);        // i need to send another
  rejoin_ack_gather.erase(who);  // i'll need/get another.

  dout(10) << " wants_resolve " << wants_resolve << dendl;
  dout(10) << " got_resolve " << got_resolve << dendl;
  dout(10) << " rejoin_sent " << rejoin_sent << dendl;
  dout(10) << " rejoin_gather " << rejoin_gather << dendl;
  dout(10) << " rejoin_ack_gather " << rejoin_ack_gather << dendl;

 
  // tell the migrator too.
  migrator->handle_mds_failure_or_stop(who);

  // kick any discovers that are waiting
  kick_discovers(who);
  
  // clean up any requests slave to/from this node
  list<MDRequest*> finish;
  for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
       p != active_requests.end();
       ++p) {
    // slave to the failed node?
    if (p->second->slave_to_mds == who) {
      if (p->second->slave_did_prepare()) {
	dout(10) << " slave request " << *p->second << " uncommitted, will resolve shortly" << dendl;
      } else {
	dout(10) << " slave request " << *p->second << " has no prepare, finishing up" << dendl;
	if (p->second->slave_request)
	  p->second->aborted = true;
	else
	  finish.push_back(p->second);
      }
    }
    
    // failed node is slave?
    if (!p->second->committing) {
      if (p->second->more()->witnessed.count(who)) {
	dout(10) << " master request " << *p->second << " no longer witnessed by slave mds" << who
		 << dendl;
	// discard this peer's prepare (if any)
	p->second->more()->witnessed.erase(who);
      }
      
      if (p->second->more()->waiting_on_slave.count(who)) {
	dout(10) << " master request " << *p->second << " waiting for slave mds" << who
		 << " to recover" << dendl;
	// retry request when peer recovers
	p->second->more()->waiting_on_slave.erase(who);
	mds->wait_for_active_peer(who, new C_MDS_RetryRequest(this, p->second));
      }
    }
  }

  while (!finish.empty()) {
    dout(10) << "cleaning up slave request " << *finish.front() << dendl;
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
  dout(7) << "handle_mds_recovery mds" << who << dendl;

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
      for (CDir::map_t::iterator p = d->items.begin();
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
  dout(7) << "set_recovery_set " << s << dendl;
  recovery_set = s;
}


/*
 * during resolve state, we share resolves to determine who
 * is authoritative for which trees.  we expect to get an resolve
 * from _everyone_ in the recovery_set (the mds cluster at the time of
 * the first failure).
 */
void MDCache::handle_resolve(MMDSResolve *m)
{
  dout(7) << "handle_resolve from " << m->get_source() << dendl;
  int from = m->get_source().num();

  // ambiguous slave requests?
  if (!m->slave_requests.empty()) {
    MMDSResolveAck *ack = new MMDSResolveAck;

    for (list<metareqid_t>::iterator p = m->slave_requests.begin();
	 p != m->slave_requests.end();
	 ++p) {
      if (mds->sessionmap.have_completed_request(*p)) {
	// COMMIT
	dout(10) << " ambiguous slave request " << *p << " will COMMIT" << dendl;
	ack->add_commit(*p);
      } else {
	// ABORT
	dout(10) << " ambiguous slave request " << *p << " will ABORT" << dendl;
	ack->add_abort(*p);      
      }
    }

    mds->send_message_mds(ack, from);
  }

  // am i a surviving ambiguous importer?
  if (mds->is_active() || mds->is_stopping()) {
    // check for any import success/failure (from this node)
    map<dirfrag_t, list<dirfrag_t> >::iterator p = my_ambiguous_imports.begin();
    while (p != my_ambiguous_imports.end()) {
      map<dirfrag_t, list<dirfrag_t> >::iterator next = p;
      next++;
      CDir *dir = get_dirfrag(p->first);
      assert(dir);
      dout(10) << "checking ambiguous import " << *dir << dendl;
      if (migrator->is_importing(dir->dirfrag()) &&
	  migrator->get_import_peer(dir->dirfrag()) == from) {
	assert(migrator->get_import_state(dir->dirfrag()) == Migrator::IMPORT_ACKING);
	
	// check if sender claims the subtree
	bool claimed_by_sender = false;
	for (map<dirfrag_t, list<dirfrag_t> >::iterator q = m->subtrees.begin();
	     q != m->subtrees.end();
	     ++q) {
	  CDir *base = get_dirfrag(q->first);
	  if (!base || !base->contains(dir)) 
	    continue;  // base not dir or an ancestor of dir, clearly doesn't claim dir.

	  bool inside = true;
	  for (list<dirfrag_t>::iterator r = q->second.begin();
	       r != q->second.end();
	       ++r) {
	    CDir *bound = get_dirfrag(*r);
	    if (bound && bound->contains(dir)) {
	      inside = false;  // nope, bound is dir or parent of dir, not inside.
	      break;
	    }
	  }
	  if (inside)
	    claimed_by_sender = true;
	}

	if (claimed_by_sender) {
	  dout(7) << "ambiguous import failed on " << *dir << dendl;
	  migrator->import_reverse(dir);
	} else {
	  dout(7) << "ambiguous import succeeded on " << *dir << dendl;
	  migrator->import_finish(dir);
	}
	my_ambiguous_imports.erase(p);  // no longer ambiguous.
      }
      p = next;
    }
  }    

  // update my dir_auth values
  for (map<dirfrag_t, list<dirfrag_t> >::iterator pi = m->subtrees.begin();
       pi != m->subtrees.end();
       ++pi) {
    CInode *diri = get_inode(pi->first.ino);
    if (!diri) continue;
    bool forced = diri->dirfragtree.force_to_leaf(pi->first.frag);
    if (forced) {
      dout(10) << " forced frag " << pi->first.frag << " to leaf in " 
	       << diri->dirfragtree 
	       << " on " << pi->first << dendl;
    }

    CDir *dir = diri->get_dirfrag(pi->first.frag);
    if (!dir) continue;

    adjust_bounded_subtree_auth(dir, pi->second, from);
    try_subtree_merge(dir);
  }

  show_subtrees();


  // note ambiguous imports too
  for (map<dirfrag_t, list<dirfrag_t> >::iterator pi = m->ambiguous_imports.begin();
       pi != m->ambiguous_imports.end();
       ++pi) {
    dout(10) << "noting ambiguous import on " << pi->first << " bounds " << pi->second << dendl;
    other_ambiguous_imports[from][pi->first].swap( pi->second );
  }
  
  // did i get them all?
  got_resolve.insert(from);
  
  maybe_resolve_finish();

  delete m;
}

void MDCache::maybe_resolve_finish()
{
  if (got_resolve != recovery_set) {
    dout(10) << "maybe_resolve_finish still waiting for more resolves, got (" << got_resolve 
	     << "), need (" << recovery_set << ")" << dendl;
  } 
  else if (!need_resolve_ack.empty()) {
    dout(10) << "maybe_resolve_finish still waiting for resolve_ack from (" << need_resolve_ack << ")" << dendl;
  } 
  else {
    dout(10) << "maybe_resolve_finish got all resolves+resolve_acks, done." << dendl;
    disambiguate_imports();
    if (mds->is_resolve()) {
      recalc_auth_bits();
      trim_non_auth(); 
      mds->resolve_done();
    }
  } 
}

void MDCache::handle_resolve_ack(MMDSResolveAck *ack)
{
  dout(10) << "handle_resolve_ack " << *ack << " from " << ack->get_source() << dendl;
  int from = ack->get_source().num();

  for (list<metareqid_t>::iterator p = ack->commit.begin();
       p != ack->commit.end();
       ++p) {
    dout(10) << " commit on slave " << *p << dendl;
    
    if (mds->is_resolve()) {
      // replay
      assert(uncommitted_slave_updates[from].count(*p));
      uncommitted_slave_updates[from][*p]->commit.replay(mds);
      delete uncommitted_slave_updates[from][*p];
      uncommitted_slave_updates[from].erase(*p);
      // log commit
      mds->mdlog->submit_entry(new ESlaveUpdate(mds->mdlog, "unknown", *p, from, ESlaveUpdate::OP_COMMIT));
    } else {
      MDRequest *mdr = request_get(*p);
      assert(mdr->slave_request == 0);  // shouldn't be doing anything!
      request_finish(mdr);
    }
  }

  for (list<metareqid_t>::iterator p = ack->abort.begin();
       p != ack->abort.end();
       ++p) {
    dout(10) << " abort on slave " << *p << dendl;

    if (mds->is_resolve()) {
      assert(uncommitted_slave_updates[from].count(*p));
      uncommitted_slave_updates[from][*p]->rollback.replay(mds);
      delete uncommitted_slave_updates[from][*p];
      uncommitted_slave_updates[from].erase(*p);
      mds->mdlog->submit_entry(new ESlaveUpdate(mds->mdlog, "unknown", *p, from, ESlaveUpdate::OP_ROLLBACK));
    } else {
      MDRequest *mdr = request_get(*p);
      if (mdr->more()->slave_commit) {
	mdr->more()->slave_commit->finish(-1);
	delete mdr->more()->slave_commit;
	mdr->more()->slave_commit = 0;
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
  dout(10) << "disambiguate_imports" << dendl;

  // other nodes' ambiguous imports
  for (map<int, map<dirfrag_t, list<dirfrag_t> > >::iterator p = other_ambiguous_imports.begin();
       p != other_ambiguous_imports.end();
       ++p) {
    int who = p->first;
    dout(10) << "ambiguous imports for mds" << who << dendl;

    for (map<dirfrag_t, list<dirfrag_t> >::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << " ambiguous import " << q->first << " bounds " << q->second << dendl;
      CDir *dir = get_dirfrag(q->first);
      if (!dir) continue;
      
      if (dir->authority().first == CDIR_AUTH_UNKNOWN ||   // if i am resolving
	  dir->is_ambiguous_auth()) {                      // if i am a surviving bystander
	dout(10) << "  mds" << who << " did import " << *dir << dendl;
	adjust_bounded_subtree_auth(dir, q->second, who);
	try_subtree_merge(dir);
      } else {
	dout(10) << "  mds" << who << " did not import " << *dir << dendl;
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
      dout(10) << "ambiguous import auth known, must not be me " << *dir << dendl;
      cancel_ambiguous_import(q->first);
      mds->mdlog->submit_entry(new EImportFinish(dir, false));
    } else {
      dout(10) << "ambiguous import auth unknown, must be me " << *dir << dendl;
      finish_ambiguous_import(q->first);
      mds->mdlog->submit_entry(new EImportFinish(dir, true));
    }
  }
  assert(my_ambiguous_imports.empty());

  if (mds->is_resolve()) {
    // verify all my subtrees are unambiguous!
    for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      CDir *dir = p->first;
      if (dir->is_ambiguous_dir_auth()) {
	dout(0) << "disambiguate_imports uh oh, dir_auth is still ambiguous for " << *dir << dendl;
	show_subtrees();
      }
      assert(!dir->is_ambiguous_dir_auth());
    }
  }

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
	   << dendl;
  my_ambiguous_imports.erase(df);
}

void MDCache::finish_ambiguous_import(dirfrag_t df)
{
  assert(my_ambiguous_imports.count(df));
  list<dirfrag_t> bounds;
  bounds.swap(my_ambiguous_imports[df]);
  my_ambiguous_imports.erase(df);
  
  dout(10) << "finish_ambiguous_import " << df
	   << " bounds " << bounds
	   << dendl;
  CDir *dir = get_dirfrag(df);
  assert(dir);
  
  // adjust dir_auth, import maps
  adjust_bounded_subtree_auth(dir, bounds, mds->get_nodeid());
  try_subtree_merge(dir);
}


/** recalc_auth_bits()
 * once subtree auth is disambiguated, we need to adjust all the 
 * auth and dirty bits in our cache before moving on.
 */
void MDCache::recalc_auth_bits()
{
  dout(7) << "recalc_auth_bits" << dendl;

  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    list<CDir*> dfq;  // dirfrag queue
    dfq.push_back(p->first);

    bool auth = p->first->authority().first == mds->get_nodeid();
    dout(10) << " subtree auth=" << auth << " for " << *p->first << dendl;

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
      for (CDir::map_t::iterator q = dir->items.begin();
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



// ===========================================================================
// REJOIN


/*
 * rejoin phase!
 *
 * this initiates rejoin.  it shoudl be called before we get any
 * rejoin or rejoin_ack messages (or else mdsmap distribution is broken).
 *
 * we start out by sending rejoins to everyone in the recovery set.
 *
 * if we are rejoin, send for all regions in our cache.
 * if we are active|stopping, send only to nodes that are are rejoining.
 */
void MDCache::rejoin_send_rejoins()
{
  dout(10) << "rejoin_send_rejoins with recovery_set " << recovery_set << dendl;

  map<int, MMDSCacheRejoin*> rejoins;

  // encode cap list once.
  bufferlist cap_export_bl;
  if (mds->is_rejoin()) {
    ::_encode(cap_exports, cap_export_bl);
    ::_encode(cap_export_paths, cap_export_bl);
  }

  // if i am rejoining, send a rejoin to everyone.
  // otherwise, just send to others who are rejoining.
  for (set<int>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) {
    if (*p == mds->get_nodeid())  continue;  // nothing to myself!
    if (rejoin_sent.count(*p)) continue;     // already sent a rejoin to this node!
    if (mds->is_rejoin()) {
      rejoin_gather.insert(*p);
      rejoins[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_WEAK);
      rejoins[*p]->copy_cap_exports(cap_export_bl);
    } else if (mds->mdsmap->is_rejoin(*p))
      rejoins[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_STRONG);
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

    rejoin_walk(dir, rejoins[auth]);
  }
  
  // rejoin root inodes, too
  for (map<int, MMDSCacheRejoin*>::iterator p = rejoins.begin();
       p != rejoins.end();
       ++p) {
    if (mds->is_rejoin()) {
      // weak
      if (p->first == 0 && root) 
	p->second->add_weak_inode(root->ino());
      if (get_inode(MDS_INO_STRAY(p->first))) 
	p->second->add_weak_inode(MDS_INO_STRAY(p->first));
    } else {
      // strong
      if (p->first == 0 && root) {
	p->second->add_weak_inode(root->ino());
	p->second->add_strong_inode(root->ino(), root->get_replica_nonce(),
				    root->get_caps_wanted(),
				    root->authlock.get_state(),
				    root->linklock.get_state(),
				    root->dirfragtreelock.get_state(),
				    root->filelock.get_state(),
				    root->dirlock.get_state());
      }
      if (CInode *in = get_inode(MDS_INO_STRAY(p->first))) {
    	p->second->add_weak_inode(in->ino());
  	p->second->add_strong_inode(in->ino(), in->get_replica_nonce(),
				    in->get_caps_wanted(),
				    in->authlock.get_state(),
				    in->linklock.get_state(),
				    in->dirfragtreelock.get_state(),
				    in->filelock.get_state(),
				    in->dirlock.get_state());
      }
    }
  }  

  if (!mds->is_rejoin()) {
    // i am survivor.  send strong rejoin.
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
	  
	  dout(15) << " " << *p->second << " authpin on " << **q << dendl;
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
	  
	  dout(15) << " " << *p->second << " xlock on " << **q << " " << *(*q)->get_parent() << dendl;
	  MDSCacheObjectInfo i;
	  (*q)->get_parent()->set_object_info(i);
	  if (i.ino)
	    rejoin->add_inode_xlock(i.ino, (*q)->get_type(), p->second->reqid);
	  else
	    rejoin->add_dentry_xlock(i.dirfrag, i.dname, p->second->reqid);
	}
      }
    }
  }

  // send the messages
  for (map<int,MMDSCacheRejoin*>::iterator p = rejoins.begin();
       p != rejoins.end();
       ++p) {
    assert(rejoin_sent.count(p->first) == 0);
    assert(rejoin_ack_gather.count(p->first) == 0);
    rejoin_sent.insert(p->first);
    rejoin_ack_gather.insert(p->first);
    mds->send_message_mds(p->second, p->first);
  }

  // nothing?
  if (mds->is_rejoin() && rejoins.empty()) {
    dout(10) << "nothing to rejoin" << dendl;
    mds->rejoin_done();
  }
}


/** 
 * rejoin_walk - build rejoin declarations for a subtree
 * 
 * @dir subtree root
 * @rejoin rejoin message
 *
 * from a rejoining node:
 *  weak dirfrag
 *  weak dentries (w/ connectivity)
 *
 * from a surviving node:
 *  strong dirfrag
 *  strong dentries (no connectivity!)
 *  strong inodes
 */
void MDCache::rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin)
{
  dout(10) << "rejoin_walk " << *dir << dendl;

  list<CDir*> nested;  // finish this dir, then do nested items
  
  if (mds->is_rejoin()) {
    // WEAK
    dout(15) << " add_weak_dirfrag " << *dir << dendl;
    rejoin->add_weak_dirfrag(dir->dirfrag());

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) {
      CDentry *dn = p->second;
      dout(15) << " add_weak_primary_dentry " << *dn << dendl;
      assert(dn->is_primary());
      assert(dn->inode->is_dir());
      rejoin->add_weak_primary_dentry(dir->dirfrag(), p->first, dn->get_inode()->ino());
      dn->get_inode()->get_nested_dirfrags(nested);

      if (dn->get_inode()->dirlock.is_updated()) {
	// include full inode to shed any dirtyscattered state
	rejoin->add_full_inode(dn->get_inode()->inode, 
			       dn->get_inode()->symlink,
			       dn->get_inode()->dirfragtree);
	dn->get_inode()->dirlock.clear_updated();
      }
    }
  } else {
    // STRONG
    dout(15) << " add_strong_dirfrag " << *dir << dendl;
    rejoin->add_strong_dirfrag(dir->dirfrag(), dir->get_replica_nonce(), dir->get_dir_rep());

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) {
      CDentry *dn = p->second;
      dout(15) << " add_strong_dentry " << *dn << dendl;
      rejoin->add_strong_dentry(dir->dirfrag(), p->first, 
				dn->is_primary() ? dn->get_inode()->ino():inodeno_t(0),
				dn->is_remote() ? dn->get_remote_ino():inodeno_t(0),
				dn->is_remote() ? dn->get_remote_d_type():0, 
				dn->get_replica_nonce(),
				dn->lock.get_state());
      if (dn->is_primary()) {
	CInode *in = dn->get_inode();
	dout(15) << " add_strong_inode " << *in << dendl;
	rejoin->add_strong_inode(in->ino(), in->get_replica_nonce(),
				 in->get_caps_wanted(),
				 in->authlock.get_state(),
				 in->linklock.get_state(),
				 in->dirfragtreelock.get_state(),
				 in->filelock.get_state(),
				 in->dirlock.get_state());
	in->get_nested_dirfrags(nested);
      }
    }
  }

  // recurse into nested dirs
  for (list<CDir*>::iterator p = nested.begin(); 
       p != nested.end();
       ++p)
    rejoin_walk(*p, rejoin);
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
  dout(7) << "handle_cache_rejoin " << *m << " from " << m->get_source() 
	  << " (" << m->get_payload().length() << " bytes)"
	  << dendl;

  switch (m->op) {
  case MMDSCacheRejoin::OP_WEAK:
    handle_cache_rejoin_weak(m);
    break;
  case MMDSCacheRejoin::OP_STRONG:
    handle_cache_rejoin_strong(m);
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


/*
 * handle_cache_rejoin_weak
 *
 * the sender 
 *  - is recovering from their journal.
 *  - may have incorrect (out of date) inode contents
 *  - will include full inodes IFF they contain dirty scatterlock content
 *
 * if the sender didn't trim_non_auth(), they
 *  - may have incorrect (out of date) dentry/inode linkage
 *  - may have deleted/purged inodes
 * and i may have to go to disk to get accurate inode contents.  yuck.
 */
void MDCache::handle_cache_rejoin_weak(MMDSCacheRejoin *weak)
{
  int from = weak->get_source().num();

  // possible response(s)
  MMDSCacheRejoin *ack = 0;      // if survivor
  bool survivor = false;  // am i a survivor?
  
  if (mds->is_active() || mds->is_stopping()) {
    survivor = true;
    dout(10) << "i am a surivivor, and will ack immediately" << dendl;
    ack = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);

    // check cap exports
    for (map<inodeno_t,map<int,inode_caps_reconnect_t> >::iterator p = weak->cap_exports.begin();
	 p != weak->cap_exports.end();
	 ++p) {
      CInode *in = get_inode(p->first);
      if (!in || !in->is_auth()) continue;
      for (map<int,inode_caps_reconnect_t>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	dout(10) << " claiming cap import " << p->first << " client" << q->first << " on " << *in << dendl;
	rejoin_import_cap(in, q->first, q->second, from);
      }
    }
  } else {
    assert(mds->is_rejoin());

    // check cap exports.
    for (map<inodeno_t,map<int,inode_caps_reconnect_t> >::iterator p = weak->cap_exports.begin();
	 p != weak->cap_exports.end();
	 ++p) {
      CInode *in = get_inode(p->first);
      if (in && !in->is_auth()) continue;
      if (!in) {
	if (!path_is_mine(weak->cap_export_paths[p->first]))
	  continue;
	cap_import_paths[p->first] = weak->cap_export_paths[p->first];
	dout(10) << " noting cap import " << p->first << " path " << weak->cap_export_paths[p->first] << dendl;
      }
      
      // note
      for (map<int,inode_caps_reconnect_t>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	dout(10) << " claiming cap import " << p->first << " client" << q->first << dendl;
	cap_imports[p->first][q->first][from] = q->second;
      }
    }
  }

  // full inodes?
  //   dirty scatterlock content!
  for (list<MMDSCacheRejoin::inode_full>::iterator p = weak->full_inodes.begin();
       p != weak->full_inodes.end();
       ++p) {
    CInode *in = get_inode(p->inode.ino);
    if (!in) continue;
    if (p->inode.mtime > in->inode.mtime) in->inode.mtime = p->inode.mtime;
    dout(10) << " got dirty inode scatterlock content " << *in << dendl;
    in->dirlock.set_updated();
  }

  // walk weak map
  for (map<dirfrag_t, map<string, MMDSCacheRejoin::dn_weak> >::iterator p = weak->weak.begin();
       p != weak->weak.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) dout(0) << " missing dirfrag " << p->first << dendl;
    assert(dir);

    int nonce = dir->add_replica(from);
    dout(10) << " have " << *dir << dendl;
    if (ack) 
      ack->add_strong_dirfrag(p->first, nonce, dir->dir_rep);
    
    // weak dentries
    for (map<string,MMDSCacheRejoin::dn_weak>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first);
      assert(dn);
      assert(dn->is_primary());
      
      if (survivor && dn->is_replica(from)) 
	dentry_remove_replica(dn, from);  // this induces a lock gather completion
      int dnonce = dn->add_replica(from);
      dout(10) << " have " << *dn << dendl;
      if (ack) 
	ack->add_strong_dentry(p->first, q->first,
			       dn->get_inode()->ino(), inodeno_t(0), 0, 
			       dnonce, dn->lock.get_replica_state());

      // inode
      CInode *in = dn->get_inode();
      assert(in);

      if (survivor && in->is_replica(from)) 
	inode_remove_replica(in, from);    // this induces a lock gather completion
      int inonce = in->add_replica(from);
      dout(10) << " have " << *in << dendl;

      // scatter the dirlock, just in case?
      if (!survivor && in->is_dir())
	in->dirlock.set_state(LOCK_SCATTER);

      if (ack) {
	ack->add_full_inode(in->inode, in->symlink, in->dirfragtree);
	ack->add_strong_inode(in->ino(), 
			      inonce,
			      0,
			      in->authlock.get_replica_state(), 
			      in->linklock.get_replica_state(), 
			      in->dirfragtreelock.get_replica_state(), 
			      in->filelock.get_replica_state(),
			      in->dirlock.get_replica_state());
      }
    }
  }
  
  // weak base inodes?  (root, stray, etc.)
  for (set<inodeno_t>::iterator p = weak->weak_inodes.begin();
       p != weak->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    assert(in);   // hmm fixme wrt stray?
    if (survivor && in->is_replica(from)) 
      inode_remove_replica(in, from);    // this induces a lock gather completion
    int inonce = in->add_replica(from);
    dout(10) << " have base " << *in << dendl;
    
    if (ack) 
      ack->add_strong_inode(in->ino(), 
			    inonce,
			    0,
			    in->authlock.get_replica_state(), 
			    in->linklock.get_replica_state(), 
			    in->dirfragtreelock.get_replica_state(), 
			    in->filelock.get_replica_state(),
			    in->dirlock.get_replica_state());
  }

  if (survivor) {
    // survivor.  do everything now.
    rejoin_scour_survivor_replicas(from, ack);
    mds->send_message_mds(ack, from);
  } else {
    // done?
    assert(rejoin_gather.count(from));
    rejoin_gather.erase(from);
    if (rejoin_gather.empty()) {
      rejoin_gather_finish();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
    }
  }
}


/**
 * parallel_fetch -- make a pass at fetching a bunch of paths in parallel
 *
 * @pathmap - map of inodeno to full pathnames.  we remove items from this map 
 *            as we discover we have them.
 *
 * returns a C_Gather* is there is work to do.  caller is responsible for setting
 * the C_Gather completer.
 */
C_Gather *MDCache::parallel_fetch(map<inodeno_t,string>& pathmap)
{
  dout(10) << "parallel_fetch on " << pathmap.size() << " paths" << dendl;

  // scan list
  set<CDir*> fetch_queue;
  map<inodeno_t,string>::iterator p = pathmap.begin();
  while (p != pathmap.end()) {
    CInode *in = get_inode(p->first);
    if (in) {
      dout(15) << " have " << *in << dendl;
      pathmap.erase(p++);
      continue;
    }

    // traverse
    dout(17) << " missing " << p->first << " at " << p->second << dendl;
    filepath path(p->second);
    CDir *dir = path_traverse_to_dir(path);
    assert(dir);
    fetch_queue.insert(dir);
    p++;
  }

  if (pathmap.empty()) {
    dout(10) << "parallel_fetch done" << dendl;
    assert(fetch_queue.empty());
    return false;
  }

  // do a parallel fetch
  C_Gather *gather = new C_Gather;
  for (set<CDir*>::iterator p = fetch_queue.begin();
       p != fetch_queue.end();
       ++p) {
    dout(10) << "parallel_fetch fetching " << **p << dendl;
    (*p)->fetch(gather->new_sub());
  }
  
  return gather;
}



/*
 * rejoin_scour_survivor_replica - remove source from replica list on unmentioned objects
 *
 * all validated replicas are acked with a strong nonce, etc.  if that isn't in the
 * ack, the replica dne, and we can remove it from our replica maps.
 */
void MDCache::rejoin_scour_survivor_replicas(int from, MMDSCacheRejoin *ack)
{
  dout(10) << "rejoin_scour_survivor_replicas from mds" << from << dendl;

  // FIXME: what about root and stray inodes.
  
  for (hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    CInode *in = p->second;
    
    // inode?
    if (in->is_auth() &&
	in->is_replica(from) &&
	ack->strong_inodes.count(p->second->ino()) == 0) {
      inode_remove_replica(in, from);
      dout(10) << " rem " << *in << dendl;
    }

    if (!in->is_dir()) continue;
    
    list<CDir*> dfs;
    in->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin();
	 p != dfs.end();
	 ++p) {
      CDir *dir = *p;
      
      if (dir->is_auth() &&
	  dir->is_replica(from) &&
	  ack->strong_dirfrags.count(dir->dirfrag()) == 0) {
	dir->remove_replica(from);
	dout(10) << " rem " << *dir << dendl;
      } 
      
      // dentries
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	
	if (dn->is_replica(from) &&
	    (ack->strong_dentries.count(dir->dirfrag()) == 0 ||
	     ack->strong_dentries[dir->dirfrag()].count(dn->get_name()) == 0)) {
	  dentry_remove_replica(dn, from);
	  dout(10) << " rem " << *dn << dendl;
	}
      }
    }
  }
}


CInode *MDCache::rejoin_invent_inode(inodeno_t ino)
{
  CInode *in = new CInode(this);
  memset(&in->inode, 0, sizeof(inode_t));
  in->inode.ino = ino;
  in->state_set(CInode::STATE_REJOINUNDEF);
  add_inode(in);
  rejoin_undef_inodes.insert(in);
  dout(10) << " invented " << *in << dendl;
  return in;
}


void MDCache::handle_cache_rejoin_strong(MMDSCacheRejoin *strong)
{
  int from = strong->get_source().num();

  // only a recovering node will get a strong rejoin.
  assert(mds->is_rejoin());      

  MMDSCacheRejoin *missing = 0;  // if i'm missing something..
  
  // strong dirfrags/dentries.
  //  also process auth_pins, xlocks.
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = strong->strong_dirfrags.begin();
       p != strong->strong_dirfrags.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) {
      CInode *in = get_inode(p->first.ino);
      if (!in) in = rejoin_invent_inode(p->first.ino);
      if (!in->is_dir()) {
	assert(in->state_test(CInode::STATE_REJOINUNDEF));
	in->inode.mode = S_IFDIR;
      }
      dir = in->get_or_open_dirfrag(this, p->first.frag);
    } else {
      dout(10) << " have " << *dir << dendl;
    }
    dir->add_replica(from);
    dir->dir_rep = p->second.dir_rep;

    for (map<string,MMDSCacheRejoin::dn_strong>::iterator q = strong->strong_dentries[p->first].begin();
	 q != strong->strong_dentries[p->first].end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first);
      if (!dn) {
	if (q->second.is_remote()) {
	  dn = dir->add_remote_dentry(q->first, q->second.remote_ino, q->second.remote_d_type);
	} else if (q->second.is_null()) {
	  dn = dir->add_null_dentry(q->first);
	} else {
	  CInode *in = get_inode(q->second.ino);
	  if (!in) in = rejoin_invent_inode(q->second.ino);
	  dn = dir->add_primary_dentry(q->first, in);

	  dout(10) << " missing " << q->second.ino << dendl;
	  if (!missing) missing = new MMDSCacheRejoin(MMDSCacheRejoin::OP_MISSING);
	  missing->add_weak_inode(q->second.ino);  // we want it back!
	}
	dout(10) << " invented " << *dn << dendl;
      }

      // dn auth_pin?
      if (strong->authpinned_dentries.count(p->first) &&
	  strong->authpinned_dentries[p->first].count(q->first)) {
	metareqid_t ri = strong->authpinned_dentries[p->first][q->first];
	dout(10) << " dn authpin by " << ri << " on " << *dn << dendl;
	
	// get/create slave mdrequest
	MDRequest *mdr;
	if (have_request(ri))
	  mdr = request_get(ri);
	else
	  mdr = request_start_slave(ri, from);
	mdr->auth_pin(dn);
      }

      // dn xlock?
      if (strong->xlocked_dentries.count(p->first) &&
	  strong->xlocked_dentries[p->first].count(q->first)) {
	metareqid_t ri = strong->xlocked_dentries[p->first][q->first];
	dout(10) << " dn xlock by " << ri << " on " << *dn << dendl;
	MDRequest *mdr = request_get(ri);  // should have this from auth_pin above.
	assert(mdr->is_auth_pinned(dn));
	dn->lock.set_state(LOCK_LOCK);
	dn->lock.get_xlock(mdr);
	mdr->xlocks.insert(&dn->lock);
	mdr->locks.insert(&dn->lock);
      }

      dn->add_replica(from);
      dout(10) << " have " << *dn << dendl;
      
      // inode?
      if (dn->is_primary()) {
	CInode *in = dn->get_inode();
	assert(in);

	if (strong->strong_inodes.count(in->ino())) {
	  MMDSCacheRejoin::inode_strong &is = strong->strong_inodes[in->ino()];

	  // caps_wanted
	  if (is.caps_wanted) {
	    in->mds_caps_wanted[from] = is.caps_wanted;
	    dout(15) << " inode caps_wanted " << cap_string(is.caps_wanted)
		     << " on " << *in << dendl;
	  } 
	  
	  // scatterlock?
	  if (is.dirlock == LOCK_SCATTER ||
	      is.dirlock == LOCK_GLOCKC)  // replica still has wrlocks
	    in->dirlock.set_state(LOCK_SCATTER);
	  
	  // auth pin?
	  if (strong->authpinned_inodes.count(in->ino())) {
	    metareqid_t ri = strong->authpinned_inodes[in->ino()];
	    dout(10) << " inode authpin by " << ri << " on " << *in << dendl;
	    
	    // get/create slave mdrequest
	    MDRequest *mdr;
	    if (have_request(ri))
	      mdr = request_get(ri);
	    else
	      mdr = request_start_slave(ri, from);
	    mdr->auth_pin(in);
	  }
	  
	  // xlock(s)?
	  if (strong->xlocked_inodes.count(in->ino())) {
	    for (map<int,metareqid_t>::iterator r = strong->xlocked_inodes[in->ino()].begin();
		 r != strong->xlocked_inodes[in->ino()].end();
		 ++r) {
	      SimpleLock *lock = in->get_lock(r->first);
	      dout(10) << " inode xlock by " << r->second << " on " << *lock << " on " << *in << dendl;
	      MDRequest *mdr = request_get(r->second);  // should have this from auth_pin above.
	      assert(mdr->is_auth_pinned(in));
	      lock->set_state(LOCK_LOCK);
	      lock->get_xlock(mdr);
	      mdr->xlocks.insert(lock);
	      mdr->locks.insert(lock);
	    }
	  }
	} else {
	  dout(10) << " sender has dentry but not inode, adding them as a replica" << dendl;
	}
	
	in->add_replica(from);
	dout(10) << " have " << *in << dendl;
      }
    }
  }

  // base inodes?  (root, stray, etc.)
  for (set<inodeno_t>::iterator p = strong->weak_inodes.begin();
       p != strong->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    dout(10) << " have base " << *in << dendl;
    in->add_replica(from);
  }

  // send missing?
  if (missing) {
    // we expect a FULL soon.
    mds->send_message_mds(missing, from);
  } else {
    // done?
    assert(rejoin_gather.count(from));
    rejoin_gather.erase(from);
    if (rejoin_gather.empty()) {
      rejoin_gather_finish();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
    }
  }
}


void MDCache::handle_cache_rejoin_ack(MMDSCacheRejoin *ack)
{
  dout(7) << "handle_cache_rejoin_ack from " << ack->get_source() << dendl;
  int from = ack->get_source().num();

  list<Context*> waiters;
  
  // dirs
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = ack->strong_dirfrags.begin();
       p != ack->strong_dirfrags.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) continue;  // must have trimmed?

    dir->set_replica_nonce(p->second.nonce);
    dir->state_clear(CDir::STATE_REJOINING);
    dout(10) << " got " << *dir << dendl;

    // dentries
    for (map<string,MMDSCacheRejoin::dn_strong>::iterator q = ack->strong_dentries[p->first].begin();
	 q != ack->strong_dentries[p->first].end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first);
      if (!dn) continue;  // must have trimmed?

      // hmm, did we have the proper linkage here?
      if (dn->is_null() &&
	  !q->second.is_null()) {
	dout(10) << " had bad (missing) linkage for " << *dn << dendl;
	if (q->second.is_remote()) {
	  dn->dir->link_remote_inode(dn, q->second.remote_ino, q->second.remote_d_type);
	} else {
	  CInode *in = get_inode(q->second.ino);
	  assert(in == 0);  // a rename would have been caught be the resolve stage.
	  // barebones inode; the full inode loop below will clean up.
	  in = new CInode(this, false);
	  in->inode.ino = q->second.ino;
	  add_inode(in);
	  dn->dir->link_primary_inode(dn, in); 
	}
      }
      else if (!dn->is_null() &&
	       q->second.is_null()) {
	dout(-10) << " had bad linkage for " << *dn << dendl;
	/* 
	 * this should happen:
	 *  if we're a survivor, any unlink should commit or rollback during
	 * the resolve stage.
	 *  if we failed, we shouldn't have non-auth leaf dentries at all
	 */
	assert(0);  // uh oh.	
      }
      dn->set_replica_nonce(q->second.nonce);
      mds->locker->rejoin_set_state(&dn->lock, q->second.lock, waiters);
      dn->state_clear(CDentry::STATE_REJOINING);
      dout(10) << " got " << *dn << dendl;
    }
  }

  // full inodes
  for (list<MMDSCacheRejoin::inode_full>::iterator p = ack->full_inodes.begin();
       p != ack->full_inodes.end();
       ++p) {
    CInode *in = get_inode(p->inode.ino);
    if (!in) continue;
    in->inode = p->inode;
    in->symlink = p->symlink;
    in->dirfragtree = p->dirfragtree;
    dout(10) << " got inode content " << *in << dendl;
  }

  // inodes
  for (map<inodeno_t, MMDSCacheRejoin::inode_strong>::iterator p = ack->strong_inodes.begin();
       p != ack->strong_inodes.end();
       ++p) {
    CInode *in = get_inode(p->first);
    if (!in) continue;
    in->set_replica_nonce(p->second.nonce);
    mds->locker->rejoin_set_state(&in->authlock, p->second.authlock, waiters);
    mds->locker->rejoin_set_state(&in->linklock, p->second.linklock, waiters);
    mds->locker->rejoin_set_state(&in->dirfragtreelock, p->second.dirfragtreelock, waiters);
    mds->locker->rejoin_set_state(&in->filelock, p->second.filelock, waiters);
    mds->locker->rejoin_set_state(&in->dirlock, p->second.dirlock, waiters);
    in->state_clear(CInode::STATE_REJOINING);
    dout(10) << " got " << *in << dendl;
  }

  // done?
  assert(rejoin_ack_gather.count(from));
  rejoin_ack_gather.erase(from);
  if (mds->is_rejoin() && 
      rejoin_gather.empty() &&     // make sure we've gotten our FULL inodes, too.
      rejoin_ack_gather.empty()) {
    mds->rejoin_done();
  } else {
    dout(7) << "still need rejoin from (" << rejoin_gather << ")"
	    << ", rejoin_ack from (" << rejoin_ack_gather << ")" << dendl;
  }
}




void MDCache::handle_cache_rejoin_missing(MMDSCacheRejoin *missing)
{
  dout(7) << "handle_cache_rejoin_missing from " << missing->get_source() << dendl;

  MMDSCacheRejoin *full = new MMDSCacheRejoin(MMDSCacheRejoin::OP_FULL);
  
  // inodes
  for (set<inodeno_t>::iterator p = missing->weak_inodes.begin();
       p != missing->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    if (!in) {
      dout(10) << " don't have inode " << *p << dendl;
      continue; // we must have trimmed it after the originalo rejoin
    }
    
    dout(10) << " sending " << *in << dendl;
    full->add_full_inode(in->inode, in->symlink, in->dirfragtree);
  }

  mds->send_message_mds(full, missing->get_source().num());
}

void MDCache::handle_cache_rejoin_full(MMDSCacheRejoin *full)
{
  dout(7) << "handle_cache_rejoin_full from " << full->get_source() << dendl;
  int from = full->get_source().num();
  
  // integrate full inodes
  for (list<MMDSCacheRejoin::inode_full>::iterator p = full->full_inodes.begin();
       p != full->full_inodes.end();
       ++p) {
    CInode *in = get_inode(p->inode.ino);
    assert(in);

    set<CInode*>::iterator q = rejoin_undef_inodes.find(in);
    if (q != rejoin_undef_inodes.end()) {
      CInode *in = *q;
      in->inode = p->inode;
      in->symlink = p->symlink;
      in->dirfragtree = p->dirfragtree;
      in->state_clear(CInode::STATE_REJOINUNDEF);
      dout(10) << " got full " << *in << dendl;
      rejoin_undef_inodes.erase(q);
    } else {
      dout(10) << " had full " << *in << dendl;
    }
  }

  // done?
  assert(rejoin_gather.count(from));
  rejoin_gather.erase(from);
  if (rejoin_gather.empty()) {
    rejoin_gather_finish();
  } else {
    dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
  }
}



/**
 * rejoin_trim_undef_inodes() -- remove REJOINUNDEF flagged inodes
 *
 * FIXME: wait, can this actually happen?  a survivor should generate cache trim
 * messages that clean these guys up...
 */
void MDCache::rejoin_trim_undef_inodes()
{
  dout(10) << "rejoin_trim_undef_inodes" << dendl;

  while (!rejoin_undef_inodes.empty()) {
    set<CInode*>::iterator p = rejoin_undef_inodes.begin();
    CInode *in = *p;
    rejoin_undef_inodes.erase(p);

    in->clear_replica_map();
    
    // close out dirfrags
    if (in->is_dir()) {
      list<CDir*> dfls;
      in->get_dirfrags(dfls);
      for (list<CDir*>::iterator p = dfls.begin();
	   p != dfls.end();
	   ++p) {
	CDir *dir = *p;
	dir->clear_replica_map();

	for (CDir::map_t::iterator p = dir->items.begin();
	     p != dir->items.end();
	     ++p) {
	  CDentry *dn = p->second;
	  dn->clear_replica_map();

	  dout(10) << " trimming " << *dn << dendl;
	  dir->remove_dentry(dn);
	}

	dout(10) << " trimming " << *dir << dendl;
	in->close_dirfrag(dir->dirfrag().frag);
      }
    }
    
    CDentry *dn = in->get_parent_dn();
    if (dn) {
      dn->clear_replica_map();
      dout(10) << " trimming " << *dn << dendl;
      dn->dir->remove_dentry(dn);
    } else {
      dout(10) << " trimming " << *in << dendl;
      remove_inode(in);
    }
  }

  assert(rejoin_undef_inodes.empty());
}

class C_MDC_RejoinGatherFinish : public Context {
  MDCache *cache;
public:
  C_MDC_RejoinGatherFinish(MDCache *c) : cache(c) {}
  void finish(int r) {
    cache->rejoin_gather_finish();
  }
};



void MDCache::rejoin_gather_finish() 
{
  dout(10) << "rejoin_gather_finish" << dendl;
  assert(mds->is_rejoin());

  rejoin_trim_undef_inodes();

  // fetch paths?
  //  do this before ack, since some inodes we may have already gotten
  //  from surviving MDSs.
  if (!cap_import_paths.empty()) {
    C_Gather *gather = parallel_fetch(cap_import_paths);
    if (gather) {
      gather->set_finisher(new C_MDC_RejoinGatherFinish(this));
      return;
    }
  }
  
  // process cap imports
  //  ino -> client -> frommds -> capex
  for (map<inodeno_t,map<int, map<int,inode_caps_reconnect_t> > >::iterator p = cap_imports.begin();
       p != cap_imports.end();
       ++p) {
    CInode *in = get_inode(p->first);
    assert(in);
    mds->server->add_reconnected_cap_inode(in);
    for (map<int, map<int,inode_caps_reconnect_t> >::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) 
      for (map<int,inode_caps_reconnect_t>::iterator r = q->second.begin();
	   r != q->second.end();
	   ++r) 
	if (r->first >= 0)
	  rejoin_import_cap(in, q->first, r->second, r->first);
  }
  
  mds->server->process_reconnected_caps();
  
  rejoin_send_acks();

  // did we already get our acks too?
  // this happens when the rejoin_gather has to wait on a MISSING/FULL exchange.
  if (rejoin_ack_gather.empty())
    mds->rejoin_done();
}

void MDCache::rejoin_import_cap(CInode *in, int client, inode_caps_reconnect_t& icr, int frommds)
{
  dout(10) << "rejoin_import_cap for client" << client << " from mds" << frommds
	   << " on " << *in << dendl;

  Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(client));
  assert(session);

  // add cap
  in->reconnect_cap(client, icr, session->caps);
  
  // send REAP
  Capability *cap = in->get_client_cap(client);
  assert(cap); // ?
  MClientFileCaps *reap = new MClientFileCaps(CEPH_CAP_OP_IMPORT,
					      in->inode,
					      cap->get_last_seq(),
					      cap->pending(),
					      cap->wanted());
  reap->set_migrate_mds(frommds); // reap from whom?
  mds->messenger->send_message(reap, session->inst);
}

void MDCache::rejoin_send_acks()
{
  dout(7) << "rejoin_send_acks" << dendl;
  
  // send acks to everyone in the recovery set
  map<int,MMDSCacheRejoin*> ack;
  for (set<int>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) 
    ack[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);
  
  // walk subtrees
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin(); 
       p != subtrees.end();
       p++) {
    CDir *dir = p->first;
    if (!dir->is_auth()) continue;
    dout(10) << "subtree " << *dir << dendl;
    
    // auth items in this subtree
    list<CDir*> dq;
    dq.push_back(dir);

    while (!dq.empty()) {
      CDir *dir = dq.front();
      dq.pop_front();
      
      // dir
      for (map<int,int>::iterator r = dir->replicas_begin();
	   r != dir->replicas_end();
	   ++r) 
	ack[r->first]->add_strong_dirfrag(dir->dirfrag(), r->second, dir->dir_rep);
	   
      for (CDir::map_t::iterator q = dir->items.begin();
	   q != dir->items.end();
	   ++q) {
	CDentry *dn = q->second;

	// dentry
	for (map<int,int>::iterator r = dn->replicas_begin();
	     r != dn->replicas_end();
	     ++r) 
	  ack[r->first]->add_strong_dentry(dir->dirfrag(), dn->name, 
					   dn->is_primary() ? dn->get_inode()->ino():inodeno_t(0),
					   dn->is_remote() ? dn->get_remote_ino():inodeno_t(0),
					   dn->is_remote() ? dn->get_remote_d_type():0,
					   r->second,
					   dn->lock.get_replica_state());
	
	if (!dn->is_primary()) continue;

	// inode
	CInode *in = dn->inode;

	for (map<int,int>::iterator r = in->replicas_begin();
	     r != in->replicas_end();
	     ++r) {
	  ack[r->first]->add_full_inode(in->inode, in->symlink, in->dirfragtree);
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

  // root inodes too
  if (root) 
    for (map<int,int>::iterator r = root->replicas_begin();
	 r != root->replicas_end();
	 ++r) {
      ack[r->first]->add_full_inode(root->inode, root->symlink, root->dirfragtree);
      ack[r->first]->add_strong_inode(root->ino(), r->second, 0,
				      root->authlock.get_replica_state(),
				      root->linklock.get_replica_state(),
				      root->dirfragtreelock.get_replica_state(),
				      root->filelock.get_replica_state(),
				      root->dirlock.get_replica_state());
    }
  if (stray)
    for (map<int,int>::iterator r = stray->replicas_begin();
	 r != stray->replicas_end();
	 ++r) {
      ack[r->first]->add_full_inode(stray->inode, stray->symlink, stray->dirfragtree);
      ack[r->first]->add_strong_inode(stray->ino(), r->second, 0,
				      stray->authlock.get_replica_state(),
				      stray->linklock.get_replica_state(),
				      stray->dirfragtreelock.get_replica_state(),
				      stray->filelock.get_replica_state(),
				      stray->dirlock.get_replica_state());
    }

  // send acks
  for (map<int,MMDSCacheRejoin*>::iterator p = ack.begin();
       p != ack.end();
       ++p) 
    mds->send_message_mds(p->second, p->first);
  
}



// ===============================================================================


void MDCache::set_root(CInode *in)
{
  assert(root == 0);
  root = in;
  base_inodes.insert(in);
}






// **************
// Inode purging -- reliably removing deleted file's objects

class C_MDC_PurgeFinish : public Context {
  MDCache *mdc;
  CInode *in;
  off_t newsize, oldsize;
public:
  C_MDC_PurgeFinish(MDCache *c, CInode *i, off_t ns, off_t os) :
    mdc(c), in(i), newsize(ns), oldsize(os) {}
  void finish(int r) {
    mdc->purge_inode_finish(in, newsize, oldsize);
  }
};
class C_MDC_PurgeFinish2 : public Context {
  MDCache *mdc;
  CInode *in;
  off_t newsize, oldsize;
public:
  C_MDC_PurgeFinish2(MDCache *c, CInode *i, off_t ns, off_t os) : 
    mdc(c), in(i), newsize(ns), oldsize(os) {}
  void finish(int r) {
    mdc->purge_inode_finish_2(in, newsize, oldsize);
  }
};

/* purge_inode in
 * will be called by on unlink or rmdir or truncate or purge
 * caller responsible for journaling a matching EUpdate
 */
void MDCache::purge_inode(CInode *in, off_t newsize, off_t oldsize, LogSegment *ls)
{
  dout(10) << "purge_inode " << oldsize << " -> " << newsize
	   << " on " << *in
	   << dendl;

  assert(oldsize >= newsize);

  purging[in][newsize] = oldsize;
  purging_ls[in][newsize] = ls;
  ls->purging_inodes[in][newsize] = oldsize;
  
  _do_purge_inode(in, newsize, oldsize);
}

void MDCache::_do_purge_inode(CInode *in, off_t newsize, off_t oldsize)
{
  in->get(CInode::PIN_PURGING);

  // remove
  if (in->inode.size > 0) {
    mds->filer->remove(in->inode, newsize, oldsize,
		       0, new C_MDC_PurgeFinish(this, in, newsize, oldsize));
  } else {
    // no need, empty file, just log it
    purge_inode_finish(in, newsize, oldsize);
  }
}

void MDCache::purge_inode_finish(CInode *in, off_t newsize, off_t oldsize)
{
  dout(10) << "purge_inode_finish " << oldsize << " -> " << newsize
	   << " on " << *in << dendl;
  
  // log completion
  mds->mdlog->submit_entry(new EPurgeFinish(in->ino(), newsize, oldsize),
			   new C_MDC_PurgeFinish2(this, in, newsize, oldsize));
}

void MDCache::purge_inode_finish_2(CInode *in, off_t newsize, off_t oldsize)
{
  dout(10) << "purge_inode_finish_2 " << oldsize << " -> " << newsize 
	   << " on " << *in << dendl;

  // remove from purging list
  LogSegment *ls = purging_ls[in][newsize];
  purging[in].erase(newsize);
  purging_ls[in].erase(newsize);
  if (purging[in].empty()) {
    purging.erase(in);
    purging_ls.erase(in);
  }

  assert(ls->purging_inodes.count(in));
  assert(ls->purging_inodes[in].count(newsize));
  assert(ls->purging_inodes[in][newsize] == oldsize);
  ls->purging_inodes[in].erase(newsize);
  if (ls->purging_inodes[in].empty())
    ls->purging_inodes.erase(in);
  
  in->put(CInode::PIN_PURGING);

  // tell anyone who cares (log flusher?)
  if (purging.count(in) == 0 ||
      purging[in].rbegin()->first < newsize) {
    list<Context*> ls;
    ls.swap(waiting_for_purge[in][newsize]);
    waiting_for_purge[in].erase(newsize);
    if (waiting_for_purge[in].empty())
      waiting_for_purge.erase(in);
    finish_contexts(ls, 0);
  }

  // done with inode?
  if (in->get_num_ref() == 0) 
    remove_inode(in);
}

void MDCache::add_recovered_purge(CInode *in, off_t newsize, off_t oldsize, LogSegment *ls)
{
  assert(purging[in].count(newsize) == 0);
  purging[in][newsize] = oldsize;
  purging_ls[in][newsize] = ls;
  ls->purging_inodes[in][newsize] = oldsize;
}

void MDCache::remove_recovered_purge(CInode *in, off_t newsize, off_t oldsize)
{
  purging[in].erase(newsize);
}

void MDCache::start_recovered_purges()
{
  dout(10) << "start_recovered_purges (" << purging.size() << " purges)" << dendl;

  for (map<CInode*, map<off_t, off_t> >::iterator p = purging.begin();
       p != purging.end();
       ++p) {
    for (map<off_t,off_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << "start_recovered_purges " 
	       << q->second << " -> " << q->first
	       << " on " << *p->first
	       << dendl;
      _do_purge_inode(p->first, q->first, q->second);
    }
  }
}



// ================================================================================
// cache trimming


/*
 * note: only called while MDS is active or stopping... NOT during recovery.
 * however, we may expire a replica whose authority is recovering.
 * 
 */
bool MDCache::trim(int max) 
{
  // trim LRU
  if (max < 0) {
    max = lru.lru_get_max();
    if (!max) return false;
  }
  dout(7) << "trim max=" << max << "  cur=" << lru.lru_get_size() << dendl;

  map<int, MCacheExpire*> expiremap;

  // trim dentries from the LRU
  while (lru.lru_get_size() > (unsigned)max) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;
    trim_dentry(dn, expiremap);
  }

  // trim base inodes?
  if (max == 0) {
    set<CInode*>::iterator p = base_inodes.begin();
    while (p != base_inodes.end()) {
      CInode *in = *p++;
      list<CDir*> ls;
      in->get_dirfrags(ls);
      for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
	CDir *dir = *p;
	if (dir->get_num_ref() == 1)  // subtree pin
	  trim_dirfrag(dir, 0, expiremap);
      }
      if (in->get_num_ref() == 0)
	trim_inode(0, in, 0, expiremap);
    }
  }

  // send any expire messages
  send_expire_messages(expiremap);

  return true;
}

void MDCache::send_expire_messages(map<int, MCacheExpire*>& expiremap)
{
  // send expires
  for (map<int, MCacheExpire*>::iterator it = expiremap.begin();
       it != expiremap.end();
       it++) {
    dout(7) << "sending cache_expire to " << it->first << dendl;
    mds->send_message_mds(it->second, it->first);
  }
}


void MDCache::trim_dentry(CDentry *dn, map<int, MCacheExpire*>& expiremap)
{
  dout(12) << "trim_dentry " << *dn << dendl;

  CDir *dir = dn->get_dir();
  assert(dir);
  
  CDir *con = get_subtree_root(dir);
  assert(con);
  
  dout(12) << " in container " << *con << dendl;

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
      
      dout(12) << "  sending expire to mds" << a << " on " << *dn << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_dentry(con->dirfrag(), dir->dirfrag(), dn->get_name(), dn->get_replica_nonce());
    }
  }

  // adjust the dir state
  // NOTE: we can safely remove a clean, null dentry without effecting
  //       directory completeness.
  // (do this _before_ we unlink the inode, below!)
  if (!(dn->is_null() && dn->is_clean())) 
    dir->state_clear(CDir::STATE_COMPLETE); 
  
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
    
  // remove dentry
  dir->remove_dentry(dn);
  
  // reexport?
  if (dir->get_size() == 0 && dir->is_subtree_root())
    migrator->export_empty_import(dir);
  
  if (mds->logger) mds->logger->inc("cex");
}


void MDCache::trim_dirfrag(CDir *dir, CDir *con, map<int, MCacheExpire*>& expiremap)
{
  dout(15) << "trim_dirfrag " << *dir << dendl;

  if (dir->is_subtree_root()) {
    assert(!dir->is_auth() ||
	   (!dir->is_replicated() && dir->inode->is_base()));
    remove_subtree(dir);	// remove from subtree map
  }
  assert(dir->get_num_ref() == 0);

  CInode *in = dir->get_inode();

  if (!dir->is_auth()) {
    pair<int,int> auth = dir->authority();
    
    // was this an auth delegation?  (if so, slightly modified container)
    dirfrag_t condf;
    if (dir->is_subtree_root()) {
      dout(12) << " subtree root, container is " << *dir << dendl;
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

      dout(12) << "  sending expire to mds" << a << " on   " << *dir << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_dir(condf, dir->dirfrag(), dir->replica_nonce);
    }
  }
  
  in->close_dirfrag(dir->dirfrag().frag);
}

void MDCache::trim_inode(CDentry *dn, CInode *in, CDir *con, map<int, MCacheExpire*>& expiremap)
{
  dout(15) << "trim_inode " << *in << dendl;
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
      df = dirfrag_t(0,frag_t());   // must be a root or stray inode.

    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (con && mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds" << a << " on " << *in << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_inode(df, in->ino(), in->get_replica_nonce());
    }
  }

  /*
  if (in->is_auth()) {
    if (in->hack_accessed)
      mds->logger->inc("outt");
    else {
      mds->logger->inc("outut");
      mds->logger->favg("oututl", g_clock.now() - in->hack_load_stamp);
    }
  }
  */
    
  // unlink
  if (dn)
    dn->get_dir()->unlink_inode(dn);
  remove_inode(in);
}


/**
 * trim_non_auth - remove any non-auth items from our cache
 *
 * this reduces the amount of non-auth metadata in our cache, reducing the 
 * load incurred by the rejoin phase.
 *
 * the only non-auth items that remain are those that are needed to 
 * attach our own subtrees to the root.  
 *
 * when we are done, all dentries will be in the top bit of the lru.
 *
 * why we have to do this:
 *  we may not have accurate linkage for non-auth items.  which means we will 
 *  know which subtree it falls into, and can not be sure to declare it to the
 *  correct authority.  
 */
void MDCache::trim_non_auth()
{
  dout(7) << "trim_non_auth" << dendl;
  
  // temporarily pin all subtree roots
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       p++) 
    p->first->get(CDir::PIN_SUBTREETEMP);

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
      dout(15) << "trim_non_auth removing " << *dn << dendl;
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

  // move everything in the pintail to the top bit of the lru.
  lru.lru_touch_entire_pintail();

  // unpin all subtrees
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       p++) 
    p->first->put(CDir::PIN_SUBTREETEMP);

  show_subtrees();
}

void MDCache::handle_cache_expire(MCacheExpire *m)
{
  int from = m->get_from();
  
  dout(7) << "cache_expire from mds" << from << dendl;

  if (mds->get_state() < MDSMap::STATE_REJOIN) {
    delete m;
    return;
  }

  // loop over realms
  for (map<dirfrag_t,MCacheExpire::realm>::iterator p = m->realms.begin();
       p != m->realms.end();
       ++p) {
    // check container?
    if (p->first.ino > 0) {
      CInode *coni = get_inode(p->first.ino);
      assert(coni);  // we had better have this.
      CDir *con = coni->get_approx_dirfrag(p->first.frag);
      assert(con);
      
      if (!con->is_auth() ||
	  (con->is_auth() && con->is_exporting() &&
	   migrator->get_export_state(con) == Migrator::EXPORT_WARNING &&
	   migrator->export_has_warned(con,from))) {
	// not auth.
	dout(7) << "delaying nonauth|warned expires for " << *con << dendl;
	assert(con->is_frozen_tree_root());
	
	// make a message container
	if (delayed_expire[con].count(from) == 0) 
	  delayed_expire[con][from] = new MCacheExpire(from);
	
	// merge these expires into it
	delayed_expire[con][from]->add_realm(p->first, p->second);
	continue;
      }
      dout(7) << "expires for " << *con << dendl;
    } else {
      dout(7) << "containerless expires (root, stray inodes)" << dendl;
    }

    // INODES
    for (map<inodeno_t,int>::iterator it = p->second.inodes.begin();
	 it != p->second.inodes.end();
	 it++) {
      CInode *in = get_inode(it->first);
      int nonce = it->second;
      
      if (!in) {
	dout(0) << " inode expire on " << it->first << " from " << from 
		<< ", don't have it" << dendl;
	assert(in);
      }        
      assert(in->is_auth());
      
      // check nonce
      if (nonce == in->get_replica_nonce(from)) {
	// remove from our cached_by
	dout(7) << " inode expire on " << *in << " from mds" << from 
		<< " cached_by was " << in->get_replicas() << dendl;
	inode_remove_replica(in, from);
      } 
      else {
	// this is an old nonce, ignore expire.
	dout(7) << " inode expire on " << *in << " from mds" << from
		<< " with old nonce " << nonce
		<< " (current " << in->get_replica_nonce(from) << "), dropping" 
		<< dendl;
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
	dout(0) << " dir expire on " << it->first << " from " << from 
		<< ", don't have it" << dendl;
	assert(dir);
      }  
      assert(dir->is_auth());
      
      // check nonce
      if (nonce == dir->get_replica_nonce(from)) {
	// remove from our cached_by
	dout(7) << " dir expire on " << *dir << " from mds" << from
		<< " replicas was " << dir->replica_map << dendl;
	dir->remove_replica(from);
      } 
      else {
	// this is an old nonce, ignore expire.
	dout(7) << " dir expire on " << *dir << " from mds" << from 
		<< " with old nonce " << nonce << " (current " << dir->get_replica_nonce(from)
		<< "), dropping" << dendl;
	assert(dir->get_replica_nonce(from) > nonce);
      }
    }
    
    // DENTRIES
    for (map<dirfrag_t, map<string,int> >::iterator pd = p->second.dentries.begin();
	 pd != p->second.dentries.end();
	 ++pd) {
      dout(10) << " dn expires in dir " << pd->first << dendl;
      CInode *diri = get_inode(pd->first.ino);
      assert(diri);
      CDir *dir = diri->get_dirfrag(pd->first.frag);
      
      if (!dir) {
	dout(0) << " dn expires on " << pd->first << " from " << from
		<< ", must have refragmented" << dendl;
      } else {
	assert(dir->is_auth());
      }
      
      for (map<string,int>::iterator p = pd->second.begin();
	   p != pd->second.end();
	   ++p) {
	int nonce = p->second;
	CDentry *dn;
	
	if (dir) {
	  dn = dir->lookup(p->first);
	} else {
	  // which dirfrag for this dentry?
	  CDir *dir = diri->get_dirfrag(diri->pick_dirfrag(p->first));
	  assert(dir->is_auth());
	  dn = dir->lookup(p->first);
	} 

	if (!dn) 
	  dout(0) << "  missing dentry for " << p->first << " in " << *dir << dendl;
	assert(dn);
	
	if (nonce == dn->get_replica_nonce(from)) {
	  dout(7) << "  dentry_expire on " << *dn << " from mds" << from << dendl;
	  dentry_remove_replica(dn, from);
	} 
	else {
	  dout(7) << "  dentry_expire on " << *dn << " from mds" << from
		  << " with old nonce " << nonce << " (current " << dn->get_replica_nonce(from)
		  << "), dropping" << dendl;
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
  dout(7) << "process_delayed_expire on " << *dir << dendl;
  for (map<int,MCacheExpire*>::iterator p = delayed_expire[dir].begin();
       p != delayed_expire[dir].end();
       ++p) 
    handle_cache_expire(p->second);
  delayed_expire.erase(dir);  
}

void MDCache::discard_delayed_expire(CDir *dir)
{
  dout(7) << "discard_delayed_expire on " << *dir << dendl;
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
  if (in->authlock.remove_replica(from)) mds->locker->simple_eval_gather(&in->authlock);
  if (in->linklock.remove_replica(from)) mds->locker->simple_eval_gather(&in->linklock);
  if (in->dirfragtreelock.remove_replica(from)) mds->locker->simple_eval_gather(&in->dirfragtreelock);
  if (in->filelock.remove_replica(from)) mds->locker->file_eval_gather(&in->filelock);
  if (in->dirlock.remove_replica(from)) mds->locker->scatter_eval_gather(&in->dirlock);
  
  // alone now?
  /*
  if (!in->is_replicated()) {
    mds->locker->simple_eval_gather(&in->authlock);
    mds->locker->simple_eval_gather(&in->linklock);
    mds->locker->simple_eval_gather(&in->dirfragtreelock);
    mds->locker->file_eval_gather(&in->filelock);
    mds->locker->scatter_eval_gather(&in->dirlock);
  }
  */
}

void MDCache::dentry_remove_replica(CDentry *dn, int from)
{
  dn->remove_replica(from);

  // fix lock
  if (dn->lock.remove_replica(from) ||
      !dn->is_replicated())
    mds->locker->simple_eval_gather(&dn->lock);
}



// =========================================================================================
// shutdown

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
  dout(0) << "shutdown_check at " << g_clock.now() << dendl;

  // cache
  int o = g_conf.debug_mds;
  g_conf.debug_mds = 10;
  show_cache();
  g_conf.debug_mds = o;
  mds->timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  // this
  dout(0) << "lru size now " << lru.lru_get_size() << dendl;
  dout(0) << "log len " << mds->mdlog->get_num_events() << dendl;


  if (mds->filer->is_active()) 
    dout(0) << "filer still active" << dendl;
}

void MDCache::shutdown_start()
{
  dout(2) << "shutdown_start" << dendl;

  if (g_conf.mds_shutdown_check)
    mds->timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  //  g_conf.debug_mds = 10;
}



bool MDCache::shutdown_pass()
{
  dout(7) << "shutdown_pass" << dendl;

  if (mds->is_stopped()) {
    dout(7) << " already shut down" << dendl;
    show_cache();
    show_subtrees();
    return true;
  }

  // flush batching eopens, so that we can properly expire them.
  mds->server->journal_opens();    // hrm, this is sort of a hack.

  // flush what we can from the log
  mds->mdlog->set_max_events(0);
  mds->mdlog->trim();

  if (mds->mdlog->get_num_segments() > 1) {
    dout(7) << "still >1 segments, waiting for log to trim" << dendl;
    return false;
  }

  // empty stray dir
  if (!shutdown_export_strays()) {
    dout(7) << "waiting for strays to migrate" << dendl;
    return false;
  }

  // trim cache
  trim(0);
  dout(5) << "lru size now " << lru.lru_get_size() << dendl;

  // SUBTREES
  if (!subtrees.empty() &&
      mds->get_nodeid() != 0 && 
      !migrator->is_exporting() //&&
      //!migrator->is_importing()
      ) {
    dout(7) << "looking for subtrees to export to mds0" << dendl;
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
    int max = 5; // throttle shutdown exports.. hack!
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
      CDir *dir = *p;
      int dest = dir->get_inode()->authority().first;
      if (dest > 0 && !mds->mdsmap->is_active(dest)) dest = 0;
      dout(7) << "sending " << *dir << " back to mds" << dest << dendl;
      migrator->export_dir(dir, dest);
      if (--max == 0) break;
    }
  }

  if (!shutdown_export_caps()) {
    dout(7) << "waiting for residual caps to export" << dendl;
    return false;
  }

  // subtrees map not empty yet?
  if (!subtrees.empty()) {
    dout(7) << "still have " << num_subtrees() << " subtrees" << dendl;
    show_subtrees();
    migrator->show_importing();
    migrator->show_exporting();
    if (!migrator->is_importing() && !migrator->is_exporting())
      show_cache();
    return false;
  }
  assert(subtrees.empty());
  assert(!migrator->is_exporting());
  assert(!migrator->is_importing());

  // (only do this once!)
  if (!mds->mdlog->is_capped()) {
    dout(7) << "capping the log" << dendl;
    mds->mdlog->cap();
    mds->mdlog->trim();
  }
  
  if (!mds->mdlog->empty()) {
    dout(7) << "waiting for log to flush.. " << mds->mdlog->get_num_events() 
	    << " in " << mds->mdlog->get_num_segments() << " segments" << dendl;
    return false;
  }
  
  if (!did_shutdown_log_cap) {
    // flush journal header
    dout(7) << "writing header for (now-empty) journal" << dendl;
    assert(mds->mdlog->empty());
    mds->mdlog->write_head(0);  
    // NOTE: filer active checker below will block us until this completes.
    did_shutdown_log_cap = true;
    return false;
  }

  // filer active?
  if (mds->filer->is_active()) {
    dout(7) << "filer still active" << dendl;
    return false;
  }

  // trim what we can from the cache
  if (lru.lru_get_size() > 0) {
    dout(7) << "there's still stuff in the cache: " << lru.lru_get_size() << dendl;
    show_cache();
    //dump();
    return false;
  } 
  
  // done!
  dout(2) << "shutdown done." << dendl;
  return true;
}

bool MDCache::shutdown_export_strays()
{
  if (mds->get_nodeid() == 0) return true;
  if (!stray) return true;
  
  bool done = true;
  static set<inodeno_t> exported_strays;
  list<CDir*> dfs;

  stray->get_dirfrags(dfs);
  while (!dfs.empty()) {
    CDir *dir = dfs.front();
    dfs.pop_front();

    if (!dir->is_complete()) {
      dir->fetch(0);
      done = false;
    }
    
    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 p++) {
      CDentry *dn = p->second;
      if (dn->is_null()) continue;
      done = false;
      
      // FIXME: we'll deadlock if a rename fails.
      if (exported_strays.count(dn->get_inode()->ino()) == 0) {
	exported_strays.insert(dn->get_inode()->ino());
	migrate_stray(dn, mds->get_nodeid(), 0);  // send to root!
      }
    }
  }

  return done;
}

bool MDCache::shutdown_export_caps()
{
  // export caps?
  //  note: this runs more often than it should.
  static bool exported_caps = false;
  static set<CDir*> exported_caps_in;
  if (!exported_caps) {
    dout(7) << "searching for caps to export" << dendl;
    exported_caps = true;

    list<CDir*> dirq;
    for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      if (exported_caps_in.count(p->first)) continue;
      if (p->first->is_auth() ||
	  p->first->is_ambiguous_auth())
	exported_caps = false; // we'll have to try again
      else {
	dirq.push_back(p->first);
	exported_caps_in.insert(p->first);
      }
    }
    while (!dirq.empty()) {
      CDir *dir = dirq.front();
      dirq.pop_front();
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	if (!dn->is_primary()) continue;
	CInode *in = dn->get_inode();
	if (in->is_dir())
	  in->get_nested_dirfrags(dirq);
	if (in->is_any_caps() && !in->state_test(CInode::STATE_EXPORTINGCAPS))
	  migrator->export_caps(in);
      }
    }
  }

  return true;
}





// ========= messaging ==============


void MDCache::dispatch(Message *m)
{
  switch (m->get_type()) {

    // RESOLVE
  case MSG_MDS_RESOLVE:
    handle_resolve((MMDSResolve*)m);
    break;
  case MSG_MDS_RESOLVEACK:
    handle_resolve_ack((MMDSResolveAck*)m);
    break;

    // REJOIN
  case MSG_MDS_CACHEREJOIN:
    handle_cache_rejoin((MMDSCacheRejoin*)m);
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

  case MSG_MDS_DIRUPDATE:
    handle_dir_update((MDirUpdate*)m);
    break;

  case MSG_MDS_CACHEEXPIRE:
    handle_cache_expire((MCacheExpire*)m);
    break;



  case MSG_MDS_DENTRYUNLINK:
    handle_dentry_unlink((MDentryUnlink*)m);
    break;


  case MSG_MDS_FRAGMENTNOTIFY:
    handle_fragment_notify((MMDSFragmentNotify*)m);
    break;
    

    
  default:
    dout(7) << "cache unknown message " << m->get_type() << dendl;
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
  if (mdr) {
    dout(20) << "_get_waiter retryrequest" << dendl;
    return new C_MDS_RetryRequest(this, mdr);
  } else {
    dout(20) << "_get_waiter retrymessage" << dendl;
    return new C_MDS_RetryMessage(mds, req);
  }
}

int MDCache::path_traverse(MDRequest *mdr, Message *req,     // who
			   filepath& origpath,               // what
                           vector<CDentry*>& trace,          // result
                           bool follow_trailing_symlink,     // how
                           int onfail)
{
  assert(mdr || req);
  bool null_okay = (onfail == MDS_TRAVERSE_DISCOVERXLOCK);
  bool noperm = (onfail == MDS_TRAVERSE_DISCOVER ||
		 onfail == MDS_TRAVERSE_DISCOVERXLOCK);

  // keep a list of symlinks we touch to avoid loops
  set< pair<CInode*, string> > symlinks_resolved; 

  // root
  CInode *cur = get_inode(origpath.get_ino());
  if (cur == NULL) {
    dout(7) << "traverse: opening base ino " << origpath.get_ino() << dendl;
    if (origpath.get_ino() == MDS_INO_ROOT)
      open_root(_get_waiter(mdr, req));
    else if (MDS_INO_IS_STRAY(origpath.get_ino())) 
      open_foreign_stray(origpath.get_ino() - MDS_INO_STRAY_OFFSET, _get_waiter(mdr, req));
    else {
      assert(0);  // hrm.. broken
      return -EIO;
    }
    return 1;
  }

  if (mds->logger) mds->logger->inc("t");

  // start trace
  trace.clear();

  // make our own copy, since we'll modify when we hit symlinks
  filepath path = origpath;  

  unsigned depth = 0;
  while (depth < path.depth()) {
    dout(12) << "traverse: path seg depth " << depth << " = " << path[depth] << dendl;
    
    if (!cur->is_dir()) {
      dout(7) << "traverse: " << *cur << " not a dir " << dendl;
      return -ENOTDIR;
    }

    // open dir
    frag_t fg = cur->pick_dirfrag(path[depth]);
    CDir *curdir = cur->get_dirfrag(fg);
    if (!curdir) {
      if (cur->is_auth()) {
        // parent dir frozen_dir?
        if (cur->is_frozen_dir()) {
          dout(7) << "traverse: " << *cur->get_parent_dir() << " is frozen_dir, waiting" << dendl;
          cur->get_parent_dn()->get_dir()->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req));
          return 1;
        }
        curdir = cur->get_or_open_dirfrag(this, fg);
      } else {
        // discover?
	dout(10) << "traverse: need dirfrag " << fg << ", doing discover from " << *cur << dendl;
	discover_path(cur, path.postfixpath(depth), _get_waiter(mdr, req),
		      onfail == MDS_TRAVERSE_DISCOVERXLOCK);
	if (mds->logger) mds->logger->inc("tdis");
        return 1;
      }
    }
    assert(curdir);

    // frozen?
    /*
    if (curdir->is_frozen()) {
    // doh!
      // FIXME: traverse is allowed?
      dout(7) << "traverse: " << *curdir << " is frozen, waiting" << dendl;
      curdir->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req));
      if (onfinish) delete onfinish;
      return 1;
    }
    */

    // must read directory hard data (permissions, x bit) to traverse
    if (!noperm && 
	!mds->locker->simple_rdlock_try(&cur->authlock, 0)) {
      dout(7) << "traverse: waiting on authlock rdlock on " << *cur << dendl;
      cur->authlock.add_waiter(SimpleLock::WAIT_RD, _get_waiter(mdr, req));
      return 1;
    }
    
    // check permissions?
    // XXX
    
    // ..?
    if (path[depth] == "..") {
      trace.pop_back();
      depth++;
      cur = cur->get_parent_inode();
      dout(10) << "traverse: following .. back to " << *cur << dendl;
      continue;
    }


    // dentry
    CDentry *dn = curdir->lookup(path[depth]);

    // null and last_bit and xlocked by me?
    if (dn && dn->is_null() && null_okay) {
      dout(10) << "traverse: hit null dentry at tail of traverse, succeeding" << dendl;
      trace.push_back(dn);
      break; // done!
    }

    if (dn && !dn->is_null()) {
      // dentry exists.  xlocked?
      if (!noperm && dn->lock.is_xlocked() && dn->lock.get_xlocked_by() != mdr) {
        dout(10) << "traverse: xlocked dentry at " << *dn << dendl;
        dn->lock.add_waiter(SimpleLock::WAIT_RD, _get_waiter(mdr, req));
	if (mds->logger) mds->logger->inc("tlock");
        return 1;
      }

      // do we have inode?
      if (!dn->inode) {
        assert(dn->is_remote());
        // do i have it?
        CInode *in = get_inode(dn->get_remote_ino());
        if (in) {
          dout(7) << "linking in remote in " << *in << dendl;
          dn->link_remote(in);
        } else {
          dout(7) << "remote link to " << dn->get_remote_ino() << ", which i don't have" << dendl;
	  assert(mdr);  // we shouldn't hit non-primary dentries doing a non-mdr traversal!
          open_remote_ino(dn->get_remote_ino(), mdr, _get_waiter(mdr, req));
	  if (mds->logger) mds->logger->inc("trino");
          return 1;
        }        
      }

      // symlink?
      if (dn->inode->is_symlink() &&
          (follow_trailing_symlink || depth < path.depth()-1)) {
        // symlink, resolve!
        filepath sym = dn->inode->symlink;
        dout(10) << "traverse: hit symlink " << *dn->inode << " to " << sym << dendl;

        // break up path components
        // /head/symlink/tail
        filepath head = path.prefixpath(depth);
        filepath tail = path.postfixpath(depth+1);
        dout(10) << "traverse: path head = " << head << dendl;
        dout(10) << "traverse: path tail = " << tail << dendl;
        
        if (symlinks_resolved.count(pair<CInode*,string>(dn->inode, tail.get_path()))) {
          dout(10) << "already hit this symlink, bailing to avoid the loop" << dendl;
          return -ELOOP;
        }
        symlinks_resolved.insert(pair<CInode*,string>(dn->inode, tail.get_path()));

        // start at root?
        if (dn->inode->symlink[0] == '/') {
          // absolute
          trace.clear();
          depth = 0;
	  path = dn->inode->symlink;
	  path.append(tail);
          dout(10) << "traverse: absolute symlink, path now " << path << " depth " << depth << dendl;
        } else {
          // relative
          path = head;
          path.append(sym);
          path.append(tail);
          dout(10) << "traverse: relative symlink, path now " << path << " depth " << depth << dendl;
        }
        continue;        
      }

      // forwarder wants replicas?
      if (mdr && mdr->client_request && 
	  mdr->client_request->get_mds_wants_replica_in_dirino()) {
	dout(30) << "traverse: REP is here, " 
		 << mdr->client_request->get_mds_wants_replica_in_dirino() 
		 << " vs " << curdir->dirfrag() << dendl;
	
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
		     << req->get_source() << " dn " << *dn << dendl; 
	  } else {
	    dout(10) << "traverse: REP replicating to " << req->get_source() << " dn " << *dn << dendl;
	    MDiscoverReply *reply = new MDiscoverReply(curdir->dirfrag());
	    reply->add_dentry( dn->replicate_to( from ) );
	    if (dn->is_primary())
	      reply->add_inode( dn->inode->replicate_to( from ) );
	    mds->send_message_mds(reply, req->get_source().num());
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
    dout(12) << "traverse: miss on dentry " << path[depth] << " in " << *curdir << dendl;
    
    if (curdir->is_auth()) {
      // dentry is mine.
      if (curdir->is_complete()) {
        // file not found
        return -ENOENT;
      } else {
	// directory isn't complete; reload
        dout(7) << "traverse: incomplete dir contents for " << *cur << ", fetching" << dendl;
        touch_inode(cur);
        curdir->fetch(_get_waiter(mdr, req));
	if (mds->logger) mds->logger->inc("tdirf");
        return 1;
      }
    } else {
      // dirfrag/dentry is not mine.
      pair<int,int> dauth = curdir->authority();

      if ((onfail == MDS_TRAVERSE_DISCOVER ||
           onfail == MDS_TRAVERSE_DISCOVERXLOCK)) {
	dout(7) << "traverse: discover from " << path[depth] << " from " << *curdir << dendl;
	discover_path(curdir, path.postfixpath(depth), _get_waiter(mdr, req),
		      onfail == MDS_TRAVERSE_DISCOVERXLOCK);
	if (mds->logger) mds->logger->inc("tdis");
        return 1;
      } 
      if (onfail == MDS_TRAVERSE_FORWARD) {
        // forward
        dout(7) << "traverse: not auth for " << path << " in " << *curdir << dendl;
	
	if (curdir->is_ambiguous_auth()) {
	  // wait
	  dout(7) << "traverse: waiting for single auth in " << *curdir << dendl;
	  curdir->add_waiter(CDir::WAIT_SINGLEAUTH, _get_waiter(mdr, req));
	  return 1;
	} 

	dout(7) << "traverse: forwarding, not auth for " << *curdir << dendl;
	
	// request replication?
	if (mdr && mdr->client_request && curdir->is_rep()) {
	  dout(15) << "traverse: REP fw to mds" << dauth << ", requesting rep under "
		   << *curdir << " req " << *(MClientRequest*)req << dendl;
	  mdr->client_request->set_mds_wants_replica_in_dirino(curdir->ino());
	  req->clear_payload();  // reencode!
	}
	
	if (mdr) 
	  request_forward(mdr, dauth.first);
	else
	  mds->forward_message_mds(req, dauth.first);
	
	if (mds->logger) mds->logger->inc("tfw");
	return 2;
      }    
      if (onfail == MDS_TRAVERSE_FAIL)
        return -ENOENT;  // not necessarily exactly true....
    }
    
    assert(0);  // i shouldn't get here
  }
  
  // success.
  if (mds->logger) mds->logger->inc("thit");
  return 0;
}

bool MDCache::path_is_mine(filepath& path)
{
  dout(15) << "path_is_mine " << path << dendl;
  
  // start at root.  FIXME.
  CInode *cur = root;
  assert(cur);

  for (unsigned i=0; i<path.depth(); i++) {
    dout(15) << "path_is_mine seg " << i << ": " << path[i] << " under " << *cur << dendl;
    frag_t fg = cur->pick_dirfrag(path[i]);
    CDir *dir = cur->get_dirfrag(fg);
    if (!dir) return cur->is_auth();
    CDentry *dn = dir->lookup(path[i]);
    if (!dn) return dir->is_auth();
    assert(dn->is_primary());
    cur = dn->get_inode();
  }

  return cur->is_auth();
}

/**
 * path_traverse_to_dir -- traverse to deepest dir we have
 *
 * @path - path to traverse (as far as we can)
 *
 * assumes we _don't_ have the full path.  (if we do, we return NULL.)
 */
CDir *MDCache::path_traverse_to_dir(filepath& path)
{
  CInode *cur = root;
  assert(cur);
  for (unsigned i=0; i<path.depth(); i++) {
    dout(20) << "path_traverse_to_dir seg " << i << ": " << path[i] << " under " << *cur << dendl;
    frag_t fg = cur->pick_dirfrag(path[i]);
    CDir *dir = cur->get_or_open_dirfrag(this, fg);
    CDentry *dn = dir->lookup(path[i]);
    if (!dn) return dir;
    assert(dn->is_primary());
    cur = dn->get_inode();
  }

  return NULL; // oh, we have the full path.
}


/**
 * open_remote_dir -- open up a remote dirfrag
 *
 * @diri - base inode
 * @approxfg - approximate fragment.
 * @fin - completion callback
 */
void MDCache::open_remote_dirfrag(CInode *diri, frag_t approxfg, Context *fin) 
{
  dout(10) << "open_remote_dir on " << *diri << dendl;
  
  assert(diri->is_dir());
  assert(!diri->is_auth());
  assert(diri->get_dirfrag(approxfg) == 0);

  int auth = diri->authority().first;

  if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
    discover_dir_frag(diri, approxfg, fin);
  } else {
    // mds is down or recovering.  forge a replica!
    forge_replica_dir(diri, approxfg, auth);
  }
}


/** 
 * get_dentry_inode - get or open inode
 *
 * @dn the dentry
 * @mdr current request
 *
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
    dout(7) << "get_dentry_inode linking in remote in " << *in << dendl;
    dn->link_remote(in);
    return in;
  } else {
    dout(10) << "get_dentry_inode on remote dn, opening inode for " << *dn << dendl;
    open_remote_ino(dn->get_remote_ino(), mdr, new C_MDS_RetryRequest(this, mdr));
    return 0;
  }
}

class C_MDC_RetryOpenRemoteIno : public Context {
  MDCache *mdcache;
  inodeno_t ino;
  MDRequest *mdr;
  Context *onfinish;
public:
  C_MDC_RetryOpenRemoteIno(MDCache *mdc, inodeno_t i, MDRequest *r, Context *c) :
    mdcache(mdc), ino(i), mdr(r), onfinish(c) {}
  void finish(int r) {
    mdcache->open_remote_ino(ino, mdr, onfinish);
  }
};


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
  dout(7) << "open_remote_ino on " << ino << dendl;
  
  C_MDC_OpenRemoteIno *c = new C_MDC_OpenRemoteIno(this, ino, mdr, onfinish);
  mds->anchorclient->lookup(ino, c->anchortrace, c);
}

void MDCache::open_remote_ino_2(inodeno_t ino,
                                MDRequest *mdr,
                                vector<Anchor>& anchortrace,
                                Context *onfinish)
{
  dout(7) << "open_remote_ino_2 on " << ino
	  << ", trace depth is " << anchortrace.size() << dendl;
  
  // find deepest cached inode in prefix
  unsigned i = anchortrace.size();  // i := array index + 1
  CInode *in = 0;
  while (1) {
    // inode?
    dout(10) << " " << i << ": " << anchortrace[i-1] << dendl;
    in = get_inode(anchortrace[i-1].ino);
    if (in) break;
    i--;
    if (!i) {
      in = get_inode(anchortrace[i].dirfrag.ino);
      assert(in);  // actually, we may need to open the root or a foreign stray inode, here.
      break;
    }
  }
  dout(10) << "deepest cached inode at " << i << " is " << *in << dendl;

  if (in->ino() == ino) {
    // success
    dout(10) << "open_remote_ino_2 have " << *in << dendl;
    onfinish->finish(0);
    delete onfinish;
    return;
  } 

  // open dirfrag beneath *in
  frag_t frag = anchortrace[i].dirfrag.frag;

  if (!in->dirfragtree.contains(frag)) {
    dout(10) << "frag " << frag << " not valid, requerying anchortable" << dendl;
    open_remote_ino(ino, mdr, onfinish);
    return;
  }

  CDir *dir = in->get_dirfrag(frag);

  if (!dir && !in->is_auth()) {
    dout(10) << "opening remote dirfrag " << frag << " under " << *in << dendl;
    /* FIXME: we re-query the anchortable just to avoid a fragtree update race */
    open_remote_dirfrag(in, frag,
			new C_MDC_RetryOpenRemoteIno(this, ino, mdr, onfinish));
    return;
  }

  if (!dir && in->is_auth())
    dir = in->get_or_open_dirfrag(this, frag);
  
  assert(dir);
  if (dir->is_auth()) {
    if (dir->is_complete()) {
      // hrm.  requery anchor table.
      dout(10) << "expected ino " << anchortrace[i].ino
	       << " in complete dir " << *dir
	       << ", requerying anchortable"
	       << dendl;
      open_remote_ino(ino, mdr, onfinish);
    } else {
      dout(10) << "need ino " << anchortrace[i].ino
	       << ", fetching incomplete dir " << *dir
	       << dendl;
      dir->fetch(new C_MDC_OpenRemoteIno(this, ino, anchortrace, mdr, onfinish));
    }
  } else {
    // hmm, discover.
    dout(10) << "have remote dirfrag " << *dir << ", discovering " 
	     << anchortrace[i].ino << dendl;
    discover_ino(dir, anchortrace[i].ino, 
		 new C_MDC_OpenRemoteIno(this, ino, anchortrace, mdr, onfinish));
  }
}




void MDCache::make_trace(vector<CDentry*>& trace, CInode *in)
{
  CInode *parent = in->get_parent_inode();
  if (parent) {
    make_trace(trace, parent);

    CDentry *dn = in->get_parent_dn();
    dout(15) << "make_trace adding " << *dn << dendl;
    trace.push_back(dn);
  }
}


MDRequest *MDCache::request_start(MClientRequest *req)
{
  // did we win a forward race against a slave?
  if (active_requests.count(req->get_reqid())) {
    MDRequest *mdr = active_requests[req->get_reqid()];
    if (mdr->is_slave()) {
      dout(10) << "request_start already had " << *mdr << ", cleaning up" << dendl;
      request_cleanup(mdr);
      delete mdr;
    } else {
      dout(10) << "request_start already processing " << *mdr << ", dropping new msg" << dendl;
      delete req;
      return 0;
    }
  }

  // register new client request
  MDRequest *mdr = new MDRequest(req->get_reqid(), req);
  active_requests[req->get_reqid()] = mdr;
  dout(7) << "request_start " << *mdr << dendl;
  return mdr;
}

MDRequest *MDCache::request_start_slave(metareqid_t ri, int by)
{
  MDRequest *mdr = new MDRequest(ri, by);
  assert(active_requests.count(mdr->reqid) == 0);
  active_requests[mdr->reqid] = mdr;
  dout(7) << "request_start_slave " << *mdr << " by mds" << by << dendl;
  return mdr;
}


MDRequest *MDCache::request_get(metareqid_t rid)
{
  assert(active_requests.count(rid));
  dout(7) << "request_get " << rid << " " << *active_requests[rid] << dendl;
  return active_requests[rid];
}

void MDCache::request_finish(MDRequest *mdr)
{
  dout(7) << "request_finish " << *mdr << dendl;

  // slave finisher?
  if (mdr->more()->slave_commit) {
    mdr->more()->slave_commit->finish(0);
    delete mdr->more()->slave_commit;
    mdr->more()->slave_commit = 0;
  }

  if (mdr->client_request && mds->logger) {
    mds->logger->inc("reply");
    mds->logger->favg("replyl", g_clock.now() - mdr->client_request->get_recv_stamp());
  }

  delete mdr->client_request;
  delete mdr->slave_request;
  request_cleanup(mdr);
}


void MDCache::request_forward(MDRequest *mdr, int who, int port)
{
  dout(7) << "request_forward " << *mdr << " to mds" << who << " req " << *mdr << dendl;
  
  mds->forward_message_mds(mdr->client_request, who);  
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
	       << " on " << *(*p)->get_parent() << dendl;
      (*p)->put_xlock();
      mdr->locks.erase(*p);
      mdr->xlocks.erase(p++);
    }
  }
}

void MDCache::request_cleanup(MDRequest *mdr)
{
  dout(15) << "request_cleanup " << *mdr << dendl;
  metareqid_t ri = mdr->reqid;

  // clear ref, trace
  mdr->ref = 0;
  mdr->trace.clear();

  // clean up slaves
  //  (will implicitly drop remote dn pins)
  for (set<int>::iterator p = mdr->more()->slaves.begin();
       p != mdr->more()->slaves.end();
       ++p) {
    MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_FINISH);
    mds->send_message_mds(r, *p);
  }
  // strip foreign xlocks out of lock lists, since the OP_FINISH drops them implicitly.
  request_forget_foreign_locks(mdr);


  // drop locks
  mds->locker->drop_locks(mdr);

  // drop (local) auth pins
  mdr->drop_local_auth_pins();

  // drop stickydirs
  for (set<CInode*>::iterator p = mdr->stickydirs.begin();
       p != mdr->stickydirs.end();
       ++p) 
    (*p)->put_stickydirs();

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
    dout(7) << "anchor_create not authpinnable, waiting on " << *in << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE, onfinish);
    return;
  }

  // wait
  in->add_waiter(CInode::WAIT_ANCHORED, onfinish);

  // already anchoring?
  if (in->state_test(CInode::STATE_ANCHORING)) {
    dout(7) << "anchor_create already anchoring " << *in << dendl;
    return;
  }

  dout(7) << "anchor_create " << *in << dendl;

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
  LogSegment *ls;
public:
  C_MDC_AnchorCreateLogged(MDCache *c, CInode *i, version_t t, LogSegment *s) : 
    cache(c), in(i), atid(t), ls(s) {}
  void finish(int r) {
    cache->_anchor_create_logged(in, atid, ls);
  }
};

void MDCache::_anchor_create_prepared(CInode *in, version_t atid)
{
  dout(10) << "_anchor_create_prepared " << *in << " atid " << atid << dendl;
  assert(in->inode.anchored == false);

  // update the logged inode copy
  inode_t *pi = in->project_inode();
  pi->anchored = true;
  pi->version = in->pre_dirty();

  // note anchor transaction
  EUpdate *le = new EUpdate(mds->mdlog, "anchor_create");
  le->metablob.add_dir_context(in->get_parent_dir());
  le->metablob.add_primary_dentry(in->parent, true, 0, pi);
  le->metablob.add_anchor_transaction(atid);
  mds->mdlog->submit_entry(le, new C_MDC_AnchorCreateLogged(this, in, atid,
							    mds->mdlog->get_current_segment()));
}


void MDCache::_anchor_create_logged(CInode *in, version_t atid, LogSegment *ls)
{
  dout(10) << "_anchor_create_logged on " << *in << dendl;

  // unpin
  assert(in->state_test(CInode::STATE_ANCHORING));
  in->state_clear(CInode::STATE_ANCHORING);
  in->put(CInode::PIN_ANCHORING);
  in->auth_unpin();
  
  // apply update to cache
  in->pop_and_dirty_projected_inode(ls);
  
  // tell the anchortable we've committed
  mds->anchorclient->commit(atid, ls);

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
    dout(7) << "anchor_destroy not authpinnable, waiting on " << *in << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE, onfinish);
    return;
  }

  // wait
  if (onfinish)
    in->add_waiter(CInode::WAIT_UNANCHORED, onfinish);

  // already anchoring?
  if (in->state_test(CInode::STATE_UNANCHORING)) {
    dout(7) << "anchor_destroy already unanchoring " << *in << dendl;
    return;
  }

  dout(7) << "anchor_destroy " << *in << dendl;

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
  LogSegment *ls;
public:
  C_MDC_AnchorDestroyLogged(MDCache *c, CInode *i, version_t t, LogSegment *l) :
    cache(c), in(i), atid(t), ls(l) {}
  void finish(int r) {
    cache->_anchor_destroy_logged(in, atid, ls);
  }
};

void MDCache::_anchor_destroy_prepared(CInode *in, version_t atid)
{
  dout(10) << "_anchor_destroy_prepared " << *in << " atid " << atid << dendl;

  assert(in->inode.anchored == true);

  // update the logged inode copy
  inode_t *pi = in->project_inode();
  pi->anchored = true;
  pi->version = in->pre_dirty();

  // log + wait
  EUpdate *le = new EUpdate(mds->mdlog, "anchor_destroy");
  le->metablob.add_dir_context(in->get_parent_dir());
  le->metablob.add_primary_dentry(in->parent, true, 0, pi);
  le->metablob.add_anchor_transaction(atid);
  mds->mdlog->submit_entry(le, new C_MDC_AnchorDestroyLogged(this, in, atid, mds->mdlog->get_current_segment()));
}


void MDCache::_anchor_destroy_logged(CInode *in, version_t atid, LogSegment *ls)
{
  dout(10) << "_anchor_destroy_logged on " << *in << dendl;
  
  // unpin
  assert(in->state_test(CInode::STATE_UNANCHORING));
  in->state_clear(CInode::STATE_UNANCHORING);
  in->put(CInode::PIN_UNANCHORING);
  in->auth_unpin();
  
  // apply update to cache
  in->pop_and_dirty_projected_inode(ls);

  // tell the anchortable we've committed
  mds->anchorclient->commit(atid, ls);

  // trigger waiters
  in->finish_waiting(CInode::WAIT_UNANCHORED, 0);
}


// -------------------------------------------------------------------------------
// STRAYS

void MDCache::eval_stray(CDentry *dn)
{
  dout(10) << "eval_stray " << *dn << dendl;
  assert(dn->is_primary());
  CInode *in = dn->inode;
  assert(in);

  if (!dn->is_auth()) return;  // has to be mine

  // purge?
  if (in->inode.nlink == 0) {
    if (dn->is_replicated() || in->is_any_caps()) return;  // wait
    if (!in->dirfrags.empty()) return;  // wait for dirs to close/trim
    _purge_stray(dn);
  }
  else if (in->inode.nlink == 1) {
    // trivial reintegrate?
    if (!in->remote_parents.empty()) {
      CDentry *rlink = *in->remote_parents.begin();
      if (rlink->is_auth() && rlink->dir->can_auth_pin())
	reintegrate_stray(dn, rlink);
      
      if (!rlink->is_auth() && dn->is_auth())
	migrate_stray(dn, mds->get_nodeid(), rlink->authority().first);
    }
  } else {
    // wait for next use.
  }
}

void MDCache::eval_remote(CDentry *dn)
{
  dout(10) << "eval_remote " << *dn << dendl;
  assert(dn->is_remote());
  CInode *in = dn->get_inode();
  if (!in) return;

  // refers to stray?
  if (in->get_parent_dn()->get_dir()->get_inode()->is_stray()) {
    if (in->is_auth())
      eval_stray(in->get_parent_dn());
    else
      migrate_stray(in->get_parent_dn(), in->authority().first, mds->get_nodeid());
  }
}


class C_MDC_PurgeStray : public Context {
  MDCache *cache;
  CDentry *dn;
  version_t pdv;
  LogSegment *ls;
public:
  C_MDC_PurgeStray(MDCache *c, CDentry *d, version_t v, LogSegment *s) : 
    cache(c), dn(d), pdv(v), ls(s) { }
  void finish(int r) {
    cache->_purge_stray_logged(dn, pdv, ls);
  }
};

void MDCache::_purge_stray(CDentry *dn)
{
  dout(10) << "_purge_stray " << *dn << " " << *dn->inode << dendl;
  assert(!dn->is_replicated());

  // log removal
  version_t pdv = dn->pre_dirty();

  EUpdate *le = new EUpdate(mds->mdlog, "purge_stray");
  le->metablob.add_dir_context(dn->dir);
  le->metablob.add_null_dentry(dn, true);
  le->metablob.add_inode_truncate(dn->inode->ino(), 0, dn->inode->inode.size);

  mds->mdlog->submit_entry(le, new C_MDC_PurgeStray(this, dn, pdv, mds->mdlog->get_current_segment()));


}

void MDCache::_purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls)
{
  dout(10) << "_purge_stray_logged " << *dn << " " << *dn->inode << dendl;
  CInode *in = dn->inode;
  
  // dirty+unlink dentry
  dn->dir->mark_dirty(pdv, ls);
  dn->dir->unlink_inode(dn);
  dn->dir->remove_dentry(dn);

  // purge+remove inode
  in->mark_clean();
  purge_inode(in, 0, in->inode.size, ls);
}



void MDCache::reintegrate_stray(CDentry *straydn, CDentry *rdn)
{
  dout(10) << "reintegrate_stray " << *straydn << " into " << *rdn << dendl;
  
  // rename it to another mds.
  filepath src;
  straydn->make_path(src);
  filepath dst;
  rdn->make_path(dst);

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME, mds->messenger->get_myinst());
  req->set_filepath(src);
  req->set_filepath2(dst);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, rdn->authority().first);
}
 

void MDCache::migrate_stray(CDentry *dn, int from, int to)
{
  dout(10) << "migrate_stray from mds" << from << " to mds" << to 
	   << " " << *dn << " " << *dn->inode << dendl;

  // rename it to another mds.
  string dname;
  dn->get_inode()->name_stray_dentry(dname);
  filepath src(dname, MDS_INO_STRAY(from));
  filepath dst(dname, MDS_INO_STRAY(to));

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME, mds->messenger->get_myinst());
  req->set_filepath(src);
  req->set_filepath2(dst);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, to);
}




// ========================================================================================
// DISCOVER
/*

  - for all discovers (except base_inos, e.g. root, stray), waiters are attached
  to the parent metadata object in the cache (pinning it).
  
  - the discover is also registered under the per-mds discover_ hashes, so that 
  waiters can be kicked in the event of a failure.  that is, every discover will 
  be followed by a reply, unless the remote node fails..
  
  - each discover_reply must reliably decrement the discover_ counts.

  - base_inos are the exception.  those waiters are under waiting_for_base_ino.

*/

void MDCache::discover_base_ino(inodeno_t want_ino,
				Context *onfinish,
				int from) 
{
  dout(7) << "discover_base_ino " << want_ino << " from mds" << from << dendl;
  if (waiting_for_base_ino[from].count(want_ino) == 0) {
    filepath want_path;
    MDiscover *dis = new MDiscover(mds->get_nodeid(),
				   want_ino,
				   want_path,
				   false);
    mds->send_message_mds(dis, from);
  }

  waiting_for_base_ino[from][want_ino].push_back(onfinish);
}


void MDCache::discover_dir_frag(CInode *base,
				frag_t approx_fg,
				Context *onfinish,
				int from)
{
  if (from < 0) from = base->authority().first;

  dout(7) << "discover_dir_frag " << base->ino() << " " << approx_fg
	  << " from mds" << from << dendl;

  if (!base->is_waiter_for(CInode::WAIT_DIR) || !onfinish) {    // this is overly conservative
    filepath want_path;
    MDiscover *dis = new MDiscover(mds->get_nodeid(),
				   base->ino(),
				   want_path,
				   true);  // need the base dir open
    dis->set_base_dir_frag(approx_fg);
    mds->send_message_mds(dis, from);
  }

  // register + wait
  if (onfinish) 
    base->add_waiter(CInode::WAIT_DIR, onfinish);
  discover_dir[from][base->ino()]++;
}

void MDCache::discover_path(CInode *base,
			    filepath want_path,
			    Context *onfinish,
			    bool want_xlocked,
			    int from)
{
  if (from < 0) from = base->authority().first;

  dout(7) << "discover_path " << base->ino() << " " << want_path << " from mds" << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;

  if (base->is_ambiguous_auth()) {
    dout(10) << " waiting for single auth on " << *base << dendl;
    base->add_waiter(CInode::WAIT_SINGLEAUTH, onfinish);
    return;
  } 

  if (!base->is_waiter_for(CInode::WAIT_DIR) || !onfinish) {    // this is overly conservative
    MDiscover *dis = new MDiscover(mds->get_nodeid(),
				   base->ino(),
				   want_path,
				   true,        // we want the base dir; we are relative to ino.
				   want_xlocked);
    mds->send_message_mds(dis, from);
  }

  // register + wait
  if (onfinish) base->add_waiter(CInode::WAIT_DIR, onfinish);
  discover_dir[from][base->ino()]++;
}

void MDCache::discover_path(CDir *base,
			    filepath want_path,
			    Context *onfinish,
			    bool want_xlocked)
{
  int from = base->authority().first;

  dout(7) << "discover_path " << base->dirfrag() << " " << want_path << " from mds" << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;

  if (base->is_ambiguous_auth()) {
    dout(7) << " waiting for single auth on " << *base << dendl;
    base->add_waiter(CDir::WAIT_SINGLEAUTH, onfinish);
    return;
  }

  if (!base->is_waiting_for_dentry(want_path[0]) || !onfinish) {
    MDiscover *dis = new MDiscover(mds->get_nodeid(),
				   base->ino(),
				   want_path,
				   false,   // no base dir; we are relative to dir
				   want_xlocked);
    mds->send_message_mds(dis, from);
  }

  // register + wait
  if (onfinish) base->add_dentry_waiter(want_path[0], onfinish);
  discover_dir_sub[from][base->dirfrag()]++;
}

void MDCache::discover_ino(CDir *base,
			   inodeno_t want_ino,
			   Context *onfinish,
			   bool want_xlocked)
{
  int from = base->authority().first;

  dout(7) << "discover_ino " << base->dirfrag() << " " << want_ino << " from mds" << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;
  
  if (!base->is_waiting_for_ino(want_ino)) {
    MDiscover *dis = new MDiscover(mds->get_nodeid(),
				   base->dirfrag(),
				   want_ino,
				   want_xlocked);
    mds->send_message_mds(dis, from);
  }
  
  // register + wait
  base->add_ino_waiter(want_ino, onfinish);
  discover_dir_sub[from][base->dirfrag()]++;
}



void MDCache::kick_discovers(int who)
{
  list<Context*> waiters;

  for (hash_map<inodeno_t, list<Context*> >::iterator p = waiting_for_base_ino[who].begin();
       p != waiting_for_base_ino[who].end();
       ++p) {
    dout(10) << "kick_discovers on base ino " << p->first << dendl;
    mds->queue_waiters(p->second);
  }
  waiting_for_base_ino.erase(who);

  for (hash_map<inodeno_t,int>::iterator p = discover_dir[who].begin();
       p != discover_dir[who].end();
       ++p) {
    CInode *in = get_inode(p->first);
    if (!in) continue;
    dout(10) << "kick_discovers dir waiters on " << *in << dendl;
    in->take_waiting(CInode::WAIT_DIR, waiters);
  }
  discover_dir.erase(who);

  for (hash_map<dirfrag_t,int>::iterator p = discover_dir_sub[who].begin();
       p != discover_dir_sub[who].end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) continue;
    dout(10) << "kick_discovers dentry+ino waiters on " << *dir << dendl;
    dir->take_sub_waiting(waiters);
  }
  discover_dir_sub.erase(who);

  mds->queue_waiters(waiters);
}



void MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();

  assert(dis->get_asker() != whoami);

  /*
  if (mds->get_state() < MDSMap::STATE_ACTIVE) {
    dout(-7) << "discover_reply NOT ACTIVE YET" << dendl;
    delete dis;
    return;
  }
  */


  CInode *cur = 0;
  MDiscoverReply *reply = new MDiscoverReply(dis);

  // get started.
  if (dis->get_base_ino() == MDS_INO_ROOT) {
    // wants root
    dout(7) << "handle_discover from mds" << dis->get_asker()
	    << " wants root + " << dis->get_want().get_path() << dendl;

    assert(mds->get_nodeid() == 0);
    assert(root->is_auth());

    // add root
    reply->add_inode( root->replicate_to( dis->get_asker() ) );
    dout(10) << "added root " << *root << dendl;

    cur = root;
  }
  else if (dis->get_base_ino() == MDS_INO_STRAY(whoami)) {
    // wants root
    dout(7) << "handle_discover from mds" << dis->get_asker()
	    << " wants stray + " << dis->get_want().get_path() << dendl;
    
    reply->add_inode( stray->replicate_to( dis->get_asker() ) );
    dout(10) << "added stray " << *stray << dendl;

    cur = stray;
  }
  else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino());
    
    if (!cur) {
      dout(7) << "handle_discover mds" << dis->get_asker() 
	      << " don't have base ino " << dis->get_base_ino() 
	      << dendl;
      reply->set_flag_error_dir();
    }

    if (dis->wants_base_dir()) {
      dout(7) << "handle_discover mds" << dis->get_asker() 
	      << " wants basedir+" << dis->get_want().get_path() 
	      << " has " << *cur 
	      << dendl;
    } else {
      dout(7) << "handle_discover mds" << dis->get_asker() 
	      << " wants " << dis->get_want().get_path()
	      << " has " << *cur
	      << dendl;
    }
  }

  assert(reply);
  
  // add content
  // do some fidgeting to include a dir if they asked for the base dir, or just root.
  for (unsigned i = 0; 
       cur && (i < dis->get_want().depth() || dis->get_want().depth() == 0); 
       i++) {

    // -- figure out the dir

    // is *cur even a dir at all?
    if (!cur->is_dir()) {
      dout(7) << *cur << " not a dir" << dendl;
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
      assert(dis->wants_base_dir() || dis->get_want_ino() || dis->get_base_ino() < MDS_INO_BASE);
    }
    CDir *curdir = cur->get_dirfrag(fg);

    if ((!curdir && !cur->is_auth()) ||
	(curdir && !curdir->is_auth())) {

	/* before:
	 * ONLY set flag if empty!!
	 * otherwise requester will wake up waiter(s) _and_ continue with discover,
	 * resulting in duplicate discovers in flight,
	 * which can wreak havoc when discovering rename srcdn (which may move)
	 */

      if (reply->is_empty()) {
	// only hint if empty.
	//  someday this could be better, but right now the waiter logic isn't smart enough.
	
	// hint
	if (curdir) {
	  dout(7) << " not dirfrag auth, setting dir_auth_hint for " << *curdir << dendl;
	  reply->set_dir_auth_hint(curdir->authority().first);
	} else {
	  dout(7) << " dirfrag not open, not inode auth, setting dir_auth_hint for " 
		  << *cur << dendl;
	  reply->set_dir_auth_hint(cur->authority().first);
	}
	
	// note error dentry, if any
	//  NOTE: important, as it allows requester to issue an equivalent discover
	//        to whomever we hint at.
	if (dis->get_want().depth() > i)
	  reply->set_error_dentry(dis->get_dentry(i));
      }

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
	dout(7) << *curdir << " is frozen, empty reply, waiting" << dendl;
	curdir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	delete reply;
	return;
      } else {
	dout(7) << *curdir << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
    }
    
    // add dir
    if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "handle_discover not adding unwanted base dir " << *curdir << dendl;
    } else {
      assert(!curdir->is_ambiguous_auth()); // would be frozen.
      reply->add_dir( curdir->replicate_to(dis->get_asker()) );
      dout(7) << "handle_discover added dir " << *curdir << dendl;
    }

    // lookup
    CDentry *dn = 0;
    if (dis->get_want_ino()) {
      // lookup by ino
      CInode *in = get_inode(dis->get_want_ino());
      if (in && in->is_auth() && in->get_parent_dn()->get_dir() == curdir)
	dn = in->get_parent_dn();
    } else if (dis->get_want().depth() > 0) {
      // lookup dentry
      dn = curdir->lookup( dis->get_dentry(i) );
    } else 
      break; // done!
          
    // incomplete dir?
    if (!dn) {
      if (!curdir->is_complete()) {
	// readdir
	dout(7) << "incomplete dir contents for " << *curdir << ", fetching" << dendl;
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
		<< *curdir << dendl;
	reply->set_flag_error_ino();
	break;
      }
      
      // send null dentry
      dout(7) << "dentry " << dis->get_dentry(i) << " dne, returning null in "
	      << *curdir << dendl;
      dn = curdir->add_null_dentry(dis->get_dentry(i));
    }
    assert(dn);

    // xlocked dentry?
    //  ...always block on non-tail items (they are unrelated)
    //  ...allow xlocked tail disocvery _only_ if explicitly requested
    bool tailitem = (dis->get_want().depth() == 0) || (i == dis->get_want().depth() - 1);
    if (dn->lock.is_xlocked()) {
      // is this the last (tail) item in the discover traversal?
      if (tailitem && dis->wants_xlocked()) {
	dout(7) << "handle_discover allowing discovery of xlocked tail " << *dn << dendl;
      } else {
	dout(7) << "handle_discover blocking on xlocked " << *dn << dendl;
	dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryMessage(mds, dis));
	delete reply;
	return;
      }
    }

    // frozen inode?
    if (dn->is_primary() && dn->inode->is_frozen()) {
      if (tailitem && dis->wants_xlocked()) {
	dout(7) << "handle_discover allowing discovery of frozen tail " << *dn->inode << dendl;
      } else if (reply->is_empty()) {
	dout(7) << *dn->inode << " is frozen, empty reply, waiting" << dendl;
	dn->inode->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	delete reply;
	return;
      } else {
	dout(7) << *dn->inode << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
    }

    // add dentry
    reply->add_dentry( dn->replicate_to( dis->get_asker() ) );
    dout(7) << "handle_discover added dentry " << *dn << dendl;
    
    if (!dn->is_primary()) break;  // stop on null or remote link.
    
    // add inode
    CInode *next = dn->inode;
    assert(next->is_auth());
    
    reply->add_inode( next->replicate_to( dis->get_asker() ) );
    dout(7) << "handle_discover added inode " << *next << dendl;
    
    // descend, keep going.
    cur = next;
    continue;
  }

  // how did we do?
  assert(!reply->is_empty());
  dout(7) << "handle_discover sending result back to asker mds" << dis->get_asker() << dendl;
  mds->send_message_mds(reply, dis->get_asker());

  delete dis;
}


void MDCache::handle_discover_reply(MDiscoverReply *m) 
{
  /*
  if (mds->get_state() < MDSMap::STATE_ACTIVE) {
    dout(-7) << "discover_reply NOT ACTIVE YET" << dendl;
    delete m;
    return;
  }
  */

  list<Context*> finished, error;
  int from = m->get_source().num();

  // starting point
  CInode *cur = get_inode(m->get_base_ino());

  if (m->has_base_inode()) {
    assert(m->get_base_ino() < MDS_INO_BASE);
    assert(!m->has_base_dentry());
    assert(!m->has_base_dir());

    // add base inode
    cur = add_replica_inode(m->get_inode(0), NULL, finished);

    dout(7) << "discover_reply got base inode " << *cur << dendl;
    
    // take waiters
    finished.swap(waiting_for_base_ino[from][cur->ino()]);
    waiting_for_base_ino[from].erase(cur->ino());
  }
  assert(cur);

  dout(7) << "discover_reply " << *cur
	  << " + " << m->get_num_dentries() << " dn, "
	  << m->get_num_inodes() << " inodes"
	  << dendl;
  
  // fyi
  if (m->is_flag_error_dir()) 
    dout(7) << " flag error, dir" << dendl;
  if (m->is_flag_error_dn()) 
    dout(7) << " flag error, dentry = " << m->get_error_dentry() << dendl;
  if (m->is_flag_error_ino()) 
    dout(7) << " flag error, ino = " << m->get_wanted_ino() << dendl;

  dout(10) << "depth = " << m->get_depth()
	   << ", has base_dir/base_dn/root = " 
	   << m->has_base_dir() << " / " << m->has_base_dentry() << " / " << m->has_base_inode()
	   << ", num dirs/dentries/inodes = " 
	   << m->get_num_dirs() << " / " << m->get_num_dentries() << " / " << m->get_num_inodes()
	   << dendl;
  
  // decrement discover counters
  if (m->get_wanted_base_dir()) {
    inodeno_t ino = m->get_base_ino();
    assert(discover_dir[from].count(ino));
    if (--discover_dir[from][ino] == 0)
      discover_dir[from].erase(ino);
  } else if (m->get_base_ino() >= MDS_INO_BASE) {
    dirfrag_t df(m->get_base_ino(), m->get_base_dir_frag());
    assert(discover_dir_sub[from].count(df));
    if (--discover_dir_sub[from][df] == 0)
      discover_dir_sub[from].erase(df);
  }

  // loop over discover results.
  // indexes follow each ([[dir] dentry] inode) 
  // can start, end with any type.
  for (int i=m->has_base_inode(); i<m->get_depth(); i++) {
    dout(10) << "discover_reply i=" << i << " cur " << *cur << dendl;

    // dir
    frag_t fg;
    CDir *curdir = 0;
    if (i > 0 || m->has_base_dir()) {
      assert(m->get_dir(i).get_dirfrag().ino == cur->ino());
      fg = m->get_dir(i).get_dirfrag().frag;
      curdir = add_replica_dir(cur, fg, m->get_dir(i), 
			       m->get_source().num(),
			       finished);
    }
    if (!curdir) {
      fg = cur->pick_dirfrag(m->get_dentry(i).get_dname());
      curdir = cur->get_dirfrag(fg);
    }
    
    // dentry error?
    if (i == m->get_depth()-1 && (m->is_flag_error_dn() || m->is_flag_error_ino())) {
      // error!
      assert(cur->is_dir());
      if (curdir) {
	if (m->get_error_dentry().length()) {
	  dout(7) << " flag_error on dentry " << m->get_error_dentry()
		  << ", triggering dentry" << dendl;
	  curdir->take_dentry_waiting(m->get_error_dentry(), error);
	} else {
	  dout(7) << " flag_error on ino " << m->get_wanted_ino()
		  << ", triggering ino" << dendl;
	  curdir->take_ino_waiting(m->get_wanted_ino(), error);
	}
      } else {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() 
		<< ", triggering dir?" << dendl;
        cur->take_waiting(CInode::WAIT_DIR, error);
      }
      break;
    }

    assert(curdir);

    // dentry
    CDentry *dn = 0;
    if (i >= m->get_last_dentry()) break;
    if (i > 0 || m->has_base_dentry()) 
      dn = add_replica_dentry(curdir, m->get_dentry(i), finished);

    // inode
    if (i >= m->get_last_inode()) break;
    cur = add_replica_inode(m->get_inode(i), dn, finished);
  }

  // dir error?
  // or dir_auth hint?
  if (m->is_flag_error_dir() && !cur->is_dir()) {
    // not a dir.
    cur->take_waiting(CInode::WAIT_DIR, error);
  } else if (m->is_flag_error_dir() ||
	     (m->get_dir_auth_hint() != CDIR_AUTH_UNKNOWN &&
	      m->get_dir_auth_hint() != mds->get_nodeid())) {
    int who = m->get_dir_auth_hint();
    if (who == mds->get_nodeid()) who = -1;
    if (who >= 0)
      dout(7) << " dir_auth_hint is " << m->get_dir_auth_hint() << dendl;

    // try again?
    if (m->get_error_dentry().length()) {
      // wanted a dentry
      frag_t fg = cur->pick_dirfrag(m->get_error_dentry());
      CDir *dir = cur->get_dirfrag(fg);
      if (dir) {
	// don't actaully need the hint, now
	if (dir->lookup(m->get_error_dentry()) == 0 &&
	    dir->is_waiting_for_dentry(m->get_error_dentry())) 
	  discover_path(dir, m->get_error_dentry(), 0, m->get_wanted_xlocked()); 
	else 
	  dout(7) << " doing nothing, have dir but nobody is waiting on dentry " 
		  << m->get_error_dentry() << dendl;
      } else {
	if (cur->is_waiter_for(CInode::WAIT_DIR)) 
	  discover_path(cur, m->get_error_dentry(), 0, m->get_wanted_xlocked(), who);
	else
	  dout(7) << " doing nothing, nobody is waiting for dir" << dendl;
      }
    } else {
      // wanted just the dir
      frag_t fg = m->get_base_dir_frag();
      if (cur->get_dirfrag(fg) == 0 && cur->is_waiter_for(CInode::WAIT_DIR))
	discover_dir_frag(cur, fg, 0, who);
      else
	dout(7) << " doing nothing, nobody is waiting for dir" << dendl;	  
    }
  }

  // waiters
  finish_contexts(error, -ENOENT);  // finish errors directly
  mds->queue_waiters(finished);

  // done
  delete m;
}



// ----------------------------
// REPLICAS

CDir *MDCache::add_replica_dir(CInode *diri, 
			       frag_t fg, CDirDiscover &dis, int from,
			       list<Context*>& finished)
{
  // add it (_replica_)
  CDir *dir = diri->get_dirfrag(fg);

  if (dir) {
    // had replica. update w/ new nonce.
    dis.update_dir(dir);
    dout(7) << "add_replica_dir had " << *dir << " nonce " << dir->replica_nonce << dendl;
  } else {
    // force frag to leaf in the diri tree
    if (!diri->dirfragtree.is_leaf(fg)) {
      dout(7) << "add_replica_dir forcing frag " << fg << " to leaf in the fragtree "
	      << diri->dirfragtree << dendl;
      diri->dirfragtree.force_to_leaf(fg);
    }

    // add replica.
    dir = diri->add_dirfrag( new CDir(diri, fg, this, false) );
    dis.update_dir(dir);

    // is this a dir_auth delegation boundary?
    if (from != diri->authority().first ||
	diri->is_ambiguous_auth() ||
	diri->ino() < MDS_INO_BASE)
      adjust_subtree_auth(dir, from);
    
    dout(7) << "add_replica_dir added " << *dir << " nonce " << dir->replica_nonce << dendl;
    
    // get waiters
    diri->take_waiting(CInode::WAIT_DIR, finished);
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

  dout(7) << "forge_replica_dir added " << *dir << " while mds" << from << " is down" << dendl;

  return dir;
}
		
CDentry *MDCache::add_replica_dentry(CDir *dir, CDentryDiscover &dis, list<Context*>& finished)
{
  CDentry *dn = dir->lookup( dis.get_dname() );
  
  // have it?
  if (dn) {
    dis.update_dentry(dn);
    dout(7) << "add_replica_dentry had " << *dn << dendl;
  } else {
    dn = dir->add_null_dentry(dis.get_dname());
    dis.update_dentry(dn);
    dis.init_dentry_lock(dn);
    dout(7) << "add_replica_dentry added " << *dn << dendl;
  }
  
  // remote_ino linkage?
  if (dis.get_remote_ino()) {
    if (dn->is_null()) 
      dir->link_remote_inode(dn, dis.get_remote_ino(), dis.get_remote_d_type());
    
    // hrm.  yeah.
    assert(dn->is_remote() && dn->get_remote_ino() == dis.get_remote_ino());
  }

  dir->take_dentry_waiting(dis.get_dname(), finished);

  return dn;
}

CInode *MDCache::add_replica_inode(CInodeDiscover& dis, CDentry *dn, list<Context*>& finished)
{
  CInode *in = get_inode(dis.get_ino());
  if (!in) {
    in = new CInode(this, false);
    dis.update_inode(in);
    dis.init_inode_locks(in);
    add_inode(in);
    if (in->is_base()) {
      if (in->ino() == MDS_INO_ROOT)
	in->inode_auth.first = 0;
      else if (MDS_INO_IS_STRAY(in->ino())) 
	in->inode_auth.first = in->ino() - MDS_INO_STRAY_OFFSET;
      else
	assert(0);
    }
    dout(10) << "add_replica_inode added " << *in << dendl;
    if (dn && dn->is_null()) 
      dn->dir->link_primary_inode(dn, in);
  } else {
    dis.update_inode(in);
    dout(10) << "add_replica_inode had " << *in << dendl;
  }

  if (dn) {
    assert(dn->is_primary());
    assert(dn->inode == in);
    
    dn->get_dir()->take_ino_waiting(in->ino(), finished);
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
  CInode *strayin = add_replica_inode(indis, NULL, finished);
  dout(15) << "strayin " << *strayin << dendl;
  
  // dir
  CDirDiscover dirdis;
  dirdis._decode(bl, off);
  CDir *straydir = add_replica_dir(strayin, dirdis.get_dirfrag().frag, dirdis,
					    from, finished);
  dout(15) << "straydir " << *straydir << dendl;
  
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
    dout(7) << "sending inode_update on " << *in << " to " << *it << dendl;
    assert(*it != mds->get_nodeid());
    mds->send_message_mds(new MInodeUpdate(in, in->get_cached_by_nonce(*it)), *it);
  }

  return 0;
}


void MDCache::handle_inode_update(MInodeUpdate *m)
{
  inodeno_t ino = m->get_ino();
  CInode *in = get_inode(m->get_ino());
  if (!in) {
    //dout(7) << "inode_update on " << m->get_ino() << ", don't have it, ignoring" << dendl;
    dout(7) << "inode_update on " << m->get_ino() << ", don't have it, sending expire" << dendl;
    MCacheExpire *expire = new MCacheExpire(mds->get_nodeid());
    expire->add_inode(m->get_ino(), m->get_nonce());
    mds->send_message_mds(expire, m->get_source().num());
    goto out;
  }

  if (in->is_auth()) {
    dout(7) << "inode_update on " << *in << ", but i'm the authority!" << dendl;
    assert(0); // this should never happen
  }
  
  dout(7) << "inode_update on " << *in << dendl;

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
  
  dout(7) << "sending dir_update on " << *dir << " bcast " << bcast << " to " << who << dendl;

  filepath path;
  dir->inode->make_path(path);

  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = who.begin();
       it != who.end();
       it++) {
    if (*it == whoami) continue;
    //if (*it == except) continue;
    dout(7) << "sending dir_update on " << *dir << " to " << *it << dendl;

    mds->send_message_mds(new MDirUpdate(dir->dirfrag(),
					 dir->dir_rep,
					 dir->dir_rep_by,
					 path,
					 bcast),
			  *it);
  }

  return 0;
}


void MDCache::handle_dir_update(MDirUpdate *m)
{
  CDir *dir = get_dirfrag(m->get_dirfrag());
  if (!dir) {
    dout(5) << "dir_update on " << m->get_dirfrag() << ", don't have it" << dendl;

    // discover it?
    if (m->should_discover()) {
      // only try once! 
      // this is key to avoid a fragtree update race, among other things.
      m->tried_discover();        
      vector<CDentry*> trace;
      filepath path = m->get_path();

      dout(5) << "trying discover on dir_update for " << path << dendl;

      int r = path_traverse(0, m,
			    path, trace, true,
                            MDS_TRAVERSE_DISCOVER);
      if (r > 0)
        return;
      assert(r == 0);

      CInode *in = get_inode(m->get_dirfrag().ino);
      assert(in);
      open_remote_dirfrag(in, m->get_dirfrag().frag, 
			  new C_MDS_RetryMessage(mds, m));
      return;
    }

    delete m;
    return;
  }

  // update
  dout(5) << "dir_update on " << *dir << dendl;
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
    dout(7) << "handle_dentry_unlink don't have dirfrag " << m->get_dirfrag() << dendl;
  }
  else {
    CDentry *dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << "handle_dentry_unlink don't have dentry " << *dir << " dn " << m->get_dn() << dendl;
    } else {
      dout(7) << "handle_dentry_unlink on " << *dn << dendl;
      
      // move to stray?
      CDentry *straydn = 0;
      if (m->strayin) {
	list<Context*> finished;
	CInode *in = add_replica_inode(*m->strayin, NULL, finished);
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
	straydn->dir->link_primary_inode(straydn, in);

	// send caps to auth (if we're not already)
	if (in->is_any_caps() &&
	    !in->state_test(CInode::STATE_EXPORTINGCAPS))
	  migrator->export_caps(in);
	
	lru.lru_bottouch(straydn);  // move stray to end of lru

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






// ===================================================================
// FRAGMENT


/** 
 * adjust_dir_fragments -- adjust fragmentation for a directory
 *
 * @diri - directory inode
 * @basefrag - base fragment
 * @bits - bit adjustment.  positive for split, negative for merge.
 */
void MDCache::adjust_dir_fragments(CInode *diri, frag_t basefrag, int bits,
				   list<CDir*>& resultfrags, 
				   list<Context*>& waiters)
{
  dout(10) << "adjust_dir_fragments " << basefrag << " " << bits 
	   << " on " << *diri << dendl;

  // yuck.  we may have discovered the inode while it was being fragmented.
  if (!diri->dirfragtree.is_leaf(basefrag))
    diri->dirfragtree.force_to_leaf(basefrag);

  CDir *base = diri->get_or_open_dirfrag(this, basefrag);

  // adjust fragtree
  diri->dirfragtree.split(basefrag, bits);
  dout(10) << " new fragtree is " << diri->dirfragtree << dendl;

  if (bits > 0) {
    if (base) {
      CDir *baseparent = base->get_parent_dir();

      base->split(bits, resultfrags, waiters);

      // did i change the subtree map?
      if (base->is_subtree_root()) {
	// am i a bound?
	if (baseparent) {
	  CDir *parent = get_subtree_root(baseparent);
	  assert(subtrees[parent].count(base));
	  subtrees[parent].erase(base);
	  for (list<CDir*>::iterator p = resultfrags.begin();
	       p != resultfrags.end();
	       ++p) { 
	    subtrees[parent].insert(*p);
	    subtrees[*p].clear();   // new frag is now its own subtree
	  }
	}

	// adjust my bounds.
	set<CDir*> bounds;
	bounds.swap(subtrees[base]);
	subtrees.erase(base);
	for (set<CDir*>::iterator p = bounds.begin();
	     p != bounds.end();
	     ++p) {
	  CDir *frag = get_subtree_root((*p)->get_parent_dir());
	  subtrees[frag].insert(*p);
	}
	
	show_subtrees(10);
      }
    }
  } else {
    assert(base);
    base->merge(bits, waiters);
    resultfrags.push_back(base);
    assert(0); // FIXME adjust subtree map!  and clean up this code, probably.
  }
}

class C_MDC_FragmentGo : public Context {
  MDCache *mdcache;
  CInode *diri;
  list<CDir*> dirs;
  frag_t basefrag;
  int bits;
public:
  C_MDC_FragmentGo(MDCache *m, CInode *di, list<CDir*>& dls, frag_t bf, int b) : 
    mdcache(m), diri(di), dirs(dls), basefrag(bf), bits(b) { }
  virtual void finish(int r) {
    mdcache->fragment_go(diri, dirs, basefrag, bits);
  }
};

void MDCache::split_dir(CDir *dir, int bits)
{
  dout(7) << "split_dir " << *dir << " bits " << bits << dendl;
  assert(dir->is_auth());
  
  if (mds->mdsmap->is_degraded()) {
    dout(7) << "cluster degraded, no fragmenting for now" << dendl;
    return;
  }
  if (dir->inode->is_root()) {
    dout(7) << "i won't fragment root" << dendl;
    //assert(0);
    return;
  }
  if (dir->state_test(CDir::STATE_FRAGMENTING)) {
    dout(7) << "already fragmenting" << dendl;
    return;
  }
  if (!dir->can_auth_pin()) {
    dout(7) << "not authpinnable on " << *dir << dendl;
    return;
  }

  list<CDir*> startfrags;
  startfrags.push_back(dir);
  
  dir->state_set(CDir::STATE_FRAGMENTING);
  
  fragment_freeze(dir->get_inode(), startfrags, dir->get_frag(), bits);
  fragment_mark_and_complete(dir->get_inode(), startfrags, dir->get_frag(), bits);
}

/*
 * initial the freeze, blocking with an auth_pin.
 * 
 * some reason(s) we have to freeze:
 *  - on merge, version/projected version are unified from all fragments;
 *    concurrent pipelined updates in the directory will have divergent
 *    versioning... and that's no good.
 */
void MDCache::fragment_freeze(CInode *diri, list<CDir*>& frags, frag_t basefrag, int bits)
{
  C_Gather *gather = new C_Gather(new C_MDC_FragmentGo(this, diri, frags, basefrag, bits));  
  
  // freeze the dirs
  for (list<CDir*>::iterator p = frags.begin();
       p != frags.end();
       ++p) {
    CDir *dir = *p;
    dir->auth_pin(); // this will block the freeze
    dir->freeze_dir();
    assert(dir->is_freezing_dir());
    dir->add_waiter(CDir::WAIT_FROZEN, gather->new_sub());
  }
}

class C_MDC_FragmentMarking : public Context {
  MDCache *mdcache;
  CInode *diri;
  list<CDir*> dirs;
  frag_t basefrag;
  int bits;
public:
  C_MDC_FragmentMarking(MDCache *m, CInode *di, list<CDir*>& dls, frag_t bf, int b) : 
    mdcache(m), diri(di), dirs(dls), basefrag(bf), bits(b) { }
  virtual void finish(int r) {
    mdcache->fragment_mark_and_complete(diri, dirs, basefrag, bits);
  }
};

void MDCache::fragment_mark_and_complete(CInode *diri, 
					  list<CDir*>& startfrags, 
					  frag_t basefrag, int bits) 
{
  dout(10) << "fragment_mark_and_complete " << basefrag << " by " << bits 
	   << " on " << *diri << dendl;
  
  C_Gather *gather = 0;
  
  for (list<CDir*>::iterator p = startfrags.begin();
       p != startfrags.end();
       ++p) {
    CDir *dir = *p;
    
    if (!dir->is_complete()) {
      dout(15) << " fetching incomplete " << *dir << dendl;
      if (!gather) gather = new C_Gather(new C_MDC_FragmentMarking(this, diri, startfrags, basefrag, bits));
      dir->fetch(gather->new_sub(), 
		 true);  // ignore authpinnability
    } 
    else if (!dir->state_test(CDir::STATE_DNPINNEDFRAG)) {
      dout(15) << " marking " << *dir << dendl;
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	p->second->get(CDentry::PIN_FRAGMENTING);
	p->second->state_set(CDentry::STATE_FRAGMENTING);
      }
      dir->state_set(CDir::STATE_DNPINNEDFRAG);
      dir->auth_unpin();  // allow our freeze to complete
    }
    else {
      dout(15) << " marked " << *dir << dendl;
    }
  }
}


class C_MDC_FragmentStored : public Context {
  MDCache *mdcache;
  CInode *diri;
  frag_t basefrag;
  int bits;
  list<CDir*> resultfrags;
public:
  C_MDC_FragmentStored(MDCache *m, CInode *di, frag_t bf, int b, 
		       list<CDir*>& rf) : 
    mdcache(m), diri(di), basefrag(bf), bits(b), resultfrags(rf) { }
  virtual void finish(int r) {
    mdcache->fragment_stored(diri, basefrag, bits, resultfrags);
  }
};

void MDCache::fragment_go(CInode *diri, list<CDir*>& startfrags, frag_t basefrag, int bits) 
{
  dout(10) << "fragment_go " << basefrag << " by " << bits 
	   << " on " << *diri << dendl;

  // refragment
  list<CDir*> resultfrags;
  list<Context*> waiters;
  adjust_dir_fragments(diri, basefrag, bits, resultfrags, waiters);
  mds->queue_waiters(waiters);

  C_Gather *gather = new C_Gather(new C_MDC_FragmentStored(this, diri, basefrag, bits, resultfrags));

  // freeze, store resulting frags
  for (list<CDir*>::iterator p = resultfrags.begin();
       p != resultfrags.end();
       p++) {
    CDir *dir = *p;
    dout(10) << " result frag " << *dir << dendl;
    dir->state_set(CDir::STATE_FRAGMENTING);
    dir->commit(0, gather->new_sub());
    dir->_freeze_dir();
  }  
}

class C_MDC_FragmentLogged : public Context {
  MDCache *mdcache;
  CInode *diri;
  frag_t basefrag;
  int bits;
  list<CDir*> resultfrags;
  vector<version_t> pvs;
  LogSegment *ls;
public:
  C_MDC_FragmentLogged(MDCache *m, CInode *di, frag_t bf, int b, 
		       list<CDir*>& rf, vector<version_t>& p,
		       LogSegment *s) : 
    mdcache(m), diri(di), basefrag(bf), bits(b), ls(s) {
    resultfrags.swap(rf);
    pvs.swap(p);
  }
  virtual void finish(int r) {
    mdcache->fragment_logged(diri, basefrag, bits, 
			     resultfrags, pvs,
			     ls);
  }
};

void MDCache::fragment_stored(CInode *diri, frag_t basefrag, int bits,
			      list<CDir*>& resultfrags)
{
  dout(10) << "fragment_stored " << basefrag << " by " << bits 
	   << " on " << *diri << dendl;

  EFragment *le = new EFragment(mds->mdlog, diri->ino(), basefrag, bits);

  set<int> peers;
  vector<version_t> pvs;
  for (list<CDir*>::iterator p = resultfrags.begin();
       p != resultfrags.end();
       p++) {
    CDir *dir = *p;
    dout(10) << " result frag " << *dir << dendl;

    if (p == resultfrags.begin()) {
      le->metablob.add_dir_context(dir);
      // note peers
      // only do this once: all frags have identical replica_maps.
      if (peers.empty()) 
	for (map<int,int>::iterator p = dir->replica_map.begin();
	     p != dir->replica_map.end();
	     ++p) 
	  peers.insert(p->first);
    }
    
    pvs.push_back(dir->pre_dirty());
    le->metablob.add_dir(dir, true);
  }
  
  mds->mdlog->submit_entry(le,
			   new C_MDC_FragmentLogged(this, diri, basefrag, bits, 
						    resultfrags, pvs, mds->mdlog->get_current_segment()));
  
  // announcelist<CDir*>& resultfrags, 
  for (set<int>::iterator p = peers.begin();
       p != peers.end();
       ++p) {
    MMDSFragmentNotify *notify = new MMDSFragmentNotify(diri->ino(), basefrag, bits);
    if (bits < 0) {
      // freshly replicate basedir to peer on merge
      CDir *base = resultfrags.front();
      CDirDiscover *basedis = base->replicate_to(*p);
      basedis->_encode(notify->basebl);
      delete basedis;
    }
    mds->send_message_mds(notify, *p);
  }

}

void MDCache::fragment_logged(CInode *diri, frag_t basefrag, int bits,
			      list<CDir*>& resultfrags, 
			      vector<version_t>& pvs,
			      LogSegment *ls)
{
  dout(10) << "fragment_logged " << basefrag << " bits " << bits 
	   << " on " << *diri << dendl;
  
 
  // dirty resulting frags
  set<int> peers;
  vector<version_t>::iterator pv = pvs.begin();
  for (list<CDir*>::iterator p = resultfrags.begin();
       p != resultfrags.end();
       p++) {
    CDir *dir = *p;
    dout(10) << " result frag " << *dir << dendl;
    
    // dirty, unpin, unfreeze
    dir->state_clear(CDir::STATE_FRAGMENTING);  
    dir->mark_dirty(*pv, ls);
    pv++;

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) { 
      CDentry *dn = p->second;
      if (dn->state_test(CDentry::STATE_FRAGMENTING)) 
	dn->put(CDentry::PIN_FRAGMENTING);
    }

    dir->unfreeze_dir();
  }
}



void MDCache::handle_fragment_notify(MMDSFragmentNotify *notify)
{
  dout(10) << "handle_fragment_notify " << *notify << " from " << notify->get_source() << dendl;

  CInode *diri = get_inode(notify->get_ino());
  if (diri) {
    list<Context*> waiters;

    // add replica dir (for merge)?
    //  (adjust_dir_fragments expects base to already exist, if non-auth)
    if (notify->get_bits() < 0) {
      CDirDiscover basedis;
      int off = 0;
      basedis._decode(notify->basebl, off);
      add_replica_dir(diri, notify->get_basefrag(), basedis,
		      notify->get_source().num(), waiters);
    }

    // refragment
    list<CDir*> resultfrags;
    adjust_dir_fragments(diri, notify->get_basefrag(), notify->get_bits(), 
			 resultfrags, waiters);
    mds->queue_waiters(waiters);
  }

  delete notify;
}





// ==============================================================
// debug crap

void MDCache::show_subtrees(int dbl)
{
  //dout(10) << "show_subtrees" << dendl;

  if (dbl > g_conf.debug && dbl > g_conf.debug_mds) 
    return;  // i won't print anything.

  if (subtrees.empty()) {
    dout(dbl) << "show_subtrees - no subtrees" << dendl;
    return;
  }

  // root frags
  list<CDir*> basefrags;
  for (set<CInode*>::iterator p = base_inodes.begin();
       p != base_inodes.end();
       ++p) 
    (*p)->get_dirfrags(basefrags);
  //dout(15) << "show_subtrees, base dirfrags " << basefrags << dendl;
  dout(15) << "show_subtrees" << dendl;

  // queue stuff
  list<pair<CDir*,int> > q;
  string indent;
  set<CDir*> seen;

  // calc max depth
  for (list<CDir*>::iterator p = basefrags.begin(); p != basefrags.end(); ++p) 
    q.push_back(pair<CDir*,int>(*p, 0));

  set<CDir*> subtrees_seen;

  int depth = 0;
  while (!q.empty()) {
    CDir *dir = q.front().first;
    int d = q.front().second;
    q.pop_front();

    if (subtrees.count(dir) == 0) continue;

    subtrees_seen.insert(dir);

    if (d > depth) depth = d;

    // sanity check
    //dout(25) << "saw depth " << d << " " << *dir << dendl;
    if (seen.count(dir)) dout(0) << "aah, already seen " << *dir << dendl;
    assert(seen.count(dir) == 0);
    seen.insert(dir);

    // nested items?
    if (!subtrees[dir].empty()) {
      for (set<CDir*>::iterator p = subtrees[dir].begin();
	   p != subtrees[dir].end();
	   ++p) {
	//dout(25) << " saw sub " << **p << dendl;
	q.push_front(pair<CDir*,int>(*p, d+1));
      }
    }
  }


  // print tree
  for (list<CDir*>::iterator p = basefrags.begin(); p != basefrags.end(); ++p) 
    q.push_back(pair<CDir*,int>(*p, 0));

  while (!q.empty()) {
    CDir *dir = q.front().first;
    int d = q.front().second;
    q.pop_front();

    if (subtrees.count(dir) == 0) continue;

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
    dout(dbl) << indent << "|_" << pad << s << " " << auth << *dir << dendl;

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

  // verify there isn't stray crap in subtree map
  int lost = 0;
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    if (subtrees_seen.count(p->first)) continue;
    dout(10) << "*** stray/lost entry in subtree map: " << *p->first << dendl;
    lost++;
  }
  assert(lost == 0);
}


void MDCache::show_cache()
{
  dout(7) << "show_cache" << dendl;
  
  for (hash_map<inodeno_t,CInode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it++) {
    // unlinked?
    if (!it->second->parent)
      dout(7) << " unlinked " << *it->second << dendl;
    
    // dirfrags?
    list<CDir*> dfs;
    it->second->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
      CDir *dir = *p;
      dout(7) << "  dirfrag " << *dir << dendl;
	    
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	dout(7) << "   dentry " << *dn << dendl;
	if (dn->is_primary() && dn->inode) 
	  dout(7) << "    inode " << *dn->inode << dendl;
      }
    }
  }
}


void MDCache::dump_cache()
{
  if (g_conf.debug_mds < 2) return;

  char fn[20];
  sprintf(fn, "cachedump.%d.mds%d", (int)mds->mdsmap->get_epoch(), mds->get_nodeid());

  dout(1) << "dump_cache to " << fn << dendl;

  ofstream myfile;
  myfile.open(fn);
  
  for (hash_map<inodeno_t,CInode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it++) {
    list<CDir*> dfs;
    it->second->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
      CDir *dir = *p;
      myfile << *dir->inode << std::endl;
      myfile << *dir << std::endl;
      
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	myfile << *dn << std::endl;
      }
    }
  }

  myfile.close();
}
