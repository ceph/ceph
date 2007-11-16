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


#include "include/types.h"

#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDSMap.h"
#include "LogSegment.h"

#include "include/Context.h"
#include "common/Clock.h"

#include "osdc/Objecter.h"

#include <cassert>

#include "config.h"

#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << cache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") "




// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };



ostream& operator<<(ostream& out, CDir& dir)
{
  filepath path;
  dir.get_inode()->make_path(path);
  out << "[dir " << dir.dirfrag() << " " << path << "/";
  if (dir.is_auth()) {
    out << " auth";
    if (dir.is_replicated())
      out << dir.get_replicas();

    out << " pv=" << dir.get_projected_version();
    out << " v=" << dir.get_version();
    out << " cv=" << dir.get_committing_version();
    out << "/" << dir.get_committed_version();
    out << "/" << dir.get_committed_version_equivalent();
  } else {
    out << " rep@" << dir.authority();
    if (dir.get_replica_nonce() > 1)
      out << "." << dir.get_replica_nonce();
  }

  if (dir.is_rep()) out << " REP";

  if (dir.get_dir_auth() != CDIR_AUTH_DEFAULT) {
    if (dir.get_dir_auth().second == CDIR_AUTH_UNKNOWN)
      out << " dir_auth=" << dir.get_dir_auth().first;
    else
      out << " dir_auth=" << dir.get_dir_auth();
  }
  
  if (dir.get_cum_auth_pins())
    out << " ap=" << dir.get_auth_pins() << "+" << dir.get_nested_auth_pins();

  out << " state=" << dir.get_state();
  if (dir.state_test(CDir::STATE_COMPLETE)) out << "|complete";
  if (dir.state_test(CDir::STATE_FREEZINGTREE)) out << "|freezingtree";
  if (dir.state_test(CDir::STATE_FROZENTREE)) out << "|frozentree";
  //if (dir.state_test(CDir::STATE_FROZENTREELEAF)) out << "|frozentreeleaf";
  if (dir.state_test(CDir::STATE_FROZENDIR)) out << "|frozendir";
  if (dir.state_test(CDir::STATE_FREEZINGDIR)) out << "|freezingdir";
  if (dir.state_test(CDir::STATE_EXPORTBOUND)) out << "|exportbound";
  if (dir.state_test(CDir::STATE_IMPORTBOUND)) out << "|importbound";

  out << " sz=" << dir.get_nitems() << "+" << dir.get_nnull();
  if (dir.get_num_dirty())
    out << " dirty=" << dir.get_num_dirty();

  
  if (dir.get_num_ref()) {
    out << " |";
    dir.print_pin_set(out);
  }

  out << " " << &dir;
  return out << "]";
}


void CDir::print(ostream& out) 
{
  out << *this;
}




ostream& CDir::print_db_line_prefix(ostream& out) 
{
  return out << g_clock.now() << " mds" << cache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") ";
}



// -------------------------------------------------------------------
// CDir

CDir::CDir(CInode *in, frag_t fg, MDCache *mdcache, bool auth) :
  xlist_dirty(this)
{
  inode = in;
  frag = fg;
  this->cache = mdcache;
  
  nitems = 0;
  nnull = 0;
  num_dirty = 0;

  state = STATE_INITIAL;

  projected_version = version = 0;
  committing_version = 0;
  committed_version_equivalent = committed_version = 0;

  // dir_auth
  dir_auth = CDIR_AUTH_DEFAULT;

  // auth
  assert(in->is_dir());
  if (auth) 
    state |= STATE_AUTH;
 
  auth_pins = 0;
  nested_auth_pins = 0;
  request_pins = 0;

  //hack_num_accessed = -1;
  
  dir_rep = REP_NONE;
  //dir_rep = REP_ALL;      // hack: to wring out some bugs! FIXME FIXME
}




/***
 * linking fun
 */

CDentry* CDir::add_null_dentry(const string& dname)
{
  // foreign
  assert(lookup(dname) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, 0);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = projected_version;
  
  // add to dir
  assert(items.count(dn->name) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->name] = dn;
  nnull++;

  dout(12) << "add_null_dentry " << *dn << dendl;

  // pin?
  if (nnull + nitems == 1) get(PIN_CHILD);
  
  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
  return dn;
}


CDentry* CDir::add_primary_dentry(const string& dname, CInode *in) 
{
  // primary
  assert(lookup(dname) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, 0);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = projected_version;
  
  // add to dir
  assert(items.count(dn->name) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->name] = dn;
  link_inode_work( dn, in );

  dout(12) << "add_primary_dentry " << *dn << dendl;

  // pin?
  if (nnull + nitems == 1) get(PIN_CHILD);
  
  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
  return dn;
}

CDentry* CDir::add_remote_dentry(const string& dname, inodeno_t ino, unsigned char d_type) 
{
  // foreign
  assert(lookup(dname) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, ino, d_type);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = projected_version;
  
  // add to dir
  assert(items.count(dn->name) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->name] = dn;
  nitems++;

  dout(12) << "add_remote_dentry " << *dn << dendl;

  // pin?
  if (nnull + nitems == 1) get(PIN_CHILD);
  
  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
  return dn;
}



void CDir::remove_dentry(CDentry *dn) 
{
  dout(12) << "remove_dentry " << *dn << dendl;

  if (dn->inode) {
    // detach inode and dentry
    unlink_inode_work(dn);
  } else {
    // remove from null list
    //assert(null_items.count(dn->name) == 1);
    //null_items.erase(dn->name);
    nnull--;
  }
  
  // remove from list
  assert(items.count(dn->name) == 1);
  items.erase(dn->name);

  // adjust dirty counter?
  if (dn->state_test(CDentry::STATE_DIRTY))
    num_dirty--;

  cache->lru.lru_remove(dn);
  delete dn;

  // unpin?
  if (nnull + nitems == 0) put(PIN_CHILD);

  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
}

void CDir::link_remote_inode(CDentry *dn, inodeno_t ino, unsigned char d_type)
{
  dout(12) << "link_remote_inode " << *dn << " remote " << ino << dendl;
  assert(dn->is_null());

  dn->set_remote(ino, d_type);
  nitems++;
  dn->clear_dir_offset();

  //assert(null_items.count(dn->name) == 1);
  //null_items.erase(dn->name);
  nnull--;
  assert(nnull + nitems == items.size());
}

void CDir::link_primary_inode(CDentry *dn, CInode *in)
{
  dout(12) << "link_primary_inode " << *dn << " " << *in << dendl;
  assert(dn->is_null());

  link_inode_work(dn,in);
  dn->clear_dir_offset();
  
  // remove from null list
  //assert(null_items.count(dn->name) == 1);
  //null_items.erase(dn->name);
  nnull--;

  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
}

void CDir::link_inode_work( CDentry *dn, CInode *in)
{
  assert(dn->inode == 0);
  dn->inode = in;
  in->set_primary_parent(dn);

  nitems++;  // adjust dir size
  
  // set inode version
  //in->inode.version = dn->get_version();
  
  // pin dentry?
  if (in->get_num_ref())
    dn->get(CDentry::PIN_INODEPIN);
  
  // adjust auth pin count
  if (in->auth_pins + in->nested_auth_pins)
    dn->adjust_nested_auth_pins(in->auth_pins + in->nested_auth_pins);
}

void CDir::unlink_inode( CDentry *dn )
{
  if (dn->is_remote()) {
    dout(12) << "unlink_inode " << *dn << dendl;
  } else {
    dout(12) << "unlink_inode " << *dn << " " << *dn->inode << dendl;
  }

  dn->clear_dir_offset();
  unlink_inode_work(dn);

  // add to null list
  //assert(null_items.count(dn->name) == 0);
  //null_items[dn->name] = dn;
  nnull++;

  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
}

void CDir::try_remove_unlinked_dn(CDentry *dn)
{
  assert(dn->dir == this);
  assert(dn->is_null());
  assert(dn->is_dirty());
  
  /* FIXME: there is a bug in this.  i think new dentries are properly
     identified.. e.g. maybe a dentry exists, is committed, is removed, is now
     dirty+null, then reused and mistakenly considered new.. then it is removed, 
     we remove it here, the dir is fetched, and the dentry exists again.  
     
     somethign like that...
  */
  return;


  // no pins (besides dirty)?
  if (dn->get_num_ref() != 1) 
    return;

  // was the dn new?  or is the dir complete (i.e. we don't need negatives)? 
  if (dn->is_new() || is_complete()) {
    dout(10) << "try_remove_unlinked_dn " << *dn << " in " << *this << dendl;
    dn->mark_clean();
    remove_dentry(dn);

    if (version == projected_version &&
	committing_version == committed_version &&
	num_dirty == 0) {
      dout(10) << "try_remove_unlinked_dn committed_equivalent now " << version 
	       << " vs committed " << committed_version
	       << dendl;
      committed_version_equivalent = committed_version;    
    }
  }
}
  


void CDir::unlink_inode_work( CDentry *dn )
{
  CInode *in = dn->inode;

  if (dn->is_remote()) {
    // remote
    if (in) 
      dn->unlink_remote();

    dn->set_remote(0, 0);
  } else {
    // primary
    assert(dn->is_primary());
 
    // unpin dentry?
    if (in->get_num_ref())
      dn->put(CDentry::PIN_INODEPIN);
    
    // unlink auth_pin count
    if (in->auth_pins + in->nested_auth_pins)
      dn->adjust_nested_auth_pins(0 - (in->auth_pins + in->nested_auth_pins));
    
    // detach inode
    in->remove_primary_parent(dn);
    dn->inode = 0;
  }

  nitems--;   // adjust dir size
}

void CDir::remove_null_dentries() {
  dout(12) << "remove_null_dentries " << *this << dendl;

  list<CDentry*> dns;
  for (CDir::map_t::iterator it = items.begin();
       it != items.end(); 
       it++) {
    if (it->second->is_null())
      dns.push_back(it->second);
  }
  
  for (list<CDentry*>::iterator it = dns.begin();
       it != dns.end();
       it++) {
    CDentry *dn = *it;
    remove_dentry(dn);
  }
  //assert(null_items.empty());         
  assert(nnull == 0);
  assert(nnull + nitems == items.size());
}


/**
 * steal_dentry -- semi-violently move a dentry from one CDir to another
 * (*) violently, in that nitems, most pins, etc. are not correctly maintained 
 * on the old CDir corpse; must call purge_stolen() when finished.
 */
void CDir::steal_dentry(CDentry *dn)
{
  dout(15) << "steal_dentry " << *dn << dendl;

  items[dn->name] = dn;

  dn->dir->items.erase(dn->name);
  if (dn->dir->items.empty())
    dn->dir->put(PIN_CHILD);

  if (nnull + nitems == 0)
    get(PIN_CHILD);
  if (dn->is_null()) 
    nnull++;
  else
    nitems++;

  nested_auth_pins += dn->auth_pins + dn->nested_auth_pins;
  if (dn->is_dirty()) 
    num_dirty++;

  dn->dir = this;
}

void CDir::purge_stolen(list<Context*>& waiters)
{
  // take waiters _before_ unfreeze...
  take_waiting(WAIT_ANY, waiters);
  
  if (is_auth()) {
    assert(is_frozen_dir());
    unfreeze_dir();
  }

  nnull = nitems = 0;

  if (is_auth()) 
    clear_replica_map();
  if (is_dirty()) mark_clean();
  if (state_test(STATE_IMPORTBOUND)) put(PIN_IMPORTBOUND);
  if (state_test(STATE_EXPORTBOUND)) put(PIN_EXPORTBOUND);

  if (auth_pins > 0) put(PIN_AUTHPIN);

  assert(get_num_ref() == (state_test(STATE_STICKY) ? 1:0));
}

void CDir::init_fragment_pins()
{
  if (!replica_map.empty()) get(PIN_REPLICATED);
  if (state_test(STATE_DIRTY)) get(PIN_DIRTY);
  if (state_test(STATE_EXPORTBOUND)) get(PIN_EXPORTBOUND);
  if (state_test(STATE_IMPORTBOUND)) get(PIN_IMPORTBOUND);
}

void CDir::split(int bits, list<CDir*>& subs, list<Context*>& waiters)
{
  dout(10) << "split by " << bits << " bits on " << *this << dendl;

  if (cache->mds->logger) cache->mds->logger->inc("dir_sp");

  assert(is_complete() || !is_auth());

  list<frag_t> frags;
  frag.split(bits, frags);

  vector<CDir*> subfrags(1 << bits);
  
  double fac = 1.0 / (double)(1 << bits);  // for scaling load vecs

  // create subfrag dirs
  int n = 0;
  for (list<frag_t>::iterator p = frags.begin(); p != frags.end(); ++p) {
    CDir *f = new CDir(inode, *p, cache, is_auth());
    f->state_set(state & MASK_STATE_FRAGMENT_KEPT);
    f->replica_map = replica_map;
    f->dir_auth = dir_auth;
    f->init_fragment_pins();
    f->version = version;
    f->projected_version = projected_version;

    f->pop_me = pop_me;
    f->pop_me *= fac;

    // FIXME; this is an approximation
    f->pop_nested = pop_nested;
    f->pop_nested *= fac;
    f->pop_auth_subtree = pop_auth_subtree;
    f->pop_auth_subtree *= fac;
    f->pop_auth_subtree_nested = pop_auth_subtree_nested;
    f->pop_auth_subtree_nested *= fac;

    dout(10) << " subfrag " << *p << " " << *f << dendl;
    subfrags[n++] = f;
    subs.push_back(f);
    inode->add_dirfrag(f);
  }
  
  // repartition dentries
  while (!items.empty()) {
    CDir::map_t::iterator p = items.begin();
    
    CDentry *dn = p->second;
    frag_t subfrag = inode->pick_dirfrag(p->first);
    int n = subfrag.value() >> frag.bits();
    dout(15) << " subfrag " << subfrag << " n=" << n << " for " << p->first << dendl;
    CDir *f = subfrags[n];
    f->steal_dentry(dn);
  }

  purge_stolen(waiters);
  inode->close_dirfrag(frag); // selft deletion, watch out.
}

void CDir::merge(int bits, list<Context*>& waiters)
{
  dout(10) << "merge by " << bits << " bits" << dendl;

  list<frag_t> frags;
  frag.split(bits, frags);
  
  for (list<frag_t>::iterator p = frags.begin(); p != frags.end(); ++p) {
    CDir *dir = inode->get_or_open_dirfrag(cache, *p);
    assert(dir->is_complete());
    dout(10) << " subfrag " << *p << " " << *dir << dendl;
    
    // steal dentries
    while (!dir->items.empty()) 
      steal_dentry(dir->items.begin()->second);
    
    // merge replica map
    for (map<int,int>::iterator p = dir->replica_map.begin();
	 p != dir->replica_map.end();
	 ++p) 
      replica_map[p->first] = MAX(replica_map[p->first], p->second);
    
    // merge state
    state_set(dir->get_state() & MASK_STATE_FRAGMENT_KEPT);
    dir_auth = dir->dir_auth;

    dir->purge_stolen(waiters);
    inode->close_dirfrag(dir->get_frag());
  }

  init_fragment_pins();
}







CDirDiscover *CDir::replicate_to(int mds)
{
  assert(is_auth());
  return new CDirDiscover( this, add_replica(mds) );
}





/****************************************
 * WAITING
 */

void CDir::add_dentry_waiter(const string& dname, Context *c) 
{
  if (waiting_on_dentry.empty())
    get(PIN_DNWAITER);
  waiting_on_dentry[dname].push_back(c);
  dout(10) << "add_dentry_waiter dentry " << dname << " " << c << " on " << *this << dendl;
}

void CDir::take_dentry_waiting(const string& dname, list<Context*>& ls)
{
  if (waiting_on_dentry.empty()) return;
  if (waiting_on_dentry.count(dname) == 0) return;
  dout(10) << "take_dentry_waiting dentry " << dname
	   << " x " << waiting_on_dentry[dname].size() 
	   << " on " << *this << dendl;
  ls.splice(ls.end(), waiting_on_dentry[dname]);
  waiting_on_dentry.erase(dname);
  if (waiting_on_dentry.empty())
    put(PIN_DNWAITER);
}

void CDir::add_ino_waiter(inodeno_t ino, Context *c) 
{
  if (waiting_on_ino.empty())
    get(PIN_INOWAITER);
  waiting_on_ino[ino].push_back(c);
  dout(10) << "add_ino_waiter ino " << ino << " " << c << " on " << *this << dendl;
}

void CDir::take_ino_waiting(inodeno_t ino, list<Context*>& ls)
{
  if (waiting_on_ino.empty()) return;
  if (waiting_on_ino.count(ino) == 0) return;
  dout(10) << "take_ino_waiting ino " << ino
	   << " x " << waiting_on_ino[ino].size() 
	   << " on " << *this << dendl;
  ls.splice(ls.end(), waiting_on_ino[ino]);
  waiting_on_ino.erase(ino);
  if (waiting_on_ino.empty())
    put(PIN_INOWAITER);
}

void CDir::take_sub_waiting(list<Context*>& ls)
{
  dout(10) << "take_sub_waiting" << dendl;
  for (hash_map<string, list<Context*> >::iterator p = waiting_on_dentry.begin(); 
       p != waiting_on_dentry.end();
       ++p) 
    ls.splice(ls.end(), p->second);
  waiting_on_dentry.clear();
  for (hash_map<inodeno_t, list<Context*> >::iterator p = waiting_on_ino.begin(); 
       p != waiting_on_ino.end();
       ++p) 
    ls.splice(ls.end(), p->second);
  waiting_on_ino.clear();
}



void CDir::add_waiter(int tag, Context *c) 
{
  // hierarchical?

  // at free root?
  if (tag & WAIT_ATFREEZEROOT) {
    if (!(is_freezing_tree_root() || is_frozen_tree_root() ||
	  is_freezing_dir() || is_frozen_dir())) {
      // try parent
      dout(10) << "add_waiter " << tag << " " << c << " should be ATFREEZEROOT, " << *this << " is not root, trying parent" << dendl;
      inode->parent->dir->add_waiter(tag, c);
      return;
    }
  }
  
  // at subtree root?
  if (tag & WAIT_ATSUBTREEROOT) {
    if (!is_subtree_root()) {
      // try parent
      dout(10) << "add_waiter " << tag << " " << c << " should be ATSUBTREEROOT, " << *this << " is not root, trying parent" << dendl;
      inode->parent->dir->add_waiter(tag, c);
      return;
    }
  }

  MDSCacheObject::add_waiter(tag, c);
}



/* NOTE: this checks dentry waiters too */
void CDir::take_waiting(int mask, list<Context*>& ls)
{
  if (mask & WAIT_DENTRY) {
    // take each each dentry waiter
    hash_map<string, list<Context*> >::iterator it = 
      waiting_on_dentry.begin(); 
    while (it != waiting_on_dentry.end()) {
      take_dentry_waiting((it++)->first, ls);   // not post-inc
    }
  }
  
  // waiting
  MDSCacheObject::take_waiting(mask, ls);
}


void CDir::finish_waiting(int mask, int result) 
{
  dout(11) << "finish_waiting mask " << hex << mask << dec << " result " << result << " on " << *this << dendl;

  list<Context*> finished;
  take_waiting(mask, finished);
  if (result < 0)
    finish_contexts(finished, result);
  else
    cache->mds->queue_waiters(finished);
}



// dirty/clean

version_t CDir::pre_dirty(version_t min)
{
  if (min > projected_version) 
    projected_version = min;
  ++projected_version;
  dout(10) << "pre_dirty " << projected_version << dendl;
  return projected_version;
}

void CDir::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    dout(10) << "mark_dirty (was clean) " << *this << " version " << version << dendl;
    get(PIN_DIRTY);
    assert(ls);
  } else {
    dout(10) << "mark_dirty (already dirty) " << *this << " version " << version << dendl;
  }
  if (ls) 
    ls->dirty_dirfrags.push_back(&xlist_dirty);
}

void CDir::mark_dirty(version_t pv, LogSegment *ls)
{
  assert(version < pv);
  version = pv;
  _mark_dirty(ls);
}

void CDir::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << version << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);

    xlist_dirty.remove_myself();
  }
}




void CDir::first_get()
{
  inode->get(CInode::PIN_DIRFRAG);
}

void CDir::last_put()
{
  inode->put(CInode::PIN_DIRFRAG);
}



/******************************************************************************
 * FETCH and COMMIT
 */

// -----------------------
// FETCH

class C_Dir_Fetch : public Context {
 protected:
  CDir *dir;
 public:
  bufferlist bl;

  C_Dir_Fetch(CDir *d) : dir(d) { }
  void finish(int result) {
    dir->_fetched(bl);
  }
};

void CDir::fetch(Context *c, bool ignore_authpinnability)
{
  dout(10) << "fetch on " << *this << dendl;
  
  assert(is_auth());
  assert(!is_complete());

  if (!can_auth_pin() && !ignore_authpinnability) {
    dout(7) << "fetch waiting for authpinnable" << dendl;
    add_waiter(WAIT_UNFREEZE, c);
    return;
  }

  if (c) add_waiter(WAIT_COMPLETE, c);
  
  // already fetching?
  if (state_test(CDir::STATE_FETCHING)) {
    dout(7) << "already fetching; waiting" << dendl;
    return;
  }

  auth_pin();
  state_set(CDir::STATE_FETCHING);

  if (cache->mds->logger) cache->mds->logger->inc("dir_f");

  // start by reading the first hunk of it
  C_Dir_Fetch *fin = new C_Dir_Fetch(this);
  cache->mds->objecter->read( get_ondisk_object(), 
			      0, 0,   // whole object
			      cache->mds->objecter->osdmap->file_to_object_layout( get_ondisk_object(),
										   g_OSD_MDDirLayout ),
			      &fin->bl,
			      fin );
}

void CDir::_fetched(bufferlist &bl)
{
  dout(10) << "_fetched " << bl.length() 
	   << " bytes for " << *this
	   << dendl;
  
  assert(is_auth());
  assert(!is_frozen());

  // decode.
  int len = bl.length();
  int off = 0;
  version_t got_version;
  
  ::_decode(got_version, bl, off);

  dout(10) << "_fetched version " << got_version
	   << ", " << len << " bytes"
	   << dendl;
  
  int32_t n;
  ::_decode(n, bl, off);

  //int num_new_inodes_loaded = 0;

  for (int i=0; i<n; i++) {
    off_t dn_offset = off;

    // marker
    char type = bl[off];
    ++off;

    // dname
    string dname;
    ::_decode(dname, bl, off);
    dout(24) << "_fetched parsed marker '" << type << "' dname '" << dname << dendl;
    
    CDentry *dn = lookup(dname);  // existing dentry?

    if (type == 'L') {
      // hard link
      inodeno_t ino;
      unsigned char d_type;
      ::_decode(ino, bl, off);
      ::_decode(d_type, bl, off);

      if (dn) {
        if (dn->get_inode() == 0) {
          dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
        } else {
          dout(12) << "_fetched  had dentry " << *dn << dendl;
        }
      } else {
	// (remote) link
	dn = add_remote_dentry(dname, ino, d_type);
	
	// link to inode?
	CInode *in = cache->get_inode(ino);   // we may or may not have it.
	if (in) {
	  dn->link_remote(in);
	  dout(12) << "_fetched  got remote link " << ino << " which we have " << *in << dendl;
	} else {
	  dout(12) << "_fetched  got remote link " << ino << " (dont' have it)" << dendl;
	}
      }
    } 
    else if (type == 'I') {
      // inode
      
      // parse out inode
      inode_t inode;
      ::_decode(inode, bl, off);

      string symlink;
      if (inode.is_symlink())
        ::_decode(symlink, bl, off);

      fragtree_t fragtree;
      fragtree._decode(bl, off);
      
      if (dn) {
        if (dn->get_inode() == 0) {
          dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
        } else {
          dout(12) << "_fetched  had dentry " << *dn << dendl;
        }
      } else {
	// add inode
	CInode *in = 0;
	if (cache->have_inode(inode.ino)) {
	  in = cache->get_inode(inode.ino);
	  dout(-12) << "_fetched  got (but i already had) " << *in 
		   << " mode " << in->inode.mode 
		   << " mtime " << in->inode.mtime << dendl;
	  assert(0);  // this shouldn't happen!! 
	} else {
	  // inode
	  in = new CInode(cache);
	  in->inode = inode;
	  
	  // symlink?
	  if (in->is_symlink()) 
	    in->symlink = symlink;
	  
	  // dirfragtree
	  in->dirfragtree.swap(fragtree);

	  // add 
	  cache->add_inode( in );
	
	  // link
	  dn = add_primary_dentry(dname, in);
	  dout(12) << "_fetched  got " << *dn << " " << *in << dendl;

	  //in->hack_accessed = false;
	  //in->hack_load_stamp = g_clock.now();
	  //num_new_inodes_loaded++;
	}
      }
    } else {
      dout(1) << "corrupt directory, i got tag char '" << type << "' val " << (int)(type) 
	      << " at pos " << off << dendl;
      assert(0);
    }
    
    // make note of dentry position in the directory
    dn->dir_offset = dn_offset;

    /** clean underwater item?
     * Underwater item is something that is dirty in our cache from
     * journal replay, but was previously flushed to disk before the
     * mds failed.
     *
     * We only do this is committed_version == 0. that implies either
     * - this is a fetch after from a clean/empty CDir is created
     *   (and has no effect, since the dn won't exist); or
     * - this is a fetch after _recovery_, which is what we're worried 
     *   about.  Items that are marked dirty from the journal should be
     *   marked clean if they appear on disk.
     */
    if (committed_version == 0 &&     
	dn &&
	dn->get_version() <= got_version &&
	dn->is_dirty()) {
      dout(10) << "_fetched  had underwater dentry " << *dn << ", marking clean" << dendl;
      dn->mark_clean();

      if (dn->get_inode()) {
	assert(dn->get_inode()->get_version() <= got_version);
	dout(10) << "_fetched  had underwater inode " << *dn->get_inode() << ", marking clean" << dendl;
	dn->get_inode()->mark_clean();
      }
    }
  }
  //assert(off == len);   no, directories may shrink.  add this back in when we properly truncate objects on write.

  // take the loaded version?
  // only if we are a fresh CDir* with no prior state.
  if (version == 0) {
    assert(projected_version == 0);
    assert(!state_test(STATE_COMMITTING));
    projected_version = version = committing_version = committed_version = got_version;
  }

  //cache->mds->logger->inc("newin", num_new_inodes_loaded);
  //hack_num_accessed = 0;

  // mark complete, !fetching
  state_set(STATE_COMPLETE);
  state_clear(STATE_FETCHING);
  auth_unpin();

  // kick waiters
  finish_waiting(WAIT_COMPLETE, 0);
}



// -----------------------
// COMMIT

/**
 * commit
 *
 * @param want - min version i want committed
 * @param c - callback for completion
 */
void CDir::commit(version_t want, Context *c)
{
  dout(10) << "commit want " << want << " on " << *this << dendl;
  if (want == 0) want = version;

  // preconditions
  assert(want <= version || version == 0);    // can't commit the future
  assert(want > committed_version); // the caller is stupid
  assert(is_auth());
  assert(can_auth_pin());

  // note: queue up a noop if necessary, so that we always
  // get an auth_pin.
  if (!c)
    c = new C_NoopContext;

  // auth_pin on first waiter
  if (waiting_for_commit.empty())
    auth_pin();
  waiting_for_commit[want].push_back(c);
  
  // ok.
  _commit(want);
}


class C_Dir_RetryCommit : public Context {
  CDir *dir;
  version_t want;
public:
  C_Dir_RetryCommit(CDir *d, version_t v) : 
    dir(d), want(v) { }
  void finish(int r) {
    dir->_commit(want);
  }
};

class C_Dir_Committed : public Context {
  CDir *dir;
  version_t version;
public:
  C_Dir_Committed(CDir *d, version_t v) : dir(d), version(v) { }
  void finish(int r) {
    dir->_committed(version);
  }
};

void CDir::_commit(version_t want)
{
  dout(10) << "_commit want " << want << " on " << *this << dendl;

  // we can't commit things in the future.
  // (even the projected future.)
  assert(want <= version || version == 0);

  // check pre+postconditions.
  assert(is_auth());

  // already committed?
  if (committed_version >= want) {
    dout(10) << "already committed " << committed_version << " >= " << want << dendl;
    return;
  }
  // already committing >= want?
  if (committing_version >= want) {
    dout(10) << "already committing " << committing_version << " >= " << want << dendl;
    assert(state_test(STATE_COMMITTING));
    return;
  }
  
  // complete?
  if (!is_complete()) {
    dout(7) << "commit not complete, fetching first" << dendl;
    if (cache->mds->logger) cache->mds->logger->inc("dir_ffc");
    fetch(new C_Dir_RetryCommit(this, want));
    return;
  }
  
  // commit.
  committing_version = version;

  // mark committing (if not already)
  if (!state_test(STATE_COMMITTING)) {
    dout(10) << "marking committing" << dendl;
    state_set(STATE_COMMITTING);
  }
  
  if (cache->mds->logger) cache->mds->logger->inc("dir_c");

  // encode
  bufferlist bl;

  ::_encode(version, bl);
  int32_t n = nitems;
  ::_encode(n, bl);

  for (map_t::iterator it = items.begin();
       it != items.end();
       it++) {
    CDentry *dn = it->second;
    
    if (dn->is_null()) 
      continue;  // skip negative entries
    
    n--;

    // primary or remote?
    if (dn->is_remote()) {
      inodeno_t ino = dn->get_remote_ino();
      unsigned char d_type = dn->get_remote_d_type();
      dout(14) << " pos " << bl.length() << " dn '" << it->first << "' remote ino " << ino << dendl;
      
      // marker, name, ino
      bl.append( "L", 1 );         // remote link
      ::_encode(it->first, bl);
      ::_encode(ino, bl);
      ::_encode(d_type, bl);
    } else {
      // primary link
      CInode *in = dn->get_inode();
      assert(in);

      dout(14) << " pos " << bl.length() << " dn '" << it->first << "' inode " << *in << dendl;
  
      // marker, name, inode, [symlink string]
      bl.append( "I", 1 );         // inode
      ::_encode(it->first, bl);
      ::_encode(in->inode, bl);
      
      if (in->is_symlink()) {
        // include symlink destination!
        dout(18) << "    inlcuding symlink ptr " << in->symlink << dendl;
	::_encode(in->symlink, bl);
      }

      in->dirfragtree._encode(bl);
    }
  }
  assert(n == 0);

  // write it.
  cache->mds->objecter->write( get_ondisk_object(),
			       0, bl.length(),
			       cache->mds->objecter->osdmap->file_to_object_layout( get_ondisk_object(),
										    g_OSD_MDDirLayout ),
			       bl,
			       NULL, new C_Dir_Committed(this, version) );
}


/**
 * _committed
 *
 * @param v version i just committed
 */
void CDir::_committed(version_t v)
{
  dout(10) << "_committed v " << v << " on " << *this << dendl;
  assert(is_auth());
  
  // take note.
  assert(v > committed_version);
  assert(v <= committing_version);
  committed_version = v;

  // _all_ commits done?
  if (committing_version == committed_version) 
    state_clear(CDir::STATE_COMMITTING);
  
  // dir clean?
  if (committed_version == version) 
    mark_clean();

  // dentries clean?
  for (map_t::iterator it = items.begin();
       it != items.end(); ) {
    CDentry *dn = it->second;
    it++;
    
    // dentry
    if (committed_version >= dn->get_version()) {
      if (dn->is_dirty()) {
	dout(15) << " dir " << committed_version << " >= dn " << dn->get_version() << " now clean " << *dn << dendl;
	dn->mark_clean();
      } 
    } else {
      dout(15) << " dir " << committed_version << " < dn " << dn->get_version() << " still dirty " << *dn << dendl;
    }

    // inode?
    if (dn->is_primary()) {
      CInode *in = dn->get_inode();
      assert(in);
      assert(in->is_auth());
      
      if (committed_version >= in->get_version()) {
	if (in->is_dirty()) {
	  dout(15) << " dir " << committed_version << " >= inode " << in->get_version() << " now clean " << *in << dendl;
	  in->mark_clean();
	}
      } else {
	dout(15) << " dir " << committed_version << " < inode " << in->get_version() << " still dirty " << *in << dendl;
	assert(in->is_dirty());
      }
    }
  }

  // finishers?
  bool were_waiters = !waiting_for_commit.empty();
  
  map<version_t, list<Context*> >::iterator p = waiting_for_commit.begin();
  while (p != waiting_for_commit.end()) {
    map<version_t, list<Context*> >::iterator n = p;
    n++;
    if (p->first > committed_version) break; // haven't committed this far yet.
    cache->mds->queue_waiters(p->second);
    waiting_for_commit.erase(p);
    p = n;
  } 

  // unpin if we kicked the last waiter.
  if (were_waiters &&
      waiting_for_commit.empty())
    auth_unpin();
}





// IMPORT/EXPORT

void CDir::encode_export(bufferlist& bl)
{
  ::_encode_simple(version, bl);
  ::_encode_simple(committed_version, bl);
  ::_encode_simple(committed_version_equivalent, bl);

  ::_encode_simple(state, bl);
  ::_encode_simple(dir_rep, bl);

  ::_encode_simple(pop_me, bl);
  ::_encode_simple(pop_auth_subtree, bl);

  ::_encode_simple(dir_rep_by, bl);  
  ::_encode_simple(replica_map, bl);

  get(PIN_TEMPEXPORTING);
}

void CDir::finish_export(utime_t now)
{
  pop_auth_subtree_nested -= pop_auth_subtree;
  pop_me.zero(now);
  pop_auth_subtree.zero(now);
  put(PIN_TEMPEXPORTING);
}

void CDir::decode_import(bufferlist::iterator& blp)
{
  ::_decode_simple(version, blp);
  ::_decode_simple(committed_version, blp);
  ::_decode_simple(committed_version_equivalent, blp);
  committing_version = committed_version;
  projected_version = version;

  unsigned s;
  ::_decode_simple(s, blp);
  state &= MASK_STATE_IMPORT_KEPT;
  state |= (s & MASK_STATE_EXPORTED);
  if (is_dirty()) get(PIN_DIRTY);

  ::_decode_simple(dir_rep, blp);

  ::_decode_simple(pop_me, blp);
  ::_decode_simple(pop_auth_subtree, blp);
  pop_auth_subtree_nested += pop_auth_subtree;

  ::_decode_simple(dir_rep_by, blp);
  ::_decode_simple(replica_map, blp);
  if (!replica_map.empty()) get(PIN_REPLICATED);

  replica_nonce = 0;  // no longer defined
}




/********************************
 * AUTHORITY
 */

/*
 * if dir_auth.first == parent, auth is same as inode.
 * unless .second != unknown, in which case that sticks.
 */
pair<int,int> CDir::authority() 
{
  if (is_subtree_root()) 
    return dir_auth;
  else
    return inode->authority();
}

/** is_subtree_root()
 * true if this is an auth delegation point.  
 * that is, dir_auth != default (parent,unknown)
 *
 * some key observations:
 *  if i am auth:
 *    - any region bound will be an export, or frozen.
 *
 * note that this DOES heed dir_auth.pending
 */
/*
bool CDir::is_subtree_root()
{
  if (dir_auth == CDIR_AUTH_DEFAULT) {
    //dout(10) << "is_subtree_root false " << dir_auth << " != " << CDIR_AUTH_DEFAULT
    //<< " on " << ino() << dendl;
    return false;
  } else {
    //dout(10) << "is_subtree_root true " << dir_auth << " != " << CDIR_AUTH_DEFAULT
    //<< " on " << ino() << dendl;
    return true;
  }
}
*/

/** contains(x)
 * true if we are x, or an ancestor of x
 */
bool CDir::contains(CDir *x)
{
  while (1) {
    if (x == this) return true;
    x = x->get_parent_dir();
    if (x == 0) return false;    
  }
}



/** set_dir_auth
 */
void CDir::set_dir_auth(pair<int,int> a)
{ 
  dout(10) << "setting dir_auth=" << a
	   << " from " << dir_auth
	   << " on " << *this << dendl;
  
  bool was_subtree = is_subtree_root();
  bool was_ambiguous = dir_auth.second >= 0;

  // set it.
  dir_auth = a;

  // new subtree root?
  if (!was_subtree && is_subtree_root()) {
    dout(10) << " new subtree root, adjusting auth_pins" << dendl;
    
    // adjust nested auth pins
    inode->adjust_nested_auth_pins(-get_cum_auth_pins());
    
    // unpin parent of frozen dir/tree?
    if (inode->is_auth() && (is_frozen_tree_root() || is_frozen_dir()))
      inode->auth_unpin();
  } 
  if (was_subtree && !is_subtree_root()) {
    dout(10) << " old subtree root, adjusting auth_pins" << dendl;
    
    // adjust nested auth pins
    inode->adjust_nested_auth_pins(get_cum_auth_pins());

    // pin parent of frozen dir/tree?
    if (inode->is_auth() && (is_frozen_tree_root() || is_frozen_dir()))
      inode->auth_pin();
  }

  // newly single auth?
  if (was_ambiguous && dir_auth.second == CDIR_AUTH_UNKNOWN) {
    list<Context*> ls;
    take_waiting(WAIT_SINGLEAUTH, ls);
    cache->mds->queue_waiters(ls);
  }
}


/*****************************************
 * AUTH PINS and FREEZING
 *
 * the basic plan is that auth_pins only exist in auth regions, and they
 * prevent a freeze (and subsequent auth change).  
 *
 * however, we also need to prevent a parent from freezing if a child is frozen.
 * for that reason, the parent inode of a frozen directory is auth_pinned.
 *
 * the oddity is when the frozen directory is a subtree root.  if that's the case,
 * the parent inode isn't frozen.  which means that when subtree authority is adjusted
 * at the bounds, inodes for any frozen bound directories need to get auth_pins at that
 * time.
 *
 */

void CDir::auth_pin() 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

  dout(10) << "auth_pin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << dendl;

  // nest pins?
  if (is_subtree_root()) return;  // no.
  //assert(!is_import());

  inode->adjust_nested_auth_pins(1);
}

void CDir::auth_unpin() 
{
  auth_pins--;
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(10) << "auth_unpin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << dendl;
  assert(auth_pins >= 0);
  
  maybe_finish_freeze();  // pending freeze?
  
  // nest?
  if (is_subtree_root()) return;  // no.
  //assert(!is_import());

  inode->adjust_nested_auth_pins(-1);
}

void CDir::adjust_nested_auth_pins(int inc) 
{
  nested_auth_pins += inc;
  
  dout(15) << "adjust_nested_auth_pins " << inc << " on " << *this
	   << " count now " << auth_pins << " + " << nested_auth_pins << dendl;
  assert(nested_auth_pins >= 0);

  maybe_finish_freeze();  // pending freeze?
  
  // adjust my inode?
  if (is_subtree_root()) 
    return; // no, stop.

  // yes.
  inode->adjust_nested_auth_pins(inc);
}



/*****************************************************************************
 * FREEZING
 */

// FREEZE TREE

bool CDir::freeze_tree()
{
  assert(!is_frozen());
  assert(!is_freezing());

  auth_pin();
  if (is_freezeable(true)) {
    _freeze_tree();
    auth_unpin();
    return true;
  } else {
    state_set(STATE_FREEZINGTREE);
    dout(10) << "freeze_tree waiting " << *this << dendl;
    return false;
  }
}

void CDir::_freeze_tree()
{
  dout(10) << "_freeze_tree " << *this << dendl;
  assert(is_freezeable(true));

  // twiddle state
  state_clear(STATE_FREEZINGTREE);   // actually, this may get set again by next context?
  state_set(STATE_FROZENTREE);
  get(PIN_FROZEN);

  // auth_pin inode for duration of freeze, if we are not a subtree root.
  if (is_auth() && !is_subtree_root())
    inode->auth_pin();
}

void CDir::unfreeze_tree()
{
  dout(10) << "unfreeze_tree " << *this << dendl;

  if (state_test(STATE_FROZENTREE)) {
    // frozen.  unfreeze.
    state_clear(STATE_FROZENTREE);
    put(PIN_FROZEN);

    // unpin  (may => FREEZEABLE)   FIXME: is this order good?
    if (is_auth() && !is_subtree_root())
      inode->auth_unpin();

    // waiters?
    finish_waiting(WAIT_UNFREEZE);
  } else {
    finish_waiting(WAIT_FROZEN, -1);

    // freezing.  stop it.
    assert(state_test(STATE_FREEZINGTREE));
    state_clear(STATE_FREEZINGTREE);
    auth_unpin();
    
    finish_waiting(WAIT_UNFREEZE);
  }
}

bool CDir::is_freezing_tree()
{
  CDir *dir = this;
  while (1) {
    if (dir->is_freezing_tree_root()) return true;
    if (dir->is_subtree_root()) return false;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      return false; // root on replica
  }
}

bool CDir::is_frozen_tree()
{
  CDir *dir = this;
  while (1) {
    if (dir->is_frozen_tree_root()) return true;
    if (dir->is_subtree_root()) return false;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      return false;  // root on replica
  }
}

CDir *CDir::get_frozen_tree_root() 
{
  assert(is_frozen());
  CDir *dir = this;
  while (1) {
    if (dir->is_frozen_tree_root()) 
      return dir;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      assert(0);
  }
}



// FREEZE DIR

bool CDir::freeze_dir()
{
  assert(!is_frozen());
  assert(!is_freezing());
  
  auth_pin();
  if (is_freezeable_dir(true)) {
    _freeze_dir();
    auth_unpin();
    return true;
  } else {
    state_set(STATE_FREEZINGDIR);
    dout(10) << "freeze_dir + wait " << *this << dendl;
    return false;
  } 
}

void CDir::_freeze_dir()
{
  dout(10) << "_freeze_dir " << *this << dendl;
  assert(is_freezeable_dir(true));

  state_clear(STATE_FREEZINGDIR);
  state_set(STATE_FROZENDIR);
  get(PIN_FROZEN);

  if (is_auth() && !is_subtree_root())
    inode->auth_pin();  // auth_pin for duration of freeze
}


void CDir::unfreeze_dir()
{
  dout(10) << "unfreeze_dir " << *this << dendl;

  if (state_test(STATE_FROZENDIR)) {
    state_clear(STATE_FROZENDIR);
    put(PIN_FROZEN);

    // unpin  (may => FREEZEABLE)   FIXME: is this order good?
    if (is_auth() && !is_subtree_root())
      inode->auth_unpin();

    finish_waiting(WAIT_UNFREEZE);
  } else {
    finish_waiting(WAIT_FROZEN, -1);

    // still freezing. stop.
    assert(state_test(STATE_FREEZINGDIR));
    state_clear(STATE_FREEZINGDIR);
    auth_unpin();
    
    finish_waiting(WAIT_UNFREEZE);
  }
}








