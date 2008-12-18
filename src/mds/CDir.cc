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

#include "MDSMap.h"
#include "MDS.h"
#include "MDCache.h"
#include "Locker.h"
#include "MDLog.h"
#include "LogSegment.h"

#include "include/Context.h"
#include "common/Clock.h"

#include "osdc/Objecter.h"

#include "config.h"

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << cache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") "



// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };



ostream& operator<<(ostream& out, CDir& dir)
{
  filepath path;
  dir.get_inode()->make_path(path);
  out << "[dir " << dir.dirfrag() << " " << path << "/"
      << " [" << dir.first << ",head]";
  if (dir.is_auth()) {
    out << " auth";
    if (dir.is_replicated())
      out << dir.get_replicas();

    if (dir.is_projected())
      out << " pv=" << dir.get_projected_version();
    out << " v=" << dir.get_version();
    out << " cv=" << dir.get_committing_version();
    out << "/" << dir.get_committed_version();
    out << "/" << dir.get_committed_version_equivalent();
  } else {
    pair<int,int> a = dir.authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
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
    out << " ap=" << dir.get_auth_pins() 
	<< "+" << dir.get_dir_auth_pins()
	<< "+" << dir.get_nested_auth_pins();
  if (dir.get_nested_anchors())
    out << " na=" << dir.get_nested_anchors();

  out << " state=" << dir.get_state();
  if (dir.state_test(CDir::STATE_COMPLETE)) out << "|complete";
  if (dir.state_test(CDir::STATE_FREEZINGTREE)) out << "|freezingtree";
  if (dir.state_test(CDir::STATE_FROZENTREE)) out << "|frozentree";
  //if (dir.state_test(CDir::STATE_FROZENTREELEAF)) out << "|frozentreeleaf";
  if (dir.state_test(CDir::STATE_FROZENDIR)) out << "|frozendir";
  if (dir.state_test(CDir::STATE_FREEZINGDIR)) out << "|freezingdir";
  if (dir.state_test(CDir::STATE_EXPORTBOUND)) out << "|exportbound";
  if (dir.state_test(CDir::STATE_IMPORTBOUND)) out << "|importbound";

  out << " " << dir.fnode.fragstat;
  //out << "/" << dir.fnode.accounted_fragstat;
  out << " s=" << dir.fnode.fragstat.size() 
      << "=" << dir.fnode.fragstat.nfiles
      << "+" << dir.fnode.fragstat.nsubdirs;
  out << " rb=" << dir.fnode.rstat.rbytes << "/" << dir.fnode.accounted_rstat.rbytes;
  out << " rf=" << dir.fnode.rstat.rfiles << "/" << dir.fnode.accounted_rstat.rfiles;
  out << " rd=" << dir.fnode.rstat.rsubdirs << "/" << dir.fnode.accounted_rstat.rsubdirs;

  out << " hs=" << dir.get_num_head_items() << "+" << dir.get_num_head_null();
  out << ",ss=" << dir.get_num_snap_items() << "+" << dir.get_num_snap_null();
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
  xlist_dirty(this), xlist_new(this)
{
  inode = in;
  frag = fg;
  this->cache = mdcache;

  first = 2;
  
  num_head_items = num_head_null = 0;
  num_snap_items = num_snap_null = 0;
  num_dirty = 0;

  state = STATE_INITIAL;

  memset(&fnode, 0, sizeof(fnode));
  projected_version = 0;

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
  dir_auth_pins = 0;
  request_pins = 0;

  nested_anchors = 0;

  //hack_num_accessed = -1;
  
  dir_rep = REP_NONE;
  //dir_rep = REP_ALL;      // hack: to wring out some bugs! FIXME FIXME
}




CDentry *CDir::lookup(const char *name, snapid_t snap)
{ 
  dout(20) << "lookup (" << snap << ", '" << name << "')" << dendl;
  map_t::iterator iter = items.lower_bound(dentry_key_t(snap, name));
  if (iter == items.end())
    return 0;
  if (iter->second->name == name &&
      iter->second->first <= snap) {
    dout(20) << "  hit -> " << iter->first << dendl;
    return iter->second;
  }
  dout(20) << "  miss -> " << iter->first << dendl;
  return 0;
}





/***
 * linking fun
 */

CDentry* CDir::add_null_dentry(const nstring& dname,
			       snapid_t first, snapid_t last)
{
  // foreign
  assert(lookup_exact_snap(dname, last) == 0);
   
  // create dentry
  CDentry* dn = new CDentry(dname, first, last);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = get_projected_version();
  
  // add to dir
  assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->key()] = dn;
  if (last == CEPH_NOSNAP)
    num_head_null++;
  else
    num_snap_null++;

  dout(12) << "add_null_dentry " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  
  assert(get_num_any() == items.size());
  return dn;
}


CDentry* CDir::add_primary_dentry(const nstring& dname, CInode *in,
				  snapid_t first, snapid_t last) 
{
  // primary
  assert(lookup_exact_snap(dname, last) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, first, last);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = get_projected_version();
  
  // add to dir
  assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->key()] = dn;
  link_inode_work( dn, in );

  dout(12) << "add_primary_dentry " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  assert(get_num_any() == items.size());
  return dn;
}

CDentry* CDir::add_remote_dentry(const nstring& dname, inodeno_t ino, unsigned char d_type,
				 snapid_t first, snapid_t last) 
{
  // foreign
  assert(lookup_exact_snap(dname, last) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, ino, d_type, first, last);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = get_projected_version();
  
  // add to dir
  assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->key()] = dn;
  if (last == CEPH_NOSNAP)
    num_head_items++;
  else
    num_snap_items++;

  dout(12) << "add_remote_dentry " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  
  assert(get_num_any() == items.size());
  return dn;
}



void CDir::remove_dentry(CDentry *dn) 
{
  dout(12) << "remove_dentry " << *dn << dendl;

  // there should be no client leases at this point!
  assert(dn->client_lease_map.empty());

  if (dn->inode) {
    // detach inode and dentry
    unlink_inode_work(dn);
  } else {
    // remove from null list
    //assert(null_items.count(dn->name) == 1);
    //null_items.erase(dn->name);
    if (dn->last == CEPH_NOSNAP)
      num_head_null--;
    else
      num_snap_null--;
  }
  
  // remove from list
  assert(items.count(dn->key()) == 1);
  items.erase(dn->key());

  // clean?
  if (dn->is_dirty())
    dn->mark_clean();

  cache->lru.lru_remove(dn);
  delete dn;

  // unpin?
  if (get_num_any() == 0)
    put(PIN_CHILD);
  assert(get_num_any() == items.size());
}

void CDir::link_remote_inode(CDentry *dn, CInode *in)
{
  link_remote_inode(dn, in->ino(), MODE_TO_DT(in->get_projected_inode()->mode));
}

void CDir::link_remote_inode(CDentry *dn, inodeno_t ino, unsigned char d_type)
{
  dout(12) << "link_remote_inode " << *dn << " remote " << ino << dendl;
  assert(dn->is_null());

  dn->set_remote(ino, d_type);
  if (dn->last == CEPH_NOSNAP) {
    num_head_items++;
    num_head_null--;
  } else {
    num_snap_items++;
    num_snap_null--;
  }
  assert(get_num_any() == items.size());
}

void CDir::link_primary_inode(CDentry *dn, CInode *in)
{
  dout(12) << "link_primary_inode " << *dn << " " << *in << dendl;
  assert(dn->is_null());

  link_inode_work(dn,in);
  
  if (dn->last == CEPH_NOSNAP)
    num_head_null--;
  else
    num_snap_null--;

  assert(get_num_any() == items.size());
}

void CDir::link_inode_work( CDentry *dn, CInode *in)
{
  assert(dn->inode == 0);
  dn->inode = in;
  in->set_primary_parent(dn);

  if (dn->last == CEPH_NOSNAP)
    num_head_items++;
  else
    num_snap_items++;
  
  // set inode version
  //in->inode.version = dn->get_version();
  
  // pin dentry?
  if (in->get_num_ref())
    dn->get(CDentry::PIN_INODEPIN);
  
  // adjust auth pin count
  if (in->auth_pins + in->nested_auth_pins)
    dn->adjust_nested_auth_pins(in->auth_pins + in->nested_auth_pins, in->auth_pins);

  if (in->inode.anchored + in->nested_anchors)
    dn->adjust_nested_anchors(in->nested_anchors + in->inode.anchored);

  // verify open snaprealm parent
  if (in->snaprealm)
    in->snaprealm->adjust_parent();
}

void CDir::unlink_inode( CDentry *dn )
{
  if (dn->is_remote()) {
    dout(12) << "unlink_inode " << *dn << dendl;
  } else {
    dout(12) << "unlink_inode " << *dn << " " << *dn->inode << dendl;
  }

  unlink_inode_work(dn);

  if (dn->last == CEPH_NOSNAP)
    num_head_null++;
  else
    num_snap_null++;
  assert(get_num_any() == items.size());
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

    if (!is_projected() &&
	committing_version == committed_version &&
	num_dirty == 0) {
      dout(10) << "try_remove_unlinked_dn committed_equivalent now " << get_version() 
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
      dn->adjust_nested_auth_pins(0 - (in->auth_pins + in->nested_auth_pins), 0 - in->auth_pins);
    
    if (in->inode.anchored + in->nested_anchors)
      dn->adjust_nested_anchors(0 - (in->nested_anchors + in->inode.anchored));

    // detach inode
    in->remove_primary_parent(dn);
    dn->inode = 0;
  }

  if (dn->last == CEPH_NOSNAP)
    num_head_items--;
  else
    num_snap_items--;
}

void CDir::remove_null_dentries() {
  dout(12) << "remove_null_dentries " << *this << dendl;

  CDir::map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    p++;
    if (dn->is_null())
      remove_dentry(dn);
  }

  assert(num_snap_null == 0);
  assert(num_head_null == 0);
  assert(get_num_any() == items.size());
}


void CDir::purge_stale_snap_data(const set<snapid_t>& snaps)
{
  dout(10) << "purge_stale_snap_data " << snaps << dendl;

  CDir::map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    p++;

    if (dn->last == CEPH_NOSNAP)
      continue;

    set<snapid_t>::const_iterator p = snaps.lower_bound(dn->first);
    if (p == snaps.end() ||
	*p > dn->last) {
      dout(10) << " purging " << *dn << dendl;
      if (dn->is_primary() && dn->inode->is_dirty())
	dn->inode->mark_clean();
      remove_dentry(dn);
    }
  }
}


/**
 * steal_dentry -- semi-violently move a dentry from one CDir to another
 * (*) violently, in that nitems, most pins, etc. are not correctly maintained 
 * on the old CDir corpse; must call purge_stolen() when finished.
 */
void CDir::steal_dentry(CDentry *dn)
{
  dout(15) << "steal_dentry " << *dn << dendl;

  items[dn->key()] = dn;

  dn->dir->items.erase(dn->key());
  if (dn->dir->items.empty())
    dn->dir->put(PIN_CHILD);

  if (get_num_any() == 0)
    get(PIN_CHILD);
  if (dn->is_null()) {
    if (dn->last == CEPH_NOSNAP)
      num_head_null++;
    else
      num_snap_null++;
  } else {
    if (dn->last == CEPH_NOSNAP)
      num_head_items++;
    else
      num_snap_items++;

    if (dn->is_primary()) {
      inode_t *pi = dn->get_inode()->get_projected_inode();
      if (dn->get_inode()->is_dir())
	fnode.fragstat.nsubdirs++;
      else
	fnode.fragstat.nfiles++;
      fnode.rstat.rbytes += pi->accounted_rstat.rbytes;
      fnode.rstat.rfiles += pi->accounted_rstat.rfiles;
      fnode.rstat.rsubdirs += pi->accounted_rstat.rsubdirs;
      fnode.rstat.ranchors += pi->accounted_rstat.ranchors;
      fnode.rstat.rsnaprealms += pi->accounted_rstat.ranchors;
      if (pi->accounted_rstat.rctime > fnode.rstat.rctime)
	fnode.rstat.rctime = pi->accounted_rstat.rctime;
    } else if (dn->is_remote()) {
      if (dn->get_remote_d_type() == (S_IFDIR >> 12))
	fnode.fragstat.nsubdirs++;
      else
	fnode.fragstat.nfiles++;
    }
  }

  nested_auth_pins += dn->auth_pins + dn->nested_auth_pins;
  nested_anchors += dn->nested_anchors;
  if (dn->is_dirty()) 
    num_dirty++;

  dn->dir = this;
}

void CDir::purge_stolen(list<Context*>& waiters, bool replay)
{
  // take waiters _before_ unfreeze...
  if (!replay) {
    take_waiting(WAIT_ANY_MASK, waiters);
    if (is_auth()) {
      assert(is_frozen_dir());
      unfreeze_dir();
    }
  }

  num_head_items = num_head_null = 0;
  num_snap_items = num_snap_null = 0;

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

void CDir::split(int bits, list<CDir*>& subs, list<Context*>& waiters, bool replay)
{
  dout(10) << "split by " << bits << " bits on " << *this << dendl;

  if (cache->mds->logger) cache->mds->logger->inc("dir_sp");

  assert(is_complete() || !is_auth());

  list<frag_t> frags;
  frag.split(bits, frags);

  vector<CDir*> subfrags(1 << bits);
  
  double fac = 1.0 / (double)(1 << bits);  // for scaling load vecs

  nest_info_t olddiff;  // old += f - af;
  dout(10) << "           rstat " << fnode.rstat << dendl;
  dout(10) << " accounted_rstat " << fnode.accounted_rstat << dendl;
  olddiff.take_diff(fnode.rstat, fnode.accounted_rstat);
  dout(10) << "         olddiff " << olddiff << dendl;

  // create subfrag dirs
  int n = 0;
  for (list<frag_t>::iterator p = frags.begin(); p != frags.end(); ++p) {
    CDir *f = new CDir(inode, *p, cache, is_auth());
    f->state_set(state & MASK_STATE_FRAGMENT_KEPT);
    f->replica_map = replica_map;
    f->dir_auth = dir_auth;
    f->init_fragment_pins();
    f->set_version(get_version());

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
    frag_t subfrag = inode->pick_dirfrag(dn->name);
    int n = (subfrag.value() & (subfrag.mask() ^ frag.mask())) >> subfrag.mask_shift();
    dout(15) << " subfrag " << subfrag << " n=" << n << " for " << p->first << dendl;
    CDir *f = subfrags[n];
    f->steal_dentry(dn);
  }

  // fix up new frag fragstats
  for (int i=0; i<n; i++) {
    subfrags[i]->fnode.fragstat.version = fnode.fragstat.version;
    subfrags[i]->fnode.accounted_fragstat = subfrags[i]->fnode.fragstat;
    dout(10) << "      fragstat " << subfrags[i]->fnode.fragstat << " on " << *subfrags[i] << dendl;
  }

  // give any outstanding frag stat differential to first frag
  //   af[0] -= olddiff
  dout(10) << "giving olddiff " << olddiff << " to " << *subfrags[0] << dendl;
  nest_info_t zero;
  subfrags[0]->fnode.accounted_rstat.take_diff(zero, olddiff);
  dout(10) << "               " << subfrags[0]->fnode.accounted_fragstat << dendl;

  purge_stolen(waiters, replay);
  inode->close_dirfrag(frag); // selft deletion, watch out.
}

void CDir::merge(int bits, list<Context*>& waiters, bool replay)
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

    dir->purge_stolen(waiters, replay);
    inode->close_dirfrag(dir->get_frag());
  }

  init_fragment_pins();
}







/****************************************
 * WAITING
 */

void CDir::add_dentry_waiter(const nstring& dname, snapid_t snapid, Context *c) 
{
  if (waiting_on_dentry.empty())
    get(PIN_DNWAITER);
  waiting_on_dentry[string_snap_t(dname, snapid)].push_back(c);
  dout(10) << "add_dentry_waiter dentry " << dname
	   << " snap " << snapid
	   << " " << c << " on " << *this << dendl;
}

void CDir::take_dentry_waiting(const nstring& dname, snapid_t first, snapid_t last,
			       list<Context*>& ls)
{
  if (waiting_on_dentry.empty())
    return;
  
  string_snap_t lb(dname, first);
  string_snap_t ub(dname, last);
  map<string_snap_t, list<Context*> >::iterator p = waiting_on_dentry.lower_bound(lb);
  while (p != waiting_on_dentry.end() &&
	 !(ub < p->first)) {
    dout(10) << "take_dentry_waiting dentry " << dname
	     << " [" << first << "," << last << "] found waiter on snap "
	     << p->first.snapid
	     << " on " << *this << dendl;
    ls.splice(ls.end(), p->second);
    waiting_on_dentry.erase(p++);
  }

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
  for (map<string_snap_t, list<Context*> >::iterator p = waiting_on_dentry.begin(); 
       p != waiting_on_dentry.end();
       ++p) 
    ls.splice(ls.end(), p->second);
  waiting_on_dentry.clear();
  for (map<inodeno_t, list<Context*> >::iterator p = waiting_on_ino.begin(); 
       p != waiting_on_ino.end();
       ++p) 
    ls.splice(ls.end(), p->second);
  waiting_on_ino.clear();
}



void CDir::add_waiter(__u64 tag, Context *c) 
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
void CDir::take_waiting(__u64 mask, list<Context*>& ls)
{
  if ((mask & WAIT_DENTRY) && waiting_on_dentry.size()) {
    // take each each dentry waiter
    map<string_snap_t, list<Context*> >::iterator it = 
      waiting_on_dentry.begin(); 
    while (it != waiting_on_dentry.end()) {
      nstring name = it->first.name;
      snapid_t snap = it->first.snapid;
      it++;
      take_dentry_waiting(name, snap, snap, ls);
    }
    put(PIN_DNWAITER);
  }
  
  // waiting
  MDSCacheObject::take_waiting(mask, ls);
}


void CDir::finish_waiting(__u64 mask, int result) 
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

fnode_t *CDir::project_fnode()
{
  fnode_t *p = new fnode_t;
  *p = *get_projected_fnode();
  projected_fnode.push_back(p);
  dout(10) << "project_fnode " << p << dendl;
  return p;
}

void CDir::pop_and_dirty_projected_fnode(LogSegment *ls)
{
  assert(!projected_fnode.empty());
  dout(15) << "pop_and_dirty_projected_fnode " << projected_fnode.front()
	   << " v" << projected_fnode.front()->version << dendl;
  fnode = *projected_fnode.front();
  delete projected_fnode.front();
  _mark_dirty(ls);
  projected_fnode.pop_front();
}


version_t CDir::pre_dirty(version_t min)
{
  if (min > projected_version)
    projected_version = min;
  ++projected_version;
  dout(10) << "pre_dirty " << projected_version << dendl;
  return projected_version;
}

void CDir::mark_dirty(version_t pv, LogSegment *ls)
{
  assert(get_version() < pv);
  assert(pv <= projected_version);
  fnode.version = pv;
  _mark_dirty(ls);
}

void CDir::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    dout(10) << "mark_dirty (was clean) " << *this << " version " << get_version() << dendl;
    _set_dirty_flag();
    assert(ls);
  } else {
    dout(10) << "mark_dirty (already dirty) " << *this << " version " << get_version() << dendl;
  }
  if (ls) {
    ls->dirty_dirfrags.push_back(&xlist_dirty);

    // if i've never committed, i need to be before _any_ mention of me is trimmed from the journal.
    if (committed_version == 0 && !xlist_new.is_on_xlist())
      ls->new_dirfrags.push_back(&xlist_dirty);
  }
}

void CDir::mark_new(LogSegment *ls)
{
  ls->new_dirfrags.push_back(&xlist_new);
}

void CDir::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << get_version() << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);

    xlist_dirty.remove_myself();
    xlist_new.remove_myself();
  }
}


struct C_Dir_Dirty : public Context {
  CDir *dir;
  version_t pv;
  LogSegment *ls;
  C_Dir_Dirty(CDir *d, version_t p, LogSegment *l) : dir(d), pv(p), ls(l) {}
  void finish(int r) {
    dir->mark_dirty(pv, ls);
  }
};

void CDir::log_mark_dirty()
{
  MDLog *mdlog = inode->mdcache->mds->mdlog;
  version_t pv = pre_dirty();
  mdlog->wait_for_sync(new C_Dir_Dirty(this, pv, mdlog->get_current_segment()));
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

  auth_pin(this);
  state_set(CDir::STATE_FETCHING);

  if (cache->mds->logger) cache->mds->logger->inc("dir_f");

  // start by reading the first hunk of it
  C_Dir_Fetch *fin = new C_Dir_Fetch(this);
  cache->mds->objecter->read( get_ondisk_object(),
			      cache->mds->objecter->osdmap->file_to_object_layout( get_ondisk_object(),
										   g_default_mds_dir_layout ),
			      0, 0,   // whole object
			      &fin->bl, 0,
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
  bufferlist::iterator p = bl.begin();

  fnode_t got_fnode;
  ::decode(got_fnode, p);

  dout(10) << "_fetched version " << got_fnode.version
	   << ", " << len << " bytes"
	   << dendl;
  
  __s32 n;
  ::decode(n, p);

  // take the loaded fnode?
  // only if we are a fresh CDir* with no prior state.
  if (get_version() == 0) {
    assert(!is_projected());
    assert(!state_test(STATE_COMMITTING));
    fnode = got_fnode;
    projected_version = committing_version = committed_version = got_fnode.version;
  }

  // purge stale snaps?
  //  * only if we have past_parents open!
  const set<snapid_t> *snaps = 0;
  SnapRealm *realm = inode->find_snaprealm();
  if (!realm->have_past_parents_open()) {
    dout(10) << " no snap purge, one or more past parents NOT open" << dendl;
  } else if (fnode.snap_purged_thru < realm->get_last_destroyed()) {
    snaps = &realm->get_snaps();
    dout(10) << " snap_purged_thru " << fnode.snap_purged_thru
	     << " < " << realm->get_last_destroyed()
	     << ", snap purge based on " << *snaps << dendl;
    fnode.snap_purged_thru = realm->get_last_destroyed();
  }
  bool purged_any = false;


  //int num_new_inodes_loaded = 0;
  loff_t baseoff = p.get_off();
  for (int i=0; i<n; i++) {
    loff_t dn_offset = p.get_off() - baseoff;

    // marker
    char type;
    ::decode(type, p);

    // dname
    string dname;
    ::decode(dname, p);
    snapid_t first, last;
    ::decode(first, p);
    ::decode(last, p);
    dout(24) << "_fetched pos " << dn_offset << " marker '" << type << "' dname '" << dname
	     << " [" << first << "," << last << "]"
	     << dendl;

    bool stale = false;
    if (snaps && last != CEPH_NOSNAP) {
      set<snapid_t>::const_iterator p = snaps->lower_bound(first);
      if (p == snaps->end() || *p > last) {
	dout(10) << " skipping stale dentry on [" << first << "," << last << "]" << dendl;
	stale = true;
	purged_any = true;
      }
    }
    
    /*
     * look for existing dentry for _last_ snap, because unlink +
     * create may leave a "hole" (epochs during which the dentry
     * doesn't exist) but for which no explicit negative dentry is in
     * the cache.
     */
    CDentry *dn = 0;
    if (!stale)
      dn = lookup(dname, last);

    if (type == 'L') {
      // hard link
      inodeno_t ino;
      unsigned char d_type;
      ::decode(ino, p);
      ::decode(d_type, p);

      if (stale)
	continue;

      if (dn) {
        if (dn->get_inode() == 0) {
          dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
        } else {
          dout(12) << "_fetched  had dentry " << *dn << dendl;
        }
      } else {
	// (remote) link
	dn = add_remote_dentry(dname, ino, d_type, first, last);
	
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
      string symlink;
      fragtree_t fragtree;
      map<string, bufferptr> xattrs;
      bufferlist snapbl;
      map<snapid_t,old_inode_t> old_inodes;
      ::decode(inode, p);
      if (inode.is_symlink())
        ::decode(symlink, p);
      ::decode(fragtree, p);
      ::decode(xattrs, p);
      ::decode(snapbl, p);
      ::decode(old_inodes, p);
      
      if (stale)
	continue;

      if (dn) {
        if (dn->get_inode() == 0) {
          dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
        } else {
          dout(12) << "_fetched  had dentry " << *dn << dendl;
        }
      } else {
	// add inode
	CInode *in = 0;
	if (cache->have_inode(inode.ino, first)) {
	  in = cache->get_inode(inode.ino, first);
	  dout(-12) << "_fetched  got (but i already had) " << *in 
		   << " mode " << in->inode.mode 
		   << " mtime " << in->inode.mtime << dendl;
	  assert(0);  // this shouldn't happen!! 
	} else {
	  // inode
	  in = new CInode(cache, true, first, last);
	  in->inode = inode;
	  
	  // symlink?
	  if (in->is_symlink()) 
	    in->symlink = symlink;
	  
	  in->dirfragtree.swap(fragtree);
	  in->xattrs.swap(xattrs);
	  in->decode_snap_blob(snapbl);
	  in->old_inodes.swap(old_inodes);
	  if (snaps)
	    in->purge_stale_snap_data(*snaps);

	  // add 
	  cache->add_inode( in );
	
	  // link
	  dn = add_primary_dentry(dname, in, first, last);
	  dout(12) << "_fetched  got " << *dn << " " << *in << dendl;

	  //in->hack_accessed = false;
	  //in->hack_load_stamp = g_clock.now();
	  //num_new_inodes_loaded++;
	}
      }
    } else {
      dout(1) << "corrupt directory, i got tag char '" << type << "' val " << (int)(type)
	      << " at offset " << p.get_off() << dendl;
      assert(0);
    }
    
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
	dn->get_version() <= fnode.version &&
	dn->is_dirty()) {
      dout(10) << "_fetched  had underwater dentry " << *dn << ", marking clean" << dendl;
      dn->mark_clean();

      if (dn->get_inode()) {
	assert(dn->get_inode()->get_version() <= fnode.version);
	dout(10) << "_fetched  had underwater inode " << *dn->get_inode() << ", marking clean" << dendl;
	dn->get_inode()->mark_clean();
      }
    }
  }
  assert(p.end());

  //cache->mds->logger->inc("newin", num_new_inodes_loaded);
  //hack_num_accessed = 0;

  if (purged_any)
    log_mark_dirty();

  // mark complete, !fetching
  state_set(STATE_COMPLETE);
  state_clear(STATE_FETCHING);
  auth_unpin(this);

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
  if (want == 0) want = get_version();

  // preconditions
  assert(want <= get_version() || get_version() == 0);    // can't commit the future
  assert(want > committed_version); // the caller is stupid
  assert(is_auth());
  assert(can_auth_pin());

  // note: queue up a noop if necessary, so that we always
  // get an auth_pin.
  if (!c)
    c = new C_NoopContext;

  // auth_pin on first waiter
  if (waiting_for_commit.empty())
    auth_pin(this);
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
  assert(want <= get_version() || get_version() == 0);

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
  committing_version = get_version();

  // mark committing (if not already)
  if (!state_test(STATE_COMMITTING)) {
    dout(10) << "marking committing" << dendl;
    state_set(STATE_COMMITTING);
  }
  
  if (cache->mds->logger) cache->mds->logger->inc("dir_c");

  // snap purge?
  SnapRealm *realm = inode->find_snaprealm();
  const set<snapid_t> *snaps = 0;
  if (!realm->have_past_parents_open()) {
    dout(10) << " no snap purge, one or more past parents NOT open" << dendl;
  } else if (fnode.snap_purged_thru < realm->get_last_destroyed()) {
    snaps = &realm->get_snaps();
    dout(10) << " snap_purged_thru " << fnode.snap_purged_thru
	     << " < " << realm->get_last_destroyed()
	     << ", snap purge based on " << *snaps << dendl;
    fnode.snap_purged_thru = realm->get_last_destroyed();
  }

  // encode
  bufferlist bl;
  __u32 n = 0;
  map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    p++;
    
    if (dn->is_null()) 
      continue;  // skip negative entries

    if (snaps && dn->last != CEPH_NOSNAP) {
      set<snapid_t>::const_iterator p = snaps->lower_bound(dn->first);
      if (p == snaps->end() || *p > dn->last) {
	dout(10) << " purging " << *dn << dendl;
	if (dn->is_primary() && dn->inode->is_dirty())
	  dn->inode->mark_clean();
	remove_dentry(dn);
	continue;
      }
    }
    
    n++;

    // primary or remote?
    if (dn->is_remote()) {
      inodeno_t ino = dn->get_remote_ino();
      unsigned char d_type = dn->get_remote_d_type();
      dout(14) << " pos " << bl.length() << " dn '" << dn->name << "' remote ino " << ino << dendl;
      
      // marker, name, ino
      bl.append( "L", 1 );         // remote link
      ::encode(dn->name, bl);
      ::encode(dn->first, bl);
      ::encode(dn->last, bl);
      ::encode(ino, bl);
      ::encode(d_type, bl);
    } else {
      // primary link
      CInode *in = dn->get_inode();
      assert(in);

      dout(14) << " pos " << bl.length() << " dn '" << dn->name << "' inode " << *in << dendl;
  
      // marker, name, inode, [symlink string]
      bl.append( "I", 1 );         // inode
      ::encode(dn->name, bl);
      ::encode(dn->first, bl);
      ::encode(dn->last, bl);
      ::encode(in->inode, bl);
      
      if (in->is_symlink()) {
        // include symlink destination!
        dout(18) << "    including symlink ptr " << in->symlink << dendl;
	::encode(in->symlink, bl);
      }

      ::encode(in->dirfragtree, bl);
      ::encode(in->xattrs, bl);
      bufferlist snapbl;
      in->encode_snap_blob(snapbl);
      ::encode(snapbl, bl);

      if (in->is_multiversion() && snaps)
	in->purge_stale_snap_data(*snaps);
      ::encode(in->old_inodes, bl);
    }
  }

  // header
  assert(n == (num_head_items + num_snap_items));
  bufferlist finalbl;
  ::encode(fnode, finalbl);
  ::encode(n, finalbl);
  finalbl.claim_append(bl);

  // write it.
  SnapContext snapc;
  ObjectMutation m;
  m.write_full(finalbl);

  string path;
  inode->make_path_string(path);
  m.setxattr("path", path);

  cache->mds->objecter->mutate( get_ondisk_object(),
				cache->mds->objecter->osdmap->file_to_object_layout( get_ondisk_object(),
										     g_default_mds_dir_layout ),
				m, snapc, 0,
				NULL, new C_Dir_Committed(this, get_version()) );
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
  if (committed_version == get_version()) 
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
	assert(in->is_dirty() || in->last < CEPH_NOSNAP);  // special case for cow snap items (not predirtied)
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
    auth_unpin(this);
}





// IMPORT/EXPORT

void CDir::encode_export(bufferlist& bl)
{
  assert(!is_projected());
  ::encode(first, bl);
  ::encode(fnode, bl);
  ::encode(dirty_old_rstat, bl);
  ::encode(committed_version, bl);
  ::encode(committed_version_equivalent, bl);

  ::encode(state, bl);
  ::encode(dir_rep, bl);

  ::encode(pop_me, bl);
  ::encode(pop_auth_subtree, bl);

  ::encode(dir_rep_by, bl);  
  ::encode(replica_map, bl);

  get(PIN_TEMPEXPORTING);
}

void CDir::finish_export(utime_t now)
{
  pop_auth_subtree_nested -= pop_auth_subtree;
  pop_me.zero(now);
  pop_auth_subtree.zero(now);
  put(PIN_TEMPEXPORTING);
  dirty_old_rstat.clear();
}

void CDir::decode_import(bufferlist::iterator& blp)
{
  ::decode(first, blp);
  ::decode(fnode, blp);
  ::decode(dirty_old_rstat, blp);
  projected_version = fnode.version;
  ::decode(committed_version, blp);
  ::decode(committed_version_equivalent, blp);
  committing_version = committed_version;

  unsigned s;
  ::decode(s, blp);
  state &= MASK_STATE_IMPORT_KEPT;
  state |= (s & MASK_STATE_EXPORTED);
  if (is_dirty()) get(PIN_DIRTY);

  ::decode(dir_rep, blp);

  ::decode(pop_me, blp);
  ::decode(pop_auth_subtree, blp);
  pop_auth_subtree_nested += pop_auth_subtree;

  ::decode(dir_rep_by, blp);
  ::decode(replica_map, blp);
  if (!replica_map.empty()) get(PIN_REPLICATED);

  replica_nonce = 0;  // no longer defined

  // did we import some dirty scatterlock data?
  if (dirty_old_rstat.size() ||
      !(fnode.rstat == fnode.accounted_rstat))
    cache->mds->locker->mark_updated_scatterlock(&inode->nestlock);
  if (!(fnode.fragstat == fnode.accounted_fragstat))
    cache->mds->locker->mark_updated_scatterlock(&inode->filelock);
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
      inode->auth_unpin(this);
  } 
  if (was_subtree && !is_subtree_root()) {
    dout(10) << " old subtree root, adjusting auth_pins" << dendl;
    
    // adjust nested auth pins
    inode->adjust_nested_auth_pins(get_cum_auth_pins());

    // pin parent of frozen dir/tree?
    if (inode->is_auth() && (is_frozen_tree_root() || is_frozen_dir()))
      inode->auth_pin(this);
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

void CDir::auth_pin(void *by) 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by
	   << " on " << *this
	   << " count now " << auth_pins << " + " << nested_auth_pins << dendl;

  // nest pins?
  if (is_subtree_root()) return;  // no.
  //assert(!is_import());

  inode->adjust_nested_auth_pins(1);
}

void CDir::auth_unpin(void *by) 
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  assert(auth_pin_set.count(by));
  auth_pin_set.erase(auth_pin_set.find(by));
#endif
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(10) << "auth_unpin by " << by
	   << " on " << *this
	   << " count now " << auth_pins << " + " << nested_auth_pins << dendl;
  assert(auth_pins >= 0);
  
  maybe_finish_freeze();  // pending freeze?
  
  // nest?
  if (is_subtree_root()) return;  // no.
  //assert(!is_import());

  inode->adjust_nested_auth_pins(-1);
}

void CDir::adjust_nested_auth_pins(int inc, int dirinc) 
{
  nested_auth_pins += inc;
  dir_auth_pins += dirinc;
  
  dout(15) << "adjust_nested_auth_pins " << inc << "/" << dirinc << " on " << *this
	   << " count now " << auth_pins << " + " << nested_auth_pins << dendl;
  assert(nested_auth_pins >= 0);
  assert(dir_auth_pins >= 0);

  maybe_finish_freeze();  // pending freeze?
  
  // adjust my inode?
  if (is_subtree_root()) 
    return; // no, stop.

  // yes.
  inode->adjust_nested_auth_pins(inc);
}

void CDir::adjust_nested_anchors(int by)
{
  nested_anchors += by;
  dout(20) << "adjust_nested_anchors by " << by << " -> " << nested_anchors << dendl;
  assert(nested_anchors >= 0);
  inode->adjust_nested_anchors(by);
}

#ifdef MDS_VERIFY_FRAGSTAT
void CDir::verify_fragstat()
{
  assert(is_complete());
  if (inode->is_stray())
    return;

  frag_info_t c;
  memset(&c, 0, sizeof(c));

  for (map_t::iterator it = items.begin();
       it != items.end();
       it++) {
    CDentry *dn = it->second;
    if (dn->is_null())
      continue;
    
    dout(10) << " " << *dn << dendl;
    if (dn->is_primary())
      dout(10) << "     " << *dn->inode << dendl;

    if (dn->is_primary()) {
      if (dn->inode->is_dir())
	c.nsubdirs++;
      else
	c.nfiles++;
    }
    if (dn->is_remote()) {
      if (dn->get_remote_d_type() == (S_IFDIR >> 12))
	c.nsubdirs++;
      else
	c.nfiles++;
    }
  }

  if (c.nsubdirs != fnode.fragstat.nsubdirs ||
      c.nfiles != fnode.fragstat.nfiles) {
    dout(0) << "verify_fragstat failed " << fnode.fragstat << " on " << *this << dendl;
    dout(0) << "               i count " << c << dendl;
    assert(0);
  } else {
    dout(0) << "verify_fragstat ok " << fnode.fragstat << " on " << *this << dendl;
  }
}
#endif

/*****************************************************************************
 * FREEZING
 */

// FREEZE TREE

bool CDir::freeze_tree()
{
  assert(!is_frozen());
  assert(!is_freezing());

  auth_pin(this);
  if (is_freezeable(true)) {
    _freeze_tree();
    auth_unpin(this);
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
    inode->auth_pin(this);
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
      inode->auth_unpin(this);

    // waiters?
    finish_waiting(WAIT_UNFREEZE);
  } else {
    finish_waiting(WAIT_FROZEN, -1);

    // freezing.  stop it.
    assert(state_test(STATE_FREEZINGTREE));
    state_clear(STATE_FREEZINGTREE);
    auth_unpin(this);
    
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
  
  auth_pin(this);
  if (is_freezeable_dir(true)) {
    _freeze_dir();
    auth_unpin(this);
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
    inode->auth_pin(this);  // auth_pin for duration of freeze
}


void CDir::unfreeze_dir()
{
  dout(10) << "unfreeze_dir " << *this << dendl;

  if (state_test(STATE_FROZENDIR)) {
    state_clear(STATE_FROZENDIR);
    put(PIN_FROZEN);

    // unpin  (may => FREEZEABLE)   FIXME: is this order good?
    if (is_auth() && !is_subtree_root())
      inode->auth_unpin(this);

    finish_waiting(WAIT_UNFREEZE);
  } else {
    finish_waiting(WAIT_FROZEN, -1);

    // still freezing. stop.
    assert(state_test(STATE_FREEZINGDIR));
    state_clear(STATE_FREEZINGDIR);
    auth_unpin(this);
    
    finish_waiting(WAIT_UNFREEZE);
  }
}








