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
#include "Mutation.h"

#include "MDSMap.h"
#include "MDS.h"
#include "MDCache.h"
#include "Locker.h"
#include "MDLog.h"
#include "LogSegment.h"

#include "include/bloom_filter.hpp"
#include "include/Context.h"
#include "common/Clock.h"

#include "osdc/Objecter.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << cache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") "



// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };

boost::pool<> CDir::pool(sizeof(CDir));


ostream& operator<<(ostream& out, CDir& dir)
{
  string path;
  dir.get_inode()->make_path_string_projected(path);
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
  } else {
    pair<int,int> a = dir.authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
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

  // fragstat
  out << " " << dir.fnode.fragstat;
  if (!(dir.fnode.fragstat == dir.fnode.accounted_fragstat))
    out << "/" << dir.fnode.accounted_fragstat;
  if (g_conf->mds_debug_scatterstat && dir.is_projected()) {
    fnode_t *pf = dir.get_projected_fnode();
    out << "->" << pf->fragstat;
    if (!(pf->fragstat == pf->accounted_fragstat))
      out << "/" << pf->accounted_fragstat;
  }
  
  // rstat
  out << " " << dir.fnode.rstat;
  if (!(dir.fnode.rstat == dir.fnode.accounted_rstat))
    out << "/" << dir.fnode.accounted_rstat;
  if (g_conf->mds_debug_scatterstat && dir.is_projected()) {
    fnode_t *pf = dir.get_projected_fnode();
    out << "->" << pf->rstat;
    if (!(pf->rstat == pf->accounted_rstat))
      out << "/" << pf->accounted_rstat;
 }

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
  return out << ceph_clock_now(g_ceph_context) << " mds." << cache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") ";
}



// -------------------------------------------------------------------
// CDir

CDir::CDir(CInode *in, frag_t fg, MDCache *mdcache, bool auth) :
  dirty_rstat_inodes(member_offset(CInode, dirty_rstat_item)),
  item_dirty(this), item_new(this),
  pop_me(ceph_clock_now(g_ceph_context)),
  pop_nested(ceph_clock_now(g_ceph_context)),
  pop_auth_subtree(ceph_clock_now(g_ceph_context)),
  pop_auth_subtree_nested(ceph_clock_now(g_ceph_context)),
  bloom(NULL)
{
  g_num_dir++;
  g_num_dira++;

  inode = in;
  frag = fg;
  this->cache = mdcache;

  first = 2;
  
  num_head_items = num_head_null = 0;
  num_snap_items = num_snap_null = 0;
  num_dirty = 0;

  num_dentries_nested = 0;
  num_dentries_auth_subtree = 0;
  num_dentries_auth_subtree_nested = 0;

  state = STATE_INITIAL;

  memset(&fnode, 0, sizeof(fnode));
  projected_version = 0;

  committing_version = 0;
  committed_version = 0;

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

/**
 * Check the recursive statistics on size for consistency.
 * If mds_debug_scatterstat is enabled, assert for correctness,
 * otherwise just print out the mismatch and continue.
 */
bool CDir::check_rstats()
{
  dout(25) << "check_rstats on " << this << dendl;
  if (!is_complete() || !is_auth() || is_frozen()) {
    dout(10) << "check_rstats bailing out -- incomplete or non-auth or frozen dir!" << dendl;
    return true;
  }

  // fragstat
  if(!(get_num_head_items()==
      (fnode.fragstat.nfiles + fnode.fragstat.nsubdirs))) {
    dout(1) << "mismatch between head items and fnode.fragstat! printing dentries" << dendl;
    dout(1) << "get_num_head_items() = " << get_num_head_items()
             << "; fnode.fragstat.nfiles=" << fnode.fragstat.nfiles
             << " fnode.fragstat.nsubdirs=" << fnode.fragstat.nsubdirs << dendl;
    for (map_t::iterator i = items.begin(); i != items.end(); ++i) {
      //if (i->second->get_linkage()->is_primary())
        dout(1) << *(i->second) << dendl;
    }
    assert(!g_conf->mds_debug_scatterstat ||
           (get_num_head_items() ==
            (fnode.fragstat.nfiles + fnode.fragstat.nsubdirs)));
  } else {
    dout(20) << "get_num_head_items() = " << get_num_head_items()
             << "; fnode.fragstat.nfiles=" << fnode.fragstat.nfiles
             << " fnode.fragstat.nsubdirs=" << fnode.fragstat.nsubdirs << dendl;
  }

  // rstat
  nest_info_t sub_info;
  for (map_t::iterator i = items.begin(); i != items.end(); ++i) {
    if (i->second->get_linkage()->is_primary() &&
	i->second->last == CEPH_NOSNAP) {
      sub_info.add(i->second->get_linkage()->inode->inode.accounted_rstat);
    }
  }

  if ((!(sub_info.rbytes == fnode.rstat.rbytes)) ||
      (!(sub_info.rfiles == fnode.rstat.rfiles)) ||
      (!(sub_info.rsubdirs == fnode.rstat.rsubdirs))) {
    dout(1) << "mismatch between child accounted_rstats and my rstats!" << dendl;
    dout(1) << "total of child dentrys: " << sub_info << dendl;
    dout(1) << "my rstats:              " << fnode.rstat << dendl;
    for (map_t::iterator i = items.begin(); i != items.end(); ++i) {
      if (i->second->get_linkage()->is_primary()) {
        dout(1) << *(i->second) << " "
                << i->second->get_linkage()->inode->inode.accounted_rstat
                << dendl;
      }
    }
  } else {
    dout(25) << "total of child dentrys: " << sub_info << dendl;
    dout(25) << "my rstats:              " << fnode.rstat << dendl;
  }

  assert(!g_conf->mds_debug_scatterstat || sub_info.rbytes == fnode.rstat.rbytes);
  assert(!g_conf->mds_debug_scatterstat || sub_info.rfiles == fnode.rstat.rfiles);
  assert(!g_conf->mds_debug_scatterstat || sub_info.rsubdirs == fnode.rstat.rsubdirs);
  dout(10) << "check_rstats complete on " << this << dendl;
  return true;
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

CDentry* CDir::add_null_dentry(const string& dname,
			       snapid_t first, snapid_t last)
{
  // foreign
  assert(lookup_exact_snap(dname, last) == 0);
   
  // create dentry
  CDentry* dn = new CDentry(dname, inode->hash_dentry_name(dname), first, last);
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

  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

  dout(12) << "add_null_dentry " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  
  assert(get_num_any() == items.size());
  return dn;
}


CDentry* CDir::add_primary_dentry(const string& dname, CInode *in,
				  snapid_t first, snapid_t last) 
{
  // primary
  assert(lookup_exact_snap(dname, last) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, inode->hash_dentry_name(dname), first, last);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = get_projected_version();
  
  // add to dir
  assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->key()] = dn;

  dn->get_linkage()->inode = in;
  in->set_primary_parent(dn);

  link_inode_work(dn, in);

  if (dn->last == CEPH_NOSNAP)
    num_head_items++;
  else
    num_snap_items++;
  
  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

  dout(12) << "add_primary_dentry " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  assert(get_num_any() == items.size());
  return dn;
}

CDentry* CDir::add_remote_dentry(const string& dname, inodeno_t ino, unsigned char d_type,
				 snapid_t first, snapid_t last) 
{
  // foreign
  assert(lookup_exact_snap(dname, last) == 0);

  // create dentry
  CDentry* dn = new CDentry(dname, inode->hash_dentry_name(dname), ino, d_type, first, last);
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

  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

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

  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
    dn->put(CDentry::PIN_FRAGMENTING);
    dn->state_clear(CDentry::STATE_FRAGMENTING);
  }    

  if (dn->get_linkage()->is_null()) {
    if (dn->last == CEPH_NOSNAP)
      num_head_null--;
    else
      num_snap_null--;
  } else {
    if (dn->last == CEPH_NOSNAP)
      num_head_items--;
    else
      num_snap_items--;
  }

  if (!dn->get_linkage()->is_null())
    // detach inode and dentry
    unlink_inode_work(dn);
  
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
  link_remote_inode(dn, in->ino(), IFTODT(in->get_projected_inode()->mode));
}

void CDir::link_remote_inode(CDentry *dn, inodeno_t ino, unsigned char d_type)
{
  dout(12) << "link_remote_inode " << *dn << " remote " << ino << dendl;
  assert(dn->get_linkage()->is_null());

  dn->get_linkage()->set_remote(ino, d_type);

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
  assert(dn->get_linkage()->is_null());

  dn->get_linkage()->inode = in;
  in->set_primary_parent(dn);

  link_inode_work(dn, in);
  
  if (dn->last == CEPH_NOSNAP) {
    num_head_items++;
    num_head_null--;
  } else {
    num_snap_items++;
    num_snap_null--;
  }

  assert(get_num_any() == items.size());
}

void CDir::link_inode_work( CDentry *dn, CInode *in)
{
  assert(dn->get_linkage()->get_inode() == in);
  assert(in->get_parent_dn() == dn);

  // set inode version
  //in->inode.version = dn->get_version();
  
  // pin dentry?
  if (in->get_num_ref())
    dn->get(CDentry::PIN_INODEPIN);
  
  // adjust auth pin count
  if (in->auth_pins + in->nested_auth_pins)
    dn->adjust_nested_auth_pins(in->auth_pins + in->nested_auth_pins, in->auth_pins, NULL);

  if (in->inode.anchored + in->nested_anchors)
    dn->adjust_nested_anchors(in->nested_anchors + in->inode.anchored);

  // verify open snaprealm parent
  if (in->snaprealm)
    in->snaprealm->adjust_parent();
}

void CDir::unlink_inode(CDentry *dn)
{
  if (dn->get_linkage()->is_remote()) {
    dout(12) << "unlink_inode " << *dn << dendl;
  } else {
    dout(12) << "unlink_inode " << *dn << " " << *dn->get_linkage()->get_inode() << dendl;
  }

  unlink_inode_work(dn);

  if (dn->last == CEPH_NOSNAP) {
    num_head_items--;
    num_head_null++;
  } else {
    num_snap_items--;
    num_snap_null++;
  }
  assert(get_num_any() == items.size());
}


void CDir::try_remove_unlinked_dn(CDentry *dn)
{
  assert(dn->dir == this);
  assert(dn->get_linkage()->is_null());
  
  // no pins (besides dirty)?
  if (dn->get_num_ref() != dn->is_dirty()) 
    return;

  // was the dn new?
  if (dn->is_new()) {
    dout(10) << "try_remove_unlinked_dn " << *dn << " in " << *this << dendl;
    if (dn->is_dirty())
      dn->mark_clean();
    remove_dentry(dn);

    // NOTE: we may not have any more dirty dentries, but the fnode
    // still changed, so the directory must remain dirty.
  }
}


void CDir::unlink_inode_work( CDentry *dn )
{
  CInode *in = dn->get_linkage()->get_inode();

  if (dn->get_linkage()->is_remote()) {
    // remote
    if (in) 
      dn->unlink_remote(dn->get_linkage());

    dn->get_linkage()->set_remote(0, 0);
  } else {
    // primary
    assert(dn->get_linkage()->is_primary());
 
    // unpin dentry?
    if (in->get_num_ref())
      dn->put(CDentry::PIN_INODEPIN);
    
    // unlink auth_pin count
    if (in->auth_pins + in->nested_auth_pins)
      dn->adjust_nested_auth_pins(0 - (in->auth_pins + in->nested_auth_pins), 0 - in->auth_pins, NULL);
    
    if (in->inode.anchored + in->nested_anchors)
      dn->adjust_nested_anchors(0 - (in->nested_anchors + in->inode.anchored));

    // detach inode
    in->remove_primary_parent(dn);
    dn->get_linkage()->inode = 0;
  }
}

void CDir::add_to_bloom(CDentry *dn)
{
  if (!bloom) {
    /* not create bloom filter for incomplete dir that was added by log replay */
    if (!is_complete())
      return;
    bloom = new bloom_filter(100, 0.05, 0);
  }
  /* This size and false positive probability is completely random.*/
  bloom->insert(dn->name.c_str(), dn->name.size());
}

bool CDir::is_in_bloom(const string& name)
{
  if (!bloom)
    return false;
  return bloom->contains(name.c_str(), name.size());
}

void CDir::remove_bloom()
{
  delete bloom;
  bloom = NULL;
}

void CDir::remove_null_dentries() {
  dout(12) << "remove_null_dentries " << *this << dendl;

  CDir::map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    ++p;
    if (dn->get_linkage()->is_null() && !dn->is_projected())
      remove_dentry(dn);
  }

  assert(num_snap_null == 0);
  assert(num_head_null == 0);
  assert(get_num_any() == items.size());
}


bool CDir::try_trim_snap_dentry(CDentry *dn, const set<snapid_t>& snaps)
{
  assert(dn->last != CEPH_NOSNAP);
  set<snapid_t>::const_iterator p = snaps.lower_bound(dn->first);
  CDentry::linkage_t *dnl= dn->get_linkage();
  CInode *in = 0;
  if (dnl->is_primary())
    in = dnl->get_inode();
  if ((p == snaps.end() || *p > dn->last) &&
      (dn->get_num_ref() == dn->is_dirty()) &&
      (!in || in->get_num_ref() == in->is_dirty())) {
    dout(10) << " purging snapped " << *dn << dendl;
    if (in && in->is_dirty())
      in->mark_clean();
    remove_dentry(dn);
    if (in) {
      dout(10) << " purging snapped " << *in << dendl;
      cache->remove_inode(in);
    }
    return true;
  }
  return false;
}


void CDir::purge_stale_snap_data(const set<snapid_t>& snaps)
{
  dout(10) << "purge_stale_snap_data " << snaps << dendl;

  CDir::map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    ++p;

    if (dn->last == CEPH_NOSNAP)
      continue;

    try_trim_snap_dentry(dn, snaps);
  }
}


/**
 * steal_dentry -- semi-violently move a dentry from one CDir to another
 * (*) violently, in that nitems, most pins, etc. are not correctly maintained 
 * on the old CDir corpse; must call finish_old_fragment() when finished.
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
  if (dn->get_linkage()->is_null()) {
    if (dn->last == CEPH_NOSNAP)
      num_head_null++;
    else
      num_snap_null++;
  } else {
    if (dn->last == CEPH_NOSNAP)
      num_head_items++;
    else
      num_snap_items++;

    if (dn->get_linkage()->is_primary()) {
      CInode *in = dn->get_linkage()->get_inode();
      inode_t *pi = in->get_projected_inode();
      if (dn->get_linkage()->get_inode()->is_dir())
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

      // move dirty inode rstat to new dirfrag
      if (in->is_dirty_rstat())
	dirty_rstat_inodes.push_back(&in->dirty_rstat_item);
    } else if (dn->get_linkage()->is_remote()) {
      if (dn->get_linkage()->get_remote_d_type() == DT_DIR)
	fnode.fragstat.nsubdirs++;
      else
	fnode.fragstat.nfiles++;
    }
  }

  if (dn->auth_pins || dn->nested_auth_pins) {
    // use the helpers here to maintain the auth_pin invariants on the dir inode
    int ap = dn->get_num_auth_pins() + dn->get_num_nested_auth_pins();
    int dap = dn->get_num_dir_auth_pins();
    assert(dap <= ap);
    adjust_nested_auth_pins(ap, dap, NULL);
    dn->dir->adjust_nested_auth_pins(-ap, -dap, NULL);
  }

  nested_anchors += dn->nested_anchors;
  if (dn->is_dirty()) 
    num_dirty++;

  dn->dir = this;
}

void CDir::prepare_old_fragment(bool replay)
{
  // auth_pin old fragment for duration so that any auth_pinning
  // during the dentry migration doesn't trigger side effects
  if (!replay && is_auth())
    auth_pin(this);
}

void CDir::prepare_new_fragment(bool replay)
{
  if (!replay && is_auth())
    _freeze_dir();
}

void CDir::finish_old_fragment(list<Context*>& waiters, bool replay)
{
  // take waiters _before_ unfreeze...
  if (!replay) {
    take_waiting(WAIT_ANY_MASK, waiters);
    if (is_auth()) {
      auth_unpin(this);  // pinned in prepare_old_fragment
      assert(is_frozen_dir());
      unfreeze_dir();
    }
  }

  assert(nested_auth_pins == 0);
  assert(dir_auth_pins == 0);
  assert(auth_pins == 0);

  num_head_items = num_head_null = 0;
  num_snap_items = num_snap_null = 0;

  // this mirrors init_fragment_pins()
  if (is_auth()) 
    clear_replica_map();
  if (is_dirty())
    mark_clean();
  if (state_test(STATE_IMPORTBOUND))
    put(PIN_IMPORTBOUND);
  if (state_test(STATE_EXPORTBOUND))
    put(PIN_EXPORTBOUND);
  if (is_subtree_root())
    put(PIN_SUBTREE);

  if (auth_pins > 0)
    put(PIN_AUTHPIN);

  assert(get_num_ref() == (state_test(STATE_STICKY) ? 1:0));
}

void CDir::init_fragment_pins()
{
  if (!replica_map.empty())
    get(PIN_REPLICATED);
  if (state_test(STATE_DIRTY))
    get(PIN_DIRTY);
  if (state_test(STATE_EXPORTBOUND))
    get(PIN_EXPORTBOUND);
  if (state_test(STATE_IMPORTBOUND))
    get(PIN_IMPORTBOUND);
  if (is_subtree_root())
    get(PIN_SUBTREE);
}

void CDir::split(int bits, list<CDir*>& subs, list<Context*>& waiters, bool replay)
{
  dout(10) << "split by " << bits << " bits on " << *this << dendl;

  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_sp);

  assert(replay || is_complete() || !is_auth());

  list<frag_t> frags;
  frag.split(bits, frags);

  vector<CDir*> subfrags(1 << bits);
  
  double fac = 1.0 / (double)(1 << bits);  // for scaling load vecs

  nest_info_t olddiff;  // old += f - af;
  dout(10) << "           rstat " << fnode.rstat << dendl;
  dout(10) << " accounted_rstat " << fnode.accounted_rstat << dendl;
  olddiff.add_delta(fnode.rstat, fnode.accounted_rstat);
  dout(10) << "         olddiff " << olddiff << dendl;

  prepare_old_fragment(replay);

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
    f->pop_me.scale(fac);

    // FIXME; this is an approximation
    f->pop_nested = pop_nested;
    f->pop_nested.scale(fac);
    f->pop_auth_subtree = pop_auth_subtree;
    f->pop_auth_subtree.scale(fac);
    f->pop_auth_subtree_nested = pop_auth_subtree_nested;
    f->pop_auth_subtree_nested.scale(fac);

    dout(10) << " subfrag " << *p << " " << *f << dendl;
    subfrags[n++] = f;
    subs.push_back(f);
    inode->add_dirfrag(f);

    f->prepare_new_fragment(replay);
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
  bool stale_fragstat = fnode.fragstat.version != fnode.accounted_fragstat.version;
  bool stale_rstat = fnode.rstat.version != fnode.accounted_rstat.version;
  for (int i=0; i<n; i++) {
    subfrags[i]->fnode.fragstat.version = fnode.fragstat.version;
    subfrags[i]->fnode.accounted_fragstat = subfrags[i]->fnode.fragstat;
    if (i == 0) {
      if (stale_fragstat)
	subfrags[0]->fnode.accounted_fragstat.version--;
      if (stale_rstat)
	subfrags[0]->fnode.accounted_rstat.version--;
    }
    dout(10) << "      fragstat " << subfrags[i]->fnode.fragstat << " on " << *subfrags[i] << dendl;
  }

  // give any outstanding frag stat differential to first frag
  //   af[0] -= olddiff
  dout(10) << "giving olddiff " << olddiff << " to " << *subfrags[0] << dendl;
  nest_info_t zero;
  subfrags[0]->fnode.accounted_rstat.add_delta(zero, olddiff);
  dout(10) << "               " << subfrags[0]->fnode.accounted_fragstat << dendl;

  finish_old_fragment(waiters, replay);
}

void CDir::merge(list<CDir*>& subs, list<Context*>& waiters, bool replay)
{
  dout(10) << "merge " << subs << dendl;

  prepare_new_fragment(replay);

  // see if _any_ of the source frags have stale fragstat or rstat
  int stale_rstat = 0;
  int stale_fragstat = 0;

  for (list<CDir*>::iterator p = subs.begin(); p != subs.end(); ++p) {
    CDir *dir = *p;
    dout(10) << " subfrag " << dir->get_frag() << " " << *dir << dendl;
    assert(!dir->is_auth() || dir->is_complete() || replay);
    
    dir->prepare_old_fragment(replay);

    // steal dentries
    while (!dir->items.empty()) 
      steal_dentry(dir->items.begin()->second);
    
    // merge replica map
    for (map<int,int>::iterator p = dir->replica_map.begin();
	 p != dir->replica_map.end();
	 ++p) {
      int cur = replica_map[p->first];
      if (p->second > cur)
	replica_map[p->first] = p->second;
    }

    // merge version
    if (dir->get_version() > get_version())
      set_version(dir->get_version());

    // *stat versions
    if (fnode.fragstat.version < dir->fnode.fragstat.version)
      fnode.fragstat.version = dir->fnode.fragstat.version;
    if (fnode.rstat.version < dir->fnode.rstat.version)
      fnode.rstat.version = dir->fnode.rstat.version;

    if (dir->fnode.accounted_fragstat.version != dir->fnode.fragstat.version)
      stale_fragstat = 1;
    if (dir->fnode.accounted_rstat.version != dir->fnode.rstat.version)
      stale_rstat = 1;

    // sum accounted_*
    fnode.accounted_fragstat.add(dir->fnode.accounted_fragstat);
    fnode.accounted_rstat.add(dir->fnode.accounted_rstat, 1);

    // merge state
    state_set(dir->get_state() & MASK_STATE_FRAGMENT_KEPT);
    dir_auth = dir->dir_auth;

    dir->finish_old_fragment(waiters, replay);
    inode->close_dirfrag(dir->get_frag());
  }

  // offset accounted_* version by -1 if any source frag was stale
  fnode.accounted_fragstat.version = fnode.fragstat.version - stale_fragstat;
  fnode.accounted_rstat.version = fnode.rstat.version - stale_rstat;

  init_fragment_pins();
}




void CDir::resync_accounted_fragstat()
{
  fnode_t *pf = get_projected_fnode();
  inode_t *pi = inode->get_projected_inode();

  if (pf->accounted_fragstat.version != pi->dirstat.version) {
    pf->fragstat.version = pi->dirstat.version;
    dout(10) << "resync_accounted_fragstat " << pf->accounted_fragstat << " -> " << pf->fragstat << dendl;
    pf->accounted_fragstat = pf->fragstat;
  }
}

/*
 * resync rstat and accounted_rstat with inode
 */
void CDir::resync_accounted_rstat()
{
  fnode_t *pf = get_projected_fnode();
  inode_t *pi = inode->get_projected_inode();
  
  if (pf->accounted_rstat.version != pi->rstat.version) {
    pf->rstat.version = pi->rstat.version;
    dout(10) << "resync_accounted_rstat " << pf->accounted_rstat << " -> " << pf->rstat << dendl;
    pf->accounted_rstat = pf->rstat;
    dirty_old_rstat.clear();
  }
}

void CDir::assimilate_dirty_rstat_inodes()
{
  dout(10) << "assimilate_dirty_rstat_inodes" << dendl;
  for (elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
       !p.end(); ++p) {
    CInode *in = *p;
    assert(in->is_auth());
    if (in->is_frozen())
      continue;

    inode_t *pi = in->project_inode();
    pi->version = in->pre_dirty();

    inode->mdcache->project_rstat_inode_to_frag(in, this, 0, 0);
  }
  state_set(STATE_ASSIMRSTAT);
  dout(10) << "assimilate_dirty_rstat_inodes done" << dendl;
}

void CDir::assimilate_dirty_rstat_inodes_finish(Mutation *mut, EMetaBlob *blob)
{
  if (!state_test(STATE_ASSIMRSTAT))
    return;
  state_clear(STATE_ASSIMRSTAT);
  dout(10) << "assimilate_dirty_rstat_inodes_finish" << dendl;
  elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
  while (!p.end()) {
    CInode *in = *p;
    ++p;

    if (in->is_frozen())
      continue;

    CDentry *dn = in->get_projected_parent_dn();

    mut->auth_pin(in);
    mut->add_projected_inode(in);

    in->clear_dirty_rstat();
    blob->add_primary_dentry(dn, in, true);
  }

  if (!dirty_rstat_inodes.empty())
    inode->mdcache->mds->locker->mark_updated_scatterlock(&inode->nestlock);
}




/****************************************
 * WAITING
 */

void CDir::add_dentry_waiter(const string& dname, snapid_t snapid, Context *c) 
{
  if (waiting_on_dentry.empty())
    get(PIN_DNWAITER);
  waiting_on_dentry[string_snap_t(dname, snapid)].push_back(c);
  dout(10) << "add_dentry_waiter dentry " << dname
	   << " snap " << snapid
	   << " " << c << " on " << *this << dendl;
}

void CDir::take_dentry_waiting(const string& dname, snapid_t first, snapid_t last,
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
  if (!waiting_on_dentry.empty()) {
    for (map<string_snap_t, list<Context*> >::iterator p = waiting_on_dentry.begin(); 
	 p != waiting_on_dentry.end();
	 ++p) 
      ls.splice(ls.end(), p->second);
    waiting_on_dentry.clear();
    put(PIN_DNWAITER);
  }
  for (map<inodeno_t, list<Context*> >::iterator p = waiting_on_ino.begin(); 
       p != waiting_on_ino.end();
       ++p) 
    ls.splice(ls.end(), p->second);
  waiting_on_ino.clear();
}



void CDir::add_waiter(uint64_t tag, Context *c) 
{
  // hierarchical?

  // at free root?
  if (tag & WAIT_ATFREEZEROOT) {
    if (!(is_freezing_tree_root() || is_frozen_tree_root() ||
	  is_freezing_dir() || is_frozen_dir())) {
      // try parent
      dout(10) << "add_waiter " << std::hex << tag << std::dec << " " << c << " should be ATFREEZEROOT, " << *this << " is not root, trying parent" << dendl;
      inode->parent->dir->add_waiter(tag, c);
      return;
    }
  }
  
  // at subtree root?
  if (tag & WAIT_ATSUBTREEROOT) {
    if (!is_subtree_root()) {
      // try parent
      dout(10) << "add_waiter " << std::hex << tag << std::dec << " " << c << " should be ATSUBTREEROOT, " << *this << " is not root, trying parent" << dendl;
      inode->parent->dir->add_waiter(tag, c);
      return;
    }
  }

  MDSCacheObject::add_waiter(tag, c);
}



/* NOTE: this checks dentry waiters too */
void CDir::take_waiting(uint64_t mask, list<Context*>& ls)
{
  if ((mask & WAIT_DENTRY) && !waiting_on_dentry.empty()) {
    // take all dentry waiters
    while (!waiting_on_dentry.empty()) {
      map<string_snap_t, list<Context*> >::iterator p = waiting_on_dentry.begin(); 
      dout(10) << "take_waiting dentry " << p->first.name
	       << " snap " << p->first.snapid << " on " << *this << dendl;
      ls.splice(ls.end(), p->second);
      waiting_on_dentry.erase(p);
    }
    put(PIN_DNWAITER);
  }
  
  // waiting
  MDSCacheObject::take_waiting(mask, ls);
}


void CDir::finish_waiting(uint64_t mask, int result) 
{
  dout(11) << "finish_waiting mask " << hex << mask << dec << " result " << result << " on " << *this << dendl;

  list<Context*> finished;
  take_waiting(mask, finished);
  if (result < 0)
    finish_contexts(g_ceph_context, finished, result);
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
  _mark_dirty(ls);
  delete projected_fnode.front();
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
    ls->dirty_dirfrags.push_back(&item_dirty);

    // if i've never committed, i need to be before _any_ mention of me is trimmed from the journal.
    if (committed_version == 0 && !item_new.is_on_list())
      ls->new_dirfrags.push_back(&item_new);
  }
}

void CDir::mark_new(LogSegment *ls)
{
  ls->new_dirfrags.push_back(&item_new);
}

void CDir::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << get_version() << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);

    item_dirty.remove_myself();
    item_new.remove_myself();
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
  mdlog->flush();
  mdlog->wait_for_safe(new C_Dir_Dirty(this, pv, mdlog->get_current_segment()));
}

void CDir::mark_complete() {
  state_set(STATE_COMPLETE);
  remove_bloom();
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
  string want_dn;
 public:
  bufferlist bl;

  C_Dir_Fetch(CDir *d, const string& w) : dir(d), want_dn(w) { }
  void finish(int result) {
    dir->_fetched(bl, want_dn);
  }
};

void CDir::fetch(Context *c, bool ignore_authpinnability)
{
  string want;
  return fetch(c, want, ignore_authpinnability);
}

void CDir::fetch(Context *c, const string& want_dn, bool ignore_authpinnability)
{
  dout(10) << "fetch on " << *this << dendl;
  
  assert(is_auth());
  assert(!is_complete());

  if (!can_auth_pin() && !ignore_authpinnability) {
    if (c) {
      dout(7) << "fetch waiting for authpinnable" << dendl;
      add_waiter(WAIT_UNFREEZE, c);
    } else
      dout(7) << "fetch not authpinnable and no context" << dendl;
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

  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_f);

  // start by reading the first hunk of it
  C_Dir_Fetch *fin = new C_Dir_Fetch(this, want_dn);
  object_t oid = get_ondisk_object();
  object_locator_t oloc(cache->mds->mdsmap->get_metadata_pool());
  ObjectOperation rd;
  rd.tmap_get(&fin->bl, NULL);
  cache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, NULL, 0, fin);
}

void CDir::_fetched(bufferlist &bl, const string& want_dn)
{
  LogClient &clog = cache->mds->clog;
  dout(10) << "_fetched " << bl.length() 
	   << " bytes for " << *this
	   << " want_dn=" << want_dn
	   << dendl;
  
  assert(is_auth());
  assert(!is_frozen());

  // empty?!?
  if (bl.length() == 0) {
    dout(0) << "_fetched missing object for " << *this << dendl;
    clog.error() << "dir " << ino() << "." << dirfrag()
	  << " object missing on disk; some files may be lost\n";

    log_mark_dirty();

    // mark complete, !fetching
    state_set(STATE_COMPLETE);
    state_clear(STATE_FETCHING);
    auth_unpin(this);
    
    // kick waiters
    finish_waiting(WAIT_COMPLETE, 0);
    return;
  }

  // decode trivialmap.
  int len = bl.length();
  bufferlist::iterator p = bl.begin();
  
  bufferlist header;
  ::decode(header, p);
  bufferlist::iterator hp = header.begin();
  fnode_t got_fnode;
  ::decode(got_fnode, hp);

  __u32 n;
  ::decode(n, p);

  dout(10) << "_fetched version " << got_fnode.version
	   << ", " << len << " bytes, " << n << " keys"
	   << dendl;
  

  // take the loaded fnode?
  // only if we are a fresh CDir* with no prior state.
  if (get_version() == 0) {
    assert(!is_projected());
    assert(!state_test(STATE_COMMITTING));
    fnode = got_fnode;
    projected_version = committing_version = committed_version = got_fnode.version;

    if (state_test(STATE_REJOINUNDEF)) {
      assert(cache->mds->is_rejoin());
      state_clear(STATE_REJOINUNDEF);
      cache->opened_undef_dirfrag(this);
    }
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
  for (unsigned i=0; i<n; i++) {
    loff_t dn_offset = p.get_off() - baseoff;

    // dname
    string dname;
    snapid_t first, last;
    dentry_key_t::decode_helper(p, dname, last);
    
    bufferlist dndata;
    ::decode(dndata, p);
    bufferlist::iterator q = dndata.begin();
    ::decode(first, q);

    // marker
    char type;
    ::decode(type, q);

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
      ::decode(ino, q);
      ::decode(d_type, q);

      if (stale)
	continue;

      if (dn) {
        if (dn->get_linkage()->get_inode() == 0) {
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
	  dn->link_remote(dn->get_linkage(), in);
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
      ::decode(inode, q);
      if (inode.is_symlink())
        ::decode(symlink, q);
      ::decode(fragtree, q);
      ::decode(xattrs, q);
      ::decode(snapbl, q);
      ::decode(old_inodes, q);
      
      if (stale)
	continue;

      bool undef_inode = false;
      if (dn) {
	CInode *in = dn->get_linkage()->get_inode();
	if (in) {
	  dout(12) << "_fetched  had dentry " << *dn << dendl;
	  if (in->state_test(CInode::STATE_REJOINUNDEF)) {
	    assert(cache->mds->is_rejoin());
	    assert(in->vino() == vinodeno_t(inode.ino, last));
	    in->state_clear(CInode::STATE_REJOINUNDEF);
	    cache->opened_undef_inode(in);
	    undef_inode = true;
	  }
	} else
	  dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
      }

      if (!dn || undef_inode) {
	// add inode
	CInode *in = cache->get_inode(inode.ino, last);
	if (!in || undef_inode) {
	  if (undef_inode && in)
	    in->first = first;
	  else
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

	  if (undef_inode) {
	    if (inode.anchored)
	      dn->adjust_nested_anchors(1);
	  } else {
	    cache->add_inode( in ); // add
	    dn = add_primary_dentry(dname, in, first, last); // link
	  }
	  dout(12) << "_fetched  got " << *dn << " " << *in << dendl;

	  if (in->inode.is_dirty_rstat())
	    in->mark_dirty_rstat();

	  //in->hack_accessed = false;
	  //in->hack_load_stamp = ceph_clock_now(g_ceph_context);
	  //num_new_inodes_loaded++;
	} else {
	  dout(0) << "_fetched  badness: got (but i already had) " << *in
		  << " mode " << in->inode.mode
		  << " mtime " << in->inode.mtime << dendl;
	  string dirpath, inopath;
	  this->inode->make_path_string(dirpath);
	  in->make_path_string(inopath);
	  clog.error() << "loaded dup inode " << inode.ino
	    << " [" << first << "," << last << "] v" << inode.version
	    << " at " << dirpath << "/" << dname
	    << ", but inode " << in->vino() << " v" << in->inode.version
	    << " already exists at " << inopath << "\n";
	  continue;
	}
      }
    } else {
      dout(1) << "corrupt directory, i got tag char '" << type << "' val " << (int)(type)
	      << " at offset " << p.get_off() << dendl;
      assert(0);
    }
    
    if (dn && want_dn.length() && want_dn == dname) {
      dout(10) << " touching wanted dn " << *dn << dendl;
      inode->mdcache->touch_dentry(dn);
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
	dn->get_version() <= got_fnode.version &&
	dn->is_dirty()) {
      dout(10) << "_fetched  had underwater dentry " << *dn << ", marking clean" << dendl;
      dn->mark_clean();

      if (dn->get_linkage()->is_primary()) {
	assert(dn->get_linkage()->get_inode()->get_version() <= got_fnode.version);
	dout(10) << "_fetched  had underwater inode " << *dn->get_linkage()->get_inode() << ", marking clean" << dendl;
	dn->get_linkage()->get_inode()->mark_clean();
      }
    }
  }
  if (!p.end()) {
    clog.warn() << "dir " << dirfrag() << " has "
	<< bl.length() - p.get_off() << " extra bytes\n";
  }

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
void CDir::commit(version_t want, Context *c, bool ignore_authpinnability)
{
  dout(10) << "commit want " << want << " on " << *this << dendl;
  if (want == 0) want = get_version();

  // preconditions
  assert(want <= get_version() || get_version() == 0);    // can't commit the future
  assert(want > committed_version); // the caller is stupid
  assert(is_auth());
  assert(ignore_authpinnability || can_auth_pin());

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

/**
 * Try and write out the full directory to disk.
 *
 * If the bufferlist we're using exceeds max_write_size, bail out
 * and switch to _commit_partial -- it can safely break itself into
 * multiple non-atomic writes.
 */
CDir::map_t::iterator CDir::_commit_full(ObjectOperation& m, const set<snapid_t> *snaps,
                               unsigned max_write_size)
{
  dout(10) << "_commit_full" << dendl;

  // encode
  bufferlist bl;
  __u32 n = 0;

  bufferlist header;
  ::encode(fnode, header);
  max_write_size -= header.length();

  map_t::iterator p = items.begin();
  while (p != items.end() && bl.length() < max_write_size) {
    CDentry *dn = p->second;
    ++p;
    
    if (dn->linkage.is_null()) 
      continue;  // skip negative entries

    if (snaps && dn->last != CEPH_NOSNAP &&
	try_trim_snap_dentry(dn, *snaps))
      continue;
    
    n++;

    _encode_dentry(dn, bl, snaps);
  }

  if (p != items.end()) {
    assert(bl.length() > max_write_size);
    return _commit_partial(m, snaps, max_write_size);
  }

  // encode final trivialmap
  bufferlist finalbl;
  ::encode(header, finalbl);
  assert(num_head_items + num_head_null + num_snap_items + num_snap_null == items.size());
  assert(n == (num_head_items + num_snap_items));
  ::encode(n, finalbl);
  finalbl.claim_append(bl);

  // write out the full blob
  m.tmap_put(finalbl);
  return p;
}

/**
 * Flush out the modified dentries in this dir. Keep the bufferlist
 * below max_write_size; if we exceed that size then return the last
 * dentry that got committed into the bufferlist. (Note that the
 * bufferlist might be larger than requested by the size of that
 * last dentry as encoded.)
 *
 * If we're passed a last_committed_dn, skip to the next dentry after that.
 * Also, don't encode the header again -- we don't want to update it
 * on-disk until all the updates have made it through, so keep the header
 * in only the first changeset -- our caller is responsible for making sure
 * that changeset doesn't go through until after all the others do, if it's
 * necessary.
 */
CDir::map_t::iterator CDir::_commit_partial(ObjectOperation& m,
                                  const set<snapid_t> *snaps,
                                  unsigned max_write_size,
                                  map_t::iterator last_committed_dn)
{
  dout(10) << "_commit_partial" << dendl;
  bufferlist finalbl;

  // header
  if (last_committed_dn == map_t::iterator()) {
    bufferlist header;
    ::encode(fnode, header);
    finalbl.append(CEPH_OSD_TMAP_HDR);
    ::encode(header, finalbl);
  }

  // updated dentries
  map_t::iterator p = items.begin();
  if(last_committed_dn != map_t::iterator())
    p = last_committed_dn;

  while (p != items.end() && finalbl.length() < max_write_size) {
    CDentry *dn = p->second;
    ++p;
    
    if (snaps && dn->last != CEPH_NOSNAP &&
	try_trim_snap_dentry(dn, *snaps))
      continue;

    if (!dn->is_dirty())
      continue;  // skip clean dentries

    if (dn->get_linkage()->is_null()) {
      dout(10) << " rm " << dn->name << " " << *dn << dendl;
      finalbl.append(CEPH_OSD_TMAP_RMSLOPPY);
      dn->key().encode(finalbl);
    } else {
      dout(10) << " set " << dn->name << " " << *dn << dendl;
      finalbl.append(CEPH_OSD_TMAP_SET);
      _encode_dentry(dn, finalbl, snaps);
    }
  }

  // update the trivialmap at the osd
  m.tmap_update(finalbl);
  return p;
}

void CDir::_encode_dentry(CDentry *dn, bufferlist& bl,
			  const set<snapid_t> *snaps)
{
  // clear dentry NEW flag, if any.  we can no longer silently drop it.
  dn->clear_new();

  dn->key().encode(bl);

  ceph_le32 plen = init_le32(0);
  unsigned plen_off = bl.length();
  ::encode(plen, bl);

  ::encode(dn->first, bl);

  // primary or remote?
  if (dn->linkage.is_remote()) {
    inodeno_t ino = dn->linkage.get_remote_ino();
    unsigned char d_type = dn->linkage.get_remote_d_type();
    dout(14) << " pos " << bl.length() << " dn '" << dn->name << "' remote ino " << ino << dendl;
    
    // marker, name, ino
    bl.append('L');         // remote link
    ::encode(ino, bl);
    ::encode(d_type, bl);
  } else {
    // primary link
    CInode *in = dn->linkage.get_inode();
    assert(in);
    
    dout(14) << " pos " << bl.length() << " dn '" << dn->name << "' inode " << *in << dendl;
    
    // marker, name, inode, [symlink string]
    bl.append('I');         // inode
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
  
  plen = bl.length() - plen_off - sizeof(__u32);

  ceph_le32 eplen;
  eplen = plen;
  bl.copy_in(plen_off, sizeof(eplen), (char*)&eplen);
}


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

  // alrady committed an older version?
  if (committing_version > committed_version) {
    dout(10) << "already committing older " << committing_version << ", waiting for that to finish" << dendl;
    return;
  }
  
  // complete first?  (only if we're not using TMAPUP osd op)
  if (!g_conf->mds_use_tmap && !is_complete()) {
    dout(7) << "commit not complete, fetching first" << dendl;
    if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_ffc);
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
  
  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_c);

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
  }

  ObjectOperation m;
  map_t::iterator committed_dn;
  unsigned max_write_size = cache->max_dir_commit_size;

  if (is_complete() &&
      (num_dirty > (num_head_items*g_conf->mds_dir_commit_ratio))) {
    fnode.snap_purged_thru = realm->get_last_destroyed();
    committed_dn = _commit_full(m, snaps, max_write_size);
  } else {
    committed_dn = _commit_partial(m, snaps, max_write_size);
  }

  SnapContext snapc;
  object_t oid = get_ondisk_object();
  object_locator_t oloc(cache->mds->mdsmap->get_metadata_pool());

  m.priority = CEPH_MSG_PRIO_LOW;  // set priority lower than journal!

  if (committed_dn == items.end())
    cache->mds->objecter->mutate(oid, oloc, m, snapc, ceph_clock_now(g_ceph_context), 0, NULL,
                                 new C_Dir_Committed(this, get_version()));
  else { // send in a different Context
    C_GatherBuilder gather(g_ceph_context, new C_Dir_Committed(this, get_version()));
    while (committed_dn != items.end()) {
      ObjectOperation n = ObjectOperation();
      committed_dn = _commit_partial(n, snaps, max_write_size, committed_dn);
      cache->mds->objecter->mutate(oid, oloc, n, snapc, ceph_clock_now(g_ceph_context), 0, NULL,
                                  gather.new_sub());
    }
    /*
     * save the original object for last -- it contains the new header,
     * which will be committed on-disk. If we were to send it off before
     * the other commits, but die before sending them all, we'd think
     * that the on-disk state was fully committed even though it wasn't!
     * However, since the messages are strictly ordered between the MDS and
     * the OSD, and since messages to a given PG are strictly ordered, if
     * we simply send the message containing the header off last, we cannot
     * get our header into an incorrect state.
     */
    cache->mds->objecter->mutate(oid, oloc, m, snapc, ceph_clock_now(g_ceph_context), 0, NULL,
                                gather.new_sub());
    gather.activate();
  }
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

  bool stray = inode->is_stray();

  // take note.
  assert(v > committed_version);
  assert(v <= committing_version);
  committed_version = v;

  // _all_ commits done?
  if (committing_version == committed_version) 
    state_clear(CDir::STATE_COMMITTING);

  // _any_ commit, even if we've been redirtied, means we're no longer new.
  item_new.remove_myself();
  
  // dir clean?
  if (committed_version == get_version()) 
    mark_clean();

  // dentries clean?
  for (map_t::iterator it = items.begin();
       it != items.end(); ) {
    CDentry *dn = it->second;
    ++it;
    
    // inode?
    if (dn->linkage.is_primary()) {
      CInode *in = dn->linkage.get_inode();
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

    // dentry
    if (committed_version >= dn->get_version()) {
      if (dn->is_dirty()) {
	dout(15) << " dir " << committed_version << " >= dn " << dn->get_version() << " now clean " << *dn << dendl;
	dn->mark_clean();

	// drop clean null stray dentries immediately
	if (stray && 
	    dn->get_num_ref() == 0 &&
	    !dn->is_projected() &&
	    dn->get_linkage()->is_null())
	  remove_dentry(dn);
      } 
    } else {
      dout(15) << " dir " << committed_version << " < dn " << dn->get_version() << " still dirty " << *dn << dendl;
    }
  }

  // finishers?
  bool were_waiters = !waiting_for_commit.empty();
  
  map<version_t, list<Context*> >::iterator p = waiting_for_commit.begin();
  while (p != waiting_for_commit.end()) {
    map<version_t, list<Context*> >::iterator n = p;
    ++n;
    if (p->first > committed_version) {
      dout(10) << " there are waiters for " << p->first << ", committing again" << dendl;
      _commit(p->first);
      break;
    }
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
  state &= MASK_STATE_EXPORT_KEPT;
  pop_auth_subtree_nested.sub(now, cache->decayrate, pop_auth_subtree);
  pop_me.zero(now);
  pop_auth_subtree.zero(now);
  put(PIN_TEMPEXPORTING);
  dirty_old_rstat.clear();
}

void CDir::decode_import(bufferlist::iterator& blp, utime_t now, LogSegment *ls)
{
  ::decode(first, blp);
  ::decode(fnode, blp);
  ::decode(dirty_old_rstat, blp);
  projected_version = fnode.version;
  ::decode(committed_version, blp);
  committing_version = committed_version;

  unsigned s;
  ::decode(s, blp);
  state &= MASK_STATE_IMPORT_KEPT;
  state |= (s & MASK_STATE_EXPORTED);
  if (is_dirty()) {
    get(PIN_DIRTY);
    _mark_dirty(ls);
  }

  ::decode(dir_rep, blp);

  ::decode(pop_me, now, blp);
  ::decode(pop_auth_subtree, now, blp);
  pop_auth_subtree_nested.add(now, cache->decayrate, pop_auth_subtree);

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
    if (x == this)
      return true;
    x = x->get_inode()->get_projected_parent_dir();
    if (x == 0)
      return false;    
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
    if (get_cum_auth_pins())
      inode->adjust_nested_auth_pins(-1, NULL);
    
    // unpin parent of frozen dir/tree?
    if (inode->is_auth() && (is_frozen_tree_root() || is_frozen_dir()))
      inode->auth_unpin(this);
  } 
  if (was_subtree && !is_subtree_root()) {
    dout(10) << " old subtree root, adjusting auth_pins" << dendl;
    
    // adjust nested auth pins
    if (get_cum_auth_pins())
      inode->adjust_nested_auth_pins(1, NULL);

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
  if (!is_subtree_root() &&
      get_cum_auth_pins() == 1)
    inode->adjust_nested_auth_pins(1, by);
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
  
  int newcum = get_cum_auth_pins();

  maybe_finish_freeze();  // pending freeze?
  
  // nest?
  if (!is_subtree_root() &&
      newcum == 0)
    inode->adjust_nested_auth_pins(-1, by);
}

void CDir::adjust_nested_auth_pins(int inc, int dirinc, void *by)
{
  assert(inc);
  nested_auth_pins += inc;
  dir_auth_pins += dirinc;
  
  dout(15) << "adjust_nested_auth_pins " << inc << "/" << dirinc << " on " << *this
	   << " by " << by << " count now "
	   << auth_pins << " + " << nested_auth_pins << dendl;
  assert(nested_auth_pins >= 0);
  assert(dir_auth_pins >= 0);

  int newcum = get_cum_auth_pins();

  maybe_finish_freeze();  // pending freeze?
  
  // nest?
  if (!is_subtree_root()) {
    if (newcum == 0)
      inode->adjust_nested_auth_pins(-1, by);
    else if (newcum == inc)      
      inode->adjust_nested_auth_pins(1, by);
  }
}

void CDir::adjust_nested_anchors(int by)
{
  assert(by);
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
       ++it) {
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
      if (dn->get_remote_d_type() == DT_DIR)
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

struct C_Dir_AuthUnpin : public Context {
  CDir *dir;
  C_Dir_AuthUnpin(CDir *d) : dir(d) {}
  void finish(int r) {
    dir->auth_unpin(dir->get_inode());
  }
};

void CDir::maybe_finish_freeze()
{
  if (auth_pins != 1 || dir_auth_pins != 0)
    return;

  // we can freeze the _dir_ even with nested pins...
  if (state_test(STATE_FREEZINGDIR)) {
    _freeze_dir();
    auth_unpin(this);
    finish_waiting(WAIT_FROZEN);
  }

  if (nested_auth_pins != 0)
    return;

  if (state_test(STATE_FREEZINGTREE)) {
    if (!is_subtree_root() && inode->is_frozen()) {
      dout(10) << "maybe_finish_freeze !subtree root and frozen inode, waiting for unfreeze on " << inode << dendl;
      // retake an auth_pin...
      auth_pin(inode);
      // and release it when the parent inode unfreezes
      inode->add_waiter(WAIT_UNFREEZE, new C_Dir_AuthUnpin(this));
      return;
    }

    _freeze_tree();
    auth_unpin(this);
    finish_waiting(WAIT_FROZEN);
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
  //assert(is_freezeable_dir(true));
  // not always true during split because the original fragment may have frozen a while
  // ago and we're just now getting around to breaking it up.

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








