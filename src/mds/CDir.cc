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
#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"
#include "MDLog.h"
#include "LogSegment.h"

#include "common/bloom_filter.hpp"
#include "include/Context.h"
#include "common/Clock.h"

#include "osdc/Objecter.h"

#include "common/config.h"
#include "include/assert.h"
#include "include/compat.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << cache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") "

int CDir::num_frozen_trees = 0;
int CDir::num_freezing_trees = 0;

class CDirContext : public MDSInternalContextBase
{
protected:
  CDir *dir;
  MDSRank* get_mds() {return dir->cache->mds;}

public:
  explicit CDirContext(CDir *d) : dir(d) {
    assert(dir != NULL);
  }
};


class CDirIOContext : public MDSIOContextBase
{
protected:
  CDir *dir;
  MDSRank* get_mds() {return dir->cache->mds;}

public:
  explicit CDirIOContext(CDir *d) : dir(d) {
    assert(dir != NULL);
  }
};


// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };

boost::pool<> CDir::pool(sizeof(CDir));


ostream& operator<<(ostream& out, const CDir& dir)
{
  out << "[dir " << dir.dirfrag() << " " << dir.get_path() << "/"
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
    mds_authority_t a = dir.authority();
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

  out << " state=" << dir.get_state();
  if (dir.state_test(CDir::STATE_COMPLETE)) out << "|complete";
  if (dir.state_test(CDir::STATE_FREEZINGTREE)) out << "|freezingtree";
  if (dir.state_test(CDir::STATE_FROZENTREE)) out << "|frozentree";
  //if (dir.state_test(CDir::STATE_FROZENTREELEAF)) out << "|frozentreeleaf";
  if (dir.state_test(CDir::STATE_FROZENDIR)) out << "|frozendir";
  if (dir.state_test(CDir::STATE_FREEZINGDIR)) out << "|freezingdir";
  if (dir.state_test(CDir::STATE_EXPORTBOUND)) out << "|exportbound";
  if (dir.state_test(CDir::STATE_IMPORTBOUND)) out << "|importbound";
  if (dir.state_test(CDir::STATE_BADFRAG)) out << "|badfrag";

  // fragstat
  out << " " << dir.fnode.fragstat;
  if (!(dir.fnode.fragstat == dir.fnode.accounted_fragstat))
    out << "/" << dir.fnode.accounted_fragstat;
  if (g_conf->mds_debug_scatterstat && dir.is_projected()) {
    const fnode_t *pf = dir.get_projected_fnode();
    out << "->" << pf->fragstat;
    if (!(pf->fragstat == pf->accounted_fragstat))
      out << "/" << pf->accounted_fragstat;
  }
  
  // rstat
  out << " " << dir.fnode.rstat;
  if (!(dir.fnode.rstat == dir.fnode.accounted_rstat))
    out << "/" << dir.fnode.accounted_rstat;
  if (g_conf->mds_debug_scatterstat && dir.is_projected()) {
    const fnode_t *pf = dir.get_projected_fnode();
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
  cache(mdcache), inode(in), frag(fg),
  first(2),
  dirty_rstat_inodes(member_offset(CInode, dirty_rstat_item)),
  projected_version(0),  item_dirty(this), item_new(this),
  scrub_infop(NULL),
  num_head_items(0), num_head_null(0),
  num_snap_items(0), num_snap_null(0),
  num_dirty(0), committing_version(0), committed_version(0),
  dir_auth_pins(0), request_pins(0),
  dir_rep(REP_NONE),
  pop_me(ceph_clock_now(g_ceph_context)),
  pop_nested(ceph_clock_now(g_ceph_context)),
  pop_auth_subtree(ceph_clock_now(g_ceph_context)),
  pop_auth_subtree_nested(ceph_clock_now(g_ceph_context)),
  num_dentries_nested(0), num_dentries_auth_subtree(0),
  num_dentries_auth_subtree_nested(0),
  bloom(NULL),
  dir_auth(CDIR_AUTH_DEFAULT)
{
  g_num_dir++;
  g_num_dira++;

  state = STATE_INITIAL;

  memset(&fnode, 0, sizeof(fnode));

  // auth
  assert(in->is_dir());
  if (auth) 
    state |= STATE_AUTH;
}

/**
 * Check the recursive statistics on size for consistency.
 * If mds_debug_scatterstat is enabled, assert for correctness,
 * otherwise just print out the mismatch and continue.
 */
bool CDir::check_rstats(bool scrub)
{
  if (!g_conf->mds_debug_scatterstat && !scrub)
    return true;

  dout(25) << "check_rstats on " << this << dendl;
  if (!is_complete() || !is_auth() || is_frozen()) {
    assert(!scrub);
    dout(10) << "check_rstats bailing out -- incomplete or non-auth or frozen dir!" << dendl;
    return true;
  }

  frag_info_t frag_info;
  nest_info_t nest_info;
  for (map_t::iterator i = items.begin(); i != items.end(); ++i) {
    if (i->second->last != CEPH_NOSNAP)
      continue;
    CDentry::linkage_t *dnl = i->second->get_linkage();
    if (dnl->is_primary()) {
      CInode *in = dnl->get_inode();
      nest_info.add(in->inode.accounted_rstat);
      if (in->is_dir())
	frag_info.nsubdirs++;
      else
	frag_info.nfiles++;
    } else if (dnl->is_remote())
      frag_info.nfiles++;
  }

  bool good = true;
  // fragstat
  if(!frag_info.same_sums(fnode.fragstat)) {
    dout(1) << "mismatch between head items and fnode.fragstat! printing dentries" << dendl;
    dout(1) << "get_num_head_items() = " << get_num_head_items()
             << "; fnode.fragstat.nfiles=" << fnode.fragstat.nfiles
             << " fnode.fragstat.nsubdirs=" << fnode.fragstat.nsubdirs << dendl;
    good = false;
  } else {
    dout(20) << "get_num_head_items() = " << get_num_head_items()
             << "; fnode.fragstat.nfiles=" << fnode.fragstat.nfiles
             << " fnode.fragstat.nsubdirs=" << fnode.fragstat.nsubdirs << dendl;
  }

  // rstat
  if (!nest_info.same_sums(fnode.rstat)) {
    dout(1) << "mismatch between child accounted_rstats and my rstats!" << dendl;
    dout(1) << "total of child dentrys: " << nest_info << dendl;
    dout(1) << "my rstats:              " << fnode.rstat << dendl;
    good = false;
  } else {
    dout(20) << "total of child dentrys: " << nest_info << dendl;
    dout(20) << "my rstats:              " << fnode.rstat << dendl;
  }

  if (!good) {
    if (!scrub) {
      for (map_t::iterator i = items.begin(); i != items.end(); ++i) {
	CDentry *dn = i->second;
	if (dn->get_linkage()->is_primary()) {
	  CInode *in = dn->get_linkage()->inode;
	  dout(1) << *dn << " rstat " << in->inode.accounted_rstat << dendl;
	} else {
	  dout(1) << *dn << dendl;
	}
      }

      assert(frag_info.nfiles == fnode.fragstat.nfiles);
      assert(frag_info.nsubdirs == fnode.fragstat.nsubdirs);
      assert(nest_info.rbytes == fnode.rstat.rbytes);
      assert(nest_info.rfiles == fnode.rstat.rfiles);
      assert(nest_info.rsubdirs == fnode.rstat.rsubdirs);
    }
  }
  dout(10) << "check_rstats complete on " << this << dendl;
  return good;
}

CDentry *CDir::lookup(const string& name, snapid_t snap)
{ 
  dout(20) << "lookup (" << snap << ", '" << name << "')" << dendl;
  map_t::iterator iter = items.lower_bound(dentry_key_t(snap, name.c_str(),
							inode->hash_dentry_name(name)));
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

CDentry *CDir::lookup_exact_snap(const string& name, snapid_t last) {
  map_t::iterator p = items.find(dentry_key_t(last, name.c_str(),
					      inode->hash_dentry_name(name)));
  if (p == items.end())
    return NULL;
  return p->second;
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
    
    // detach inode
    in->remove_primary_parent(dn);
    dn->get_linkage()->inode = 0;
  }
}

void CDir::add_to_bloom(CDentry *dn)
{
  assert(dn->last == CEPH_NOSNAP);
  if (!bloom) {
    /* not create bloom filter for incomplete dir that was added by log replay */
    if (!is_complete())
      return;
    unsigned size = get_num_head_items() + get_num_snap_items();
    if (size < 100) size = 100;
    bloom = new bloom_filter(size, 1.0 / size, 0);
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

/** remove dirty null dentries for deleted directory. the dirfrag will be
 *  deleted soon, so it's safe to not commit dirty dentries.
 *
 *  This is called when a directory is being deleted, a prerequisite
 *  of which is that its children have been unlinked: we expect to only see
 *  null, unprojected dentries here.
 */
void CDir::try_remove_dentries_for_stray()
{
  dout(10) << __func__ << dendl;
  assert(inode->inode.nlink == 0);

  // clear dirty only when the directory was not snapshotted
  bool clear_dirty = !inode->snaprealm;

  CDir::map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    ++p;
    if (dn->last == CEPH_NOSNAP) {
      assert(!dn->is_projected());
      assert(dn->get_linkage()->is_null());
      if (clear_dirty && dn->is_dirty())
	dn->mark_clean();
      // It's OK to remove lease prematurely because we will never link
      // the dentry to inode again.
      if (dn->is_any_leases())
	dn->remove_client_leases(cache->mds->locker);
      if (dn->get_num_ref() == 0)
	remove_dentry(dn);
    } else {
      assert(!dn->is_projected());
      CDentry::linkage_t *dnl= dn->get_linkage();
      CInode *in = NULL;
      if (dnl->is_primary()) {
	in = dnl->get_inode();
	if (clear_dirty && in->is_dirty())
	  in->mark_clean();
      }
      if (clear_dirty && dn->is_dirty())
	dn->mark_clean();
      if (dn->get_num_ref() == 0) {
	remove_dentry(dn);
	if (in)
	  cache->remove_inode(in);
      }
    }
  }

  if (clear_dirty && is_dirty())
    mark_clean();
}

void CDir::touch_dentries_bottom() {
  dout(12) << "touch_dentries_bottom " << *this << dendl;

  for (CDir::map_t::iterator p = items.begin();
       p != items.end();
       ++p)
    inode->mdcache->touch_dentry_bottom(p->second);
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
  } else if (dn->last == CEPH_NOSNAP) {
      num_head_items++;

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
      fnode.rstat.rsnaprealms += pi->accounted_rstat.rsnaprealms;
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
  } else {
    num_snap_items++;
    if (dn->get_linkage()->is_primary()) {
      CInode *in = dn->get_linkage()->get_inode();
      if (in->is_dirty_rstat())
	dirty_rstat_inodes.push_back(&in->dirty_rstat_item);
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
  if (!replay && is_auth()) {
    _freeze_dir();
    mark_complete();
  }
}

void CDir::finish_old_fragment(list<MDSInternalContextBase*>& waiters, bool replay)
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

void CDir::split(int bits, list<CDir*>& subs, list<MDSInternalContextBase*>& waiters, bool replay)
{
  dout(10) << "split by " << bits << " bits on " << *this << dendl;

  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_split);

  assert(replay || is_complete() || !is_auth());

  list<frag_t> frags;
  frag.split(bits, frags);

  vector<CDir*> subfrags(1 << bits);
  
  double fac = 1.0 / (double)(1 << bits);  // for scaling load vecs

  version_t rstat_version = inode->get_projected_inode()->rstat.version;
  version_t dirstat_version = inode->get_projected_inode()->dirstat.version;

  nest_info_t rstatdiff;
  frag_info_t fragstatdiff;
  if (fnode.accounted_rstat.version == rstat_version)
    rstatdiff.add_delta(fnode.accounted_rstat, fnode.rstat);
  if (fnode.accounted_fragstat.version == dirstat_version) {
    bool touched_mtime;
    fragstatdiff.add_delta(fnode.accounted_fragstat, fnode.fragstat, touched_mtime);
  }
  dout(10) << " rstatdiff " << rstatdiff << " fragstatdiff " << fragstatdiff << dendl;

  prepare_old_fragment(replay);

  // create subfrag dirs
  int n = 0;
  for (list<frag_t>::iterator p = frags.begin(); p != frags.end(); ++p) {
    CDir *f = new CDir(inode, *p, cache, is_auth());
    f->state_set(state & (MASK_STATE_FRAGMENT_KEPT | STATE_COMPLETE));
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

    f->set_dir_auth(get_dir_auth());
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

  // FIXME: handle dirty old rstat

  // fix up new frag fragstats
  for (int i=0; i<n; i++) {
    CDir *f = subfrags[i];
    f->fnode.rstat.version = rstat_version;
    f->fnode.accounted_rstat = f->fnode.rstat;
    f->fnode.fragstat.version = dirstat_version;
    f->fnode.accounted_fragstat = f->fnode.fragstat;
    dout(10) << " rstat " << f->fnode.rstat << " fragstat " << f->fnode.fragstat
	     << " on " << *f << dendl;
  }

  // give any outstanding frag stat differential to first frag
  dout(10) << " giving rstatdiff " << rstatdiff << " fragstatdiff" << fragstatdiff
           << " to " << *subfrags[0] << dendl;
  subfrags[0]->fnode.accounted_rstat.add(rstatdiff);
  subfrags[0]->fnode.accounted_fragstat.add(fragstatdiff);

  finish_old_fragment(waiters, replay);
}

void CDir::merge(list<CDir*>& subs, list<MDSInternalContextBase*>& waiters, bool replay)
{
  dout(10) << "merge " << subs << dendl;

  set_dir_auth(subs.front()->get_dir_auth());
  prepare_new_fragment(replay);

  nest_info_t rstatdiff;
  frag_info_t fragstatdiff;
  bool touched_mtime;
  version_t rstat_version = inode->get_projected_inode()->rstat.version;
  version_t dirstat_version = inode->get_projected_inode()->dirstat.version;

  for (list<CDir*>::iterator p = subs.begin(); p != subs.end(); ++p) {
    CDir *dir = *p;
    dout(10) << " subfrag " << dir->get_frag() << " " << *dir << dendl;
    assert(!dir->is_auth() || dir->is_complete() || replay);

    if (dir->fnode.accounted_rstat.version == rstat_version)
      rstatdiff.add_delta(dir->fnode.accounted_rstat, dir->fnode.rstat);
    if (dir->fnode.accounted_fragstat.version == dirstat_version)
      fragstatdiff.add_delta(dir->fnode.accounted_fragstat, dir->fnode.fragstat,
			     touched_mtime);

    dir->prepare_old_fragment(replay);

    // steal dentries
    while (!dir->items.empty()) 
      steal_dentry(dir->items.begin()->second);
    
    // merge replica map
    for (compact_map<mds_rank_t,unsigned>::iterator p = dir->replicas_begin();
	 p != dir->replicas_end();
	 ++p) {
      unsigned cur = replica_map[p->first];
      if (p->second > cur)
	replica_map[p->first] = p->second;
    }

    // merge version
    if (dir->get_version() > get_version())
      set_version(dir->get_version());

    // merge state
    state_set(dir->get_state() & MASK_STATE_FRAGMENT_KEPT);
    dir_auth = dir->dir_auth;

    dir->finish_old_fragment(waiters, replay);
    inode->close_dirfrag(dir->get_frag());
  }

  if (is_auth() && !replay)
    mark_complete();

  // FIXME: merge dirty old rstat
  fnode.rstat.version = rstat_version;
  fnode.accounted_rstat = fnode.rstat;
  fnode.accounted_rstat.add(rstatdiff);

  fnode.fragstat.version = dirstat_version;
  fnode.accounted_fragstat = fnode.fragstat;
  fnode.accounted_fragstat.add(fragstatdiff);

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

    inode->mdcache->project_rstat_inode_to_frag(in, this, 0, 0, NULL);
  }
  state_set(STATE_ASSIMRSTAT);
  dout(10) << "assimilate_dirty_rstat_inodes done" << dendl;
}

void CDir::assimilate_dirty_rstat_inodes_finish(MutationRef& mut, EMetaBlob *blob)
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

void CDir::add_dentry_waiter(const string& dname, snapid_t snapid, MDSInternalContextBase *c) 
{
  if (waiting_on_dentry.empty())
    get(PIN_DNWAITER);
  waiting_on_dentry[string_snap_t(dname, snapid)].push_back(c);
  dout(10) << "add_dentry_waiter dentry " << dname
	   << " snap " << snapid
	   << " " << c << " on " << *this << dendl;
}

void CDir::take_dentry_waiting(const string& dname, snapid_t first, snapid_t last,
			       list<MDSInternalContextBase*>& ls)
{
  if (waiting_on_dentry.empty())
    return;
  
  string_snap_t lb(dname, first);
  string_snap_t ub(dname, last);
  compact_map<string_snap_t, list<MDSInternalContextBase*> >::iterator p = waiting_on_dentry.lower_bound(lb);
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

void CDir::take_sub_waiting(list<MDSInternalContextBase*>& ls)
{
  dout(10) << "take_sub_waiting" << dendl;
  if (!waiting_on_dentry.empty()) {
    for (compact_map<string_snap_t, list<MDSInternalContextBase*> >::iterator p = waiting_on_dentry.begin();
	 p != waiting_on_dentry.end();
	 ++p) 
      ls.splice(ls.end(), p->second);
    waiting_on_dentry.clear();
    put(PIN_DNWAITER);
  }
}



void CDir::add_waiter(uint64_t tag, MDSInternalContextBase *c) 
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
void CDir::take_waiting(uint64_t mask, list<MDSInternalContextBase*>& ls)
{
  if ((mask & WAIT_DENTRY) && !waiting_on_dentry.empty()) {
    // take all dentry waiters
    while (!waiting_on_dentry.empty()) {
      compact_map<string_snap_t, list<MDSInternalContextBase*> >::iterator p = waiting_on_dentry.begin();
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

  list<MDSInternalContextBase*> finished;
  take_waiting(mask, finished);
  if (result < 0)
    finish_contexts(g_ceph_context, finished, result);
  else
    cache->mds->queue_waiters(finished);
}



// dirty/clean

fnode_t *CDir::project_fnode()
{
  assert(get_version() != 0);
  fnode_t *p = new fnode_t;
  *p = *get_projected_fnode();
  projected_fnode.push_back(p);

  if (scrub_infop && scrub_infop->last_scrub_dirty) {
    p->localized_scrub_stamp = scrub_infop->last_local.time;
    p->localized_scrub_version = scrub_infop->last_local.version;
    p->recursive_scrub_stamp = scrub_infop->last_recursive.time;
    p->recursive_scrub_version = scrub_infop->last_recursive.version;
    scrub_infop->last_scrub_dirty = false;
    scrub_maybe_delete_info();
  }

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
    item_dirty.remove_myself();
    item_new.remove_myself();

    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
  }
}


struct C_Dir_Dirty : public CDirContext {
  version_t pv;
  LogSegment *ls;
  C_Dir_Dirty(CDir *d, version_t p, LogSegment *l) : CDirContext(d), pv(p), ls(l) {}
  void finish(int r) {
    dir->mark_dirty(pv, ls);
    dir->auth_unpin(dir);
  }
};

// caller should hold auth pin of this
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
void CDir::fetch(MDSInternalContextBase *c, bool ignore_authpinnability)
{
  string want;
  return fetch(c, want, ignore_authpinnability);
}

void CDir::fetch(MDSInternalContextBase *c, const string& want_dn, bool ignore_authpinnability)
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

  // unlinked directory inode shouldn't have any entry
  if (inode->inode.nlink == 0 && !inode->snaprealm) {
    dout(7) << "fetch dirfrag for unlinked directory, mark complete" << dendl;
    if (get_version() == 0) {
      set_version(1);

      if (state_test(STATE_REJOINUNDEF)) {
	assert(cache->mds->is_rejoin());
	state_clear(STATE_REJOINUNDEF);
	cache->opened_undef_dirfrag(this);
      }
    }
    mark_complete();

    if (c)
      cache->mds->queue_waiter(c);
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

  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_fetch);

  _omap_fetch(want_dn);
}

class C_IO_Dir_TMAP_Fetched : public CDirIOContext {
 protected:
  string want_dn;
 public:
  bufferlist bl;

  C_IO_Dir_TMAP_Fetched(CDir *d, const string& w) : CDirIOContext(d), want_dn(w) { }
  void finish(int r) {
    dir->_tmap_fetched(bl, want_dn, r);
  }
};

void CDir::_tmap_fetch(const string& want_dn)
{
  // start by reading the first hunk of it
  C_IO_Dir_TMAP_Fetched *fin = new C_IO_Dir_TMAP_Fetched(this, want_dn);
  object_t oid = get_ondisk_object();
  object_locator_t oloc(cache->mds->mdsmap->get_metadata_pool());
  ObjectOperation rd;
  rd.tmap_get(&fin->bl, NULL);
  cache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, NULL, 0,
			     new C_OnFinisher(fin, cache->mds->finisher));
}

void CDir::_tmap_fetched(bufferlist& bl, const string& want_dn, int r)
{
  LogChannelRef clog = cache->mds->clog;
  dout(10) << "_tmap_fetched " << bl.length()  << " bytes for " << *this
	   << " want_dn=" << want_dn << dendl;

  assert(r == 0 || r == -ENOENT);
  assert(is_auth());
  assert(!is_frozen());

  bufferlist header;
  map<string, bufferlist> omap;

  if (bl.length() == 0) {
    r = -ENODATA;
  } else {
    bufferlist::iterator p = bl.begin();
    ::decode(header, p);
    ::decode(omap, p);

    if (!p.end()) {
      clog->warn() << "tmap buffer of dir " << dirfrag() << " has "
		  << bl.length() - p.get_off() << " extra bytes\n";
    }
    bl.clear();
  }

  _omap_fetched(header, omap, want_dn, r);
}

class C_IO_Dir_OMAP_Fetched : public CDirIOContext {
 protected:
  string want_dn;
 public:
  bufferlist hdrbl;
  map<string, bufferlist> omap;
  bufferlist btbl;
  int ret1, ret2, ret3;

  C_IO_Dir_OMAP_Fetched(CDir *d, const string& w) : 
    CDirIOContext(d), want_dn(w),
    ret1(0), ret2(0), ret3(0) {}
  void finish(int r) {
    // check the correctness of backtrace
    if (r >= 0 && ret3 != -ECANCELED)
      dir->inode->verify_diri_backtrace(btbl, ret3);
    if (r >= 0) r = ret1;
    if (r >= 0) r = ret2;
    dir->_omap_fetched(hdrbl, omap, want_dn, r);
  }
};

void CDir::_omap_fetch(const string& want_dn)
{
  C_IO_Dir_OMAP_Fetched *fin = new C_IO_Dir_OMAP_Fetched(this, want_dn);
  object_t oid = get_ondisk_object();
  object_locator_t oloc(cache->mds->mdsmap->get_metadata_pool());
  ObjectOperation rd;
  rd.omap_get_header(&fin->hdrbl, &fin->ret1);
  rd.omap_get_vals("", "", (uint64_t)-1, &fin->omap, &fin->ret2);
  // check the correctness of backtrace
  if (g_conf->mds_verify_backtrace > 0 && frag == frag_t()) {
    rd.getxattr("parent", &fin->btbl, &fin->ret3);
    rd.set_last_op_flags(CEPH_OSD_OP_FLAG_FAILOK);
  } else {
    fin->ret3 = -ECANCELED;
  }

  cache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, NULL, 0,
			     new C_OnFinisher(fin, cache->mds->finisher));
}

CDentry *CDir::_load_dentry(
    const std::string &key,
    const std::string &dname,
    const snapid_t last,
    bufferlist &bl,
    const int pos,
    const std::set<snapid_t> *snaps,
    bool *force_dirty,
    list<CInode*> *undef_inodes)
{
  bufferlist::iterator q = bl.begin();

  snapid_t first;
  ::decode(first, q);

  // marker
  char type;
  ::decode(type, q);

  dout(20) << "_fetched pos " << pos << " marker '" << type << "' dname '" << dname
           << " [" << first << "," << last << "]"
           << dendl;

  bool stale = false;
  if (snaps && last != CEPH_NOSNAP) {
    set<snapid_t>::const_iterator p = snaps->lower_bound(first);
    if (p == snaps->end() || *p > last) {
      dout(10) << " skipping stale dentry on [" << first << "," << last << "]" << dendl;
      stale = true;
    }
  }
  
  /*
   * look for existing dentry for _last_ snap, because unlink +
   * create may leave a "hole" (epochs during which the dentry
   * doesn't exist) but for which no explicit negative dentry is in
   * the cache.
   */
  CDentry *dn;
  if (stale)
    dn = lookup_exact_snap(dname, last);
  else
    dn = lookup(dname, last);

  if (type == 'L') {
    // hard link
    inodeno_t ino;
    unsigned char d_type;
    ::decode(ino, q);
    ::decode(d_type, q);

    if (stale) {
      if (!dn) {
        stale_items.insert(key);
        *force_dirty = true;
      }
      return dn;
    }

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
    
    // Load inode data before looking up or constructing CInode
    InodeStore inode_data;
    inode_data.decode_bare(q);
    
    if (stale) {
      if (!dn) {
        stale_items.insert(key);
        *force_dirty = true;
      }
      return dn;
    }

    bool undef_inode = false;
    if (dn) {
      CInode *in = dn->get_linkage()->get_inode();
      if (in) {
        dout(12) << "_fetched  had dentry " << *dn << dendl;
        if (in->state_test(CInode::STATE_REJOINUNDEF)) {
          undef_inodes->push_back(in);
          undef_inode = true;
        }
      } else
        dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
    }

    if (!dn || undef_inode) {
      // add inode
      CInode *in = cache->get_inode(inode_data.inode.ino, last);
      if (!in || undef_inode) {
        if (undef_inode && in)
          in->first = first;
        else
          in = new CInode(cache, true, first, last);
        
        in->inode = inode_data.inode;
        // symlink?
        if (in->is_symlink()) 
          in->symlink = inode_data.symlink;
        
        in->dirfragtree.swap(inode_data.dirfragtree);
        in->xattrs.swap(inode_data.xattrs);
        in->old_inodes.swap(inode_data.old_inodes);
	if (!in->old_inodes.empty()) {
	  snapid_t min_first = in->old_inodes.rbegin()->first + 1;
	  if (min_first > in->first)
	    in->first = min_first;
	}

        in->oldest_snap = inode_data.oldest_snap;
        in->decode_snap_blob(inode_data.snap_blob);
        if (snaps && !in->snaprealm)
          in->purge_stale_snap_data(*snaps);

        if (!undef_inode) {
          cache->add_inode(in); // add
          dn = add_primary_dentry(dname, in, first, last); // link
        }
        dout(12) << "_fetched  got " << *dn << " " << *in << dendl;

        if (in->inode.is_dirty_rstat())
          in->mark_dirty_rstat();

        if (inode->is_stray()) {
          dn->state_set(CDentry::STATE_STRAY);
          if (in->inode.nlink == 0)
            in->state_set(CInode::STATE_ORPHAN);
        }

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
        cache->mds->clog->error() << "loaded dup inode " << inode_data.inode.ino
          << " [" << first << "," << last << "] v" << inode_data.inode.version
          << " at " << dirpath << "/" << dname
          << ", but inode " << in->vino() << " v" << in->inode.version
          << " already exists at " << inopath << "\n";
        return dn;
      }
    }
  } else {
    std::ostringstream oss;
    oss << "Invalid tag char '" << type << "' pos " << pos;
    throw buffer::malformed_input(oss.str());
  }

  return dn;
}

void CDir::_omap_fetched(bufferlist& hdrbl, map<string, bufferlist>& omap,
			 const string& want_dn, int r)
{
  LogChannelRef clog = cache->mds->clog;
  dout(10) << "_fetched header " << hdrbl.length() << " bytes "
	   << omap.size() << " keys for " << *this
	   << " want_dn=" << want_dn << dendl;

  assert(r == 0 || r == -ENOENT || r == -ENODATA);
  assert(is_auth());
  assert(!is_frozen());

  if (hdrbl.length() == 0) {
    if (r != -ENODATA) { // called by _tmap_fetched() ?
      dout(10) << "_fetched 0 byte from omap, retry tmap" << dendl;
      _tmap_fetch(want_dn);
      return;
    }

    dout(0) << "_fetched missing object for " << *this << dendl;

    clog->error() << "dir " << dirfrag() << " object missing on disk; some "
                     "files may be lost (" << get_path() << ")";

    go_bad();
    return;
  }

  fnode_t got_fnode;
  {
    bufferlist::iterator p = hdrbl.begin();
    try {
      ::decode(got_fnode, p);
    } catch (const buffer::error &err) {
      derr << "Corrupt fnode in dirfrag " << dirfrag()
        << ": " << err << dendl;
      clog->warn() << "Corrupt fnode header in " << dirfrag() << ": "
		  << err << " (" << get_path() << ")";
      go_bad();
      return;
    }
    if (!p.end()) {
      clog->warn() << "header buffer of dir " << dirfrag() << " has "
		  << hdrbl.length() - p.get_off() << " extra bytes ("
                  << get_path() << ")";
      go_bad();
      return;
    }
  }

  dout(10) << "_fetched version " << got_fnode.version << dendl;
  
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

  list<CInode*> undef_inodes;

  // purge stale snaps?
  // only if we have past_parents open!
  bool force_dirty = false;
  const set<snapid_t> *snaps = NULL;
  SnapRealm *realm = inode->find_snaprealm();
  if (!realm->have_past_parents_open()) {
    dout(10) << " no snap purge, one or more past parents NOT open" << dendl;
  } else if (fnode.snap_purged_thru < realm->get_last_destroyed()) {
    snaps = &realm->get_snaps();
    dout(10) << " snap_purged_thru " << fnode.snap_purged_thru
	     << " < " << realm->get_last_destroyed()
	     << ", snap purge based on " << *snaps << dendl;
    if (get_num_snap_items() == 0) {
      fnode.snap_purged_thru = realm->get_last_destroyed();
      force_dirty = true;
    }
  }

  unsigned pos = omap.size() - 1;
  for (map<string, bufferlist>::reverse_iterator p = omap.rbegin();
       p != omap.rend();
       ++p, --pos) {
    string dname;
    snapid_t last;
    dentry_key_t::decode_helper(p->first, dname, last);

    CDentry *dn = NULL;
    try {
      dn = _load_dentry(
            p->first, dname, last, p->second, pos, snaps,
            &force_dirty, &undef_inodes);
    } catch (const buffer::error &err) {
      cache->mds->clog->warn() << "Corrupt dentry '" << dname << "' in "
                                  "dir frag " << dirfrag() << ": "
                               << err << "(" << get_path() << ")";

      // Remember that this dentry is damaged.  Subsequent operations
      // that try to act directly on it will get their EIOs, but this
      // dirfrag as a whole will continue to look okay (minus the
      // mysteriously-missing dentry)
      go_bad_dentry(last, dname);

      // Anyone who was WAIT_DENTRY for this guy will get kicked
      // to RetryRequest, and hit the DamageTable-interrogating path.
      // Stats will now be bogus because we will think we're complete,
      // but have 1 or more missing dentries.
      continue;
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

  //cache->mds->logger->inc("newin", num_new_inodes_loaded);

  // mark complete, !fetching
  mark_complete();
  state_clear(STATE_FETCHING);

  if (scrub_infop && scrub_infop->need_scrub_local) {
    scrub_infop->need_scrub_local = false;
    scrub_local();
  }

  // open & force frags
  while (!undef_inodes.empty()) {
    CInode *in = undef_inodes.front();
    undef_inodes.pop_front();
    in->state_clear(CInode::STATE_REJOINUNDEF);
    cache->opened_undef_inode(in);
  }

  // dirty myself to remove stale snap dentries
  if (force_dirty && !is_dirty() && !inode->mdcache->is_readonly())
    log_mark_dirty();
  else
    auth_unpin(this);

  // kick waiters
  finish_waiting(WAIT_COMPLETE, 0);
}

void CDir::_go_bad()
{
  if (get_version() == 0)
    set_version(1);
  state_set(STATE_BADFRAG);
  // mark complete, !fetching
  mark_complete();
  state_clear(STATE_FETCHING);
  auth_unpin(this);

  // kick waiters
  finish_waiting(WAIT_COMPLETE, -EIO);
}

void CDir::go_bad_dentry(snapid_t last, const std::string &dname)
{
  const bool fatal = cache->mds->damage_table.notify_dentry(
      inode->ino(), frag, last, dname, get_path() + "/" + dname);
  if (fatal) {
    cache->mds->damaged();
    assert(0);  // unreachable, damaged() respawns us
  }
}

void CDir::go_bad()
{
  const bool fatal = cache->mds->damage_table.notify_dirfrag(
      inode->ino(), frag, get_path());
  if (fatal) {
    cache->mds->damaged();
    assert(0);  // unreachable, damaged() respawns us
  }

  _go_bad();
}

// -----------------------
// COMMIT

/**
 * commit
 *
 * @param want - min version i want committed
 * @param c - callback for completion
 */
void CDir::commit(version_t want, MDSInternalContextBase *c, bool ignore_authpinnability, int op_prio)
{
  dout(10) << "commit want " << want << " on " << *this << dendl;
  if (want == 0) want = get_version();

  // preconditions
  assert(want <= get_version() || get_version() == 0);    // can't commit the future
  assert(want > committed_version); // the caller is stupid
  assert(is_auth());
  assert(ignore_authpinnability || can_auth_pin());

  if (inode->inode.nlink == 0 && !inode->snaprealm) {
    dout(7) << "commit dirfrag for unlinked directory, mark clean" << dendl;
    try_remove_dentries_for_stray();
    if (c)
      cache->mds->queue_waiter(c);
    return;
  }

  // note: queue up a noop if necessary, so that we always
  // get an auth_pin.
  if (!c)
    c = new C_MDSInternalNoop;

  // auth_pin on first waiter
  if (waiting_for_commit.empty())
    auth_pin(this);
  waiting_for_commit[want].push_back(c);
  
  // ok.
  _commit(want, op_prio);
}

class C_IO_Dir_Committed : public CDirIOContext {
  version_t version;
public:
  C_IO_Dir_Committed(CDir *d, version_t v) : CDirIOContext(d), version(v) { }
  void finish(int r) {
    dir->_committed(r, version);
  }
};

/**
 * Flush out the modified dentries in this dir. Keep the bufferlist
 * below max_write_size;
 */
void CDir::_omap_commit(int op_prio)
{
  dout(10) << "_omap_commit" << dendl;

  unsigned max_write_size = cache->max_dir_commit_size;
  unsigned write_size = 0;

  if (op_prio < 0)
    op_prio = CEPH_MSG_PRIO_DEFAULT;

  // snap purge?
  const set<snapid_t> *snaps = NULL;
  SnapRealm *realm = inode->find_snaprealm();
  if (!realm->have_past_parents_open()) {
    dout(10) << " no snap purge, one or more past parents NOT open" << dendl;
  } else if (fnode.snap_purged_thru < realm->get_last_destroyed()) {
    snaps = &realm->get_snaps();
    dout(10) << " snap_purged_thru " << fnode.snap_purged_thru
	     << " < " << realm->get_last_destroyed()
	     << ", snap purge based on " << *snaps << dendl;
    // fnode.snap_purged_thru = realm->get_last_destroyed();
  }

  set<string> to_remove;
  map<string, bufferlist> to_set;

  C_GatherBuilder gather(g_ceph_context,
			 new C_OnFinisher(new C_IO_Dir_Committed(this,
								 get_version()),
					  cache->mds->finisher));

  SnapContext snapc;
  object_t oid = get_ondisk_object();
  object_locator_t oloc(cache->mds->mdsmap->get_metadata_pool());

  if (!stale_items.empty()) {
    for (compact_set<string>::iterator p = stale_items.begin();
	 p != stale_items.end();
	 ++p) {
      to_remove.insert(*p);
      write_size += (*p).length();
    }
    stale_items.clear();
  }

  for (map_t::iterator p = items.begin();
      p != items.end(); ) {
    CDentry *dn = p->second;
    ++p;

    string key;
    dn->key().encode(key);

    if (dn->last != CEPH_NOSNAP &&
	snaps && try_trim_snap_dentry(dn, *snaps)) {
      dout(10) << " rm " << key << dendl;
      write_size += key.length();
      to_remove.insert(key);
      continue;
    }

    if (!dn->is_dirty() &&
	(!dn->state_test(CDentry::STATE_FRAGMENTING) || dn->get_linkage()->is_null()))
      continue;  // skip clean dentries

    if (dn->get_linkage()->is_null()) {
      dout(10) << " rm " << dn->name << " " << *dn << dendl;
      write_size += key.length();
      to_remove.insert(key);
    } else {
      dout(10) << " set " << dn->name << " " << *dn << dendl;
      bufferlist dnbl;
      _encode_dentry(dn, dnbl, snaps);
      write_size += key.length() + dnbl.length();
      to_set[key].swap(dnbl);
    }

    if (write_size >= max_write_size) {
      ObjectOperation op;
      op.priority = op_prio;

      // don't create new dirfrag blindly
      if (!is_new() && !state_test(CDir::STATE_FRAGMENTING))
	op.stat(NULL, (ceph::real_time*) NULL, NULL);

      op.tmap_to_omap(true); // convert tmap to omap

      if (!to_set.empty())
	op.omap_set(to_set);
      if (!to_remove.empty())
	op.omap_rm_keys(to_remove);

      cache->mds->objecter->mutate(oid, oloc, op, snapc,
				   ceph::real_clock::now(g_ceph_context),
				   0, NULL, gather.new_sub());

      write_size = 0;
      to_set.clear();
      to_remove.clear();
    }
  }

  ObjectOperation op;
  op.priority = op_prio;

  // don't create new dirfrag blindly
  if (!is_new() && !state_test(CDir::STATE_FRAGMENTING))
    op.stat(NULL, (ceph::real_time*)NULL, NULL);

  op.tmap_to_omap(true); // convert tmap to omap

  /*
   * save the header at the last moment.. If we were to send it off before other
   * updates, but die before sending them all, we'd think that the on-disk state
   * was fully committed even though it wasn't! However, since the messages are
   * strictly ordered between the MDS and the OSD, and since messages to a given
   * PG are strictly ordered, if we simply send the message containing the header
   * off last, we cannot get our header into an incorrect state.
   */
  bufferlist header;
  ::encode(fnode, header);
  op.omap_set_header(header);

  if (!to_set.empty())
    op.omap_set(to_set);
  if (!to_remove.empty())
    op.omap_rm_keys(to_remove);

  cache->mds->objecter->mutate(oid, oloc, op, snapc,
			       ceph::real_clock::now(g_ceph_context),
			       0, NULL, gather.new_sub());

  gather.activate();
}

void CDir::_encode_dentry(CDentry *dn, bufferlist& bl,
			  const set<snapid_t> *snaps)
{
  // clear dentry NEW flag, if any.  we can no longer silently drop it.
  dn->clear_new();

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

    if (in->is_multiversion()) {
      if (!in->snaprealm) {
	if (snaps)
	  in->purge_stale_snap_data(*snaps);
      } else if (in->snaprealm->have_past_parents_open()) {
	in->purge_stale_snap_data(in->snaprealm->get_snaps());
      }
    }

    bufferlist snap_blob;
    in->encode_snap_blob(snap_blob);
    in->encode_bare(bl, cache->mds->mdsmap->get_up_features(), &snap_blob);
  }
}

void CDir::_commit(version_t want, int op_prio)
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
  
  // commit.
  committing_version = get_version();

  // mark committing (if not already)
  if (!state_test(STATE_COMMITTING)) {
    dout(10) << "marking committing" << dendl;
    state_set(STATE_COMMITTING);
  }
  
  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_commit);

  _omap_commit(op_prio);
}


/**
 * _committed
 *
 * @param v version i just committed
 */
void CDir::_committed(int r, version_t v)
{
  if (r < 0) {
    // the directory could be partly purged during MDS failover
    if (r == -ENOENT && committed_version == 0 &&
	inode->inode.nlink == 0 && inode->snaprealm) {
      inode->state_set(CInode::STATE_MISSINGOBJS);
      r = 0;
    }
    if (r < 0) {
      dout(1) << "commit error " << r << " v " << v << dendl;
      cache->mds->clog->error() << "failed to commit dir " << dirfrag() << " object,"
				<< " errno " << r << "\n";
      cache->mds->handle_write_error(r);
      return;
    }
  }

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
  
  compact_map<version_t, list<MDSInternalContextBase*> >::iterator p = waiting_for_commit.begin();
  while (p != waiting_for_commit.end()) {
    compact_map<version_t, list<MDSInternalContextBase*> >::iterator n = p;
    ++n;
    if (p->first > committed_version) {
      dout(10) << " there are waiters for " << p->first << ", committing again" << dendl;
      _commit(p->first, -1);
      break;
    }
    cache->mds->queue_waiters(p->second);
    waiting_for_commit.erase(p);
    p = n;
  } 

  // try drop dentries in this dirfrag if it's about to be purged
  if (inode->inode.nlink == 0 && inode->snaprealm)
    cache->maybe_eval_stray(inode, true);

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
      !(fnode.rstat == fnode.accounted_rstat)) {
    cache->mds->locker->mark_updated_scatterlock(&inode->nestlock);
    ls->dirty_dirfrag_nest.push_back(&inode->item_dirty_dirfrag_nest);
  }
  if (!(fnode.fragstat == fnode.accounted_fragstat)) {
    cache->mds->locker->mark_updated_scatterlock(&inode->filelock);
    ls->dirty_dirfrag_dir.push_back(&inode->item_dirty_dirfrag_dir);
  }
  if (is_dirty_dft()) {
    if (inode->dirfragtreelock.get_state() != LOCK_MIX &&
	inode->dirfragtreelock.is_stable()) {
      // clear stale dirtydft
      state_clear(STATE_DIRTYDFT);
    } else {
      cache->mds->locker->mark_updated_scatterlock(&inode->dirfragtreelock);
      ls->dirty_dirfrag_dirfragtree.push_back(&inode->item_dirty_dirfrag_dirfragtree);
    }
  }
}




/********************************
 * AUTHORITY
 */

/*
 * if dir_auth.first == parent, auth is same as inode.
 * unless .second != unknown, in which case that sticks.
 */
mds_authority_t CDir::authority() const
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
void CDir::set_dir_auth(mds_authority_t a)
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
    list<MDSInternalContextBase*> ls;
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
    ++num_freezing_trees;
    dout(10) << "freeze_tree waiting " << *this << dendl;
    return false;
  }
}

void CDir::_freeze_tree()
{
  dout(10) << "_freeze_tree " << *this << dendl;
  assert(is_freezeable(true));

  // twiddle state
  if (state_test(STATE_FREEZINGTREE)) {
    state_clear(STATE_FREEZINGTREE);   // actually, this may get set again by next context?
    --num_freezing_trees;
  }
  state_set(STATE_FROZENTREE);
  ++num_frozen_trees;
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
    --num_frozen_trees;

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
    --num_freezing_trees;
    auth_unpin(this);
    
    finish_waiting(WAIT_UNFREEZE);
  }
}

bool CDir::is_freezing_tree() const
{
  if (num_freezing_trees == 0)
    return false;
  const CDir *dir = this;
  while (1) {
    if (dir->is_freezing_tree_root()) return true;
    if (dir->is_subtree_root()) return false;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      return false; // root on replica
  }
}

bool CDir::is_frozen_tree() const
{
  if (num_frozen_trees == 0)
    return false;
  const CDir *dir = this;
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

class C_Dir_AuthUnpin : public CDirContext {
  public:
  explicit C_Dir_AuthUnpin(CDir *d) : CDirContext(d) {}
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

/**
 * Slightly less complete than operator<<, because this is intended
 * for identifying a directory and its state rather than for dumping
 * debug output.
 */
void CDir::dump(Formatter *f) const
{
  assert(f != NULL);

  f->dump_stream("path") << get_path();

  f->dump_stream("dirfrag") << dirfrag();
  f->dump_int("snapid_first", first);

  f->dump_stream("projected_version") << get_projected_version();
  f->dump_stream("version") << get_version();
  f->dump_stream("committing_version") << get_committing_version();
  f->dump_stream("committed_version") << get_committed_version();

  f->dump_bool("is_rep", is_rep());

  if (get_dir_auth() != CDIR_AUTH_DEFAULT) {
    if (get_dir_auth().second == CDIR_AUTH_UNKNOWN) {
      f->dump_stream("dir_auth") << get_dir_auth().first;
    } else {
      f->dump_stream("dir_auth") << get_dir_auth();
    }
  } else {
    f->dump_string("dir_auth", "");
  }

  f->open_array_section("states");
  MDSCacheObject::dump_states(f);
  if (state_test(CDir::STATE_COMPLETE)) f->dump_string("state", "complete");
  if (state_test(CDir::STATE_FREEZINGTREE)) f->dump_string("state", "freezingtree");
  if (state_test(CDir::STATE_FROZENTREE)) f->dump_string("state", "frozentree");
  if (state_test(CDir::STATE_FROZENDIR)) f->dump_string("state", "frozendir");
  if (state_test(CDir::STATE_FREEZINGDIR)) f->dump_string("state", "freezingdir");
  if (state_test(CDir::STATE_EXPORTBOUND)) f->dump_string("state", "exportbound");
  if (state_test(CDir::STATE_IMPORTBOUND)) f->dump_string("state", "importbound");
  if (state_test(CDir::STATE_BADFRAG)) f->dump_string("state", "badfrag");
  f->close_section();

  MDSCacheObject::dump(f);
}

/****** Scrub Stuff *******/

void CDir::scrub_info_create() const
{
  assert(!scrub_infop);

  // break out of const-land to set up implicit initial state
  CDir *me = const_cast<CDir*>(this);
  fnode_t *fn = me->get_projected_fnode();

  scrub_info_t *si = new scrub_info_t();

  si->last_recursive.version = si->recursive_start.version =
      fn->recursive_scrub_version;
  si->last_recursive.time = si->recursive_start.time =
      fn->recursive_scrub_stamp;

  si->last_local.version = fn->localized_scrub_version;
  si->last_local.time = fn->localized_scrub_stamp;

  me->scrub_infop = si;
}

void CDir::scrub_initialize(const ScrubHeaderRefConst& header)
{
  dout(20) << __func__ << dendl;
  assert(is_complete());
  assert(header != nullptr);

  // FIXME: weird implicit construction, is someone else meant
  // to be calling scrub_info_create first?
  scrub_info();
  assert(scrub_infop && !scrub_infop->directory_scrubbing);

  scrub_infop->recursive_start.version = get_projected_version();
  scrub_infop->recursive_start.time = ceph_clock_now(g_ceph_context);

  scrub_infop->directories_to_scrub.clear();
  scrub_infop->directories_scrubbing.clear();
  scrub_infop->directories_scrubbed.clear();
  scrub_infop->others_to_scrub.clear();
  scrub_infop->others_scrubbing.clear();
  scrub_infop->others_scrubbed.clear();

  for (map_t::iterator i = items.begin();
      i != items.end();
      ++i) {
    // TODO: handle snapshot scrubbing
    if (i->first.snapid != CEPH_NOSNAP)
      continue;

    CDentry::linkage_t *dnl = i->second->get_projected_linkage();
    if (dnl->is_primary()) {
      if (dnl->get_inode()->is_dir())
	scrub_infop->directories_to_scrub.insert(i->first);
      else
	scrub_infop->others_to_scrub.insert(i->first);
    } else if (dnl->is_remote()) {
      // TODO: check remote linkage
    }
  }
  scrub_infop->directory_scrubbing = true;
  scrub_infop->header = header;
}

void CDir::scrub_finished()
{
  dout(20) << __func__ << dendl;
  assert(scrub_infop && scrub_infop->directory_scrubbing);

  assert(scrub_infop->directories_to_scrub.empty());
  assert(scrub_infop->directories_scrubbing.empty());
  scrub_infop->directories_scrubbed.clear();
  assert(scrub_infop->others_to_scrub.empty());
  assert(scrub_infop->others_scrubbing.empty());
  scrub_infop->others_scrubbed.clear();
  scrub_infop->directory_scrubbing = false;

  scrub_infop->last_recursive = scrub_infop->recursive_start;
  scrub_infop->last_scrub_dirty = true;
}

int CDir::_next_dentry_on_set(set<dentry_key_t>& dns, bool missing_okay,
                              MDSInternalContext *cb, CDentry **dnout)
{
  dentry_key_t dnkey;
  CDentry *dn;

  while (!dns.empty()) {
    set<dentry_key_t>::iterator front = dns.begin();
    dnkey = *front;
    dn = lookup(dnkey.name);
    if (!dn) {
      if (!is_complete() &&
          (!has_bloom() || is_in_bloom(dnkey.name))) {
        // need to re-read this dirfrag
        fetch(cb);
        return EAGAIN;
      }
      // okay, we lost it
      if (missing_okay) {
	dout(15) << " we no longer have directory dentry "
		 << dnkey.name << ", assuming it got renamed" << dendl;
	dns.erase(dnkey);
	continue;
      } else {
	dout(5) << " we lost dentry " << dnkey.name
		<< ", bailing out because that's impossible!" << dendl;
	assert(0);
      }
    }
    // okay, we got a  dentry
    dns.erase(dnkey);

    if (dn->get_projected_version() < scrub_infop->last_recursive.version &&
	!(scrub_infop->header->get_force())) {
      dout(15) << " skip dentry " << dnkey.name
	       << ", no change since last scrub" << dendl;
      continue;
    }

    *dnout = dn;
    return 0;
  }
  *dnout = NULL;
  return ENOENT;
}

int CDir::scrub_dentry_next(MDSInternalContext *cb, CDentry **dnout)
{
  dout(20) << __func__ << dendl;
  assert(scrub_infop && scrub_infop->directory_scrubbing);

  dout(20) << "trying to scrub directories underneath us" << dendl;
  int rval = _next_dentry_on_set(scrub_infop->directories_to_scrub, true,
                                 cb, dnout);
  if (rval == 0) {
    dout(20) << __func__ << " inserted to directories scrubbing: "
      << *dnout << dendl;
    scrub_infop->directories_scrubbing.insert((*dnout)->key());
  } else if (rval < 0 || rval == EAGAIN) {
    // we don't need to do anything else
  } else { // we emptied out the directory scrub set
    assert(rval == ENOENT);
    dout(20) << "no directories left, moving on to other kinds of dentries"
             << dendl;
    
    rval = _next_dentry_on_set(scrub_infop->others_to_scrub, false, cb, dnout);
    if (rval == 0) {
      dout(20) << __func__ << " inserted to others scrubbing: "
        << *dnout << dendl;
      scrub_infop->others_scrubbing.insert((*dnout)->key());
    }
  }
  dout(20) << " returning " << rval << " with dn=" << *dnout << dendl;
  return rval;
}

void CDir::scrub_dentries_scrubbing(list<CDentry*> *out_dentries)
{
  dout(20) << __func__ << dendl;
  assert(scrub_infop && scrub_infop->directory_scrubbing);

  for (set<dentry_key_t>::iterator i =
        scrub_infop->directories_scrubbing.begin();
      i != scrub_infop->directories_scrubbing.end();
      ++i) {
    CDentry *d = lookup(i->name, i->snapid);
    assert(d);
    out_dentries->push_back(d);
  }
  for (set<dentry_key_t>::iterator i = scrub_infop->others_scrubbing.begin();
      i != scrub_infop->others_scrubbing.end();
      ++i) {
    CDentry *d = lookup(i->name, i->snapid);
    assert(d);
    out_dentries->push_back(d);
  }
}

void CDir::scrub_dentry_finished(CDentry *dn)
{
  dout(20) << __func__ << " on dn " << *dn << dendl;
  assert(scrub_infop && scrub_infop->directory_scrubbing);
  dentry_key_t dn_key = dn->key();
  if (scrub_infop->directories_scrubbing.count(dn_key)) {
    scrub_infop->directories_scrubbing.erase(dn_key);
    scrub_infop->directories_scrubbed.insert(dn_key);
  } else {
    assert(scrub_infop->others_scrubbing.count(dn_key));
    scrub_infop->others_scrubbing.erase(dn_key);
    scrub_infop->others_scrubbed.insert(dn_key);
  }
}

void CDir::scrub_maybe_delete_info()
{
  if (scrub_infop &&
      !scrub_infop->directory_scrubbing &&
      !scrub_infop->need_scrub_local &&
      !scrub_infop->last_scrub_dirty &&
      !scrub_infop->pending_scrub_error &&
      scrub_infop->dirty_scrub_stamps.empty()) {
    delete scrub_infop;
    scrub_infop = NULL;
  }
}

bool CDir::scrub_local()
{
  assert(is_complete());
  bool rval = check_rstats(true);

  scrub_info();
  if (rval) {
    scrub_infop->last_local.time = ceph_clock_now(g_ceph_context);
    scrub_infop->last_local.version = get_projected_version();
    scrub_infop->pending_scrub_error = false;
    scrub_infop->last_scrub_dirty = true;
  } else {
    scrub_infop->pending_scrub_error = true;
    if (scrub_infop->header->get_repair())
      cache->repair_dirfrag_stats(this);
  }
  return rval;
}

std::string CDir::get_path() const
{
  std::string path;
  get_inode()->make_path_string(path, true);
  return path;
}

