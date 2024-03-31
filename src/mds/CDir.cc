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

#include <string_view>
#include <algorithm>

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
#include "MDBalancer.h"
#include "SnapClient.h"

#include "common/bloom_filter.hpp"
#include "common/likely.h"
#include "include/Context.h"
#include "common/Clock.h"

#include "osdc/Objecter.h"

#include "common/config.h"
#include "include/ceph_assert.h"
#include "include/compat.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") "

using namespace std;

int CDir::num_frozen_trees = 0;
int CDir::num_freezing_trees = 0;

CDir::fnode_const_ptr CDir::empty_fnode = CDir::allocate_fnode();

class CDirContext : public MDSContext
{
protected:
  CDir *dir;
  MDSRank* get_mds() override {return dir->mdcache->mds;}

public:
  explicit CDirContext(CDir *d) : dir(d) {
    ceph_assert(dir != NULL);
  }
};


class CDirIOContext : public MDSIOContextBase
{
protected:
  CDir *dir;
  MDSRank* get_mds() override {return dir->mdcache->mds;}

public:
  explicit CDirIOContext(CDir *d) : dir(d) {
    ceph_assert(dir != NULL);
  }
};


// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };


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
  
  if (dir.get_auth_pins() || dir.get_dir_auth_pins()) {
    out << " ap=" << dir.get_auth_pins() 
	<< "+" << dir.get_dir_auth_pins();
#ifdef MDS_AUTHPIN_SET
    dir.print_authpin_set(out);
#endif
  }

  out << " state=" << dir.get_state();
  if (dir.state_test(CDir::STATE_COMPLETE)) out << "|complete";
  if (dir.state_test(CDir::STATE_FREEZINGTREE)) out << "|freezingtree";
  if (dir.state_test(CDir::STATE_FROZENTREE)) out << "|frozentree";
  if (dir.state_test(CDir::STATE_AUXSUBTREE)) out << "|auxsubtree";
  if (dir.state_test(CDir::STATE_FROZENDIR)) out << "|frozendir";
  if (dir.state_test(CDir::STATE_FREEZINGDIR)) out << "|freezingdir";
  if (dir.state_test(CDir::STATE_EXPORTBOUND)) out << "|exportbound";
  if (dir.state_test(CDir::STATE_IMPORTBOUND)) out << "|importbound";
  if (dir.state_test(CDir::STATE_BADFRAG)) out << "|badfrag";
  if (dir.state_test(CDir::STATE_FRAGMENTING)) out << "|fragmenting";
  if (dir.state_test(CDir::STATE_CREATING)) out << "|creating";
  if (dir.state_test(CDir::STATE_COMMITTING)) out << "|committing";
  if (dir.state_test(CDir::STATE_FETCHING)) out << "|fetching";
  if (dir.state_test(CDir::STATE_EXPORTING)) out << "|exporting";
  if (dir.state_test(CDir::STATE_IMPORTING)) out << "|importing";
  if (dir.state_test(CDir::STATE_STICKY)) out << "|sticky";
  if (dir.state_test(CDir::STATE_DNPINNEDFRAG)) out << "|dnpinnedfrag";
  if (dir.state_test(CDir::STATE_ASSIMRSTAT)) out << "|assimrstat";

  // fragstat
  out << " " << dir.get_fnode()->fragstat;
  if (!(dir.get_fnode()->fragstat == dir.get_fnode()->accounted_fragstat))
    out << "/" << dir.get_fnode()->accounted_fragstat;
  if (g_conf()->mds_debug_scatterstat && dir.is_projected()) {
    const auto& pf = dir.get_projected_fnode();
    out << "->" << pf->fragstat;
    if (!(pf->fragstat == pf->accounted_fragstat))
      out << "/" << pf->accounted_fragstat;
  }
  
  // rstat
  out << " " << dir.get_fnode()->rstat;
  if (!(dir.get_fnode()->rstat == dir.get_fnode()->accounted_rstat))
    out << "/" << dir.get_fnode()->accounted_rstat;
  if (g_conf()->mds_debug_scatterstat && dir.is_projected()) {
    const auto& pf = dir.get_projected_fnode();
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


void CDir::print(ostream& out) const
{
  out << *this;
}




ostream& CDir::print_db_line_prefix(ostream& out) const
{
  return out << ceph_clock_now() << " mds." << mdcache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") ";
}



// -------------------------------------------------------------------
// CDir

CDir::CDir(CInode *in, frag_t fg, MDCache *mdc, bool auth) :
  mdcache(mdc), inode(in), frag(fg),
  dirty_rstat_inodes(member_offset(CInode, dirty_rstat_item)),
  dirty_dentries(member_offset(CDentry, item_dir_dirty)),
  item_dirty(this), item_new(this),
  lock_caches_with_auth_pins(member_offset(MDLockCache::DirItem, item_dir)),
  freezing_inodes(member_offset(CInode, item_freezing_inode)),
  dir_rep(REP_NONE),
  pop_me(mdc->decayrate),
  pop_nested(mdc->decayrate),
  pop_auth_subtree(mdc->decayrate),
  pop_auth_subtree_nested(mdc->decayrate),
  pop_lru_subdirs(member_offset(CInode, item_pop_lru)),
  dir_auth(CDIR_AUTH_DEFAULT)
{
  // auth
  ceph_assert(in->is_dir());
  if (auth)
    state_set(STATE_AUTH);
}

/**
 * Check the recursive statistics on size for consistency.
 * If mds_debug_scatterstat is enabled, assert for correctness,
 * otherwise just print out the mismatch and continue.
 */
bool CDir::check_rstats(bool scrub)
{
  if (!g_conf()->mds_debug_scatterstat && !scrub)
    return true;

  dout(25) << "check_rstats on " << this << dendl;
  if (!is_complete() || !is_auth() || is_frozen()) {
    dout(3) << "check_rstats " << (scrub ? "(scrub) " : "")
            << "bailing out -- incomplete or non-auth or frozen dir on " 
            << *this << dendl;
    return !scrub;
  }

  frag_info_t frag_info;
  nest_info_t nest_info;
  for (auto i = items.begin(); i != items.end(); ++i) {
    if (i->second->last != CEPH_NOSNAP)
      continue;
    CDentry::linkage_t *dnl = i->second->get_linkage();
    if (dnl->is_primary()) {
      CInode *in = dnl->get_inode();
      nest_info.add(in->get_inode()->accounted_rstat);
      if (in->is_dir())
	frag_info.nsubdirs++;
      else
	frag_info.nfiles++;
    } else if (dnl->is_remote())
      frag_info.nfiles++;
  }

  bool good = true;
  // fragstat
  if(!frag_info.same_sums(fnode->fragstat)) {
    dout(1) << "mismatch between head items and fnode.fragstat! printing dentries" << dendl;
    dout(1) << "get_num_head_items() = " << get_num_head_items()
             << "; fnode.fragstat.nfiles=" << fnode->fragstat.nfiles
             << " fnode.fragstat.nsubdirs=" << fnode->fragstat.nsubdirs << dendl;
    good = false;
  } else {
    dout(20) << "get_num_head_items() = " << get_num_head_items()
             << "; fnode.fragstat.nfiles=" << fnode->fragstat.nfiles
             << " fnode.fragstat.nsubdirs=" << fnode->fragstat.nsubdirs << dendl;
  }

  // rstat
  if (!nest_info.same_sums(fnode->rstat)) {
    dout(1) << "mismatch between child accounted_rstats and my rstats!" << dendl;
    dout(1) << "total of child dentries: " << nest_info << dendl;
    dout(1) << "my rstats:              " << fnode->rstat << dendl;
    good = false;
  } else {
    dout(20) << "total of child dentries: " << nest_info << dendl;
    dout(20) << "my rstats:              " << fnode->rstat << dendl;
  }

  if (!good) {
    if (!scrub) {
      for (auto i = items.begin(); i != items.end(); ++i) {
	CDentry *dn = i->second;
	if (dn->get_linkage()->is_primary()) {
	  CInode *in = dn->get_linkage()->inode;
	  dout(1) << *dn << " rstat " << in->get_inode()->accounted_rstat << dendl;
	} else {
	  dout(1) << *dn << dendl;
	}
      }

      ceph_assert(frag_info.nfiles == fnode->fragstat.nfiles);
      ceph_assert(frag_info.nsubdirs == fnode->fragstat.nsubdirs);
      ceph_assert(nest_info.rbytes == fnode->rstat.rbytes);
      ceph_assert(nest_info.rfiles == fnode->rstat.rfiles);
      ceph_assert(nest_info.rsubdirs == fnode->rstat.rsubdirs);
    }
  }
  dout(10) << "check_rstats complete on " << this << dendl;
  return good;
}

void CDir::adjust_num_inodes_with_caps(int d)
{
  // FIXME: smarter way to decide if adding 'this' to open file table
  if (num_inodes_with_caps == 0 && d > 0)
    mdcache->open_file_table.add_dirfrag(this);
  else if (num_inodes_with_caps > 0 && num_inodes_with_caps == -d)
    mdcache->open_file_table.remove_dirfrag(this);

  num_inodes_with_caps += d;
  ceph_assert(num_inodes_with_caps >= 0);
}

CDentry *CDir::lookup(std::string_view name, snapid_t snap)
{ 
  dout(20) << "lookup (" << name << ", '" << snap << "')" << dendl;
  auto iter = items.lower_bound(dentry_key_t(snap, name, inode->hash_dentry_name(name)));
  if (iter == items.end())
    return 0;
  if (iter->second->get_name() == name &&
      iter->second->first <= snap &&
      iter->second->last >= snap) {
    dout(20) << "  hit -> " << iter->first << dendl;
    return iter->second;
  }
  dout(20) << "  miss -> " << iter->first << dendl;
  return 0;
}

CDentry *CDir::lookup_exact_snap(std::string_view name, snapid_t last) {
  dout(20) << __func__ << " (" << last << ", '" << name << "')" << dendl;
  auto p = items.find(dentry_key_t(last, name, inode->hash_dentry_name(name)));
  if (p == items.end())
    return NULL;
  return p->second;
}

void CDir::adjust_dentry_lru(CDentry *dn)
{
  bool bottom_lru;
  if (dn->get_linkage()->is_primary()) {
    bottom_lru = !is_auth() && inode->is_stray();
  } else if (dn->get_linkage()->is_remote()) {
    bottom_lru = false;
  } else {
    bottom_lru = !is_auth();
  }
  if (bottom_lru) {
    if (!dn->state_test(CDentry::STATE_BOTTOMLRU)) {
      mdcache->lru.lru_remove(dn);
      mdcache->bottom_lru.lru_insert_mid(dn);
      dn->state_set(CDentry::STATE_BOTTOMLRU);
    }
  } else {
    if (dn->state_test(CDentry::STATE_BOTTOMLRU)) {
      mdcache->bottom_lru.lru_remove(dn);
      mdcache->lru.lru_insert_mid(dn);
      dn->state_clear(CDentry::STATE_BOTTOMLRU);
    }
  }
}

/***
 * linking fun
 */

CDentry* CDir::add_null_dentry(std::string_view dname,
			       snapid_t first, snapid_t last)
{
  // foreign
  ceph_assert(lookup_exact_snap(dname, last) == 0);
   
  // create dentry
  CDentry* dn = new CDentry(dname, inode->hash_dentry_name(dname), "", first, last);
  dn->dir = this;
  dn->version = get_projected_version();
  dn->check_corruption(true);
  if (is_auth()) {
    dn->state_set(CDentry::STATE_AUTH);
    mdcache->lru.lru_insert_mid(dn);
  } else {
    mdcache->bottom_lru.lru_insert_mid(dn);
    dn->state_set(CDentry::STATE_BOTTOMLRU);
  }
  
  // add to dir
  ceph_assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->get_name()) == 0);

  items[dn->key()] = dn;
  if (last == CEPH_NOSNAP)
    num_head_null++;
  else
    num_snap_null++;

  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

  dout(12) << __func__ << " " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  
  ceph_assert(get_num_any() == items.size());
  return dn;
}


CDentry* CDir::add_primary_dentry(std::string_view dname, CInode *in,
                                  mempool::mds_co::string alternate_name,
				  snapid_t first, snapid_t last) 
{
  // primary
  ceph_assert(lookup_exact_snap(dname, last) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, inode->hash_dentry_name(dname), std::move(alternate_name), first, last);
  dn->dir = this;
  dn->version = get_projected_version();
  dn->check_corruption(true);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  if (is_auth() || !inode->is_stray()) {
    mdcache->lru.lru_insert_mid(dn);
  } else {
    mdcache->bottom_lru.lru_insert_mid(dn);
    dn->state_set(CDentry::STATE_BOTTOMLRU);
  }

  // add to dir
  ceph_assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->get_name()) == 0);

  items[dn->key()] = dn;

  dn->get_linkage()->inode = in;

  link_inode_work(dn, in);

  if (dn->last == CEPH_NOSNAP)
    num_head_items++;
  else
    num_snap_items++;
  
  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

  dout(12) << __func__ << " " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  ceph_assert(get_num_any() == items.size());
  return dn;
}

CDentry* CDir::add_remote_dentry(std::string_view dname, inodeno_t ino, unsigned char d_type,
                                 mempool::mds_co::string alternate_name,
				 snapid_t first, snapid_t last) 
{
  // foreign
  ceph_assert(lookup_exact_snap(dname, last) == 0);

  // create dentry
  CDentry* dn = new CDentry(dname, inode->hash_dentry_name(dname), std::move(alternate_name), ino, d_type, first, last);
  dn->dir = this;
  dn->version = get_projected_version();
  dn->check_corruption(true);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  mdcache->lru.lru_insert_mid(dn);
  
  // add to dir
  ceph_assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->get_name()) == 0);

  items[dn->key()] = dn;
  if (last == CEPH_NOSNAP)
    num_head_items++;
  else
    num_snap_items++;

  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

  dout(12) << __func__ << " " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  
  ceph_assert(get_num_any() == items.size());
  return dn;
}



void CDir::remove_dentry(CDentry *dn) 
{
  dout(12) << __func__ << " " << *dn << dendl;

  // there should be no client leases at this point!
  ceph_assert(dn->client_lease_map.empty());

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
  ceph_assert(items.count(dn->key()) == 1);
  items.erase(dn->key());

  // clean?
  if (dn->is_dirty())
    dn->mark_clean();

  if (dn->state_test(CDentry::STATE_BOTTOMLRU))
    mdcache->bottom_lru.lru_remove(dn);
  else
    mdcache->lru.lru_remove(dn);
  delete dn;

  // unpin?
  if (get_num_any() == 0)
    put(PIN_CHILD);
  ceph_assert(get_num_any() == items.size());
}

void CDir::link_remote_inode(CDentry *dn, CInode *in)
{
  link_remote_inode(dn, in->ino(), IFTODT(in->get_projected_inode()->mode));
}

void CDir::link_remote_inode(CDentry *dn, inodeno_t ino, unsigned char d_type)
{
  dout(12) << __func__ << " " << *dn << " remote " << ino << dendl;
  ceph_assert(dn->get_linkage()->is_null());

  dn->get_linkage()->set_remote(ino, d_type);

  if (dn->state_test(CDentry::STATE_BOTTOMLRU)) {
    mdcache->bottom_lru.lru_remove(dn);
    mdcache->lru.lru_insert_mid(dn);
    dn->state_clear(CDentry::STATE_BOTTOMLRU);
  }

  if (dn->last == CEPH_NOSNAP) {
    num_head_items++;
    num_head_null--;
  } else {
    num_snap_items++;
    num_snap_null--;
  }
  ceph_assert(get_num_any() == items.size());
}

void CDir::link_primary_inode(CDentry *dn, CInode *in)
{
  dout(12) << __func__ << " " << *dn << " " << *in << dendl;
  ceph_assert(dn->get_linkage()->is_null());

  dn->get_linkage()->inode = in;

  link_inode_work(dn, in);

  if (dn->state_test(CDentry::STATE_BOTTOMLRU) &&
      (is_auth() || !inode->is_stray())) {
    mdcache->bottom_lru.lru_remove(dn);
    mdcache->lru.lru_insert_mid(dn);
    dn->state_clear(CDentry::STATE_BOTTOMLRU);
  }
  
  if (dn->last == CEPH_NOSNAP) {
    num_head_items++;
    num_head_null--;
  } else {
    num_snap_items++;
    num_snap_null--;
  }

  ceph_assert(get_num_any() == items.size());
}

void CDir::link_inode_work( CDentry *dn, CInode *in)
{
  ceph_assert(dn->get_linkage()->get_inode() == in);
  in->set_primary_parent(dn);

  // set inode version
  //in->inode.version = dn->get_version();
  
  // pin dentry?
  if (in->get_num_ref())
    dn->get(CDentry::PIN_INODEPIN);

  if (in->state_test(CInode::STATE_TRACKEDBYOFT))
    mdcache->open_file_table.notify_link(in);
  if (in->is_any_caps())
    adjust_num_inodes_with_caps(1);
  
  // adjust auth pin count
  if (in->auth_pins)
    dn->adjust_nested_auth_pins(in->auth_pins, NULL);

  if (in->is_freezing_inode())
    freezing_inodes.push_back(&in->item_freezing_inode);
  else if (in->is_frozen_inode() || in->is_frozen_auth_pin())
    num_frozen_inodes++;

  // verify open snaprealm parent
  if (in->snaprealm)
    in->snaprealm->adjust_parent();
  else if (in->is_any_caps())
    in->move_to_realm(inode->find_snaprealm());

  bool is_quiesced = inode->is_quiesced();
  if (is_quiesced) {
    mdcache->add_quiesce(inode, in);
  }
}

void CDir::unlink_inode(CDentry *dn, bool adjust_lru)
{
  if (dn->get_linkage()->is_primary()) {
    dout(12) << __func__ << " " << *dn << " " << *dn->get_linkage()->get_inode() << dendl;
  } else {
    dout(12) << __func__ << " " << *dn << dendl;
  }

  unlink_inode_work(dn);

  if (adjust_lru && !is_auth() &&
      !dn->state_test(CDentry::STATE_BOTTOMLRU)) {
    mdcache->lru.lru_remove(dn);
    mdcache->bottom_lru.lru_insert_mid(dn);
    dn->state_set(CDentry::STATE_BOTTOMLRU);
  }

  if (dn->last == CEPH_NOSNAP) {
    num_head_items--;
    num_head_null++;
  } else {
    num_snap_items--;
    num_snap_null++;
  }
  ceph_assert(get_num_any() == items.size());
}

void CDir::try_remove_unlinked_dn(CDentry *dn)
{
  ceph_assert(dn->dir == this);
  ceph_assert(dn->get_linkage()->is_null());
  
  // no pins (besides dirty)?
  if (dn->get_num_ref() != dn->is_dirty()) 
    return;

  // was the dn new?
  if (dn->is_new()) {
    dout(10) << __func__ << " " << *dn << " in " << *this << dendl;
    if (dn->is_dirty())
      dn->mark_clean();
    remove_dentry(dn);

    // NOTE: we may not have any more dirty dentries, but the fnode
    // still changed, so the directory must remain dirty.
  }
}


void CDir::unlink_inode_work(CDentry *dn)
{
  CInode *in = dn->get_linkage()->get_inode();

  if (dn->get_linkage()->is_remote()) {
    // remote
    if (in) 
      dn->unlink_remote(dn->get_linkage());

    dn->get_linkage()->set_remote(0, 0);
  } else if (dn->get_linkage()->is_primary()) {
    // primary
    // unpin dentry?
    if (in->get_num_ref())
      dn->put(CDentry::PIN_INODEPIN);

    if (in->state_test(CInode::STATE_TRACKEDBYOFT))
      mdcache->open_file_table.notify_unlink(in);
    if (in->is_any_caps())
      adjust_num_inodes_with_caps(-1);
    
    // unlink auth_pin count
    if (in->auth_pins)
      dn->adjust_nested_auth_pins(-in->auth_pins, nullptr);

    if (in->is_freezing_inode())
      in->item_freezing_inode.remove_myself();
    else if (in->is_frozen_inode() || in->is_frozen_auth_pin())
      num_frozen_inodes--;

    // detach inode
    in->remove_primary_parent(dn);
    if (in->is_dir())
      in->item_pop_lru.remove_myself();
    dn->get_linkage()->inode = 0;
  } else {
    ceph_assert(!dn->get_linkage()->is_null());
  }
}

void CDir::add_to_bloom(CDentry *dn)
{
  ceph_assert(dn->last == CEPH_NOSNAP);
  if (!bloom) {
    /* not create bloom filter for incomplete dir that was added by log replay */
    if (!is_complete())
      return;

    /* don't maintain bloom filters in standby replay (saves cycles, and also
     * avoids need to implement clearing it in EExport for #16924) */
    if (mdcache->mds->is_standby_replay()) {
      return;
    }

    unsigned size = get_num_head_items() + get_num_snap_items();
    if (size < 100) size = 100;
    bloom.reset(new bloom_filter(size, 1.0 / size, 0));
  }
  /* This size and false positive probability is completely random.*/
  bloom->insert(dn->get_name().data(), dn->get_name().size());
}

bool CDir::is_in_bloom(std::string_view name)
{
  if (!bloom)
    return false;
  return bloom->contains(name.data(), name.size());
}

void CDir::remove_null_dentries() {
  dout(12) << __func__ << " " << *this << dendl;

  auto p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    ++p;
    if (dn->get_linkage()->is_null() && !dn->is_projected())
      remove_dentry(dn);
  }

  ceph_assert(num_snap_null == 0);
  ceph_assert(num_head_null == 0);
  ceph_assert(get_num_any() == items.size());
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
  ceph_assert(get_parent_dir()->inode->is_stray());

  // clear dirty only when the directory was not snapshotted
  bool clear_dirty = !inode->snaprealm;

  auto p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    ++p;
    if (dn->last == CEPH_NOSNAP) {
      ceph_assert(!dn->is_projected());
      ceph_assert(dn->get_linkage()->is_null());
      if (clear_dirty && dn->is_dirty())
	dn->mark_clean();
      // It's OK to remove lease prematurely because we will never link
      // the dentry to inode again.
      if (dn->is_any_leases())
	dn->remove_client_leases(mdcache->mds->locker);
      if (dn->get_num_ref() == 0)
	remove_dentry(dn);
    } else {
      ceph_assert(!dn->is_projected());
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
	  mdcache->remove_inode(in);
      }
    }
  }

  if (clear_dirty && is_dirty())
    mark_clean();
}

bool CDir::try_trim_snap_dentry(CDentry *dn, const set<snapid_t>& snaps)
{
  if (dn->last == CEPH_NOSNAP) {
    return false;
  }
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
      mdcache->remove_inode(in);
    }
    return true;
  }
  return false;
}


/**
 * steal_dentry -- semi-violently move a dentry from one CDir to another
 * (*) violently, in that nitems, most pins, etc. are not correctly maintained 
 * on the old CDir corpse; must call finish_old_fragment() when finished.
 */
void CDir::steal_dentry(CDentry *dn)
{
  dout(15) << __func__ << " " << *dn << dendl;

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

    auto _fnode = _get_fnode();

    if (dn->get_linkage()->is_primary()) {
      CInode *in = dn->get_linkage()->get_inode();
      const auto& pi = in->get_projected_inode();
      if (in->is_dir()) {
	_fnode->fragstat.nsubdirs++;
	if (in->item_pop_lru.is_on_list())
	  pop_lru_subdirs.push_back(&in->item_pop_lru);
      } else {
	_fnode->fragstat.nfiles++;
      }
      _fnode->rstat.rbytes += pi->accounted_rstat.rbytes;
      _fnode->rstat.rfiles += pi->accounted_rstat.rfiles;
      _fnode->rstat.rsubdirs += pi->accounted_rstat.rsubdirs;
      _fnode->rstat.rsnaps += pi->accounted_rstat.rsnaps;
      if (pi->accounted_rstat.rctime > fnode->rstat.rctime)
	_fnode->rstat.rctime = pi->accounted_rstat.rctime;

      if (in->is_any_caps())
	adjust_num_inodes_with_caps(1);

      // move dirty inode rstat to new dirfrag
      if (in->is_dirty_rstat())
	dirty_rstat_inodes.push_back(&in->dirty_rstat_item);
    } else if (dn->get_linkage()->is_remote()) {
      if (dn->get_linkage()->get_remote_d_type() == DT_DIR)
	_fnode->fragstat.nsubdirs++;
      else
	_fnode->fragstat.nfiles++;
    }
  } else {
    num_snap_items++;
    if (dn->get_linkage()->is_primary()) {
      CInode *in = dn->get_linkage()->get_inode();
      if (in->is_dirty_rstat())
	dirty_rstat_inodes.push_back(&in->dirty_rstat_item);
    }
  }

  {
    int dap = dn->get_num_dir_auth_pins();
    if (dap) {
      adjust_nested_auth_pins(dap, NULL);
      dn->dir->adjust_nested_auth_pins(-dap, NULL);
    }
  }

  if (dn->is_dirty()) {
    dirty_dentries.push_back(&dn->item_dir_dirty);
    num_dirty++;
  }

  dn->dir = this;
}

void CDir::prepare_old_fragment(map<string_snap_t, MDSContext::vec >& dentry_waiters, bool replay)
{
  // auth_pin old fragment for duration so that any auth_pinning
  // during the dentry migration doesn't trigger side effects
  if (!replay && is_auth())
    auth_pin(this);

  if (!waiting_on_dentry.empty()) {
    for (const auto &p : waiting_on_dentry) {
      std::copy(p.second.begin(), p.second.end(),
		std::back_inserter(dentry_waiters[p.first]));
    }
    waiting_on_dentry.clear();
    put(PIN_DNWAITER);
  }
}

void CDir::prepare_new_fragment(bool replay)
{
  if (!replay && is_auth()) {
    _freeze_dir();
    mark_complete();
  }
  inode->add_dirfrag(this);
}

void CDir::finish_old_fragment(MDSContext::vec& waiters, bool replay)
{
  // take waiters _before_ unfreeze...
  if (!replay) {
    take_waiting(WAIT_ANY_MASK, waiters);
    if (is_auth()) {
      auth_unpin(this);  // pinned in prepare_old_fragment
      ceph_assert(is_frozen_dir());
      unfreeze_dir();
    }
  }

  ceph_assert(dir_auth_pins == 0);
  ceph_assert(auth_pins == 0);

  num_head_items = num_head_null = 0;
  num_snap_items = num_snap_null = 0;
  adjust_num_inodes_with_caps(-num_inodes_with_caps);

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

  ceph_assert(get_num_ref() == (state_test(STATE_STICKY) ? 1:0));
}

void CDir::init_fragment_pins()
{
  if (is_replicated())
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

void CDir::split(int bits, std::vector<CDir*>* subs, MDSContext::vec& waiters, bool replay)
{
  dout(10) << "split by " << bits << " bits on " << *this << dendl;

  ceph_assert(replay || is_complete() || !is_auth());

  frag_vec_t frags;
  frag.split(bits, frags);

  vector<CDir*> subfrags(1 << bits);
  
  double fac = 1.0 / (double)(1 << bits);  // for scaling load vecs

  version_t rstat_version = inode->get_projected_inode()->rstat.version;
  version_t dirstat_version = inode->get_projected_inode()->dirstat.version;

  nest_info_t rstatdiff;
  frag_info_t fragstatdiff;
  if (fnode->accounted_rstat.version == rstat_version)
    rstatdiff.add_delta(fnode->accounted_rstat, fnode->rstat);
  if (fnode->accounted_fragstat.version == dirstat_version)
    fragstatdiff.add_delta(fnode->accounted_fragstat, fnode->fragstat);
  dout(10) << " rstatdiff " << rstatdiff << " fragstatdiff " << fragstatdiff << dendl;

  map<string_snap_t, MDSContext::vec > dentry_waiters;
  prepare_old_fragment(dentry_waiters, replay);

  // create subfrag dirs
  int n = 0;
  for (const auto& fg : frags) {
    CDir *f = new CDir(inode, fg, mdcache, is_auth());
    f->state_set(state & (MASK_STATE_FRAGMENT_KEPT | STATE_COMPLETE));
    f->get_replicas() = get_replicas();
    f->pop_me = pop_me;
    f->pop_me.scale(fac);

    // FIXME; this is an approximation
    f->pop_nested = pop_nested;
    f->pop_nested.scale(fac);
    f->pop_auth_subtree = pop_auth_subtree;
    f->pop_auth_subtree.scale(fac);
    f->pop_auth_subtree_nested = pop_auth_subtree_nested;
    f->pop_auth_subtree_nested.scale(fac);

    dout(10) << " subfrag " << fg << " " << *f << dendl;
    subfrags[n++] = f;
    subs->push_back(f);

    f->set_dir_auth(get_dir_auth());
    f->freeze_tree_state = freeze_tree_state;
    f->prepare_new_fragment(replay);
    f->init_fragment_pins();
  }
  
  // repartition dentries
  while (!items.empty()) {
    auto p = items.begin();
    
    CDentry *dn = p->second;
    frag_t subfrag = inode->pick_dirfrag(dn->get_name());
    int n = (subfrag.value() & (subfrag.mask() ^ frag.mask())) >> subfrag.mask_shift();
    dout(15) << " subfrag " << subfrag << " n=" << n << " for " << p->first << dendl;
    CDir *f = subfrags[n];
    f->steal_dentry(dn);
  }

  for (const auto &p : dentry_waiters) {
    frag_t subfrag = inode->pick_dirfrag(p.first.name);
    int n = (subfrag.value() & (subfrag.mask() ^ frag.mask())) >> subfrag.mask_shift();
    CDir *f = subfrags[n];

    if (f->waiting_on_dentry.empty())
      f->get(PIN_DNWAITER);
    std::copy(p.second.begin(), p.second.end(),
	      std::back_inserter(f->waiting_on_dentry[p.first]));
  }

  // FIXME: handle dirty old rstat

  // fix up new frag fragstats
  for (int i = 0; i < n; i++) {
    CDir *f = subfrags[i];
    auto _fnode = f->_get_fnode();
    _fnode->version = f->projected_version = get_version();
    _fnode->rstat.version = rstat_version;
    _fnode->accounted_rstat = _fnode->rstat;
    _fnode->fragstat.version = dirstat_version;
    _fnode->accounted_fragstat = _fnode->fragstat;
    dout(10) << " rstat " << _fnode->rstat << " fragstat " << _fnode->fragstat
	     << " on " << *f << dendl;

    if (i == 0) {
      // give any outstanding frag stat differential to first frag
      dout(10) << " giving rstatdiff " << rstatdiff << " fragstatdiff" << fragstatdiff
	       << " to " << *subfrags[0] << dendl;
      _fnode->accounted_rstat.add(rstatdiff);
      _fnode->accounted_fragstat.add(fragstatdiff);
    }
  }

  finish_old_fragment(waiters, replay);
}

void CDir::merge(const std::vector<CDir*>& subs, MDSContext::vec& waiters, bool replay)
{
  dout(10) << "merge " << subs << dendl;

  ceph_assert(subs.size() > 0);

  set_dir_auth(subs.front()->get_dir_auth());
  freeze_tree_state = subs.front()->freeze_tree_state;

  for (const auto& dir : subs) {
    ceph_assert(get_dir_auth() == dir->get_dir_auth());
    ceph_assert(freeze_tree_state == dir->freeze_tree_state);
  }

  prepare_new_fragment(replay);

  auto _fnode = _get_fnode();

  nest_info_t rstatdiff;
  frag_info_t fragstatdiff;
  bool touched_mtime, touched_chattr;
  version_t rstat_version = inode->get_projected_inode()->rstat.version;
  version_t dirstat_version = inode->get_projected_inode()->dirstat.version;

  map<string_snap_t, MDSContext::vec > dentry_waiters;

  for (const auto& dir : subs) {
    dout(10) << " subfrag " << dir->get_frag() << " " << *dir << dendl;
    ceph_assert(!dir->is_auth() || dir->is_complete() || replay);

    if (dir->get_fnode()->accounted_rstat.version == rstat_version)
      rstatdiff.add_delta(dir->get_fnode()->accounted_rstat, dir->get_fnode()->rstat);
    if (dir->get_fnode()->accounted_fragstat.version == dirstat_version)
      fragstatdiff.add_delta(dir->get_fnode()->accounted_fragstat, dir->get_fnode()->fragstat,
			     &touched_mtime, &touched_chattr);

    dir->prepare_old_fragment(dentry_waiters, replay);

    // steal dentries
    while (!dir->items.empty()) 
      steal_dentry(dir->items.begin()->second);
    
    // merge replica map
    for (const auto &p : dir->get_replicas()) {
      unsigned cur = get_replicas()[p.first];
      if (p.second > cur)
	get_replicas()[p.first] = p.second;
    }

    // merge version
    if (dir->get_version() > _fnode->version)
      _fnode->version = projected_version = dir->get_version();

    // merge state
    state_set(dir->get_state() & MASK_STATE_FRAGMENT_KEPT);

    dir->finish_old_fragment(waiters, replay);
    inode->close_dirfrag(dir->get_frag());
  }

  if (!dentry_waiters.empty()) {
    get(PIN_DNWAITER);
    for (const auto &p : dentry_waiters) {
      std::copy(p.second.begin(), p.second.end(),
		std::back_inserter(waiting_on_dentry[p.first]));
    }
  }

  if (is_auth() && !replay)
    mark_complete();

  // FIXME: merge dirty old rstat
  _fnode->rstat.version = rstat_version;
  _fnode->accounted_rstat = _fnode->rstat;
  _fnode->accounted_rstat.add(rstatdiff);

  _fnode->fragstat.version = dirstat_version;
  _fnode->accounted_fragstat = _fnode->fragstat;
  _fnode->accounted_fragstat.add(fragstatdiff);

  init_fragment_pins();
}




void CDir::resync_accounted_fragstat()
{
  auto pf = _get_projected_fnode();
  const auto& pi = inode->get_projected_inode();

  if (pf->accounted_fragstat.version != pi->dirstat.version) {
    pf->fragstat.version = pi->dirstat.version;
    dout(10) << __func__ << " " << pf->accounted_fragstat << " -> " << pf->fragstat << dendl;
    pf->accounted_fragstat = pf->fragstat;
  }
}

/*
 * resync rstat and accounted_rstat with inode
 */
void CDir::resync_accounted_rstat()
{
  auto pf = _get_projected_fnode();
  const auto& pi = inode->get_projected_inode();
  
  if (pf->accounted_rstat.version != pi->rstat.version) {
    pf->rstat.version = pi->rstat.version;
    dout(10) << __func__ << " " << pf->accounted_rstat << " -> " << pf->rstat << dendl;
    pf->accounted_rstat = pf->rstat;
    dirty_old_rstat.clear();
  }
}

void CDir::assimilate_dirty_rstat_inodes(MutationRef& mut)
{
  dout(10) << __func__ << dendl;
  for (elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
       !p.end(); ++p) {
    CInode *in = *p;
    ceph_assert(in->is_auth());
    if (in->is_frozen())
      continue;

    mut->auth_pin(in);

    auto pi = in->project_inode(mut);
    pi.inode->version = in->pre_dirty();

    mdcache->project_rstat_inode_to_frag(mut, in, this, 0, 0, nullptr);
  }
  state_set(STATE_ASSIMRSTAT);
  dout(10) << __func__ << " done" << dendl;
}

void CDir::assimilate_dirty_rstat_inodes_finish(EMetaBlob *blob)
{
  if (!state_test(STATE_ASSIMRSTAT))
    return;
  state_clear(STATE_ASSIMRSTAT);
  dout(10) << __func__ << dendl;
  elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
  while (!p.end()) {
    CInode *in = *p;
    ++p;

    if (in->is_frozen())
      continue;

    CDentry *dn = in->get_projected_parent_dn();

    in->clear_dirty_rstat();
    blob->add_primary_dentry(dn, in, true);
  }

  if (!dirty_rstat_inodes.empty())
    mdcache->mds->locker->mark_updated_scatterlock(&inode->nestlock);
}




/****************************************
 * WAITING
 */

void CDir::add_dentry_waiter(std::string_view dname, snapid_t snapid, MDSContext *c) 
{
  if (waiting_on_dentry.empty())
    get(PIN_DNWAITER);
  waiting_on_dentry[string_snap_t(dname, snapid)].push_back(c);
  dout(10) << __func__ << " dentry " << dname
	   << " snap " << snapid
	   << " " << c << " on " << *this << dendl;
}

void CDir::take_dentry_waiting(std::string_view dname, snapid_t first, snapid_t last,
			       MDSContext::vec& ls)
{
  if (waiting_on_dentry.empty())
    return;
  
  string_snap_t lb(dname, first);
  string_snap_t ub(dname, last);
  auto it = waiting_on_dentry.lower_bound(lb);
  while (it != waiting_on_dentry.end() &&
	 !(ub < it->first)) {
    dout(10) << __func__ << " " << dname
	     << " [" << first << "," << last << "] found waiter on snap "
	     << it->first.snapid
	     << " on " << *this << dendl;
    std::copy(it->second.begin(), it->second.end(), std::back_inserter(ls));
    waiting_on_dentry.erase(it++);
  }

  if (waiting_on_dentry.empty())
    put(PIN_DNWAITER);
}

void CDir::add_waiter(uint64_t tag, MDSContext *c) 
{
  // hierarchical?
  
  // at subtree root?
  if (tag & WAIT_ATSUBTREEROOT) {
    if (!is_subtree_root()) {
      // try parent
      dout(10) << "add_waiter " << std::hex << tag << std::dec << " " << c << " should be ATSUBTREEROOT, " << *this << " is not root, trying parent" << dendl;
      inode->parent->dir->add_waiter(tag, c);
      return;
    }
  }

  ceph_assert(!(tag & WAIT_CREATED) || state_test(STATE_CREATING));

  MDSCacheObject::add_waiter(tag, c);
}



/* NOTE: this checks dentry waiters too */
void CDir::take_waiting(uint64_t mask, MDSContext::vec& ls)
{
  if ((mask & WAIT_DENTRY) && !waiting_on_dentry.empty()) {
    // take all dentry waiters
    for (const auto &p : waiting_on_dentry) {
      dout(10) << "take_waiting dentry " << p.first.name
	       << " snap " << p.first.snapid << " on " << *this << dendl;
      std::copy(p.second.begin(), p.second.end(), std::back_inserter(ls));
    }
    waiting_on_dentry.clear();
    put(PIN_DNWAITER);
  }
  
  // waiting
  MDSCacheObject::take_waiting(mask, ls);
}


void CDir::finish_waiting(uint64_t mask, int result) 
{
  dout(11) << __func__ << " mask " << hex << mask << dec << " result " << result << " on " << *this << dendl;

  MDSContext::vec finished;
  take_waiting(mask, finished);
  if (result < 0)
    finish_contexts(g_ceph_context, finished, result);
  else
    mdcache->mds->queue_waiters(finished);
}



// dirty/clean

CDir::fnode_ptr CDir::project_fnode(const MutationRef& mut)
{
  ceph_assert(get_version() != 0);

  if (mut && mut->is_projected(this))
    return std::const_pointer_cast<fnode_t>(projected_fnode.back());

  auto pf = allocate_fnode(*get_projected_fnode());

  if (scrub_infop && scrub_infop->last_scrub_dirty) {
    pf->localized_scrub_stamp = scrub_infop->last_local.time;
    pf->localized_scrub_version = scrub_infop->last_local.version;
    pf->recursive_scrub_stamp = scrub_infop->last_recursive.time;
    pf->recursive_scrub_version = scrub_infop->last_recursive.version;
    scrub_infop->last_scrub_dirty = false;
    scrub_maybe_delete_info();
  }

  projected_fnode.emplace_back(pf);
  if (mut)
    mut->add_projected_node(this);
  dout(10) << __func__ <<  " " << pf.get() << dendl;
  return pf;
}

void CDir::pop_and_dirty_projected_fnode(LogSegment *ls, const MutationRef& mut)
{
  ceph_assert(!projected_fnode.empty());
  auto pf = std::move(projected_fnode.front());
  dout(15) << __func__ << " " << pf.get() << " v" << pf->version << dendl;

  projected_fnode.pop_front();
  if (mut)
    mut->remove_projected_node(this);

  reset_fnode(std::move(pf));
  _mark_dirty(ls);
}

version_t CDir::pre_dirty(version_t min)
{
  if (min > projected_version)
    projected_version = min;
  ++projected_version;
  dout(10) << __func__ << " " << projected_version << dendl;
  return projected_version;
}

void CDir::mark_dirty(LogSegment *ls, version_t pv)
{
  ceph_assert(is_auth());

  if (pv) {
    ceph_assert(get_version() < pv);
    ceph_assert(pv <= projected_version);
    ceph_assert(!projected_fnode.empty() &&
	        pv <= projected_fnode.front()->version);
  }

  _mark_dirty(ls);
}

void CDir::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    dout(10) << __func__ << " (was clean) " << *this << " version " << get_version() << dendl;
    _set_dirty_flag();
    ceph_assert(ls);
  } else {
    dout(10) << __func__ << " (already dirty) " << *this << " version " << get_version() << dendl;
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
  state_clear(STATE_CREATING);

  MDSContext::vec waiters;
  take_waiting(CDir::WAIT_CREATED, waiters);
  mdcache->mds->queue_waiters(waiters);
}

void CDir::set_fresh_fnode(fnode_const_ptr&& ptr) {
  ceph_assert(inode->is_auth());
  ceph_assert(!is_projected());
  ceph_assert(!state_test(STATE_COMMITTING));
  reset_fnode(std::move(ptr));
  projected_version = committing_version = committed_version = get_version();

  if (state_test(STATE_REJOINUNDEF)) {
    ceph_assert(mdcache->mds->is_rejoin());
    state_clear(STATE_REJOINUNDEF);
    mdcache->opened_undef_dirfrag(this);
  }
}

void CDir::mark_clean()
{
  dout(10) << __func__ << " " << *this << " version " << get_version() << dendl;
  if (state_test(STATE_DIRTY)) {
    item_dirty.remove_myself();
    item_new.remove_myself();

    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
  }
}

// caller should hold auth pin of this
void CDir::log_mark_dirty()
{
  if (is_dirty() || projected_version > get_version())
    return; // noop if it is already dirty or will be dirty

  auto _fnode = allocate_fnode(*get_fnode());
  _fnode->version = pre_dirty();
  reset_fnode(std::move(_fnode));
  mark_dirty(mdcache->mds->mdlog->get_current_segment());
}

void CDir::mark_complete() {
  state_set(STATE_COMPLETE);
  bloom.reset();
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
void CDir::fetch(std::string_view dname, snapid_t last,
		 MDSContext *c, bool ignore_authpinnability)
{
  if (dname.empty())
    dout(10) << "fetch on " << *this << dendl;
  else
    dout(10) << "fetch key(" << dname << ", '" << last << "')" << dendl;
  
  ceph_assert(is_auth());
  ceph_assert(!is_complete());

  if (!ignore_authpinnability && !can_auth_pin()) {
    if (c) {
      dout(7) << "fetch waiting for authpinnable" << dendl;
      add_waiter(WAIT_UNFREEZE, c);
    } else
      dout(7) << "fetch not authpinnable and no context" << dendl;
    return;
  }

  // unlinked directory inode shouldn't have any entry
  if (CDir *pdir = get_parent_dir();
      pdir && pdir->inode->is_stray() && !inode->snaprealm) {
    dout(7) << "fetch dirfrag for unlinked directory, mark complete" << dendl;
    if (get_version() == 0) {
      auto _fnode = allocate_fnode();
      _fnode->version = 1;
      set_fresh_fnode(std::move(_fnode));
    }
    mark_complete();

    if (c)
      mdcache->mds->queue_waiter(c);
    return;
  }

  // FIXME: to fetch a snap dentry, we need to get omap key in range
  //       [(name, last), (name, CEPH_NOSNAP))
  if (!dname.empty() && last == CEPH_NOSNAP && !g_conf().get_val<bool>("mds_dir_prefetch")) {
    dentry_key_t key(last, dname, inode->hash_dentry_name(dname));
    fetch_keys({key}, c);
    return;
  }

  if (c)
    add_waiter(WAIT_COMPLETE, c);
  
  // already fetching?
  if (state_test(CDir::STATE_FETCHING)) {
    dout(7) << "already fetching; waiting" << dendl;
    return;
  }

  auth_pin(this);
  state_set(CDir::STATE_FETCHING);

  _omap_fetch(nullptr, nullptr);

  if (mdcache->mds->logger)
    mdcache->mds->logger->inc(l_mds_dir_fetch_complete);
  mdcache->mds->balancer->hit_dir(this, META_POP_FETCH);
}

void CDir::fetch_keys(const std::vector<dentry_key_t>& keys, MDSContext *c)
{
  dout(10) << __func__ << " " << keys.size() << " keys on " << *this << dendl;
  ceph_assert(is_auth());
  ceph_assert(!is_complete());

  if (CDir *pdir = get_parent_dir();
      pdir && pdir->inode->is_stray() && !inode->snaprealm) {
    fetch(c, true);
    return;
  }

  MDSContext::vec_alloc<mempool::mds_co::pool_allocator> *fallback_waiting = nullptr;
  std::set<std::string> str_keys;
  for (auto& key : keys) {
    ceph_assert(key.snapid == CEPH_NOSNAP);
    if (waiting_on_dentry.empty())
      get(PIN_DNWAITER);
    auto em = waiting_on_dentry.emplace(std::piecewise_construct,
					std::forward_as_tuple(key.name, key.snapid),
					std::forward_as_tuple());
    if (!em.second) {
      if (!fallback_waiting)
	fallback_waiting = &em.first->second;
      continue;
    }

    if (c) {
      em.first->second.push_back(c);
      c = nullptr;
    }

    string str;
    key.encode(str);
    str_keys.emplace(std::move(str));
  }

  if (str_keys.empty()) {
    if (c && fallback_waiting) {
      fallback_waiting->push_back(c);
      c = nullptr;
    }

    if (get_version() > 0) {
      dout(7) << "fetch keys, all are already being fetched" << dendl;
      ceph_assert(!c);
      return;
    }
  }

  if (state_test(CDir::STATE_FETCHING)) {
    dout(7) << "fetch keys, waiting for full fetch" << dendl;
    if (c)
      add_waiter(WAIT_COMPLETE, c);
    return;
  }

  auth_pin(this);
  _omap_fetch(&str_keys, c);

  if (mdcache->mds->logger)
    mdcache->mds->logger->inc(l_mds_dir_fetch_keys);
  mdcache->mds->balancer->hit_dir(this, META_POP_FETCH);
}

class C_IO_Dir_OMAP_FetchedMore : public CDirIOContext {
  MDSContext *fin;
public:
  const version_t omap_version;
  bufferlist hdrbl;
  bool more = false;
  map<string, bufferlist> omap;      ///< carry-over from before
  map<string, bufferlist> omap_more; ///< new batch
  int ret;
  C_IO_Dir_OMAP_FetchedMore(CDir *d, version_t v, MDSContext *f) :
    CDirIOContext(d), fin(f), omap_version(v), ret(0) { }
  void finish(int r) {
    if (omap_version < dir->get_committed_version()) {
      omap.clear();
      dir->_omap_fetch(nullptr, fin);
      return;
    }

    // merge results
    if (omap.empty()) {
      omap.swap(omap_more);
    } else {
      omap.insert(omap_more.begin(), omap_more.end());
    }
    if (more) {
      dir->_omap_fetch_more(omap_version, hdrbl, omap, fin);
    } else {
      dir->_omap_fetched(hdrbl, omap, true, {}, r);
      if (fin)
	fin->complete(r);
    }
  }
  void print(ostream& out) const override {
    out << "dirfrag_fetch_more(" << dir->dirfrag() << ")";
  }
};

class C_IO_Dir_OMAP_Fetched : public CDirIOContext {
  MDSContext *fin;
public:
  const version_t omap_version;
  bool complete = true;
  std::set<string> keys;
  bufferlist hdrbl;
  bool more = false;
  map<string, bufferlist> omap;
  bufferlist btbl;
  int ret1, ret2, ret3;

  C_IO_Dir_OMAP_Fetched(CDir *d, MDSContext *f) :
    CDirIOContext(d), fin(f),
    omap_version(d->get_committing_version()),
    ret1(0), ret2(0), ret3(0) { }
  void finish(int r) override {
    // check the correctness of backtrace
    if (r >= 0 && ret3 != -CEPHFS_ECANCELED)
      dir->inode->verify_diri_backtrace(btbl, ret3);
    if (r >= 0) r = ret1;
    if (r >= 0) r = ret2;

    if (more) {
      if (omap_version < dir->get_committed_version()) {
	dir->_omap_fetch(nullptr, fin);
      } else {
	dir->_omap_fetch_more(omap_version, hdrbl, omap, fin);
      }
      return;
    }

    dir->_omap_fetched(hdrbl, omap, complete, keys, r);
    if (fin)
      fin->complete(r);
  }
  void print(ostream& out) const override {
    out << "dirfrag_fetch(" << dir->dirfrag() << ")";
  }
};

void CDir::_omap_fetch(std::set<string> *keys, MDSContext *c)
{
  C_IO_Dir_OMAP_Fetched *fin = new C_IO_Dir_OMAP_Fetched(this, c);
  object_t oid = get_ondisk_object();
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());
  ObjectOperation rd;
  rd.omap_get_header(&fin->hdrbl, &fin->ret1);
  if (keys) {
    fin->complete = false;
    fin->keys.swap(*keys);
    rd.omap_get_vals_by_keys(fin->keys, &fin->omap, &fin->ret2);
  } else {
    ceph_assert(!c);
    rd.omap_get_vals("", "", g_conf()->mds_dir_keys_per_op,
		     &fin->omap, &fin->more, &fin->ret2);
  }
  // check the correctness of backtrace
  if (g_conf()->mds_verify_backtrace > 0 && frag == frag_t()) {
    rd.getxattr("parent", &fin->btbl, &fin->ret3);
    rd.set_last_op_flags(CEPH_OSD_OP_FLAG_FAILOK);
  } else {
    fin->ret3 = -CEPHFS_ECANCELED;
  }

  mdcache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, NULL, 0,
			     new C_OnFinisher(fin, mdcache->mds->finisher));
}

void CDir::_omap_fetch_more(version_t omap_version, bufferlist& hdrbl,
			    map<string, bufferlist>& omap, MDSContext *c)
{
  // we have more omap keys to fetch!
  object_t oid = get_ondisk_object();
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());
  auto fin = new C_IO_Dir_OMAP_FetchedMore(this, omap_version, c);
  fin->hdrbl = std::move(hdrbl);
  fin->omap.swap(omap);
  ObjectOperation rd;
  rd.omap_get_vals(fin->omap.rbegin()->first,
		   "", /* filter prefix */
		   g_conf()->mds_dir_keys_per_op,
		   &fin->omap_more,
		   &fin->more,
		   &fin->ret);
  mdcache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, NULL, 0,
			     new C_OnFinisher(fin, mdcache->mds->finisher));
}

CDentry *CDir::_load_dentry(
    std::string_view key,
    std::string_view dname,
    const snapid_t last,
    bufferlist &bl,
    const int pos,
    const std::set<snapid_t> *snaps,
    double rand_threshold,
    bool *force_dirty)
{
  auto q = bl.cbegin();

  snapid_t first;
  decode(first, q);

  // marker
  char type;
  decode(type, q);

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

  if (type == 'L' || type == 'l') {
    // hard link
    inodeno_t ino;
    unsigned char d_type;
    mempool::mds_co::string alternate_name;

    CDentry::decode_remote(type, ino, d_type, alternate_name, q);

    if (stale) {
      if (!dn) {
        stale_items.insert(mempool::mds_co::string(key));
        *force_dirty = true;
      }
      return dn;
    }

    if (dn) {
      CDentry::linkage_t *dnl = dn->get_linkage();
      dout(12) << "_fetched had " << (dnl->is_null() ? "NEG" : "") << " dentry " << *dn << dendl;
      if (committed_version == 0 &&
	  dnl->is_remote() &&
	  dn->is_dirty() &&
	  ino == dnl->get_remote_ino() &&
	  d_type == dnl->get_remote_d_type() &&
          alternate_name == dn->get_alternate_name()) {
	// see comment below
	dout(10) << "_fetched  had underwater dentry " << *dn << ", marking clean" << dendl;
	dn->mark_clean();
      }
    } else {
      // (remote) link
      dn = add_remote_dentry(dname, ino, d_type, std::move(alternate_name), first, last);

      // link to inode?
      CInode *in = mdcache->get_inode(ino);   // we may or may not have it.
      if (in) {
        dn->link_remote(dn->get_linkage(), in);
        dout(12) << "_fetched  got remote link " << ino << " which we have " << *in << dendl;
      } else {
        dout(12) << "_fetched  got remote link " << ino << " (don't have it)" << dendl;
      }
    }
  }
  else if (type == 'I' || type == 'i') {
    InodeStore inode_data;
    mempool::mds_co::string alternate_name;
    // inode
    // Load inode data before looking up or constructing CInode
    if (type == 'i') {
      DECODE_START(2, q);
      if (struct_v >= 2) {
        decode(alternate_name, q);
      }
      inode_data.decode(q);
      DECODE_FINISH(q);
    } else {
      inode_data.decode_bare(q);
    }

    if (stale) {
      if (!dn) {
        stale_items.insert(mempool::mds_co::string(key));
        *force_dirty = true;
      }
      return dn;
    }

    bool undef_inode = false;
    if (dn) {
      CDentry::linkage_t *dnl = dn->get_linkage();
      dout(12) << "_fetched had " << (dnl->is_null() ? "NEG" : "") << " dentry " << *dn << dendl;

      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	if (in->state_test(CInode::STATE_REJOINUNDEF)) {
	  undef_inode = true;
	} else if (committed_version == 0 &&
		   dn->is_dirty() &&
		   inode_data.inode->ino == in->ino() &&
		   inode_data.inode->version == in->get_version()) {
	  /* clean underwater item?
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
	  dout(10) << "_fetched  had underwater dentry " << *dn << ", marking clean" << dendl;
	  dn->mark_clean();
	  dout(10) << "_fetched  had underwater inode " << *dnl->get_inode() << ", marking clean" << dendl;
	  in->mark_clean();
	}
      }
    }

    if (!dn || undef_inode) {
      // add inode
      CInode *in = mdcache->get_inode(inode_data.inode->ino, last);
      if (!in || undef_inode) {
        if (undef_inode && in)
          in->first = first;
        else
          in = new CInode(mdcache, true, first, last);
        
        in->reset_inode(std::move(inode_data.inode));
        in->reset_xattrs(std::move(inode_data.xattrs));
        // symlink?
        if (in->is_symlink()) 
          in->symlink = inode_data.symlink;
        
        in->dirfragtree.swap(inode_data.dirfragtree);
        in->reset_old_inodes(std::move(inode_data.old_inodes));
        if (in->is_any_old_inodes()) {
	  snapid_t min_first = in->get_old_inodes()->rbegin()->first + 1;
	  if (min_first > in->first)
	    in->first = min_first;
	}

        in->oldest_snap = inode_data.oldest_snap;
        in->decode_snap_blob(inode_data.snap_blob);
        if (snaps && !in->snaprealm)
          in->purge_stale_snap_data(*snaps);

        if (!undef_inode) {
          mdcache->add_inode(in); // add
	  mdcache->insert_taken_inos(in->ino());
          dn = add_primary_dentry(dname, in, std::move(alternate_name), first, last); // link
        }
        dout(12) << "_fetched  got " << *dn << " " << *in << dendl;

        if (in->get_inode()->is_dirty_rstat())
          in->mark_dirty_rstat();

        in->maybe_ephemeral_rand(rand_threshold);
        //in->hack_accessed = false;
        //in->hack_load_stamp = ceph_clock_now();
        //num_new_inodes_loaded++;
      } else if (g_conf().get_val<bool>("mds_hack_allow_loading_invalid_metadata")) {
	dout(20) << "hack: adding duplicate dentry for " << *in << dendl;
	dn = add_primary_dentry(dname, in, std::move(alternate_name), first, last);
      } else {
        dout(0) << "_fetched  badness: got (but i already had) " << *in
                << " mode " << in->get_inode()->mode
                << " mtime " << in->get_inode()->mtime << dendl;
        string dirpath, inopath;
        this->inode->make_path_string(dirpath);
        in->make_path_string(inopath);
        mdcache->mds->clog->error() << "loaded dup inode " << inode_data.inode->ino
          << " [" << first << "," << last << "] v" << inode_data.inode->version
          << " at " << dirpath << "/" << dname
          << ", but inode " << in->vino() << " v" << in->get_version()
	  << " already exists at " << inopath;
        return dn;
      }
    }
  } else {
    CachedStackStringStream css;
    *css << "Invalid tag char '" << type << "' pos " << pos;
    throw buffer::malformed_input(css->str());
  }

  return dn;
}

void CDir::_omap_fetched(bufferlist& hdrbl, map<string, bufferlist>& omap,
			 bool complete, const std::set<string>& keys, int r)
{
  LogChannelRef clog = mdcache->mds->clog;
  dout(10) << "_fetched header " << hdrbl.length() << " bytes "
	   << omap.size() << " keys for " << *this << dendl;

  ceph_assert(r == 0 || r == -CEPHFS_ENOENT || r == -CEPHFS_ENODATA);
  ceph_assert(is_auth());
  ceph_assert(!is_frozen());

  if (hdrbl.length() == 0) {
    dout(0) << "_fetched missing object for " << *this << dendl;

    clog->error() << "dir " << dirfrag() << " object missing on disk; some "
                     "files may be lost (" << get_path() << ")";

    go_bad(complete);
    return;
  }

  fnode_t got_fnode;
  {
    auto p = hdrbl.cbegin();
    try {
      decode(got_fnode, p);
    } catch (const buffer::error &err) {
      derr << "Corrupt fnode in dirfrag " << dirfrag()
	   << ": " << err.what() << dendl;
      clog->warn() << "Corrupt fnode header in " << dirfrag() << ": "
		   << err.what() << " (" << get_path() << ")";
      go_bad(complete);
      return;
    }
    if (!p.end()) {
      clog->warn() << "header buffer of dir " << dirfrag() << " has "
		  << hdrbl.length() - p.get_off() << " extra bytes ("
                  << get_path() << ")";
      go_bad(complete);
      return;
    }
  }

  dout(10) << "_fetched version " << got_fnode.version << dendl;
  
  // take the loaded fnode?
  // only if we are a fresh CDir* with no prior state.
  if (get_version() == 0) {
    set_fresh_fnode(allocate_fnode(got_fnode));
  }

  list<CInode*> undef_inodes;

  // purge stale snaps?
  bool force_dirty = false;
  const set<snapid_t> *snaps = NULL;
  SnapRealm *realm = inode->find_snaprealm();
  if (fnode->snap_purged_thru < realm->get_last_destroyed()) {
    snaps = &realm->get_snaps();
    dout(10) << " snap_purged_thru " << fnode->snap_purged_thru
	     << " < " << realm->get_last_destroyed()
	     << ", snap purge based on " << *snaps << dendl;
    if (get_num_snap_items() == 0) {
      const_cast<snapid_t&>(fnode->snap_purged_thru) = realm->get_last_destroyed();
      force_dirty = true;
    }
  }


  MDSContext::vec finished;
  std::vector<string_snap_t> null_keys;

  auto k_it = keys.rbegin();
  auto w_it = waiting_on_dentry.rbegin();
  std::string_view last_name = "";

  auto proc_waiters = [&](const string_snap_t& key) {
    bool touch = false;
    if (last_name < key.name) {
      // string_snap_t and key string are not in the same order
      w_it = decltype(w_it)(waiting_on_dentry.upper_bound(key));
    }
    while (w_it != waiting_on_dentry.rend()) {
      int cmp = w_it->first.compare(key);
      if (cmp < 0)
	break;
      if (cmp == 0) {
	touch = true;
	std::copy(w_it->second.begin(), w_it->second.end(),
		  std::back_inserter(finished));
	waiting_on_dentry.erase(std::next(w_it).base());
	if (waiting_on_dentry.empty())
	  put(PIN_DNWAITER);
	break;
      }
      ++w_it;
    }
    return touch;
  };
  auto proc_nulls_and_waiters = [&](const string& str_key, const string_snap_t& key) {
    bool touch = false;
    int count = 0;

    while (k_it != keys.rend()) {
      int cmp = k_it->compare(str_key);
      if (cmp < 0)
	break;
      if (cmp == 0) {
	touch = true;
	proc_waiters(key);
	++k_it;
	break;
      }
      string_snap_t n_key;
      dentry_key_t::decode_helper(*k_it, n_key.name, n_key.snapid);
      ceph_assert(n_key.snapid == CEPH_NOSNAP);
      proc_waiters(n_key);
      last_name = std::string_view(k_it->c_str(), n_key.name.length());
      null_keys.emplace_back(std::move(n_key));
      ++k_it;

      if (!(++count % mdcache->mds->heartbeat_reset_grace()))
        mdcache->mds->heartbeat_reset();
    }
    return touch;
  };

  int count = 0;
  unsigned pos = omap.size() - 1;
  double rand_threshold = get_inode()->get_ephemeral_rand();
  for (auto p = omap.rbegin(); p != omap.rend(); ++p, --pos) {
    string_snap_t key;
    dentry_key_t::decode_helper(p->first, key.name, key.snapid);
    bool touch = false;

    if (key.snapid == CEPH_NOSNAP) {
      if (complete) {
	touch = proc_waiters(key);
      } else {
	touch = proc_nulls_and_waiters(p->first, key);
      }
      last_name = std::string_view(p->first.c_str(), key.name.length());
    }

    if (!(++count % mdcache->mds->heartbeat_reset_grace()))
      mdcache->mds->heartbeat_reset();

    CDentry *dn = nullptr;
    try {
      dn = _load_dentry(
            p->first, key.name, key.snapid, p->second, pos, snaps,
            rand_threshold, &force_dirty);
    } catch (const buffer::error &err) {
      mdcache->mds->clog->warn() << "Corrupt dentry '" << key.name << "' in "
                                  "dir frag " << dirfrag() << ": "
                               << err.what() << "(" << get_path() << ")";

      // Remember that this dentry is damaged.  Subsequent operations
      // that try to act directly on it will get their CEPHFS_EIOs, but this
      // dirfrag as a whole will continue to look okay (minus the
      // mysteriously-missing dentry)
      go_bad_dentry(key.snapid, key.name);

      // Anyone who was WAIT_DENTRY for this guy will get kicked
      // to RetryRequest, and hit the DamageTable-interrogating path.
      // Stats will now be bogus because we will think we're complete,
      // but have 1 or more missing dentries.
      continue;
    }

    if (!dn)
      continue;

    if (touch) {
      dout(10) << " touching wanted dn " << *dn << dendl;
      mdcache->touch_dentry(dn);
    }

    CDentry::linkage_t *dnl = dn->get_linkage();
    if (dnl->is_primary() && dnl->get_inode()->state_test(CInode::STATE_REJOINUNDEF))
      undef_inodes.push_back(dnl->get_inode());
  }

  if (complete) {
    if (!waiting_on_dentry.empty()) {
      for (auto &p : waiting_on_dentry) {
	std::copy(p.second.begin(), p.second.end(), std::back_inserter(finished));
	if (p.first.snapid == CEPH_NOSNAP)
	  null_keys.emplace_back(p.first);
      }
      waiting_on_dentry.clear();
      put(PIN_DNWAITER);
    }
  } else {
    proc_nulls_and_waiters("", string_snap_t());
  }

  if (!null_keys.empty()) {
    snapid_t first = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
    for (auto& key : null_keys) {
      CDentry* dn = lookup(key.name, key.snapid);
      if (dn) {
	dout(12) << "_fetched got null for key " << key << ", have " << *dn << dendl;
      } else {
	dn = add_null_dentry(key.name, first, key.snapid);
	dout(12) << "_fetched got null for key " << key << ", added " << *dn << dendl;
      }
      mdcache->touch_dentry(dn);

      if (!(++count % mdcache->mds->heartbeat_reset_grace(2)))
        mdcache->mds->heartbeat_reset();
    }
  }

  //cache->mds->logger->inc("newin", num_new_inodes_loaded);

  // mark complete, !fetching
  if (complete) {
    mark_complete();
    state_clear(STATE_FETCHING);
    take_waiting(WAIT_COMPLETE, finished);
  }

  // open & force frags
  while (!undef_inodes.empty()) {
    CInode *in = undef_inodes.front();

    undef_inodes.pop_front();
    in->state_clear(CInode::STATE_REJOINUNDEF);
    mdcache->opened_undef_inode(in);

    if (!(++count % mdcache->mds->heartbeat_reset_grace()))
      mdcache->mds->heartbeat_reset();
  }

  // dirty myself to remove stale snap dentries
  if (force_dirty && !mdcache->is_readonly())
    log_mark_dirty();

  auth_unpin(this);

  if (!finished.empty())
    mdcache->mds->queue_waiters(finished);
}

void CDir::go_bad_dentry(snapid_t last, std::string_view dname)
{
  dout(10) << __func__ << " " << dname << dendl;
  std::string path(get_path());
  path += "/";
  path += dname;
  const bool fatal = mdcache->mds->damage_table.notify_dentry(
      inode->ino(), frag, last, dname, path);
  if (fatal) {
    mdcache->mds->damaged();
    ceph_abort();  // unreachable, damaged() respawns us
  }
}

void CDir::go_bad(bool complete)
{
  dout(10) << __func__ << " " << frag << dendl;
  const bool fatal = mdcache->mds->damage_table.notify_dirfrag(
      inode->ino(), frag, get_path());
  if (fatal) {
    mdcache->mds->damaged();
    ceph_abort();  // unreachable, damaged() respawns us
  }

  if (complete) {
    if (get_version() == 0) {
      auto _fnode = allocate_fnode();
      _fnode->version = 1;
      reset_fnode(std::move(_fnode));
    }
    
    state_set(STATE_BADFRAG);
    mark_complete();
  }

  state_clear(STATE_FETCHING);
  auth_unpin(this);
  finish_waiting(WAIT_COMPLETE, -CEPHFS_EIO);
}

// -----------------------
// COMMIT

/**
 * commit
 *
 * @param want - min version i want committed
 * @param c - callback for completion
 */
void CDir::commit(version_t want, MDSContext *c, bool ignore_authpinnability, int op_prio)
{
  dout(10) << "commit want " << want << " on " << *this << dendl;
  if (want == 0) want = get_version();

  // preconditions
  ceph_assert(want <= get_version() || get_version() == 0);    // can't commit the future
  ceph_assert(want > committed_version); // the caller is stupid
  ceph_assert(is_auth());
  ceph_assert(ignore_authpinnability || can_auth_pin());

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
  void finish(int r) override {
    dir->_committed(r, version);
  }
  void print(ostream& out) const override {
    out << "dirfrag_committed(" << dir->dirfrag() << ")";
  }
};

class C_IO_Dir_Commit_Ops : public Context {
public:
  C_IO_Dir_Commit_Ops(CDir *d, int pr,
		      vector<CDir::dentry_commit_item> &&s, bufferlist &&bl,
		      vector<string> &&r,
		      mempool::mds_co::compact_set<mempool::mds_co::string> &&stales) :
    dir(d), op_prio(pr) {
    metapool = dir->mdcache->mds->get_metadata_pool();
    version = dir->get_version();
    is_new = dir->is_new();
    to_set.swap(s);
    dfts.swap(bl);
    to_remove.swap(r);
    stale_items.swap(stales);
  }

  void finish(int r) override {
    dir->_omap_commit_ops(r, op_prio, metapool, version, is_new, to_set, dfts,
			  to_remove, stale_items);
  }

private:
  CDir *dir;
  int op_prio;
  int64_t metapool;
  version_t version;
  bool is_new;
  vector<CDir::dentry_commit_item> to_set;
  bufferlist dfts;
  vector<string> to_remove;
  mempool::mds_co::compact_set<mempool::mds_co::string> stale_items;
};

// This is doing the same thing with the InodeStoreBase::encode()
void CDir::_encode_primary_inode_base(dentry_commit_item &item, bufferlist &dfts,
                                      bufferlist &bl)
{
  ENCODE_START(6, 4, bl);
  encode(*item.inode, bl, item.features);

  if (!item.symlink.empty())
    encode(item.symlink, bl);

  // dirfragtree
  dfts.splice(0, item.dft_len, &bl);

  if (item.xattrs)
    encode(*item.xattrs, bl);
  else
    encode((__u32)0, bl);

  if (item.snaprealm) {
    bufferlist snapr_bl;
    encode(item.srnode, snapr_bl);
    encode(snapr_bl, bl);
  } else {
    encode(bufferlist(), bl);
  }

  if (item.old_inodes)
    encode(*item.old_inodes, bl, item.features);
  else
    encode((__u32)0, bl);

  encode(item.oldest_snap, bl);
  encode(item.damage_flags, bl);
  ENCODE_FINISH(bl);
}

// This is not locked by mds_lock
void CDir::_omap_commit_ops(int r, int op_prio, int64_t metapool, version_t version, bool _new,
			    vector<dentry_commit_item> &to_set, bufferlist &dfts,
                            vector<string>& to_remove,
			    mempool::mds_co::compact_set<mempool::mds_co::string> &stales)
{
  dout(10) << __func__ << dendl;

  if (r < 0) {
    mdcache->mds->handle_write_error_with_lock(r);
    return;
  }

  C_GatherBuilder gather(g_ceph_context,
                         new C_OnFinisher(new C_IO_Dir_Committed(this, version),
			 mdcache->mds->finisher));

  SnapContext snapc;
  object_t oid = get_ondisk_object();
  object_locator_t oloc(metapool);

  map<string, bufferlist> _set;
  set<string> _rm;

  unsigned max_write_size = mdcache->max_dir_commit_size;
  unsigned write_size = 0;

  auto commit_one = [&](bool header=false) {
    ObjectOperation op;

    /*
     * Shouldn't submit empty op to Rados, which could cause
     * the cephfs to become readonly.
     */
    ceph_assert(header || !_set.empty() || !_rm.empty());


    // don't create new dirfrag blindly
    if (!_new)
      op.stat(nullptr, nullptr, nullptr);

    /*
     * save the header at the last moment.. If we were to send it off before
     * other updates, but die before sending them all, we'd think that the
     * on-disk state was fully committed even though it wasn't! However, since
     * the messages are strictly ordered between the MDS and the OSD, and
     * since messages to a given PG are strictly ordered, if we simply send
     * the message containing the header off last, we cannot get our header
     * into an incorrect state.
     */
    if (header) {
      bufferlist header;
      encode(*fnode, header);
      op.omap_set_header(header);
    }

    op.priority = op_prio;
    if (!_set.empty())
      op.omap_set(_set);
    if (!_rm.empty())
      op.omap_rm_keys(_rm);
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
                                   ceph::real_clock::now(),
                                   0, gather.new_sub());
    write_size = 0;
    _set.clear();
    _rm.clear();
  };

  int count = 0;
  for (auto &key : stales) {
    unsigned size = key.length() + sizeof(__u32);
    if (write_size > 0 && write_size + size > max_write_size)
      commit_one();

    write_size += size;
    _rm.emplace(key);

    if (!(++count % mdcache->mds->heartbeat_reset_grace(2)))
      mdcache->mds->heartbeat_reset();
  }

  for (auto &key : to_remove) {
    unsigned size = key.length() + sizeof(__u32);
    if (write_size > 0 && write_size + size > max_write_size)
      commit_one();

    write_size += size;
    _rm.emplace(std::move(key));

    if (!(++count % mdcache->mds->heartbeat_reset_grace(2)))
      mdcache->mds->heartbeat_reset();
  }

  bufferlist bl;
  using ceph::encode;
  for (auto &item : to_set) {
    encode(item.first, bl);
    if (item.is_remote) {
      // remote link
      CDentry::encode_remote(item.ino, item.d_type, item.alternate_name, bl);
    } else {
      // marker, name, inode, [symlink string]
      bl.append('i');         // inode

      ENCODE_START(2, 1, bl);
      encode(item.alternate_name, bl);
      _encode_primary_inode_base(item, dfts, bl);
      ENCODE_FINISH(bl);
    }

    unsigned size = item.key.length() + bl.length() + 2 * sizeof(__u32);
    if (write_size > 0 && write_size + size > max_write_size)
      commit_one();

    write_size += size;
    _set[std::move(item.key)].swap(bl);

    if (!(++count % mdcache->mds->heartbeat_reset_grace()))
      mdcache->mds->heartbeat_reset();
  }

  commit_one(true);
  gather.activate();
}

/**
 * Flush out the modified dentries in this dir. Keep the bufferlist
 * below max_write_size;
 */
void CDir::_omap_commit(int op_prio)
{
  dout(10) << __func__ << dendl;

  if (op_prio < 0)
    op_prio = CEPH_MSG_PRIO_DEFAULT;

  // snap purge?
  const set<snapid_t> *snaps = NULL;
  SnapRealm *realm = inode->find_snaprealm();
  if (fnode->snap_purged_thru < realm->get_last_destroyed()) {
    snaps = &realm->get_snaps();
    dout(10) << " snap_purged_thru " << fnode->snap_purged_thru
	     << " < " << realm->get_last_destroyed()
	     << ", snap purge based on " << *snaps << dendl;
    // fnode.snap_purged_thru = realm->get_last_destroyed();
  }

  size_t items_count = 0;
  if (state_test(CDir::STATE_FRAGMENTING) && is_new()) {
    items_count = get_num_head_items() + get_num_snap_items();
  } else {
    for (elist<CDentry*>::iterator it = dirty_dentries.begin(); !it.end(); ++it)
      ++items_count;
  }

  vector<string> to_remove;
  // reverve enough memories, which maybe larger than the actually needed
  to_remove.reserve(items_count);

  vector<dentry_commit_item> to_set;
  // reverve enough memories, which maybe larger than the actually needed
  to_set.reserve(items_count);

  // for dir fragtrees
  bufferlist dfts(CEPH_PAGE_SIZE);

  auto write_one = [&](CDentry *dn) {
    string key;
    dn->key().encode(key);

    if (!dn->corrupt_first_loaded) {
      dn->check_corruption(false);
    }

    if (snaps && try_trim_snap_dentry(dn, *snaps)) {
      dout(10) << " rm " << key << dendl;
      to_remove.emplace_back(std::move(key));
      return;
    }

    if (dn->get_linkage()->is_null()) {
      dout(10) << " rm " << dn->get_name() << " " << *dn << dendl;
      to_remove.emplace_back(std::move(key));
    } else {
      dout(10) << " set " << dn->get_name() << " " << *dn << dendl;

      uint64_t off = dfts.length();
      // try to reserve new size if there has less
      // than 1/8 page space
      uint64_t left = CEPH_PAGE_SIZE - off % CEPH_PAGE_SIZE;
      if (left < CEPH_PAGE_SIZE / 8)
        dfts.reserve(left + CEPH_PAGE_SIZE);

      auto& item = to_set.emplace_back();
      item.key = std::move(key);
      _parse_dentry(dn, item, snaps, dfts);
      item.dft_len = dfts.length() - off;
    }
  };

  int count = 0;
  if (state_test(CDir::STATE_FRAGMENTING) && is_new()) {
    ceph_assert(committed_version == 0);
    for (auto p = items.begin(); p != items.end(); ) {
      CDentry *dn = p->second;
      ++p;
      if (dn->get_linkage()->is_null())
	continue;
      write_one(dn);

      if (!(++count % mdcache->mds->heartbeat_reset_grace()))
        mdcache->mds->heartbeat_reset();
    }
  } else {
    for (auto p = dirty_dentries.begin(); !p.end(); ) {
      CDentry *dn = *p;
      ++p;
      write_one(dn);

      if (!(++count % mdcache->mds->heartbeat_reset_grace()))
        mdcache->mds->heartbeat_reset();
    }
  }

  auto c = new C_IO_Dir_Commit_Ops(this, op_prio, std::move(to_set), std::move(dfts),
                                   std::move(to_remove), std::move(stale_items));
  stale_items.clear();
  mdcache->mds->finisher->queue(c);
}

void CDir::_parse_dentry(CDentry *dn, dentry_commit_item &item,
			 const set<snapid_t> *snaps, bufferlist &bl)
{
  // clear dentry NEW flag, if any.  we can no longer silently drop it.
  dn->clear_new();

  item.first = dn->first;

  // primary or remote?
  auto& linkage = dn->linkage;
  item.alternate_name = dn->get_alternate_name();
  if (linkage.is_remote()) {
    item.is_remote = true;
    item.ino = linkage.get_remote_ino();
    item.d_type = linkage.get_remote_d_type();
    dout(14) << " dn '" << dn->get_name() << "' remote ino " << item.ino << dendl;
  } else if (linkage.is_primary()) {
    // primary link
    CInode *in = linkage.get_inode();
    ceph_assert(in);

    dout(14) << " dn '" << dn->get_name() << "' inode " << *in << dendl;

    if (in->is_multiversion()) {
      if (!in->snaprealm) {
	if (snaps)
	  in->purge_stale_snap_data(*snaps);
      } else {
	in->purge_stale_snap_data(in->snaprealm->get_snaps());
      }
    }

    if (in->snaprealm) {
      item.snaprealm = true;
      item.srnode = in->snaprealm->srnode;
    }
    item.features = mdcache->mds->mdsmap->get_up_features();
    item.inode = in->inode;
    if (in->inode->is_symlink())
      item.symlink = in->symlink;
    using ceph::encode;
    encode(in->dirfragtree, bl);
    item.xattrs = in->xattrs;
    item.old_inodes = in->old_inodes;
    item.oldest_snap = in->oldest_snap;
    item.damage_flags = in->damage_flags;
  } else {
    ceph_assert(!linkage.is_null());
  }
}

void CDir::_commit(version_t want, int op_prio)
{
  dout(10) << "_commit want " << want << " on " << *this << dendl;

  // we can't commit things in the future.
  // (even the projected future.)
  ceph_assert(want <= get_version() || get_version() == 0);

  // check pre+postconditions.
  ceph_assert(is_auth());

  // already committed?
  if (committed_version >= want) {
    dout(10) << "already committed " << committed_version << " >= " << want << dendl;
    return;
  }
  // already committing >= want?
  if (committing_version >= want) {
    dout(10) << "already committing " << committing_version << " >= " << want << dendl;
    ceph_assert(state_test(STATE_COMMITTING));
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
  
  if (mdcache->mds->logger) mdcache->mds->logger->inc(l_mds_dir_commit);

  mdcache->mds->balancer->hit_dir(this, META_POP_STORE);

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
    if (r == -CEPHFS_ENOENT && committed_version == 0 &&
	!inode->is_base() && get_parent_dir()->inode->is_stray()) {
      r = 0;
      if (inode->snaprealm)
	inode->state_set(CInode::STATE_MISSINGOBJS);
    }
    if (r < 0) {
      dout(1) << "commit error " << r << " v " << v << dendl;
      mdcache->mds->clog->error() << "failed to commit dir " << dirfrag() << " object,"
				<< " errno " << r;
      mdcache->mds->handle_write_error(r);
      return;
    }
  }

  dout(10) << "_committed v " << v << " on " << *this << dendl;
  ceph_assert(is_auth());

  bool stray = inode->is_stray();

  // take note.
  ceph_assert(v > committed_version);
  ceph_assert(v <= committing_version);
  committed_version = v;

  // _all_ commits done?
  if (committing_version == committed_version) 
    state_clear(CDir::STATE_COMMITTING);

  // _any_ commit, even if we've been redirtied, means we're no longer new.
  item_new.remove_myself();
  
  // dir clean?
  if (committed_version == get_version()) 
    mark_clean();

  int count = 0;

  // dentries clean?
  for (auto p = dirty_dentries.begin(); !p.end(); ) {
    CDentry *dn = *p;
    ++p;
    
    // inode?
    if (dn->linkage.is_primary()) {
      CInode *in = dn->linkage.get_inode();
      ceph_assert(in);
      ceph_assert(in->is_auth());
      
      if (committed_version >= in->get_version()) {
	if (in->is_dirty()) {
	  dout(15) << " dir " << committed_version << " >= inode " << in->get_version() << " now clean " << *in << dendl;
	  in->mark_clean();
	}
      } else {
	dout(15) << " dir " << committed_version << " < inode " << in->get_version() << " still dirty " << *in << dendl;
	ceph_assert(in->is_dirty() || in->last < CEPH_NOSNAP);  // special case for cow snap items (not predirtied)
      }
    }

    // dentry
    if (committed_version >= dn->get_version()) {
      dout(15) << " dir " << committed_version << " >= dn " << dn->get_version() << " now clean " << *dn << dendl;
      dn->mark_clean();

      // drop clean null stray dentries immediately
      if (stray &&
	  dn->get_num_ref() == 0 &&
	  !dn->is_projected() &&
	  dn->get_linkage()->is_null())
	remove_dentry(dn);
    } else {
      dout(15) << " dir " << committed_version << " < dn " << dn->get_version() << " still dirty " << *dn << dendl;
      ceph_assert(dn->is_dirty());
    }

    if (!(++count % mdcache->mds->heartbeat_reset_grace()))
      mdcache->mds->heartbeat_reset();
  }

  // finishers?
  bool were_waiters = !waiting_for_commit.empty();

  auto it = waiting_for_commit.begin();
  while (it != waiting_for_commit.end()) {
    auto _it = it;
    ++_it;
    if (it->first > committed_version) {
      dout(10) << " there are waiters for " << it->first << ", committing again" << dendl;
      _commit(it->first, -1);
      break;
    }
    MDSContext::vec t;
    for (const auto &waiter : it->second)
      t.push_back(waiter);
    mdcache->mds->queue_waiters(t);
    waiting_for_commit.erase(it);
    it = _it;

    if (!(++count % mdcache->mds->heartbeat_reset_grace()))
      mdcache->mds->heartbeat_reset();
  }

  // try drop dentries in this dirfrag if it's about to be purged
  if (!inode->is_base() && get_parent_dir()->inode->is_stray() &&
      inode->snaprealm)
    mdcache->maybe_eval_stray(inode, true);

  // unpin if we kicked the last waiter.
  if (were_waiters &&
      waiting_for_commit.empty())
    auth_unpin(this);
}




// IMPORT/EXPORT

mds_rank_t CDir::get_export_pin(bool inherit) const
{
  mds_rank_t export_pin = inode->get_export_pin(inherit);
  if (export_pin == MDS_RANK_EPHEMERAL_DIST)
    export_pin = mdcache->hash_into_rank_bucket(ino(), get_frag());
  else if (export_pin == MDS_RANK_EPHEMERAL_RAND)
    export_pin = mdcache->hash_into_rank_bucket(ino());
  return export_pin;
}

bool CDir::is_exportable(mds_rank_t dest) const
{
  mds_rank_t export_pin = get_export_pin();
  if (export_pin == dest)
    return true;
  if (export_pin >= 0)
    return false;
  return true;
}

void CDir::encode_export(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  ceph_assert(!is_projected());
  encode(first, bl);
  encode(*fnode, bl);
  encode(dirty_old_rstat, bl);
  encode(committed_version, bl);

  encode(state, bl);
  encode(dir_rep, bl);

  encode(pop_me, bl);
  encode(pop_auth_subtree, bl);

  encode(dir_rep_by, bl);  
  encode(get_replicas(), bl);

  get(PIN_TEMPEXPORTING);
  ENCODE_FINISH(bl);
}

void CDir::finish_export()
{
  state &= MASK_STATE_EXPORT_KEPT;
  pop_nested.sub(pop_auth_subtree);
  pop_auth_subtree_nested.sub(pop_auth_subtree);
  pop_me.zero();
  pop_auth_subtree.zero();
  put(PIN_TEMPEXPORTING);
  dirty_old_rstat.clear();
}

void CDir::decode_import(bufferlist::const_iterator& blp, LogSegment *ls)
{
  DECODE_START(1, blp);
  decode(first, blp);
  {
    auto _fnode = allocate_fnode();
    decode(*_fnode, blp);
    reset_fnode(std::move(_fnode));
  }
  update_projected_version();

  decode(dirty_old_rstat, blp);
  decode(committed_version, blp);
  committing_version = committed_version;

  unsigned s;
  decode(s, blp);
  state &= MASK_STATE_IMPORT_KEPT;
  state_set(STATE_AUTH | (s & MASK_STATE_EXPORTED));

  if (is_dirty()) {
    get(PIN_DIRTY);
    _mark_dirty(ls);
  }

  decode(dir_rep, blp);

  decode(pop_me, blp);
  decode(pop_auth_subtree, blp);
  pop_nested.add(pop_auth_subtree);
  pop_auth_subtree_nested.add(pop_auth_subtree);

  decode(dir_rep_by, blp);
  decode(get_replicas(), blp);
  if (is_replicated()) get(PIN_REPLICATED);

  replica_nonce = 0;  // no longer defined

  // did we import some dirty scatterlock data?
  if (dirty_old_rstat.size() ||
      !(fnode->rstat == fnode->accounted_rstat)) {
    mdcache->mds->locker->mark_updated_scatterlock(&inode->nestlock);
    ls->dirty_dirfrag_nest.push_back(&inode->item_dirty_dirfrag_nest);
  }
  if (!(fnode->fragstat == fnode->accounted_fragstat)) {
    mdcache->mds->locker->mark_updated_scatterlock(&inode->filelock);
    ls->dirty_dirfrag_dir.push_back(&inode->item_dirty_dirfrag_dir);
  }
  if (is_dirty_dft()) {
    if (inode->dirfragtreelock.get_state() != LOCK_MIX &&
	inode->dirfragtreelock.is_stable()) {
      // clear stale dirtydft
      state_clear(STATE_DIRTYDFT);
    } else {
      mdcache->mds->locker->mark_updated_scatterlock(&inode->dirfragtreelock);
      ls->dirty_dirfrag_dirfragtree.push_back(&inode->item_dirty_dirfrag_dirfragtree);
    }
  }
  DECODE_FINISH(blp);
}

void CDir::abort_import()
{
  ceph_assert(is_auth());
  state_clear(CDir::STATE_AUTH);
  remove_bloom();
  clear_replica_map();
  set_replica_nonce(CDir::EXPORT_NONCE);
  if (is_dirty())
    mark_clean();

  pop_nested.sub(pop_auth_subtree);
  pop_auth_subtree_nested.sub(pop_auth_subtree);
  pop_me.zero();
  pop_auth_subtree.zero();
}

void CDir::encode_dirstat(bufferlist& bl, const session_info_t& info, const DirStat& ds) {
  if (info.has_feature(CEPHFS_FEATURE_REPLY_ENCODING)) {
    ENCODE_START(1, 1, bl);
    encode(ds.frag, bl);
    encode(ds.auth, bl);
    encode(ds.dist, bl);
    ENCODE_FINISH(bl);
  }
  else {
    encode(ds.frag, bl);
    encode(ds.auth, bl);
    encode(ds.dist, bl);
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

bool CDir::can_rep() const
{
  if (!is_rep()) 
    return true;

  unsigned mds_num = mdcache->mds->get_mds_map()->get_num_mds(MDSMap::STATE_ACTIVE);
  if ((mds_num - 1) > get_replicas().size()) 
    return true;
  
  return false;
}


/** set_dir_auth
 */
void CDir::set_dir_auth(const mds_authority_t &a)
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

    if (freeze_tree_state) {
      // only by CDir::_freeze_tree()
      ceph_assert(is_freezing_tree_root());
    }

    inode->num_subtree_roots++;   
    
    // unpin parent of frozen dir/tree?
    if (inode->is_auth()) {
      ceph_assert(!is_frozen_tree_root());
      if (is_frozen_dir())
	inode->auth_unpin(this);
    }
  } 
  if (was_subtree && !is_subtree_root()) {
    dout(10) << " old subtree root, adjusting auth_pins" << dendl;

    inode->num_subtree_roots--;

    // pin parent of frozen dir/tree?
    if (inode->is_auth()) {
      ceph_assert(!is_frozen_tree_root());
      if (is_frozen_dir())
	inode->auth_pin(this);
    }
  }

  // newly single auth?
  if (was_ambiguous && dir_auth.second == CDIR_AUTH_UNKNOWN) {
    MDSContext::vec ls;
    take_waiting(WAIT_SINGLEAUTH, ls);
    mdcache->mds->queue_waiters(ls);
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

  dout(10) << "auth_pin by " << by << " on " << *this << " count now " << auth_pins << dendl;

  if (freeze_tree_state)
    freeze_tree_state->auth_pins += 1;
}

void CDir::auth_unpin(void *by) 
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  {
    auto it = auth_pin_set.find(by);
    ceph_assert(it != auth_pin_set.end());
    auth_pin_set.erase(it);
  }
#endif
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(10) << "auth_unpin by " << by << " on " << *this << " count now " << auth_pins << dendl;
  ceph_assert(auth_pins >= 0);

  if (freeze_tree_state)
    freeze_tree_state->auth_pins -= 1;

  maybe_finish_freeze();  // pending freeze?
}

void CDir::adjust_nested_auth_pins(int dirinc, void *by)
{
  ceph_assert(dirinc);
  dir_auth_pins += dirinc;
  
  dout(15) << __func__ << " " << dirinc << " on " << *this
	   << " by " << by << " count now "
	   << auth_pins << "/" << dir_auth_pins << dendl;
  ceph_assert(dir_auth_pins >= 0);

  if (freeze_tree_state)
    freeze_tree_state->auth_pins += dirinc;

  if (dirinc < 0)
    maybe_finish_freeze();  // pending freeze?
}

#ifdef MDS_VERIFY_FRAGSTAT
void CDir::verify_fragstat()
{
  ceph_assert(is_complete());
  if (inode->is_stray())
    return;

  frag_info_t c;
  memset(&c, 0, sizeof(c));

  for (auto it = items.begin();
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

  if (c.nsubdirs != fnode->fragstat.nsubdirs ||
      c.nfiles != fnode->fragstat.nfiles) {
    dout(0) << "verify_fragstat failed " << fnode->fragstat << " on " << *this << dendl;
    dout(0) << "               i count " << c << dendl;
    ceph_abort();
  } else {
    dout(0) << "verify_fragstat ok " << fnode->fragstat << " on " << *this << dendl;
  }
}
#endif

/*****************************************************************************
 * FREEZING
 */

// FREEZE TREE

void CDir::_walk_tree(std::function<bool(CDir*)> callback)
{
  deque<CDir*> dfq;
  dfq.push_back(this);

  while (!dfq.empty()) {
    CDir *dir = dfq.front();
    dfq.pop_front();

    for (auto& p : *dir) {
      CDentry *dn = p.second;
      if (!dn->get_linkage()->is_primary())
	continue;
      CInode *in = dn->get_linkage()->get_inode();
      if (!in->is_dir())
	continue;

      auto&& dfv = in->get_nested_dirfrags();
      for (auto& dir : dfv) {
	auto ret = callback(dir);
	if (ret)
	  dfq.push_back(dir);
      }
    }
  }
}

bool CDir::freeze_tree()
{
  ceph_assert(!is_frozen());
  ceph_assert(!is_freezing());
  ceph_assert(!freeze_tree_state);

  auth_pin(this);

  // Travese the subtree to mark dirfrags as 'freezing' (set freeze_tree_state)
  // and to accumulate auth pins and record total count in freeze_tree_state.
  // when auth unpin an 'freezing' object, the counter in freeze_tree_state also
  // gets decreased. Subtree become 'frozen' when the counter reaches zero.
  freeze_tree_state = std::make_shared<freeze_tree_state_t>(this);
  freeze_tree_state->auth_pins += get_auth_pins() + get_dir_auth_pins();
  if (!lock_caches_with_auth_pins.empty())
    mdcache->mds->locker->invalidate_lock_caches(this);

  _walk_tree([this](CDir *dir) {
      if (dir->freeze_tree_state)
	return false;
      dir->freeze_tree_state = freeze_tree_state;
      freeze_tree_state->auth_pins += dir->get_auth_pins() + dir->get_dir_auth_pins();
      if (!dir->lock_caches_with_auth_pins.empty())
	mdcache->mds->locker->invalidate_lock_caches(dir);
      return true;
    }
  );

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
  dout(10) << __func__ << " " << *this << dendl;
  ceph_assert(is_freezeable(true));

  if (freeze_tree_state) {
    ceph_assert(is_auth());
  } else {
    ceph_assert(!is_auth());
    freeze_tree_state = std::make_shared<freeze_tree_state_t>(this);
  }
  freeze_tree_state->frozen = true;

  if (is_auth()) {
    mds_authority_t auth;
    bool was_subtree = is_subtree_root();
    if (was_subtree) {
      auth = get_dir_auth();
    } else {
      // temporarily prevent parent subtree from becoming frozen.
      inode->auth_pin(this);
      // create new subtree
      auth = authority();
    }

    _walk_tree([this, &auth] (CDir *dir) {
	if (dir->freeze_tree_state != freeze_tree_state) {
	  mdcache->adjust_subtree_auth(dir, auth);
	  return false;
	}
	return true;
      }
    );

    ceph_assert(auth.first >= 0);
    ceph_assert(auth.second == CDIR_AUTH_UNKNOWN);
    auth.second = auth.first;
    mdcache->adjust_subtree_auth(this, auth);
    if (!was_subtree)
      inode->auth_unpin(this);
  } else {
    // importing subtree ?
    _walk_tree([this] (CDir *dir) {
	ceph_assert(!dir->freeze_tree_state);
	dir->freeze_tree_state = freeze_tree_state;
	return true;
      }
    );
  }

  // twiddle state
  if (state_test(STATE_FREEZINGTREE)) {
    state_clear(STATE_FREEZINGTREE);
    --num_freezing_trees;
  }

  state_set(STATE_FROZENTREE);
  ++num_frozen_trees;
  get(PIN_FROZEN);
}

void CDir::unfreeze_tree()
{
  dout(10) << __func__ << " " << *this << dendl;

  MDSContext::vec unfreeze_waiters;
  take_waiting(WAIT_UNFREEZE, unfreeze_waiters);

  if (freeze_tree_state) {
    _walk_tree([this, &unfreeze_waiters](CDir *dir) {
	if (dir->freeze_tree_state != freeze_tree_state)
	  return false;
	dir->freeze_tree_state.reset();
	dir->take_waiting(WAIT_UNFREEZE, unfreeze_waiters);
	return true;
      }
    );
  }

  if (state_test(STATE_FROZENTREE)) {
    // frozen.  unfreeze.
    state_clear(STATE_FROZENTREE);
    --num_frozen_trees;

    put(PIN_FROZEN);

    if (is_auth()) {
      // must be subtree
      ceph_assert(is_subtree_root());
      // for debug purpose, caller should ensure 'dir_auth.second == dir_auth.first'
      mds_authority_t auth = get_dir_auth();
      ceph_assert(auth.first >= 0);
      ceph_assert(auth.second == auth.first);
      auth.second = CDIR_AUTH_UNKNOWN;
      mdcache->adjust_subtree_auth(this, auth);
    }
    freeze_tree_state.reset();
  } else {
    ceph_assert(state_test(STATE_FREEZINGTREE));

    // freezing.  stop it.
    state_clear(STATE_FREEZINGTREE);
    --num_freezing_trees;
    freeze_tree_state.reset();

    finish_waiting(WAIT_FROZEN, -1);
    auth_unpin(this);
  }

  mdcache->mds->queue_waiters(unfreeze_waiters);
}

void CDir::adjust_freeze_after_rename(CDir *dir)
{
  if (!freeze_tree_state || dir->freeze_tree_state != freeze_tree_state)
    return;
  CDir *newdir = dir->get_inode()->get_parent_dir();
  if (newdir == this || newdir->freeze_tree_state == freeze_tree_state)
    return;

  ceph_assert(!freeze_tree_state->frozen);
  ceph_assert(get_dir_auth_pins() > 0);

  MDSContext::vec unfreeze_waiters;

  auto unfreeze = [this, &unfreeze_waiters](CDir *dir) {
    if (dir->freeze_tree_state != freeze_tree_state)
      return false;
    int dec = dir->get_auth_pins() + dir->get_dir_auth_pins();
    // shouldn't become zero because srcdn of rename was auth pinned 
    ceph_assert(freeze_tree_state->auth_pins > dec);
    freeze_tree_state->auth_pins -= dec;
    dir->freeze_tree_state.reset();
    dir->take_waiting(WAIT_UNFREEZE, unfreeze_waiters);
    return true;
  };

  unfreeze(dir);
  dir->_walk_tree(unfreeze);

  mdcache->mds->queue_waiters(unfreeze_waiters);
}

bool CDir::can_auth_pin(int *err_ret, bool bypassfreezing) const
{
  int err;
  if (!is_auth()) {
    err = ERR_NOT_AUTH;
  } else if (is_freezing_dir()) {
    if (bypassfreezing) {
      dout(20) << "allowing authpin with freezing" << dendl;
      err = 0;
    } else {
      err = ERR_FRAGMENTING_DIR;
    }
  } else if (is_frozen_dir()) {
    err = ERR_FRAGMENTING_DIR;
  } else {
    auto p = is_freezing_or_frozen_tree();
    if (p.first && !bypassfreezing) {
      err = ERR_EXPORTING_TREE;
    } else if (p.second) {
      err = ERR_EXPORTING_TREE;
    } else {
      err = 0;
    }
  }
  if (err && err_ret)
    *err_ret = err;
  return !err;
}

class C_Dir_AuthUnpin : public CDirContext {
  public:
  explicit C_Dir_AuthUnpin(CDir *d) : CDirContext(d) {}
  void finish(int r) override {
    dir->auth_unpin(dir->get_inode());
  }
};

void CDir::maybe_finish_freeze()
{
  if (dir_auth_pins != 0)
    return;

  // we can freeze the _dir_ even with nested pins...
  if (state_test(STATE_FREEZINGDIR)) {
    if (auth_pins == 1) {
      _freeze_dir();
      auth_unpin(this);
      finish_waiting(WAIT_FROZEN);
    }
  }

  if (freeze_tree_state) {
    if (freeze_tree_state->frozen ||
	freeze_tree_state->auth_pins != 1)
      return;

    if (freeze_tree_state->dir != this) {
      freeze_tree_state->dir->maybe_finish_freeze();
      return;
    }

    ceph_assert(state_test(STATE_FREEZINGTREE));

    if (!is_subtree_root() && inode->is_frozen()) {
      dout(10) << __func__ << " !subtree root and frozen inode, waiting for unfreeze on " << inode << dendl;
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
  ceph_assert(!is_frozen());
  ceph_assert(!is_freezing());
  
  auth_pin(this);
  if (is_freezeable_dir(true)) {
    _freeze_dir();
    auth_unpin(this);
    return true;
  } else {
    state_set(STATE_FREEZINGDIR);
    if (!lock_caches_with_auth_pins.empty())
      mdcache->mds->locker->invalidate_lock_caches(this);
    dout(10) << "freeze_dir + wait " << *this << dendl;
    return false;
  } 
}

void CDir::_freeze_dir()
{
  dout(10) << __func__ << " " << *this << dendl;
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
  dout(10) << __func__ << " " << *this << dendl;

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
    ceph_assert(state_test(STATE_FREEZINGDIR));
    state_clear(STATE_FREEZINGDIR);
    auth_unpin(this);
    
    finish_waiting(WAIT_UNFREEZE);
  }
}

void CDir::enable_frozen_inode()
{
  ceph_assert(frozen_inode_suppressed > 0);
  if (--frozen_inode_suppressed == 0) {
    for (auto p = freezing_inodes.begin(); !p.end(); ) {
      CInode *in = *p;
      ++p;
      ceph_assert(in->is_freezing_inode());
      in->maybe_finish_freeze_inode();
    }
  }
}

/**
 * Slightly less complete than operator<<, because this is intended
 * for identifying a directory and its state rather than for dumping
 * debug output.
 */
void CDir::dump(Formatter *f, int flags) const
{
  ceph_assert(f != NULL);
  if (flags & DUMP_PATH) {
    f->dump_stream("path") << get_path();
  }
  if (flags & DUMP_DIRFRAG) {
    f->dump_stream("dirfrag") << dirfrag();
  }
  if (flags & DUMP_SNAPID_FIRST) {
    f->dump_int("snapid_first", first);
  }
  if (flags & DUMP_VERSIONS) {
    f->dump_stream("projected_version") << get_projected_version();
    f->dump_stream("version") << get_version();
    f->dump_stream("committing_version") << get_committing_version();
    f->dump_stream("committed_version") << get_committed_version();
  }
  if (flags & DUMP_REP) {
    f->dump_bool("is_rep", is_rep());
  }
  if (flags & DUMP_DIR_AUTH) {
    if (get_dir_auth() != CDIR_AUTH_DEFAULT) {
      if (get_dir_auth().second == CDIR_AUTH_UNKNOWN) {
        f->dump_stream("dir_auth") << get_dir_auth().first;
      } else {
        f->dump_stream("dir_auth") << get_dir_auth();
      }
    } else {
      f->dump_string("dir_auth", "");
    }
  }
  if (flags & DUMP_STATES) {
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
  }
  if (flags & DUMP_MDS_CACHE_OBJECT) {
    MDSCacheObject::dump(f);
  }
  if (flags & DUMP_ITEMS) {
    f->open_array_section("dentries");
    for (auto &p : items) {
      CDentry *dn = p.second;
      f->open_object_section("dentry");
      dn->dump(f);
      f->close_section();
    }
    f->close_section();
  }
}

void CDir::dump_load(Formatter *f)
{
  f->dump_stream("path") << get_path();
  f->dump_stream("dirfrag") << dirfrag();

  f->open_object_section("pop_me");
  pop_me.dump(f);
  f->close_section();

  f->open_object_section("pop_nested");
  pop_nested.dump(f);
  f->close_section();

  f->open_object_section("pop_auth_subtree");
  pop_auth_subtree.dump(f);
  f->close_section();

  f->open_object_section("pop_auth_subtree_nested");
  pop_auth_subtree_nested.dump(f);
  f->close_section();
}

/****** Scrub Stuff *******/

void CDir::scrub_info_create() const
{
  ceph_assert(!scrub_infop);

  // break out of const-land to set up implicit initial state
  CDir *me = const_cast<CDir*>(this);
  const auto& pf = me->get_projected_fnode();

  std::unique_ptr<scrub_info_t> si(new scrub_info_t());

  si->last_recursive.version = pf->recursive_scrub_version;
  si->last_recursive.time = pf->recursive_scrub_stamp;

  si->last_local.version = pf->localized_scrub_version;
  si->last_local.time = pf->localized_scrub_stamp;

  me->scrub_infop.swap(si);
}

void CDir::scrub_initialize(const ScrubHeaderRef& header)
{
  ceph_assert(header);
  // FIXME: weird implicit construction, is someone else meant
  // to be calling scrub_info_create first?
  scrub_info();
  scrub_infop->directory_scrubbing = true;
  scrub_infop->header = header;
  header->inc_num_pending();
}

void CDir::scrub_aborted() {
  dout(20) << __func__ << dendl;
  ceph_assert(scrub_is_in_progress());

  scrub_infop->last_scrub_dirty = false;
  scrub_infop->directory_scrubbing = false;
  scrub_infop->header->dec_num_pending();
  scrub_infop.reset();
}

void CDir::scrub_finished()
{
  dout(20) << __func__ << dendl;
  ceph_assert(scrub_is_in_progress());

  scrub_infop->last_local.time = ceph_clock_now();
  scrub_infop->last_local.version = get_version();
  if (scrub_infop->header->get_recursive())
    scrub_infop->last_recursive = scrub_infop->last_local;

  scrub_infop->last_scrub_dirty = true;

  scrub_infop->directory_scrubbing = false;
  scrub_infop->header->dec_num_pending();
}

void CDir::scrub_maybe_delete_info()
{
  if (scrub_infop &&
      !scrub_infop->directory_scrubbing &&
      !scrub_infop->last_scrub_dirty)
    scrub_infop.reset();
}

bool CDir::scrub_local()
{
  ceph_assert(is_complete());
  bool good = check_rstats(true);
  if (!good && scrub_infop->header->get_repair()) {
    mdcache->repair_dirfrag_stats(this);
    scrub_infop->header->set_repaired();
    good = true;
    mdcache->mds->damage_table.remove_dentry_damage_entry(this);
  }
  return good;
}

std::string CDir::get_path() const
{
  std::string path;
  get_inode()->make_path_string(path, true);
  return path;
}

bool CDir::should_split_fast() const
{
  // Max size a fragment can be before trigger fast splitting
  int fast_limit = g_conf()->mds_bal_split_size * g_conf()->mds_bal_fragment_fast_factor;

  // Fast path: the sum of accounted size and null dentries does not
  // exceed threshold: we definitely are not over it.
  if (get_frag_size() + get_num_head_null() <= fast_limit) {
    return false;
  }

  // Fast path: the accounted size of the frag exceeds threshold: we
  // definitely are over it
  if (get_frag_size() > fast_limit) {
    return true;
  }

  int64_t effective_size = 0;

  for (const auto &p : items) {
    const CDentry *dn = p.second;
    if (!dn->get_projected_linkage()->is_null()) {
      effective_size++;
    }
  }

  return effective_size > fast_limit;
}

bool CDir::should_merge() const
{
  if (get_frag() == frag_t())
    return false;

  if (inode->is_ephemeral_dist()) {
    unsigned min_frag_bits = mdcache->get_ephemeral_dist_frag_bits();
    if (min_frag_bits > 0 && get_frag().bits() < min_frag_bits + 1)
      return false;
  }

  return ((int)get_frag_size() + (int)get_num_snap_items()) < g_conf()->mds_bal_merge_size;
}

MEMPOOL_DEFINE_OBJECT_FACTORY(CDir, co_dir, mds_co);
MEMPOOL_DEFINE_OBJECT_FACTORY(CDir::scrub_info_t, scrub_info_t, mds_co)
