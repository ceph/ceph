// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "common/perf_counters.h"

#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Mutation.h"
#include "MDLog.h"
#include "events/EUpdate.h"

#include "StrayManager.h"

#include "osdc/Objecter.h"
#include "osdc/Filer.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".cache.strays ";
}

StrayManager::StrayManager(MDSRank *_mds) :
  lock("StrayManager::lock"),
  mds(_mds), mdcache(_mds->mdcache)
{
}

StrayManager::~StrayManager()
{
}

class StrayManagerIOContext : public MDSAsyncContextBase {
protected:
  StrayManager *sm;
  virtual MDSRank *get_mds() { return sm->mds; }
public:
  explicit StrayManagerIOContext(StrayManager *sm_) : sm(sm_) {}
};


class StrayManagerLogContext : public MDSLogContextBase {
protected:
  StrayManager *sm;
  virtual MDSRank *get_mds() { return sm->mds; }
public:
  explicit StrayManagerLogContext(StrayManager *sm_) : sm(sm_) {}
};

class C_IO_PurgeStray : public StrayManagerIOContext {
  CDentry *dn;
  CInode *in;
  uint32_t ops_allowance;
public:
  C_IO_PurgeStray(StrayManager *sm_, CDentry *d, CInode *i, uint32_t ops) :
    StrayManagerIOContext(sm_), dn(d), in(i), ops_allowance(ops) { }
  void finish(int r) {
    assert(r == 0);
    in->mutex_lock();
    sm->purge(dn, in, ops_allowance);
  }
};

/**
 * Context wrapper for _purge_stray_purged completion
 */
class C_IO_PurgeStrayPurged : public StrayManagerIOContext {
  CDentry *dn;
  bool only_head;
  // How many ops_in_flight were allocated to this purge?
  uint32_t ops_allowance;
public:
  C_IO_PurgeStrayPurged(StrayManager *sm_, CDentry *d, bool oh, uint32_t ops) : 
    StrayManagerIOContext(sm_), dn(d), only_head(oh), ops_allowance(ops) { }
  void finish(int r) {
    assert(r == 0 || r == -ENOENT);
    sm->_purge_stray_purged(dn, ops_allowance, only_head);
  }
};


void StrayManager::truncate(CDentry *dn, CInode *in, uint32_t op_allowance)
{ 
  in->mutex_assert_locked_by_me();
  assert(in); 
  dout(10) << __func__ << ": " << *dn << " " << *in << dendl;

//  num_strays_purging++;
//  logger->set(l_mdc_num_strays_purging, num_strays_purging);

  C_GatherBuilder gather(g_ceph_context,
      new C_IO_PurgeStrayPurged(this, dn, true, 0));

  SnapContext nullsnapc;

  const inode_t *pi = in->get_projected_inode();
  file_layout_t layout = pi->layout;

  uint64_t to = pi->get_max_size();
  to = MAX(pi->size, to);
  // when truncating a file, the filer does not delete stripe objects that are
  // truncated to zero. so we need to purge stripe objects up to the max size
  // the file has ever been.
  to = MAX(pi->max_size_ever, to);
  if (to > 0) {
    uint64_t num = Striper::get_num_objects(layout, to);
    dout(10) << __func__ << " 0~" << to << " objects 0~" << num
	     << " on " << *in << dendl;

    Filer *filer = mdcache->get_filer();
    // keep backtrace object
    if (num > 1) {
      filer->purge_range(in->ino(), &layout, nullsnapc, 1, num - 1,
			 ceph::real_clock::now(g_ceph_context),
			 0, gather.new_sub());
    }
    filer->zero(in->ino(), &layout, nullsnapc, 0, layout.object_size,
		ceph::real_clock::now(g_ceph_context),
		0, true, NULL, gather.new_sub());
  }

  in->mutex_unlock();
  assert(gather.has_subs());
  gather.activate();
}


void StrayManager::purge(CDentry *dn, CInode *in,  uint32_t op_allowance)
{
  in->mutex_assert_locked_by_me();
  dout(10) << __func__ << " " << *dn << " " << *in << dendl;

  SnapContext nullsnapc;

  //num_strays_purging++;
  //logger->set(l_mdc_num_strays_purging, num_strays_purging);


  // CHEAT.  there's no real need to journal our intent to purge, since
  // that is implicit in the dentry's presence and non-use in the stray
  // dir.  on recovery, we'll need to re-eval all strays anyway.
  
  C_GatherBuilder gather(g_ceph_context,
		  	 new C_IO_PurgeStrayPurged(this, dn, false, op_allowance));

  if (in->is_dir()) {
    object_locator_t oloc(mds->mdsmap->get_metadata_pool());
    std::list<frag_t> ls;
    ls.push_back(frag_t());
    /*
    if (!in->dirfragtree.is_leaf(frag_t()))
      in->dirfragtree.get_leaves(ls);
    */
    for (std::list<frag_t>::iterator p = ls.begin();
         p != ls.end();
         ++p) {
      object_t oid = CInode::get_object_name(in->ino(), *p, "");
      dout(10) << __func__ << " remove dirfrag " << oid << dendl;
      mds->objecter->remove(oid, oloc, nullsnapc,
			    ceph::real_clock::now(g_ceph_context),
			    0, NULL, gather.new_sub());
    }
    in->mutex_unlock();
    assert(gather.has_subs());
    gather.activate();
    return;
  }


  const inode_t *pi = in->get_projected_inode();
  file_layout_t layout = pi->layout;

  if (in->is_file()) {
    uint64_t to = pi->get_max_size();
    to = MAX(pi->size, to);
    // when truncating a file, the filer does not delete stripe objects that are
    // truncated to zero. so we need to purge stripe objects up to the max size
    // the file has ever been.
    to = MAX(pi->max_size_ever, to);
    if (to > 0) {
      uint64_t num = Striper::get_num_objects(layout, to);
      dout(10) << __func__ << " 0~" << to << " objects 0~" << num
	       << " on " << *in << dendl;
      mdcache->get_filer()->purge_range(in->ino(), &layout, nullsnapc, 0, num,
					ceph::real_clock::now(g_ceph_context),
					0, gather.new_sub());
    }
  }

  object_t oid = CInode::get_object_name(pi->ino, frag_t(), "");
  // remove the backtrace object if it was not purged
  if (!gather.has_subs() || !pi->layout.pool_ns.empty()) {
    object_locator_t oloc(pi->layout.pool_id);
    dout(10) << __func__ << " remove backtrace object " << oid
	     << " pool " << oloc.pool << dendl;
    mds->objecter->remove(oid, oloc, nullsnapc,
			  ceph::real_clock::now(g_ceph_context), 0,
			  NULL, gather.new_sub());
  }
  // remove old backtrace objects
  for (compact_set<int64_t>::iterator p = pi->old_pools.begin();
       p != pi->old_pools.end();
       ++p) {
    object_locator_t oloc(*p);
    dout(10) << __func__ << " remove backtrace object " << oid
	     << " old pool " << *p << dendl;
    mds->objecter->remove(oid, oloc, nullsnapc,
			  ceph::real_clock::now(g_ceph_context), 0,
			  NULL, gather.new_sub());
  }

  in->mutex_unlock();
  assert(gather.has_subs());
  gather.activate();
}

class C_PurgeStrayLogged : public StrayManagerLogContext {
  CDentry *dn;
  version_t pdv;
  MutationRef mut;
public:
  C_PurgeStrayLogged(StrayManager *sm_, CDentry *d, version_t v, MutationRef& m) : 
    StrayManagerLogContext(sm_), dn(d), pdv(v), mut(m) { }
  void finish(int r) {
    sm->_purge_stray_logged(dn, pdv, mut);
  }
};

class C_TruncateStrayLogged : public StrayManagerLogContext {
  CDentry *dn;
  MutationRef mut;
public:
  C_TruncateStrayLogged(StrayManager *sm, CDentry *d, MutationRef& m) :
    StrayManagerLogContext(sm), dn(d), mut(m) { }
  void finish(int r) {
    sm->_truncate_stray_logged(dn, mut);
  }
};

void StrayManager::_purge_stray_purged(CDentry *dn, uint32_t ops_allowance,
				       bool only_head)
{
  CInode *diri = dn->get_dir_inode();

  MutationRef mut(new MutationImpl);
  mut->lock_object(diri);

  CInode *in = dn->get_projected_linkage()->get_inode();
  mut->lock_object(in);

  dout(10) << "_purge_stray_purged " << *dn << " " << *in << dendl;

  if (only_head) {
    /* This was a ::truncate */
    
    inode_t *pi = in->project_inode();
    mut->add_projected_inode(in, false);
    pi->version = in->pre_dirty();
    pi->size = 0;
    pi->rstat.rbytes = 0;
    pi->max_size_ever = 0;
    pi->client_ranges.clear();
    pi->truncate_size = 0;
    pi->truncate_from = 0;

    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray truncate");
    le->metablob.add_primary_dentry(dn, in, true);

    mds->mdlog->start_entry(le);
    mut->ls = mds->mdlog->get_current_segment();
    mds->mdlog->submit_entry(le, new C_TruncateStrayLogged(this, dn, mut));
  } else {
    if (in->get_num_ref() != (int)in->is_dirty() ||
	dn->get_num_ref() != (int)dn->is_dirty() + !!in->get_num_ref() + 1/*PIN_PURGING*/) {
      // Nobody should be taking new references to an inode when it
      // is being purged (aside from it were 

      derr << "Rogue reference after purge to " << *dn << dendl;
      assert(0 == "rogue reference to purging inode");
    }

    // kill dentry.
    version_t pdv = dn->pre_dirty();
    dn->push_projected_linkage(); // NULL

    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray");
    le->metablob.add_destroyed_inode(in->ino());

    CDir *dir = dn->get_dir();
    fnode_t *pf = dir->project_fnode();
    pf->version = dir->pre_dirty();
    mut->add_projected_fnode(dir, false);
    if (in->is_dir())
      pf->fragstat.nsubdirs--;
    else
      pf->fragstat.nfiles--;
    pf->rstat.sub(in->get_inode()->accounted_rstat);

    le->metablob.add_null_dentry(dn, true);

    mds->mdlog->start_entry(le);
    mut->ls = mds->mdlog->get_current_segment();
    mds->mdlog->submit_entry(le, new C_PurgeStrayLogged(this, dn, pdv, mut));

    //num_strays--;
    //logger->set(l_mdc_num_strays, num_strays);
    //logger->inc(l_mdc_strays_purged);
  }

  mut->unlock_all_objects();
  mut->start_committing();

#if 0
  num_strays_purging--;
  logger->set(l_mdc_num_strays_purging, num_strays_purging);

  // Release resources
  dout(10) << __func__ << ": decrementing op allowance "
    << ops_allowance << " from " << ops_in_flight << " in flight" << dendl;
  assert(ops_in_flight >= ops_allowance);
  ops_in_flight -= ops_allowance;
  logger->set(l_mdc_num_purge_ops, ops_in_flight);
  files_purging -= 1;
  _advance();
#endif
}


void StrayManager::_purge_stray_logged(CDentry *dn, version_t pdv, MutationRef& mut)
{
  mut->wait_committing();

  CInode* strayi = dn->get_dir_inode();
  mut->lock_object(strayi);

  CInode *in = dn->get_linkage()->get_inode();
  in->mutex_lock();

  dout(10) << "_purge_stray_logged " << *dn << " " << *in << dendl;

  //assert(!in->state_test(CInode::STATE_RECOVERING));

  // unlink
  assert(dn->get_projected_linkage()->is_null());
  dn->get_dir()->unlink_inode(dn);
  dn->pop_projected_linkage();
  dn->mark_dirty(pdv, mut->ls);

  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  // drop dentry?
  if (dn->is_new()) {
//  dout(20) << " dn is new, removing" << dendl;
    dn->mark_clean();
  //dn->dir->remove_dentry(dn);
    mdcache->touch_dentry_bottom(dn);  // drop dn as quickly as possible.
  }

  mut->apply();
  mut->cleanup();

  // drop inode
  if (in->is_dirty())
    in->mark_clean();

  in->state_set(CInode::STATE_FREEING);
  in->state_clear(CInode::STATE_PURGING);

  in->mutex_unlock();

  mdcache->remove_inode(in);
}

void StrayManager::_truncate_stray_logged(CDentry *dn, MutationRef& mut)
{
  mut->wait_committing();

  CInode* strayi = dn->get_dir_inode();
  mut->lock_object(strayi);

  CInode *in = dn->get_projected_linkage()->get_inode();
  mut->lock_object(in);

  dout(10) << "_truncate_stray_logged " << *dn << " " << *in << dendl;

  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  mut->apply();
  mut->cleanup();
}

/*
bool StrayManager::eval_stray(CDentry *dn)
{
  assert(dn->state_test(CDentry::STATE_STRAY));

  CInode* diri = dn->get_dir_inode();
  CInode* in;

  {
    CObject::Locker l(diri);
    CDir *dir = dn->get_dir();

    if (dn->get_num_ref() > 0)
      return false;

    const CDentry::linkage_t *dnl = dn->get_linkage();
    if (dnl->is_null()) {
      dir->remove_dentry(dn);
      return true;
    }

    assert(dnl->is_primary());
    in = dnl->get_inode();

    if (!in->mutex_trylock()) {
      return false;
    }

    if (in->get_num_ref() > 0) {
      in->mutex_unlock();
      return false;
    }

    dn->get(CDentry::PIN_PURGING);
    dn->state_set(CDentry::STATE_PURGING);
    in->state_set(CInode::STATE_PURGING);
  }

  purge(dn, in, 0);
  return true;
}
*/

// called by MDCache::trim_inode
bool StrayManager::eval_stray(CDentry *dn, CInode *in)
{
  CInode* diri = dn->get_dir_inode();
  diri->mutex_assert_locked_by_me();
  in->mutex_assert_locked_by_me();
  assert(dn->get_num_ref() == 0);
  assert(in->get_num_ref() == 0);

  if (in->get_inode()->nlink > 0)
    return false;

  dn->get(CDentry::PIN_PURGING);
  dn->state_set(CDentry::STATE_PURGING);
  in->state_set(CInode::STATE_PURGING);

  mds->queue_context(new C_IO_PurgeStray(this, dn, in, 0));

  return true;
}

bool StrayManager::eval_stray(CDentry *dn)
{
  if (dn->state_test(CDentry::STATE_PURGING))
    return false;

  uint64_t old_state = dn->state_set(CDentry::STATE_EVALUATINGSTRAY);
  if (!(old_state & CDentry::STATE_EVALUATINGSTRAY)) {
    Mutex::Locker l(lock);
    delayed_eval_stray.push_back(make_pair(dn->get_dir(), dn->name));
  }
  return true;
}

void StrayManager::advance_delayed()
{
  lock.Lock();
  while (!delayed_eval_stray.empty()) {
    CDir *dir = delayed_eval_stray.front().first;
    string dname = delayed_eval_stray.front().second;
    delayed_eval_stray.pop_front();
    lock.Unlock();

    {
      CInode* diri = dir->get_inode();
      CObject::Locker l(diri);

      CInode *in;
      CDentry *dn = dir->__lookup(dname.c_str());
      if (!dn)
	goto next;

      dn->state_clear(CDentry::STATE_EVALUATINGSTRAY);

      if (dn->get_num_ref() > 0) {
	goto next;
      }

      const CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_null()) {
	goto next;
      }

      assert(dnl->is_primary());
      in = dnl->get_inode();
      if (!in->mutex_trylock()) {
	goto next;
      }

      if (in->get_num_ref() > 0) {
	in->mutex_unlock();
	goto next;
      }

      if (in->get_inode()->nlink == 0) {
	dn->get(CDentry::PIN_PURGING);
	dn->state_set(CDentry::STATE_PURGING);
	in->state_set(CInode::STATE_PURGING);

	mds->queue_context(new C_IO_PurgeStray(this, dn, in, 0));
      }

      in->mutex_unlock();
    }
next:
    lock.Lock();
  }
  lock.Unlock();
}

/*
void StrayManager::notify_stray_loaded(CDentry *dn, CInode *in)
{
  in->mutex_assert_locked_by_me();
  if (in->get_inode()->nlink == 0)
    dn->state_set(CDentry::STATE_ORPHANINODE);
}
*/

