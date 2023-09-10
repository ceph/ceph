// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "MDSRank.h"

#include "MDSContext.h"

#include "common/dout.h"
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

void MDSContext::complete(int r) {
  MDSRankBase *mds = get_mds();
  ceph_assert(mds != nullptr);
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));
  dout(10) << "MDSContext::complete: " << typeid(*this).name() << dendl;
  mds->heartbeat_reset();
  return Context::complete(r);
}

void MDSInternalContextWrapper::finish(int r)
{
  fin->complete(r);
}

void MDSLockingWrapper::complete(int r)
{
  if (wrapped) {
    bool do_lock = mds != nullptr;
    if (auto mds_wrapped = dynamic_cast<MDSContext*>(wrapped); mds_wrapped != nullptr) {
      do_lock = !(mds == nullptr || mds_wrapped->takes_lock());
    }
    if (do_lock) {
      std::lock_guard l(mds->get_lock());
      wrapped->complete(r);
    } else {
      wrapped->complete(r);
    }
    wrapped = nullptr;
  }
  delete this;
}

struct MDSIOContextList {
  elist<MDSIOContextBase*> list;
  ceph::spinlock lock;
  MDSIOContextList() : list(member_offset(MDSIOContextBase, list_item)) {}
  ~MDSIOContextList() {
    list.clear(); // avoid assertion in elist's destructor
  }
} ioctx_list;

MDSIOContextBase::MDSIOContextBase(bool track)
{
  created_at = ceph::coarse_mono_clock::now();
  if (track) {
    ioctx_list.lock.lock();
    ioctx_list.list.push_back(&list_item);
    ioctx_list.lock.unlock();
  }
}

MDSIOContextBase::~MDSIOContextBase()
{
  ioctx_list.lock.lock();
  list_item.remove_myself();
  ioctx_list.lock.unlock();
}

bool MDSIOContextBase::check_ios_in_flight(ceph::coarse_mono_time cutoff,
					   std::string& slow_count,
					   ceph::coarse_mono_time& oldest)
{
  static const unsigned MAX_COUNT = 100;
  unsigned slow = 0;

  ioctx_list.lock.lock();
  for (elist<MDSIOContextBase*>::iterator p = ioctx_list.list.begin(); !p.end(); ++p) {
    MDSIOContextBase *c = *p;
    if (c->created_at >= cutoff)
      break;
    ++slow;
    if (slow > MAX_COUNT)
      break;
    if (slow == 1)
      oldest = c->created_at;
  }
  ioctx_list.lock.unlock();

  if (slow > 0) {
    if (slow > MAX_COUNT)
      slow_count = std::to_string(MAX_COUNT) + "+";
    else
      slow_count = std::to_string(slow);
    return true;
  } else {
    return false;
  }
}

void MDSIOContextBase::complete(int r) {
  MDSRankBase *mds = get_mds();

  dout(10) << "MDSIOContextBase::complete: " << typeid(*this).name() << dendl;
  ceph_assert(mds != NULL);
  // Note, MDSIOContext is passed outside the MDS and, strangely, we grab the
  // lock here when MDSContext::complete would otherwise assume the lock is
  // already acquired.
  std::lock_guard l(mds->get_lock());
  complete_no_lock(r);
}

void MDSIOContextBase::complete_no_lock(int r) {
  MDSRankBase* mds = get_mds();
  if (mds->is_daemon_stopping()) {
    dout(4) << "MDSIOContextBase::complete: dropping for stopping "
            << typeid(*this).name() << dendl;
    return;
  }

  // It's possible that the osd op requests will be stuck and then times out
  // after "rados_osd_op_timeout", the mds won't know what we should it, just
  // respawn it.
  if (r == -CEPHFS_EBLOCKLISTED || r == -CEPHFS_ETIMEDOUT) {
    derr << "MDSIOContextBase: failed with " << r << ", restarting..." << dendl;
    mds->respawn();
  } else {
    MDSContext::complete(r);
  }
}

void MDSLogContextBase::complete(int r) {
  MDSRankBase* mds = get_mds();
  // we have to take the lock here to protect access
  // to the mdlog variables.
  // We account for this when we will call the _no_lock
  // version of the io context base complete method
  std::lock_guard l(mds->get_lock());

  MDLog *mdlog = get_mds()->get_log();
  pre_finish(r);
  if (log_segment) {
    log_segment->bounds_upkeep(event_start_pos, event_end_pos);
  }
  // MDSIOContext::complete() frees `this`
  auto safe_pos = event_end_pos;
  MDSIOContextBase::complete_no_lock(r);
  // safe_pos must be updated after MDSIOContext::complete() call
  mdlog->set_safe_pos(safe_pos);
}

void MDSLogContextBase::print(std::ostream& out) const
{
  out << "log_event(" << event_start_pos << ".." << event_end_pos << " @ " << *log_segment << ")";
}

void MDSIOContextWrapper::finish(int r)
{
  fin->complete(r);
}

void C_IO_Wrapper::complete(int r)
{
  if (async) {
    async = false;
    get_mds()->get_finisher()->queue(this, r);
  } else {
    MDSIOContext::complete(r);
  }
}
