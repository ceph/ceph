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

void MDSInternalContextBase::complete(int r) {
  MDSRank *mds = get_mds();

  dout(10) << "MDSInternalContextBase::complete: " << typeid(*this).name() << dendl;
  assert(mds != NULL);
  assert(mds->mds_lock.is_locked_by_me());
  MDSContext::complete(r);
}


MDSRank *MDSInternalContext::get_mds() {
  return mds;
}

MDSRank *MDSInternalContextWrapper::get_mds()
{
  return mds;
}

void MDSInternalContextWrapper::finish(int r)
{
  fin->complete(r);
}

elist<MDSIOContextBase*> MDSIOContextBase::ctx_list(member_offset(MDSIOContextBase, list_item));
std::atomic_flag MDSIOContextBase::ctx_list_lock = ATOMIC_FLAG_INIT;

MDSIOContextBase::MDSIOContextBase(bool track)
{
  created_at = ceph::coarse_mono_clock::now();
  if (track) {
    simple_spin_lock(&ctx_list_lock);
    ctx_list.push_back(&list_item);
    simple_spin_unlock(&ctx_list_lock);
  }
}

MDSIOContextBase::~MDSIOContextBase()
{
  simple_spin_lock(&ctx_list_lock);
  list_item.remove_myself();
  simple_spin_unlock(&ctx_list_lock);
}

bool MDSIOContextBase::check_ios_in_flight(ceph::coarse_mono_time cutoff,
					   std::string& slow_count,
					   ceph::coarse_mono_time& oldest)
{
  static const unsigned MAX_COUNT = 100;
  unsigned slow = 0;

  simple_spin_lock(&ctx_list_lock);
  for (elist<MDSIOContextBase*>::iterator p = ctx_list.begin(); !p.end(); ++p) {
    MDSIOContextBase *c = *p;
    if (c->created_at >= cutoff)
      break;
    ++slow;
    if (slow > MAX_COUNT)
      break;
    if (slow == 1)
      oldest = c->created_at;
  }
  simple_spin_unlock(&ctx_list_lock);

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
  MDSRank *mds = get_mds();

  dout(10) << "MDSIOContextBase::complete: " << typeid(*this).name() << dendl;
  assert(mds != NULL);
  Mutex::Locker l(mds->mds_lock);

  if (mds->is_daemon_stopping()) {
    dout(4) << "MDSIOContextBase::complete: dropping for stopping "
            << typeid(*this).name() << dendl;
    return;
  }

  if (r == -EBLACKLISTED) {
    derr << "MDSIOContextBase: blacklisted!  Restarting..." << dendl;
    mds->respawn();
  } else {
    MDSContext::complete(r);
  }
}

void MDSLogContextBase::complete(int r) {
  MDLog *mdlog = get_mds()->mdlog;
  uint64_t safe_pos = write_pos;
  pre_finish(r);
  // MDSContextBase::complete() free this
  MDSIOContextBase::complete(r);
  mdlog->set_safe_pos(safe_pos);
}

MDSRank *MDSIOContext::get_mds() {
  return mds;
}

MDSRank *MDSIOContextWrapper::get_mds() {
  return mds;
}

void MDSIOContextWrapper::finish(int r)
{
  fin->complete(r);
}

void C_IO_Wrapper::complete(int r)
{
  if (async) {
    async = false;
    get_mds()->finisher->queue(this, r);
  } else {
    MDSIOContext::complete(r);
  }
}

MDSRank *MDSInternalContextGather::get_mds()
{
  derr << "Forbidden call to MDSInternalContextGather::get_mds by " << typeid(*this).name() << dendl;
  ceph_abort();
}

