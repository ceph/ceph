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

MDSRank *MDSInternalContextGather::get_mds()
{
  derr << "Forbidden call to MDSInternalContextGather::get_mds by " << typeid(*this).name() << dendl;
  assert(0);
}

