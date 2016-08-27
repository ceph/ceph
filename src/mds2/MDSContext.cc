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

#include "MDSContext.h"

#include "MDSRank.h"
#include "MDLog.h"


#include "common/dout.h"
#define dout_subsys ceph_subsys_mds


void MDSInternalContextBase::complete(int r) {
  MDSRank *mds = get_mds();
  assert(mds != NULL);
  MDSContext::complete(r);
}

MDSRank *MDSInternalContext::get_mds() {
  return mds;
}

MDSRank *MDSInternalContextGather::get_mds()
{
  derr << "Forbidden call to MDSInternalContextGather::get_mds by " << typeid(*this).name() << dendl;
  assert(0);
}

void MDSLogContextBase::complete(int r) {
  assert(write_pos > 0);
  get_mds()->mdlog->set_safe_pos(write_pos);
  MDSContext::complete(r);
}
