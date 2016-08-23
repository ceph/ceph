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
  assert(mds != NULL);
  MDSContext::complete(r);
}

MDSRank *MDSInternalContext::get_mds() {
  return mds;
}

void MDSIOContextBase::complete(int r) {
  MDSRank *mds = get_mds();
  assert(mds != NULL);
  MDSContext::complete(r);
}

MDSRank *MDSIOContext::get_mds() {
  return mds;
}
