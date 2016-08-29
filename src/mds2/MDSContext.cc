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

#include "common/Finisher.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_mds

MDSRank *MDSContextGather::get_mds()
{
  derr << "Forbidden call to MDSContextGather::get_mds by " << typeid(*this).name() << dendl;
  assert(0);
}

void MDSAsyncContextBase::complete(int r) {
  if (finisher) {
    Finisher *f = finisher;
    finisher = NULL;
    f->queue(this, r);
    return;
  }
  MDSContextBase::complete(r);
}

void MDSLogContextBase::complete(int r) {
  assert(write_pos > 0);
  if (finisher) {
    Finisher *f = finisher;
    finisher = NULL;
    f->queue(this, r);
    return;
  }
  MDLog *mdlog = get_mds()->mdlog;
  uint64_t safe_pos = write_pos;
  // MDSContextBase::complete() free this
  MDSContextBase::complete(r);
  mdlog->set_safe_pos(safe_pos);
}
