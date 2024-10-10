// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/debug.h"
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

#include "BatchOp.h"

void BatchOp::forward(mds_rank_t target)
{
  dout(20) << __func__ << ": forwarding batch ops to " << target << ": ";
  print(*_dout);
  *_dout << dendl;
  _forward(target);
}

void BatchOp::respond(int r)
{
  dout(20) << __func__ << ": responding to batch ops with result=" << r << ": ";
  print(*_dout);
  *_dout << dendl;
  _respond(r);
}
