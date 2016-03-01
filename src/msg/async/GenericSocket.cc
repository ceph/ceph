// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "PosixStack.h"
#ifdef HAVE_DPDK
#include "msg/async/dpdk/DPDKStack.h"
#endif


std::unique_ptr<NetworkStack> NetworkStack::create(CephContext *c, const string &type, EventCenter *center)
{
  if (type == "posix")
    return std::unique_ptr<NetworkStack>(new PosixNetworkStack(c));
#ifdef HAVE_DPDK
  else if (type == "dpdk")
    return DPDKStack::create(c, center);
#endif

  return nullptr;
}
