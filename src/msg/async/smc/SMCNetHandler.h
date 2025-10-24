// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) IBM Corp. 2025
 *
 * Author: Aliaksei Makarau <aliaksei.makarau@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_ASYNC_SMCSTACK_NETHANDLER_H
#define CEPH_MSG_ASYNC_SMCSTACK_NETHANDLER_H

#include "common/config.h"
#include "msg/async/net_handler.h"

namespace ceph {
  namespace smc {
    class SMCNetHandler : public NetHandler {
      CephContext *cct;
    public:
      int create_socket(int domain, bool reuse_addr=false) override;
      SMCNetHandler(CephContext *c)
        : NetHandler(c), cct(c) {}
    };
  }
}

#endif // CEPH_MSG_ASYNC_SMCSTACK_NETHANDLER_H
