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

#ifndef CEPH_MSG_ASYNC_SMCSTACK_H
#define CEPH_MSG_ASYNC_SMCSTACK_H

#include <thread>

#include "msg/msg_types.h"
#include "msg/async/PosixStack.h"

#include "SMCNetHandler.h"

class SMCWorker : public PosixWorker {
public:
  SMCWorker(CephContext *c, unsigned i)
    : PosixWorker(c, i) {
      PosixWorker::resetNetHandler(new ceph::smc::SMCNetHandler(c));
    }
};

class SMCStack : public PosixNetworkStack {
public:
  explicit SMCStack(CephContext *c)
    : PosixNetworkStack(c) {}

  virtual Worker* create_worker(CephContext *c, unsigned worker_id) override {
    return new SMCWorker(c, worker_id);
  }
};

#endif // CEPH_MSG_ASYNC_SMCSTACK_H
