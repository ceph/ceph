// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * Copyright (C) IBM Corp. 2024
 *
 * Author: Aliaksei Makarau <aliaksei.makarau@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_ASYNC_SMCDSTACK_H
#define CEPH_MSG_ASYNC_SMCDSTACK_H

#include <thread>

#include "msg/msg_types.h"
#include "msg/async/Stack.h"
#include "SMCDNetHandler.h"

class SMCDWorker : public Worker {
  ceph::smcd::SMCDNetHandler net;
  void initialize() override;

 public:
  SMCDWorker(CephContext *c, unsigned i)
      : Worker(c, i), net(c) {}
  int listen(entity_addr_t &sa,
             unsigned addr_slot,
             const SocketOptions &opt,
             ServerSocket *socks) override;
  int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
};

class SMCDStack : public NetworkStack {
  std::vector<std::thread> threads;

  virtual Worker* create_worker(CephContext *c, unsigned worker_id) override {
    return new SMCDWorker(c, worker_id);
  }

 public:
  explicit SMCDStack(CephContext *c);

  void spawn_worker(std::function<void ()> &&func) override {
    threads.emplace_back(std::move(func));
  }
  void join_worker(unsigned i) override {
    ceph_assert(threads.size() > i && threads[i].joinable());
    threads[i].join();
  }
};

#endif //CEPH_MSG_ASYNC_SMCDSTACK_H
