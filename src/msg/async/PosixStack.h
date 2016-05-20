// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_POSIXSTACK_H_H
#define CEPH_MSG_POSIXSTACK_H_H

#include <thread>

#include "msg/msg_types.h"
#include "msg/async/net_handler.h"

#include "Stack.h"

class PosixWorker : public Worker {
  NetHandler net;
  std::thread t;
  virtual void initialize();
 public:
  PosixWorker(CephContext *c, unsigned i)
      : Worker(c, i), net(c) {}
  virtual int listen(entity_addr_t &sa, const SocketOptions &opt,
                     ServerSocket *socks) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
};

class PosixNetworkStack : public NetworkStack {
  vector<int> coreids;
  vector<std::thread> threads;

 public:
  explicit PosixNetworkStack(CephContext *c, const string &t);

  int get_cpuid(int id) {
    if (coreids.empty())
      return -1;
    return coreids[id % coreids.size()];
  }
  virtual void spawn_workers(std::vector<std::function<void ()>> &funcs) override {
    // used to tests
    for (auto &&func : funcs)
      threads.emplace_back(std::thread(std::move(func))); // this is happen in actual env
  }
  virtual void join_workers() override {
    for (auto &&t : threads)
      t.join();
    threads.clear();
  }
};

#endif //CEPH_MSG_POSIXSTACK_H_H
