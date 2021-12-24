// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_ASYNC_POSIXSTACK_H
#define CEPH_MSG_ASYNC_POSIXSTACK_H

#include <thread>

#include "msg/msg_types.h"
#include "msg/async/net_handler.h"

#include "Stack.h"
class PosixNetworkStack;
class PosixWorker : public Worker {
  ceph::NetHandler net;
  PosixNetworkStack *stack;
  void initialize() override;
 public:
  PosixWorker(PosixNetworkStack *stack, CephContext *c, unsigned i)
      : Worker(c, i), net(c), stack(stack) {}
  int listen(entity_addr_t &sa,
	     unsigned addr_slot,
	     const SocketOptions &opt,
	     ServerSocket *socks) override;
  int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
};

class PosixNetworkStack : public NetworkStack {
  std::vector<std::thread> threads;
  CephContext *cct;
  std::thread poll;
  bool done = false;
  int epfd = -1;
  std::mutex lock;
  std::unordered_map<int, ConnectedSocketImpl *> poll_conn;

  virtual Worker* create_worker(CephContext *c, unsigned worker_id) override {
    return new PosixWorker(this, c, worker_id);
  }

  int polling();
 public:
  explicit PosixNetworkStack(CephContext *c);

  void spawn_worker(std::function<void ()> &&func) override {
    threads.emplace_back(std::move(func));
    if (cct->_conf.get_val<bool>("ms_async_tcp_zerocopy")) {
      if (!poll.joinable())
        poll = std::thread(&PosixNetworkStack::polling, this);
    }
  }

  void join_worker(unsigned i) override {
    ceph_assert(threads.size() > i && threads[i].joinable());
    threads[i].join();

    if (cct->_conf.get_val<bool>("ms_async_tcp_zerocopy")) {
      if (i + 1 == threads.size()) {
        done = true;
        poll.join();
      }
    }
  }

  int get_epfd() { return epfd; }
  void register_poll(int fd, ConnectedSocketImpl *conn) {
    std::unique_lock<std::mutex> locker(lock);
    poll_conn.emplace(fd, conn);
  }

  void unregister_poll(int fd, ConnectedSocketImpl *conn) {
    std::unique_lock<std::mutex> locker(lock);
    auto it = poll_conn.find(fd);
    if (it != poll_conn.end())
      poll_conn.erase(it);
  }
};

#endif //CEPH_MSG_ASYNC_POSIXSTACK_H
