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

#include "msg/msg_types.h"
#include "msg/async/net_handler.h"

#include "GenericSocket.h"

class PosixNetworkStack : public NetworkStack {
  NetHandler net;

 public:
  explicit PosixNetworkStack(CephContext *c): NetworkStack(c), net(c) {}
  virtual int listen(entity_addr_t &sa, const SocketOptions &opt, ServerSocket *sock) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  static std::unique_ptr<NetworkStack> create(CephContext *cct, unsigned i) {
    return std::unique_ptr<NetworkStack>(std::unique_ptr<NetworkStack>(new PosixNetworkStack(cct)));
  }
  virtual bool support_zero_copy_read() const override { return false; }
  virtual int zero_copy_read(size_t, bufferptr*) override {
    return -EOPNOTSUPP;
  }
};

#endif //CEPH_MSG_POSIXSTACK_H_H
