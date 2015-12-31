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

class PosixNetworkStack : public NetWorkStack {
  NetHandler handler;
 public:
  explicit PosixNetworkStack(CephContext *c): NetworkStack(c), _reuseport(false) {}
  virtual int listen(const entity_addr_t &sa, const listen_options &opt, ServerSocket *sock) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  static std::unique_ptr<NetworkStack> create(CephContext *cct, unsigned i) {
    return std::unique_ptr<NetworkStack>(std::unique_ptr<NetworkStack>(new PosixNetworkStack(cct)));
  }
};

#endif //CEPH_MSG_POSIXSTACK_H_H