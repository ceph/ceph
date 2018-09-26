// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <seastar/core/shared_future.hh>

#include "Connection.h"
#include "Workspace.h"
#include "Session.h"

namespace ceph::net {

class SocketConnection : public Connection {
  Session s;
  Workspace workspace;

  /// becomes available when handshake completes, and when all previous messages
  /// have been sent to the output stream. send() chains new messages as
  /// continuations to this future to act as a queue
  seastar::future<> send_ready = seastar::now();

 public:
  SocketConnection(Messenger *messenger,
                   Dispatcher *disp,
                   const entity_addr_t& my_addr,
                   const entity_addr_t& peer_addr);
  SocketConnection(Messenger *messenger,
                   Dispatcher *disp,
                   const entity_addr_t& my_addr,
                   const entity_addr_t& peer_addr,
                   seastar::connected_socket&& socket);
  ~SocketConnection();

  const entity_addr_t& get_my_addr() const { return s.my_addr; };
  const entity_addr_t& get_peer_addr() const { return s.peer_addr; };

  bool is_connected() override;

  void start_connect(entity_type_t peer_type) override{
    workspace.start_connect(peer_type);
  }

  void start_accept() override {
    workspace.start_accept();
  }

  seastar::future<> send(MessageRef msg) override;

  seastar::future<> keepalive() override;

  seastar::future<> close() override;

  uint32_t connect_seq() const override {
    return s.connect_seq;
  }
  uint32_t peer_global_seq() const override {
    return s.peer_global_seq;
  }
  seq_num_t rx_seq_num() const {
    return s.in_seq;
  }
  state_t get_state() const override {
    return s.protocol->state();
  }
  bool is_server_side() const override {
    return s.policy.server;
  }
  bool is_lossy() const override {
    return s.policy.lossy;
  }

private:
  void requeue_sent() override;
  std::tuple<seq_num_t, std::queue<MessageRef>> get_out_queue() override {
    return {s.out_seq, std::move(s.out_q)};
  }

};

} // namespace ceph::net
