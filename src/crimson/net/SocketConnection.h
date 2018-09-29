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

#include "Connection.h"
#include "ProtocolV1.h"
#include "Session.h"

namespace ceph::net {

class SocketMessenger;

class SocketConnection : public Connection {
  Session s;
  std::unique_ptr<Protocol> protocol;

 public:
  SocketConnection(SocketMessenger& messenger,
                   Dispatcher& disp,
                   const entity_addr_t& my_addr);
  ~SocketConnection() {}

  const entity_addr_t& get_my_addr() const { return s.my_addr; };
  const entity_addr_t& get_peer_addr() const { return s.peer_addr; };

  bool is_connected() override {
    return protocol->is_connected();
  }

  seastar::future<> send(MessageRef msg) override {
    return protocol->send(std::move(msg));
  }

  seastar::future<> keepalive() override {
    return protocol->keepalive();
  }

  seastar::future<> close() override {
    return protocol->close();
  }

  // Only call when SocketConnection first construct
  void start_connect(const entity_addr_t& peer_addr, const entity_type_t& peer_type) {
    protocol->start_connect(peer_addr, peer_type);
  }

  // Only call when SocketConnection first construct
  void start_accept(seastar::connected_socket&& socket, const entity_addr_t& peer_addr) {
    protocol->start_accept(std::move(socket), peer_addr);
  }

  /// the number of connections initiated in this session, increment when a
  /// new connection is established
  uint32_t connect_seq() const {
    return s.connect_seq;
  }

  /// the client side should connect us with a gseq. it will be reset with a
  /// the one of exsting connection if it's greater.
  uint32_t peer_global_seq() const {
    return s.peer_global_seq;
  }
  seq_num_t rx_seq_num() const {
    return s.in_seq;
  }
  bool is_server_side() const {
    return s.policy.server;
  }
  bool is_lossy() const {
    return s.policy.lossy;
  }

  /// move all messages in the sent list back into the queue
  void requeue_sent();

  /// get all messages in the out queue
  std::tuple<seq_num_t, std::queue<MessageRef>> get_out_queue() {
    return {s.out_seq, std::move(s.out_q)};
  }

};

using SocketConnectionRef = boost::intrusive_ptr<SocketConnection>;

} // namespace ceph::net
