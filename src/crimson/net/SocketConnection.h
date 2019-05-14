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

#include <seastar/core/sharded.hh>

#include "msg/Policy.h"
#include "Connection.h"
#include "Socket.h"
#include "crimson/thread/Throttle.h"

namespace ceph::net {

class Protocol;
class SocketMessenger;
class SocketConnection;
using SocketConnectionRef = seastar::shared_ptr<SocketConnection>;

class SocketConnection : public Connection {
  SocketMessenger& messenger;
  std::unique_ptr<Protocol> protocol;

  // if acceptor side, socket_port is different from peer_addr.get_port();
  // if connector side, socket_port is different from my_addr.get_port().
  enum class side_t {
    none,
    acceptor,
    connector
  };
  side_t side = side_t::none;
  uint16_t socket_port = 0;

  ceph::net::Policy<ceph::thread::Throttle> policy;
  uint64_t features;
  void set_features(uint64_t new_features) {
    features = new_features;
  }

  /// the seq num of the last transmitted message
  seq_num_t out_seq = 0;
  /// the seq num of the last received message
  seq_num_t in_seq = 0;
  /// update the seq num of last received message
  /// @returns true if the @c seq is valid, and @c in_seq is updated,
  ///          false otherwise.
  bool update_rx_seq(seq_num_t seq);

  // messages to be resent after connection gets reset
  std::deque<MessageRef> out_q;
  // messages sent, but not yet acked by peer
  std::deque<MessageRef> sent;

  // which of the peer_addrs we're connecting to (as client)
  // or should reconnect to (as peer)
  entity_addr_t target_addr;

 public:
  SocketConnection(SocketMessenger& messenger,
                   Dispatcher& dispatcher,
                   bool is_msgr2);
  ~SocketConnection() override;

  Messenger* get_messenger() const override;

  int get_peer_type() const override {
    return peer_type;
  }

  seastar::future<bool> is_connected() override;

  seastar::future<> send(MessageRef msg) override;

  seastar::future<> keepalive() override;

  seastar::future<> close() override;

  seastar::shard_id shard_id() const override;

  void print(ostream& out) const override;

 public:
  /// start a handshake from the client's perspective,
  /// only call when SocketConnection first construct
  void start_connect(const entity_addr_t& peer_addr,
                     const entity_type_t& peer_type);
  /// start a handshake from the server's perspective,
  /// only call when SocketConnection first construct
  void start_accept(SocketFRef&& socket,
                    const entity_addr_t& peer_addr);

  seq_num_t rx_seq_num() const {
    return in_seq;
  }

  bool is_server_side() const {
    return policy.server;
  }

  bool is_lossy() const {
    return policy.lossy;
  }

  /// move all messages in the sent list back into the queue
  void requeue_sent();

  std::tuple<seq_num_t, std::deque<MessageRef>> get_out_queue() {
    return {out_seq, std::move(out_q)};
  }

  friend class Protocol;
  friend class ProtocolV1;
  friend class ProtocolV2;
};

} // namespace ceph::net
