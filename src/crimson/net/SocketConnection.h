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
#include "crimson/net/Connection.h"
#include "crimson/net/Socket.h"
#include "crimson/thread/Throttle.h"

namespace crimson::net {

class Dispatcher;
class Protocol;
class SocketMessenger;
class SocketConnection;
using SocketConnectionRef = seastar::shared_ptr<SocketConnection>;

class SocketConnection : public Connection {
  SocketMessenger& messenger;
  std::unique_ptr<Protocol> protocol;

  // if acceptor side, ephemeral_port is different from peer_addr.get_port();
  // if connector side, ephemeral_port is different from my_addr.get_port().
  enum class side_t {
    none,
    acceptor,
    connector
  };
  side_t side = side_t::none;
  uint16_t ephemeral_port = 0;
  void set_ephemeral_port(uint16_t port, side_t _side) {
    ephemeral_port = port;
    side = _side;
  }

  ceph::net::Policy<crimson::thread::Throttle> policy;

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
  std::deque<MessageRef> pending_q;
  // messages sent, but not yet acked by peer
  std::deque<MessageRef> sent;

 public:
  SocketConnection(SocketMessenger& messenger,
                   Dispatcher& dispatcher,
                   bool is_msgr2);
  ~SocketConnection() override;

  Messenger* get_messenger() const override;

  bool is_connected() const override;

#ifdef UNIT_TESTS_BUILT
  bool is_closed() const override;

  bool peer_wins() const override;
#else
  bool peer_wins() const;
#endif

  seastar::future<> send(MessageRef msg) override;

  seastar::future<> keepalive() override;

  seastar::future<> close() override;

  seastar::shard_id shard_id() const override;

  void print(ostream& out) const override;

  /// start a handshake from the client's perspective,
  /// only call when SocketConnection first construct
  void start_connect(const entity_addr_t& peer_addr,
                     const entity_type_t& peer_type);
  /// start a handshake from the server's perspective,
  /// only call when SocketConnection first construct
  void start_accept(SocketFRef&& socket,
                    const entity_addr_t& peer_addr);

  bool is_server_side() const {
    return policy.server;
  }

  bool is_lossy() const {
    return policy.lossy;
  }

  friend class Protocol;
  friend class ProtocolV1;
  friend class ProtocolV2;
};

} // namespace crimson::net
