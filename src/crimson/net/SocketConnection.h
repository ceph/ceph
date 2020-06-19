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
#include "crimson/common/throttle.h"
#include "crimson/net/chained_dispatchers.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Socket.h"

namespace crimson::net {

class Dispatcher;
class Protocol;
class SocketMessenger;
class SocketConnection;
using SocketConnectionRef = seastar::shared_ptr<SocketConnection>;

class SocketConnection : public Connection {
  SocketMessenger& messenger;
  std::unique_ptr<Protocol> protocol;

  ceph::net::Policy<crimson::common::Throttle> policy;

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

  seastar::shard_id shard_id() const;

 public:
  SocketConnection(SocketMessenger& messenger,
                   ChainedDispatchersRef& dispatcher,
                   bool is_msgr2);
  ~SocketConnection() override;

  Messenger* get_messenger() const override;

  bool is_connected() const override;

#ifdef UNIT_TESTS_BUILT
  bool is_closed_clean() const override;

  bool is_closed() const override;

  bool peer_wins() const override;
#else
  bool peer_wins() const;
#endif

  seastar::future<> send(MessageRef msg) override;

  seastar::future<> keepalive() override;

  void mark_down() override;

  void print(ostream& out) const override;

  /// start a handshake from the client's perspective,
  /// only call when SocketConnection first construct
  void start_connect(const entity_addr_t& peer_addr,
                     const entity_name_t& peer_name);
  /// start a handshake from the server's perspective,
  /// only call when SocketConnection first construct
  void start_accept(SocketRef&& socket,
                    const entity_addr_t& peer_addr);

  seastar::future<> close_clean(bool dispatch_reset);

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
