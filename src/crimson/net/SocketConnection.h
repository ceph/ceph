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
#include "crimson/net/Connection.h"
#include "crimson/net/Socket.h"

namespace crimson::net {

class ProtocolV2;
class SocketMessenger;
class SocketConnection;
using SocketConnectionRef = seastar::shared_ptr<SocketConnection>;

#ifdef UNIT_TESTS_BUILT
class Interceptor;
#endif

/**
 * ConnectionHandler
 *
 * The interface class to implement Connection, called by SocketConnection.
 *
 * The operations must be done in get_shard_id().
 */
class ConnectionHandler {
public:
  using clock_t = seastar::lowres_system_clock;

  virtual ~ConnectionHandler() = default;

  ConnectionHandler(const ConnectionHandler &) = delete;
  ConnectionHandler(ConnectionHandler &&) = delete;
  ConnectionHandler &operator=(const ConnectionHandler &) = delete;
  ConnectionHandler &operator=(ConnectionHandler &&) = delete;

  virtual seastar::shard_id get_shard_id() const = 0;

  virtual bool is_connected() const = 0;

  virtual seastar::future<> send(MessageURef) = 0;

  virtual seastar::future<> send_keepalive() = 0;

  virtual clock_t::time_point get_last_keepalive() const = 0;

  virtual clock_t::time_point get_last_keepalive_ack() const = 0;

  virtual void set_last_keepalive_ack(clock_t::time_point) = 0;

  virtual void mark_down() = 0;

protected:
  ConnectionHandler() = default;
};

class SocketConnection : public Connection {
 /*
  * Connection interfaces, public to users
  * Working in ConnectionHandler::get_shard_id()
  */
 public:
  SocketConnection(SocketMessenger& messenger,
                   ChainedDispatchers& dispatchers);

  ~SocketConnection() override;

  const seastar::shard_id get_shard_id() const override {
    return io_handler->get_shard_id();
  }

  const entity_name_t &get_peer_name() const override {
    return peer_name;
  }

  const entity_addr_t &get_peer_addr() const override {
    return peer_addr;
  }

  const entity_addr_t &get_peer_socket_addr() const override {
    return target_addr;
  }

  uint64_t get_features() const override {
    return features;
  }

  bool is_connected() const override;

  seastar::future<> send(MessageURef msg) override;

  seastar::future<> send_keepalive() override;

  clock_t::time_point get_last_keepalive() const override;

  clock_t::time_point get_last_keepalive_ack() const override;

  void set_last_keepalive_ack(clock_t::time_point when) override;

  void mark_down() override;

  bool has_user_private() const override {
    return user_private != nullptr;
  }

  user_private_t &get_user_private() override {
    assert(has_user_private());
    return *user_private;
  }

  void set_user_private(std::unique_ptr<user_private_t> new_user_private) override {
    assert(!has_user_private());
    user_private = std::move(new_user_private);
  }

  void print(std::ostream& out) const override;

 /*
  * Public to SocketMessenger
  * Working in SocketMessenger::get_shard_id();
  */
 public:
  /// start a handshake from the client's perspective,
  /// only call when SocketConnection first construct
  void start_connect(const entity_addr_t& peer_addr,
                     const entity_name_t& peer_name);

  /// start a handshake from the server's perspective,
  /// only call when SocketConnection first construct
  void start_accept(SocketFRef&& socket,
                    const entity_addr_t& peer_addr);

  seastar::future<> close_clean_yielded();

  seastar::socket_address get_local_address() const;

  seastar::shard_id get_messenger_shard_id() const;

  SocketMessenger &get_messenger() const;

  ConnectionRef get_local_shared_foreign_from_this();

private:
  void set_peer_type(entity_type_t peer_type);

  void set_peer_id(int64_t peer_id);

  void set_peer_name(entity_name_t name) {
    set_peer_type(name.type());
    set_peer_id(name.num());
  }

  void set_features(uint64_t f);

  void set_socket(Socket *s);

#ifdef UNIT_TESTS_BUILT
  bool is_protocol_ready() const override;

  bool is_protocol_standby() const override;

  bool is_protocol_closed_clean() const override;

  bool is_protocol_closed() const override;

  // peer wins if myaddr > peeraddr
  bool peer_wins() const override;

  Interceptor *interceptor = nullptr;
#else
  // peer wins if myaddr > peeraddr
  bool peer_wins() const;
#endif

private:
  const seastar::shard_id msgr_sid;

  /*
   * Core owner is messenger core, may allow to access from the I/O core.
   */
  SocketMessenger& messenger;

  std::unique_ptr<ProtocolV2> protocol;

  Socket *socket = nullptr;

  entity_name_t peer_name = {0, entity_name_t::NEW};

  entity_addr_t peer_addr;

  // which of the peer_addrs we're connecting to (as client)
  // or should reconnect to (as peer)
  entity_addr_t target_addr;

  uint64_t features = 0;

  ceph::net::Policy<crimson::common::Throttle> policy;

  uint64_t peer_global_id = 0;

  /*
   * Core owner is I/O core (mutable).
   */
  std::unique_ptr<ConnectionHandler> io_handler;

  /*
   * Core owner is up to the connection user.
   */
  std::unique_ptr<user_private_t> user_private;

  friend class IOHandler;
  friend class ProtocolV2;
  friend class FrameAssemblerV2;
};

} // namespace crimson::net

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::net::SocketConnection> : fmt::ostream_formatter {};
#endif
