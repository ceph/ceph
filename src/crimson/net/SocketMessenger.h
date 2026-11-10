// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <map>
#include <set>
#include <vector>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>

#include "crimson/net/chained_dispatchers.h"
#include "Messenger.h"
#include "Socket.h"
#include "SocketConnection.h"

namespace crimson::net {

class ShardedServerSocket;

class SocketMessenger final : public Messenger {
// Messenger public interfaces
public:
  SocketMessenger(const entity_name_t& myname,
                  const std::string& logic_name,
                  uint32_t nonce,
                  bool dispatch_only_on_this_shard,
                  bool listen_on_primary_only = false);

  ~SocketMessenger() override;

  const entity_name_t &get_myname() const override {
    return my_name;
  }

  const entity_addrvec_t &get_myaddrs() const override {
    return my_addrs;
  }

  void set_myaddrs(const entity_addrvec_t& addr) override;

  bool set_addr_unknowns(const entity_addrvec_t &addr) override;

  void set_auth_client(crimson::auth::AuthClient *ac) override {
    assert(seastar::this_shard_id() == sid);
    auth_client = ac;
  }

  void set_auth_server(crimson::auth::AuthServer *as) override {
    assert(seastar::this_shard_id() == sid);
    auth_server = as;
  }

  void set_auth_shard(seastar::shard_id shard) override {
    assert(seastar::this_shard_id() == sid);
    auth_shard = shard;
  }

  bind_ertr::future<> bind(const entity_addrvec_t& addr) override;

  /// bind one more listener per reactor core (see core_listeners), each
  /// on the first free port in ms_bind_port_min..ms_bind_port_max, on the
  /// IP of the main address. A connection accepted by core c's listener
  /// is placed on core c. Best effort: once a core gets no port, neither
  /// it nor the cores after it get a listener. Must follow a successful
  /// bind(); if called after start(), the listeners accept at once. Not
  /// available with dispatch_only_on_this_shard.
  seastar::future<> bind_core_listeners() override;

  /// the address of each core's listener, indexed by core: the current
  /// main address (thus reflecting a later-learned IP), with the core's
  /// port; empty for a core without a listener. Empty unless
  /// bind_core_listeners() was called.
  std::vector<entity_addrvec_t> get_core_addrs() const override;

  seastar::future<> start(const dispatchers_t& dispatchers) override;

  ConnectionRef connect(const entity_addr_t& peer_addr,
                        const entity_name_t& peer_name) override;

  bool owns_connection(Connection &conn) const override {
    assert(seastar::this_shard_id() == sid);
    return this == &static_cast<SocketConnection&>(conn).get_messenger();
  }

  // can only wait once
  seastar::future<> wait() override {
    assert(seastar::this_shard_id() == sid);
    return shutdown_promise.get_future();
  }

  void stop() override {
    assert(seastar::this_shard_id() == sid);
    dispatchers.clear();
  }

  bool is_started() const override {
    assert(seastar::this_shard_id() == sid);
    return !dispatchers.empty();
  }

  seastar::future<> shutdown() override;

  void print(std::ostream& out) const override {
    out << get_myname()
        << "(" << logic_name
        << ") " << get_myaddr();
  }

  SocketPolicy get_policy(entity_type_t peer_type) const override;

  SocketPolicy get_default_policy() const override;

  void set_default_policy(const SocketPolicy& p) override;

  void set_policy(entity_type_t peer_type, const SocketPolicy& p) override;

  void set_policy_throttler(entity_type_t peer_type, Throttle* throttle) override;

// SocketMessenger public interfaces
public:
  crimson::auth::AuthClient* get_auth_client() const {
    assert(seastar::this_shard_id() == sid);
    return auth_client;
  }

  crimson::auth::AuthServer* get_auth_server() const {
    assert(seastar::this_shard_id() == sid);
    return auth_server;
  }

  seastar::shard_id get_auth_shard() const {
    assert(seastar::this_shard_id() == sid);
    return auth_shard;
  }

  uint32_t get_global_seq(uint32_t old=0);

  void learned_addr(const entity_addr_t &peer_addr_for_me,
                    const SocketConnection& conn);

  /// find the registered connection from/to addr. listener_core selects
  /// a connection accepted by that core's per-core listener: a client may
  /// have one such connection per core, besides its main connection.
  SocketConnectionRef lookup_conn(
      const entity_addr_t& addr,
      std::optional<seastar::shard_id> listener_core = std::nullopt);

  /// our addresses as reached via the given listener: the main
  /// addresses, or the address of listener_core's per-core listener
  entity_addrvec_t get_myaddrs_via(
      std::optional<seastar::shard_id> listener_core) const;

  void accept_conn(SocketConnectionRef);

  void unaccept_conn(SocketConnectionRef);

  void register_conn(SocketConnectionRef);

  void unregister_conn(SocketConnectionRef);

  void closing_conn(SocketConnectionRef);

  void closed_conn(SocketConnectionRef);

  seastar::shard_id get_shard_id() const {
    return sid;
  }

#ifdef UNIT_TESTS_BUILT
  void set_interceptor(Interceptor *i) override {
    interceptor = i;
  }

  Interceptor *interceptor = nullptr;
#endif

  /// mark down all the connections registered for a, including those
  /// accepted by the per-core listeners
  seastar::future<> mark_down(const entity_addr_t& a) final;

  seastar::future<> mark_down_all_ports(const entity_addr_t& a) final;
private:
  seastar::future<> accept(SocketFRef &&, const entity_addr_t &,
                           std::optional<seastar::shard_id> listener_core);

  listen_ertr::future<> do_listen(const entity_addrvec_t& addr);

  /// try to bind to the first unused port of given address
  bind_ertr::future<> try_bind(const entity_addrvec_t& addr,
                               uint32_t min_port, uint32_t max_port);

  /// starts listening on the given address (port included)
  using listen_func_t =
    std::function<listen_ertr::future<>(const entity_addr_t&)>;
  /// call listen_fn on each port of [min_port, max_port] of the given
  /// address in turn, until one succeeds; resolves to the bound port.
  /// Fails with the last error if no port in the range is usable.
  listen_ertr::future<uint32_t> listen_on_first_free_port(
      const entity_addr_t& addr,
      uint32_t min_port, uint32_t max_port,
      listen_func_t listen_fn);

  /// the accept function of all our listeners: called on the shard the
  /// socket was placed on, hands the socket over to the messenger's
  /// shard (sid), where the connection is created and the handshake runs.
  /// listener_core: set for the per-core listeners
  seastar::future<> accept_on_primary(
      SocketRef socket,
      entity_addr_t peer_addr,
      std::optional<seastar::shard_id> listener_core);

  /// the address of core's per-core listener (which it must have)
  entity_addrvec_t get_core_addr(seastar::shard_id core) const;

  /// bind core's listener, unless an earlier core got no port
  seastar::future<> bind_core_listener(seastar::shard_id core);
  /// destroy the listeners of the cores that got no port
  seastar::future<> drop_unbound_core_listeners();
  /// accept connections on the bound core listeners
  seastar::future<> accept_on_core_listeners();

  const seastar::shard_id sid;
  // Distinguish messengers with meaningful names for debugging
  const std::string logic_name;
  const uint32_t nonce;
  const bool dispatch_only_on_sid;
  // Bind only on this messenger's shard. Per-reactor messengers must not
  // listen on every shard: those binds take the same port and the scan
  // runs out of ms_bind_port_min..max.
  const bool listen_on_primary_only;

  entity_name_t my_name;
  entity_addrvec_t my_addrs;
  crimson::auth::AuthClient* auth_client = nullptr;
  crimson::auth::AuthServer* auth_server = nullptr;
  seastar::shard_id auth_shard = seastar::this_shard_id();

  ShardedServerSocket *listener = nullptr;
  /// the per-core listeners (see bind_core_listeners()), indexed by core;
  /// core_listeners[c] places all the sockets it accepts on core c. Null
  /// for a core without a listener
  std::vector<ShardedServerSocket*> core_listeners;
  /// the port each of core_listeners is bound to, indexed by core (0:
  /// no listener)
  std::vector<uint32_t> core_ports;
  /// a core listener got no port: the cores after it get none either
  bool core_ports_exhausted = false;
  ChainedDispatchers dispatchers;
  /// registered connections, by peer address and accepting per-core
  /// listener (nullopt for the main listener, and for outgoing ones)
  using conn_key_t =
    std::pair<entity_addr_t, std::optional<seastar::shard_id>>;
  std::map<conn_key_t, SocketConnectionRef> connections;
  std::set<SocketConnectionRef> accepting_conns;
  std::vector<SocketConnectionRef> closing_conns;
  ceph::net::PolicySet<Throttle> policy_set;
  // specifying we haven't learned our addr; set false when we find it.
  bool need_addr = true;
  uint32_t global_seq = 0;
  bool started = false;
  seastar::promise<> shutdown_promise;
};

} // namespace crimson::net

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::net::SocketMessenger> : fmt::ostream_formatter {};
#endif
