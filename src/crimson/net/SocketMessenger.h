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
                  bool dispatch_only_on_this_shard);

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

  bind_ertr::future<> bind(const entity_addrvec_t& addr) override;

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

  uint32_t get_global_seq(uint32_t old=0);

  void learned_addr(const entity_addr_t &peer_addr_for_me,
                    const SocketConnection& conn);

  SocketConnectionRef lookup_conn(const entity_addr_t& addr);

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

private:
  seastar::future<> accept(SocketFRef &&, const entity_addr_t &);

  listen_ertr::future<> do_listen(const entity_addrvec_t& addr);

  /// try to bind to the first unused port of given address
  bind_ertr::future<> try_bind(const entity_addrvec_t& addr,
                               uint32_t min_port, uint32_t max_port);

  const seastar::shard_id sid;
  // Distinguish messengers with meaningful names for debugging
  const std::string logic_name;
  const uint32_t nonce;
  const bool dispatch_only_on_sid;

  entity_name_t my_name;
  entity_addrvec_t my_addrs;
  crimson::auth::AuthClient* auth_client = nullptr;
  crimson::auth::AuthServer* auth_server = nullptr;

  ShardedServerSocket *listener = nullptr;
  ChainedDispatchers dispatchers;
  std::map<entity_addr_t, SocketConnectionRef> connections;
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
