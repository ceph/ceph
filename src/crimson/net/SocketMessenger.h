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
#include <optional>
#include <set>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>

#include "msg/Policy.h"
#include "Messenger.h"
#include "SocketConnection.h"
#include "crimson/thread/Throttle.h"

namespace ceph::net {

using SocketPolicy = ceph::net::Policy<ceph::thread::Throttle>;

class SocketMessenger final : public Messenger, public seastar::peering_sharded_service<SocketMessenger> {
  const seastar::shard_id sid;
  seastar::promise<> shutdown_promise;

  std::optional<seastar::server_socket> listener;
  Dispatcher *dispatcher = nullptr;
  std::map<entity_addr_t, SocketConnectionRef> connections;
  std::set<SocketConnectionRef> accepting_conns;
  using Throttle = ceph::thread::Throttle;
  ceph::net::PolicySet<Throttle> policy_set;
  // Distinguish messengers with meaningful names for debugging
  const std::string logic_name;
  const uint32_t nonce;

  seastar::future<> accept(seastar::connected_socket socket,
                           seastar::socket_address paddr);

  void do_bind(const entity_addrvec_t& addr);
  seastar::future<> do_start(Dispatcher *disp);
  seastar::foreign_ptr<ConnectionRef> do_connect(const entity_addr_t& peer_addr,
                                                 const entity_type_t& peer_type);
  seastar::future<> do_shutdown();
  // conn sharding options:
  // 1. Simplest: sharded by ip only
  // 2. Balanced: sharded by ip + port + nonce,
  //        but, need to move SocketConnection between cores.
  seastar::shard_id locate_shard(const entity_addr_t& addr);

 public:
  SocketMessenger(const entity_name_t& myname,
                  const std::string& logic_name,
                  uint32_t nonce);

  seastar::future<> set_myaddrs(const entity_addrvec_t& addr) override;

  // Messenger interfaces are assumed to be called from its own shard, but its
  // behavior should be symmetric when called from any shard.
  seastar::future<> bind(const entity_addrvec_t& addr) override;

  seastar::future<> try_bind(const entity_addrvec_t& addr,
                             uint32_t min_port, uint32_t max_port) override;

  seastar::future<> start(Dispatcher *dispatcher) override;

  seastar::future<ConnectionXRef> connect(const entity_addr_t& peer_addr,
                                          const entity_type_t& peer_type) override;
  // can only wait once
  seastar::future<> wait() override {
    return shutdown_promise.get_future();
  }

  seastar::future<> shutdown() override;

  Messenger* get_local_shard() override {
    return &container().local();
  }

  void print(ostream& out) const override {
    out << get_myname()
        << "(" << logic_name
        << ") " << get_myaddr();
  }

 public:
  seastar::future<> learned_addr(const entity_addr_t &peer_addr_for_me);
  void set_default_policy(const SocketPolicy& p);
  void set_policy(entity_type_t peer_type, const SocketPolicy& p);
  void set_policy_throttler(entity_type_t peer_type, Throttle* throttle);

  SocketConnectionRef lookup_conn(const entity_addr_t& addr);
  void accept_conn(SocketConnectionRef);
  void unaccept_conn(SocketConnectionRef);
  void register_conn(SocketConnectionRef);
  void unregister_conn(SocketConnectionRef);

  // required by sharded<>
  seastar::future<> stop() {
    return seastar::make_ready_future<>();
  }

  seastar::shard_id shard_id() const {
    return sid;
  }
};

} // namespace ceph::net
