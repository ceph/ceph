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
#include <seastar/core/shared_future.hh>

#include "crimson/net/chained_dispatchers.h"
#include "Messenger.h"
#include "SocketConnection.h"

namespace crimson::net {

class FixedCPUServerSocket;

class SocketMessenger final : public Messenger {
  const seastar::shard_id master_sid;
  seastar::promise<> shutdown_promise;

  FixedCPUServerSocket* listener = nullptr;
  // as we want to unregister a dispatcher from the messengers when stopping
  // that dispatcher, we have to use intrusive slist which, when used with
  // "boost::intrusive::linear<true>", can tolerate ongoing iteration of the
  // list when removing an element. However, the downside of this is that an
  // element can only be attached to one slist. So, as we need to make multiple
  // messenger reference the same set of dispatchers, we have to make them share
  // the same ChainedDispatchers, which means registering/unregistering an element
  // to one messenger will affect other messengers that share the same ChainedDispatchers.
  ChainedDispatchersRef dispatchers;
  std::map<entity_addr_t, SocketConnectionRef> connections;
  std::set<SocketConnectionRef> accepting_conns;
  ceph::net::PolicySet<Throttle> policy_set;
  // Distinguish messengers with meaningful names for debugging
  const std::string logic_name;
  const uint32_t nonce;
  // specifying we haven't learned our addr; set false when we find it.
  bool need_addr = true;
  uint32_t global_seq = 0;
  bool started = false;

  seastar::future<> do_bind(const entity_addrvec_t& addr);

 public:
  SocketMessenger(const entity_name_t& myname,
                  const std::string& logic_name,
                  uint32_t nonce);
  ~SocketMessenger() override { ceph_assert(!listener); }

  seastar::future<> set_myaddrs(const entity_addrvec_t& addr) override;

  // Messenger interfaces are assumed to be called from its own shard, but its
  // behavior should be symmetric when called from any shard.
  seastar::future<> bind(const entity_addrvec_t& addr) override;

  seastar::future<> try_bind(const entity_addrvec_t& addr,
                             uint32_t min_port, uint32_t max_port) override;

  seastar::future<> start(ChainedDispatchersRef dispatchers) override;
  void add_dispatcher(Dispatcher& disp) {
    dispatchers->push_back(disp);
  }

  ConnectionRef connect(const entity_addr_t& peer_addr,
                        const entity_name_t& peer_name) override;
  // can only wait once
  seastar::future<> wait() override {
    assert(seastar::this_shard_id() == master_sid);
    return shutdown_promise.get_future();
  }

  void remove_dispatcher(Dispatcher& disp) override {
    dispatchers->erase(disp);
  }
  bool dispatcher_chain_empty() const override {
    return !dispatchers || dispatchers->empty();
  }
  seastar::future<> shutdown() override;

  void print(ostream& out) const override {
    out << get_myname()
        << "(" << logic_name
        << ") " << get_myaddr();
  }

  SocketPolicy get_policy(entity_type_t peer_type) const override;

  SocketPolicy get_default_policy() const override;

  void set_default_policy(const SocketPolicy& p) override;

  void set_policy(entity_type_t peer_type, const SocketPolicy& p) override;

  void set_policy_throttler(entity_type_t peer_type, Throttle* throttle) override;

 public:
  seastar::future<uint32_t> get_global_seq(uint32_t old=0);
  seastar::future<> learned_addr(const entity_addr_t &peer_addr_for_me,
                                 const SocketConnection& conn);

  SocketConnectionRef lookup_conn(const entity_addr_t& addr);
  void accept_conn(SocketConnectionRef);
  void unaccept_conn(SocketConnectionRef);
  void register_conn(SocketConnectionRef);
  void unregister_conn(SocketConnectionRef);
  seastar::shard_id shard_id() const {
    assert(seastar::this_shard_id() == master_sid);
    return master_sid;
  }
};

} // namespace crimson::net
