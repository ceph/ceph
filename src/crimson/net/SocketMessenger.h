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
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>

#include "msg/Policy.h"
#include "Messenger.h"
#include "crimson/thread/Throttle.h"

namespace ceph::net {

using SocketPolicy = ceph::net::Policy<ceph::thread::Throttle>;

class SocketMessenger final : public Messenger {
  std::optional<seastar::server_socket> listener;
  Dispatcher *dispatcher = nullptr;
  std::map<entity_addr_t, ConnectionRef> connections;
  using Throttle = ceph::thread::Throttle;
  ceph::net::PolicySet<Throttle> policy_set;
  seastar::gate pending_dispatch;

  seastar::future<> dispatch(ConnectionRef conn);

  seastar::future<> accept(seastar::connected_socket socket,
                           seastar::socket_address paddr);

 public:
  SocketMessenger(const entity_name_t& myname);

  void bind(const entity_addr_t& addr) override;

  seastar::future<> start(Dispatcher *dispatcher) override;

  seastar::future<ConnectionRef> connect(const entity_addr_t& addr,
					 entity_type_t peer_type) override;

  seastar::future<> shutdown() override;
  void set_default_policy(const SocketPolicy& p);
  void set_policy(entity_type_t peer_type, const SocketPolicy& p);
  void set_policy_throttler(entity_type_t peer_type, Throttle* throttle);
  ConnectionRef lookup_conn(const entity_addr_t& addr) override;
  void unregister_conn(ConnectionRef) override;
  seastar::future<msgr_tag_t, bufferlist>
  verify_authorizer(peer_type_t peer_type,
		    auth_proto_t protocol,
		    bufferlist& auth) override;
  seastar::future<std::unique_ptr<AuthAuthorizer>>
  get_authorizer(peer_type_t peer_type,
		 bool force_new) override;
};

} // namespace ceph::net
