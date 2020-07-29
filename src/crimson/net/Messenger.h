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

#include <seastar/core/future.hh>

#include "Fwd.h"
#include "crimson/common/throttle.h"
#include "crimson/net/chained_dispatchers.h"
#include "msg/Message.h"
#include "msg/Policy.h"

class AuthAuthorizer;

namespace crimson::auth {
class AuthClient;
class AuthServer;
}

namespace crimson::net {

#ifdef UNIT_TESTS_BUILT
class Interceptor;
#endif

using Throttle = crimson::common::Throttle;
using SocketPolicy = ceph::net::Policy<Throttle>;

class Messenger {
  entity_name_t my_name;
  entity_addrvec_t my_addrs;
  uint32_t crc_flags = 0;
  crimson::auth::AuthClient* auth_client = nullptr;
  crimson::auth::AuthServer* auth_server = nullptr;
  bool require_authorizer = true;

public:
  Messenger(const entity_name_t& name)
    : my_name(name)
  {}
  virtual ~Messenger() {}

#ifdef UNIT_TESTS_BUILT
  Interceptor *interceptor = nullptr;
#endif

  entity_type_t get_mytype() const { return my_name.type(); }
  const entity_name_t& get_myname() const { return my_name; }
  const entity_addrvec_t& get_myaddrs() const { return my_addrs; }
  entity_addr_t get_myaddr() const { return my_addrs.front(); }
  virtual seastar::future<> set_myaddrs(const entity_addrvec_t& addrs) {
    my_addrs = addrs;
    return seastar::now();
  }

  /// bind to the given address
  virtual seastar::future<> bind(const entity_addrvec_t& addr) = 0;

  /// try to bind to the first unused port of given address
  virtual seastar::future<> try_bind(const entity_addrvec_t& addr,
                                     uint32_t min_port, uint32_t max_port) = 0;

  /// start the messenger
  virtual seastar::future<> start(ChainedDispatchersRef) = 0;

  /// either return an existing connection to the peer,
  /// or a new pending connection
  virtual ConnectionRef
  connect(const entity_addr_t& peer_addr,
          const entity_name_t& peer_name) = 0;

  ConnectionRef
  connect(const entity_addr_t& peer_addr,
          const entity_type_t& peer_type) {
    return connect(peer_addr, entity_name_t(peer_type, -1));
  }

  // wait for messenger shutdown
  virtual seastar::future<> wait() = 0;

  virtual void add_dispatcher(Dispatcher&) = 0;

  virtual void remove_dispatcher(Dispatcher&) = 0;

  virtual bool dispatcher_chain_empty() const = 0;
  /// stop listenening and wait for all connections to close. safe to destruct
  /// after this future becomes available
  virtual seastar::future<> shutdown() = 0;

  uint32_t get_crc_flags() const {
    return crc_flags;
  }
  void set_crc_data() {
    crc_flags |= MSG_CRC_DATA;
  }
  void set_crc_header() {
    crc_flags |= MSG_CRC_HEADER;
  }

  crimson::auth::AuthClient* get_auth_client() const { return auth_client; }
  void set_auth_client(crimson::auth::AuthClient *ac) {
    auth_client = ac;
  }
  crimson::auth::AuthServer* get_auth_server() const { return auth_server; }
  void set_auth_server(crimson::auth::AuthServer *as) {
    auth_server = as;
  }

  virtual void print(ostream& out) const = 0;

  virtual SocketPolicy get_policy(entity_type_t peer_type) const = 0;

  virtual SocketPolicy get_default_policy() const = 0;

  virtual void set_default_policy(const SocketPolicy& p) = 0;

  virtual void set_policy(entity_type_t peer_type, const SocketPolicy& p) = 0;

  virtual void set_policy_throttler(entity_type_t peer_type, Throttle* throttle) = 0;

  // allow unauthenticated connections.  This is needed for compatibility with
  // pre-nautilus OSDs, which do not authenticate the heartbeat sessions.
  bool get_require_authorizer() const {
    return require_authorizer;
  }
  void set_require_authorizer(bool r) {
    require_authorizer = r;
  }
  static MessengerRef
  create(const entity_name_t& name,
         const std::string& lname,
         const uint64_t nonce);
};

inline ostream& operator<<(ostream& out, const Messenger& msgr) {
  out << "[";
  msgr.print(out);
  out << "]";
  return out;
}

} // namespace crimson::net
