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

#include "Fwd.h"
#include "crimson/common/throttle.h"
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
public:
  Messenger() {}

  virtual ~Messenger() {}

  virtual const entity_name_t& get_myname() const = 0;

  entity_type_t get_mytype() const { return get_myname().type(); }

  virtual const entity_addrvec_t &get_myaddrs() const = 0;

  entity_addr_t get_myaddr() const { return get_myaddrs().front(); }

  virtual void set_myaddrs(const entity_addrvec_t& addrs) = 0;

  virtual bool set_addr_unknowns(const entity_addrvec_t &addrs) = 0;

  virtual void set_auth_client(crimson::auth::AuthClient *) = 0;

  virtual void set_auth_server(crimson::auth::AuthServer *) = 0;

  using bind_ertr = crimson::errorator<
    crimson::ct_error::address_in_use, // The address (range) is already bound
    crimson::ct_error::address_not_available
    >;
  /// bind to the given address
  virtual bind_ertr::future<> bind(const entity_addrvec_t& addr) = 0;

  /// start the messenger
  virtual seastar::future<> start(const dispatchers_t&) = 0;

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

  virtual bool owns_connection(Connection &) const = 0;

  // wait for messenger shutdown
  virtual seastar::future<> wait() = 0;

  // stop dispatching events and messages
  virtual void stop() = 0;

  virtual bool is_started() const = 0;

  // free internal resources before destruction, must be called after stopped,
  // and must be called if is bound.
  virtual seastar::future<> shutdown() = 0;

  virtual void print(std::ostream& out) const = 0;

  virtual SocketPolicy get_policy(entity_type_t peer_type) const = 0;

  virtual SocketPolicy get_default_policy() const = 0;

  virtual void set_default_policy(const SocketPolicy& p) = 0;

  virtual void set_policy(entity_type_t peer_type, const SocketPolicy& p) = 0;

  virtual void set_policy_throttler(entity_type_t peer_type, Throttle* throttle) = 0;

  static MessengerRef
  create(const entity_name_t& name,
         const std::string& lname,
         uint64_t nonce,
         bool dispatch_only_on_this_shard);

#ifdef UNIT_TESTS_BUILT
  virtual void set_interceptor(Interceptor *) = 0;
#endif
};

inline std::ostream& operator<<(std::ostream& out, const Messenger& msgr) {
  out << "[";
  msgr.print(out);
  out << "]";
  return out;
}

} // namespace crimson::net

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::net::Messenger> : fmt::ostream_formatter {};
#endif
