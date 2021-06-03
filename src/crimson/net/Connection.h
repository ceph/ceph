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

#include <queue>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "Fwd.h"

namespace crimson::net {

#ifdef UNIT_TESTS_BUILT
class Interceptor;
#endif

using seq_num_t = uint64_t;

class Connection : public seastar::enable_shared_from_this<Connection> {
  entity_name_t peer_name = {0, entity_name_t::NEW};

 protected:
  entity_addr_t peer_addr;

  // which of the peer_addrs we're connecting to (as client)
  // or should reconnect to (as peer)
  entity_addr_t target_addr;

  using clock_t = seastar::lowres_system_clock;
  clock_t::time_point last_keepalive;
  clock_t::time_point last_keepalive_ack;

  void set_peer_type(entity_type_t peer_type) {
    // it is not allowed to assign an unknown value when the current
    // value is known
    assert(!(peer_type == 0 &&
             peer_name.type() != 0));
    // it is not allowed to assign a different known value when the
    // current value is also known.
    assert(!(peer_type != 0 &&
             peer_name.type() != 0 &&
             peer_type != peer_name.type()));
    peer_name._type = peer_type;
  }
  void set_peer_id(int64_t peer_id) {
    // it is not allowed to assign an unknown value when the current
    // value is known
    assert(!(peer_id == entity_name_t::NEW &&
             peer_name.num() != entity_name_t::NEW));
    // it is not allowed to assign a different known value when the
    // current value is also known.
    assert(!(peer_id != entity_name_t::NEW &&
             peer_name.num() != entity_name_t::NEW &&
             peer_id != peer_name.num()));
    peer_name._num = peer_id;
  }
  void set_peer_name(entity_name_t name) {
    set_peer_type(name.type());
    set_peer_id(name.num());
  }

 public:
  uint64_t peer_global_id = 0;

 protected:
  uint64_t features = 0;

 public:
  void set_features(uint64_t new_features) {
    features = new_features;
  }
  auto get_features() const {
    return features;
  }
  bool has_feature(uint64_t f) const {
    return features & f;
  }

 public:
  Connection() {}
  virtual ~Connection() {}

#ifdef UNIT_TESTS_BUILT
  Interceptor *interceptor = nullptr;
#endif

  virtual Messenger* get_messenger() const = 0;
  const entity_addr_t& get_peer_addr() const { return peer_addr; }
  const entity_addrvec_t get_peer_addrs() const {
    return entity_addrvec_t(peer_addr);
  }
  const auto& get_peer_socket_addr() const {
    return target_addr;
  }
  const entity_name_t& get_peer_name() const { return peer_name; }
  entity_type_t get_peer_type() const { return peer_name.type(); }
  int64_t get_peer_id() const { return peer_name.num(); }

  bool peer_is_mon() const { return peer_name.is_mon(); }
  bool peer_is_mgr() const { return peer_name.is_mgr(); }
  bool peer_is_mds() const { return peer_name.is_mds(); }
  bool peer_is_osd() const { return peer_name.is_osd(); }
  bool peer_is_client() const { return peer_name.is_client(); }

  /// true if the handshake has completed and no errors have been encountered
  virtual bool is_connected() const = 0;

#ifdef UNIT_TESTS_BUILT
  virtual bool is_closed() const = 0;

  virtual bool is_closed_clean() const = 0;

  virtual bool peer_wins() const = 0;
#endif

  /// send a message over a connection that has completed its handshake
  virtual seastar::future<> send(MessageURef msg) = 0;

  /// send a keepalive message over a connection that has completed its
  /// handshake
  virtual seastar::future<> keepalive() = 0;

  // close the connection and cancel any any pending futures from read/send,
  // without dispatching any reset event
  virtual void mark_down() = 0;

  virtual void print(ostream& out) const = 0;

  void set_last_keepalive(clock_t::time_point when) {
    last_keepalive = when;
  }
  void set_last_keepalive_ack(clock_t::time_point when) {
    last_keepalive_ack = when;
  }
  auto get_last_keepalive() const { return last_keepalive; }
  auto get_last_keepalive_ack() const { return last_keepalive_ack; }

  struct user_private_t {
    virtual ~user_private_t() = default;
  };
private:
  unique_ptr<user_private_t> user_private;
public:
  bool has_user_private() const {
    return user_private != nullptr;
  }
  void set_user_private(unique_ptr<user_private_t> new_user_private) {
    user_private = std::move(new_user_private);
  }
  user_private_t &get_user_private() {
    ceph_assert(user_private);
    return *user_private;
  }
};

inline ostream& operator<<(ostream& out, const Connection& conn) {
  out << "[";
  conn.print(out);
  out << "]";
  return out;
}

} // namespace crimson::net
