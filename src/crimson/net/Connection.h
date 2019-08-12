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

namespace ceph::net {

using seq_num_t = uint64_t;

class Connection : public seastar::enable_shared_from_this<Connection> {
  entity_name_t peer_name = {0, -1};

 protected:
  entity_addr_t peer_addr;
  using clock_t = seastar::lowres_system_clock;
  clock_t::time_point last_keepalive;
  clock_t::time_point last_keepalive_ack;

  void set_peer_type(entity_type_t peer_type) { peer_name._type = peer_type; }
  void set_peer_id(int64_t peer_id) { peer_name._num = peer_id; }
  void set_peer_name(entity_name_t name) { peer_name = name; }

 public:
  uint64_t peer_global_id = 0;

 public:
  Connection() {}
  virtual ~Connection() {}

  virtual Messenger* get_messenger() const = 0;
  const entity_addr_t& get_peer_addr() const { return peer_addr; }
  const entity_name_t& get_peer_name() const { return peer_name; }
  entity_type_t get_peer_type() const { return peer_name.type(); }
  int64_t get_peer_id() const { return peer_name.num(); }

  bool peer_is_mon() const { return peer_name.is_mon(); }
  bool peer_is_mgr() const { return peer_name.is_mgr(); }
  bool peer_is_mds() const { return peer_name.is_mds(); }
  bool peer_is_osd() const { return peer_name.is_osd(); }
  bool peer_is_client() const { return peer_name.is_client(); }

  /// true if the handshake has completed and no errors have been encountered
  virtual seastar::future<bool> is_connected() = 0;

  /// send a message over a connection that has completed its handshake
  virtual seastar::future<> send(MessageRef msg) = 0;

  /// send a keepalive message over a connection that has completed its
  /// handshake
  virtual seastar::future<> keepalive() = 0;

  /// close the connection and cancel any any pending futures from read/send
  virtual seastar::future<> close() = 0;

  /// which shard id the connection lives
  virtual seastar::shard_id shard_id() const = 0;

  virtual void print(ostream& out) const = 0;

  void set_last_keepalive(clock_t::time_point when) {
    last_keepalive = when;
  }
  void set_last_keepalive_ack(clock_t::time_point when) {
    last_keepalive_ack = when;
  }
  auto get_last_keepalive() const { return last_keepalive; }
  auto get_last_keepalive_ack() const { return last_keepalive_ack; }

  seastar::shared_ptr<Connection> get_shared() {
    return shared_from_this();
  }

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

} // namespace ceph::net
