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

using seq_num_t = uint64_t;

/**
 * Connection
 *
 * Abstraction for messenger connections.
 *
 * Except when otherwise specified, methods must be invoked from the core on which
 * the connection originates.
 */
class Connection : public seastar::enable_shared_from_this<Connection> {
 public:
  using clock_t = seastar::lowres_system_clock;

  Connection() {}

  virtual ~Connection() {}

  /**
   * get_shard_id
   *
   * The shard id where the Connection is dispatching events and handling I/O.
   *
   * May be changed with the accept/connect events.
   */
  virtual const seastar::shard_id get_shard_id() const = 0;

  virtual const entity_name_t &get_peer_name() const = 0;

  entity_type_t get_peer_type() const { return get_peer_name().type(); }
  int64_t get_peer_id() const { return get_peer_name().num(); }
  bool peer_is_mon() const { return get_peer_name().is_mon(); }
  bool peer_is_mgr() const { return get_peer_name().is_mgr(); }
  bool peer_is_mds() const { return get_peer_name().is_mds(); }
  bool peer_is_osd() const { return get_peer_name().is_osd(); }
  bool peer_is_client() const { return get_peer_name().is_client(); }

  virtual const entity_addr_t &get_peer_addr() const = 0;

  const entity_addrvec_t get_peer_addrs() const {
    return entity_addrvec_t(get_peer_addr());
  }

  virtual const entity_addr_t &get_peer_socket_addr() const = 0;

  virtual uint64_t get_features() const = 0;

  bool has_feature(uint64_t f) const {
    return get_features() & f;
  }

  /// true if the handshake has completed and no errors have been encountered
  virtual bool is_connected() const = 0;

  /**
   * send
   *
   * Send a message over a connection that has completed its handshake.
   *
   * May be invoked from any core, and the send order will be preserved upon
   * the call.
   *
   * The returned future will be resolved only after the message is enqueued
   * remotely.
   */
  virtual seastar::future<> send(
      MessageURef msg) = 0;

  /**
   * send_with_throttling
   *
   * Send a message over a connection that has completed its handshake.
   *
   * May be invoked from any core, and the send order will be preserved upon
   * the call.
   *
   * TODO:
   *
   * The returned future is reserved for throttling.
   *
   * Gating is needed for graceful shutdown, to wait until the message is
   * enqueued remotely.
   */
  seastar::future<> send_with_throttling(
      MessageURef msg /* , seastar::gate & */) {
    std::ignore = send(std::move(msg));
    return seastar::now();
  }

  /**
   * send_keepalive
   *
   * Send a keepalive message over a connection that has completed its
   * handshake.
   *
   * May be invoked from any core, and the send order will be preserved upon
   * the call.
   */
  virtual seastar::future<> send_keepalive() = 0;

  virtual clock_t::time_point get_last_keepalive() const = 0;

  virtual clock_t::time_point get_last_keepalive_ack() const = 0;

  // workaround for the monitor client
  virtual void set_last_keepalive_ack(clock_t::time_point when) = 0;

  // close the connection and cancel any any pending futures from read/send,
  // without dispatching any reset event
  virtual void mark_down() = 0;

  struct user_private_t {
    virtual ~user_private_t() = default;
  };

  virtual bool has_user_private() const = 0;

  virtual user_private_t &get_user_private() = 0;

  virtual void set_user_private(std::unique_ptr<user_private_t>) = 0;

  virtual void print(std::ostream& out) const = 0;

#ifdef UNIT_TESTS_BUILT
  virtual bool is_protocol_ready() const = 0;

  virtual bool is_protocol_standby() const = 0;

  virtual bool is_protocol_closed() const = 0;

  virtual bool is_protocol_closed_clean() const = 0;

  virtual bool peer_wins() const = 0;
#endif
};

inline std::ostream& operator<<(std::ostream& out, const Connection& conn) {
  out << "[";
  conn.print(out);
  out << "]";
  return out;
}

} // namespace crimson::net

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::net::Connection> : fmt::ostream_formatter {};
#endif
