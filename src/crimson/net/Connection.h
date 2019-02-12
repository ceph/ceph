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
 protected:
  entity_addr_t peer_addr;
  peer_type_t peer_type = -1;

 public:
  Connection() {}
  virtual ~Connection() {}

  virtual Messenger* get_messenger() const = 0;
  const entity_addr_t& get_peer_addr() const { return peer_addr; }
  virtual int get_peer_type() const = 0;

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
};

inline ostream& operator<<(ostream& out, const Connection& conn) {
  out << "[";
  conn.print(out);
  out << "]";
  return out;
}

} // namespace ceph::net
