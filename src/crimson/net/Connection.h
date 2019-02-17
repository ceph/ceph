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
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "Fwd.h"
#include "crimson/osd/session.h"

namespace ceph::net {

using seq_num_t = uint64_t;
using SharedPtr = seastar::shared_ptr<seastar::shared_ptr_count_base>;
class Connection : public boost::intrusive_ref_counter<Connection,
						       boost::thread_unsafe_counter> {
  SessionRef priv;
protected:
  entity_addr_t peer_addr;
  peer_type_t peer_type = -1;

 public:
  Connection() {}
  virtual ~Connection() {}

  void set_priv(const SessionRef& o) {
    priv = o;
  }
  SessionRef get_priv() {
    return priv;
  }

  virtual Messenger* get_messenger() const = 0;
  const entity_addr_t& get_peer_addr() const { return peer_addr; }
  virtual int get_peer_type() const = 0;

  /// true if the handshake has completed and no errors have been encountered
  virtual bool is_connected() = 0;

  /// send a message over a connection that has completed its handshake
  virtual seastar::future<> send(MessageRef msg) = 0;

  /// send a keepalive message over a connection that has completed its
  /// handshake
  virtual seastar::future<> keepalive() = 0;

  /// close the connection and cancel any any pending futures from read/send
  virtual seastar::future<> close() = 0;

  virtual void print(ostream& out) const = 0;
};

inline ostream& operator<<(ostream& out, const Connection& conn) {
  out << "[";
  conn.print(out);
  out << "]";
  return out;
}

} // namespace ceph::net
