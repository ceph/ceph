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
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "Fwd.h"

namespace ceph::net {

class Messenger;

class Connection : public boost::intrusive_ref_counter<Connection,
						       boost::thread_unsafe_counter> {
  Messenger *const messenger;

 public:
  Connection(Messenger *messenger)
    : messenger(messenger) {}
  virtual ~Connection() {}

  Messenger* get_messenger() const { return messenger; }
  virtual const entity_addr_t& get_my_addr() const = 0;
  virtual const entity_addr_t& get_peer_addr() const = 0;

  /// true if the handshake has completed and no errors have been encountered
  virtual bool is_connected() = 0;

  /// send a message over a connection that has completed its handshake
  virtual seastar::future<> send(MessageRef msg) = 0;

  /// send a keepalive message over a connection that has completed its
  /// handshake
  virtual seastar::future<> keepalive() = 0;

  /// close the connection and cancel any any pending futures from read/send
  virtual seastar::future<> close() = 0;
};

using ConnectionRef = boost::intrusive_ptr<Connection>;

} // namespace ceph::net
