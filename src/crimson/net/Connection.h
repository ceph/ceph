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

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <core/future.hh>

#include "Fwd.h"

namespace ceph::net {

class Connection : public boost::intrusive_ref_counter<Connection> {
 protected:
  Messenger *const messenger;
  entity_addr_t my_addr;
  entity_addr_t peer_addr;

 public:
  Connection(Messenger *messenger, const entity_addr_t& my_addr,
             const entity_addr_t& peer_addr)
    : messenger(messenger), my_addr(my_addr), peer_addr(peer_addr) {}
  virtual ~Connection() {}

  Messenger* get_messenger() const { return messenger; }

  const entity_addr_t& get_my_addr() const { return my_addr; }
  const entity_addr_t& get_peer_addr() const { return peer_addr; }

  /// true if the handshake has completed and no errors have been encountered
  virtual bool is_connected() = 0;

  /// complete a handshake from the client's perspective
  virtual seastar::future<> client_handshake() = 0;

  /// complete a handshake from the server's perspective
  virtual seastar::future<> server_handshake() = 0;

  /// read a message from a connection that has completed its handshake
  virtual seastar::future<MessageRef> read_message() = 0;

  /// send a message over a connection that has completed its handshake
  virtual seastar::future<> send(MessageRef msg) = 0;

  /// close the connection and cancel any any pending futures from read/send
  virtual seastar::future<> close() = 0;
};

} // namespace ceph::net
