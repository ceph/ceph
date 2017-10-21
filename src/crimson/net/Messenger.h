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

#include <core/future.hh>

#include "Fwd.h"

class AuthAuthorizer;

namespace ceph::net {

class Messenger {
  entity_name_t my_name;
  entity_addr_t my_addr;

 public:
  Messenger(const entity_name_t& name)
    : my_name(name)
  {}
  virtual ~Messenger() {}

  const entity_name_t& get_myname() const { return my_name; }
  const entity_addr_t& get_myaddr() const { return my_addr; }
  void set_myaddr(const entity_addr_t& addr) {
    my_addr = addr;
  }

  /// bind to the given address
  virtual void bind(const entity_addr_t& addr) = 0;

  /// start the messenger
  virtual seastar::future<> start(Dispatcher *dispatcher) = 0;

  /// establish a client connection and complete a handshake
  virtual seastar::future<ConnectionRef> connect(const entity_addr_t& addr) = 0;

  /// stop listenening and wait for all connections to close. safe to destruct
  /// after this future becomes available
  virtual seastar::future<> shutdown() = 0;
};

} // namespace ceph::net
