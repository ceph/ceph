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
#include <seastar/core/sharded.hh>

#include "Fwd.h"

class AuthAuthorizer;

namespace crimson::net {

class Dispatcher {
 public:
  virtual ~Dispatcher() {}

  virtual seastar::future<> ms_dispatch(Connection* conn, MessageRef m) {
    return seastar::make_ready_future<>();
  }

  virtual seastar::future<> ms_handle_accept(ConnectionRef conn) {
    return seastar::make_ready_future<>();
  }

  virtual seastar::future<> ms_handle_connect(ConnectionRef conn) {
    return seastar::make_ready_future<>();
  }

  // a reset event is dispatched when the connection is closed unexpectedly.
  // is_replace=true means the reset connection is going to be replaced by
  // another accepting connection with the same peer_addr, which currently only
  // happens under lossy policy when both sides wish to connect to each other.
  virtual seastar::future<> ms_handle_reset(ConnectionRef conn, bool is_replace) {
    return seastar::make_ready_future<>();
  }

  virtual seastar::future<> ms_handle_remote_reset(ConnectionRef conn) {
    return seastar::make_ready_future<>();
  }
};

} // namespace crimson::net
