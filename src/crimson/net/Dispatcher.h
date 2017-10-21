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

namespace ceph {
namespace net {

class Dispatcher {
 public:
  virtual ~Dispatcher() {}

  virtual seastar::future<> ms_dispatch(ConnectionRef conn, MessageRef m) {
    return seastar::make_ready_future<>();
  }

  virtual seastar::future<> ms_handle_accept(ConnectionRef conn) {
    return seastar::make_ready_future<>();
  }

  virtual seastar::future<> ms_handle_connect(ConnectionRef conn) {
    return seastar::make_ready_future<>();
  }

  virtual seastar::future<> ms_handle_reset(ConnectionRef conn) {
    return seastar::make_ready_future<>();
  }

  virtual seastar::future<> ms_handle_remote_reset(ConnectionRef conn) {
    return seastar::make_ready_future<>();
  }

  // TODO: authorizer
};

} // namespace net
} // namespace ceph
