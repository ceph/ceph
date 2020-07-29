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
#include <boost/intrusive/slist.hpp>

#include "crimson/common/gated.h"
#include "Fwd.h"

class AuthAuthorizer;

namespace crimson::net {

class Dispatcher : public boost::intrusive::slist_base_hook<
			    boost::intrusive::link_mode<
			      boost::intrusive::safe_link>> {
 public:
  virtual ~Dispatcher() {}

  virtual seastar::future<> ms_dispatch(Connection* conn, MessageRef m) {
    return seastar::make_ready_future<>();
  }
  virtual void ms_handle_accept(ConnectionRef conn) {}

  virtual void ms_handle_connect(ConnectionRef conn) {}

  // a reset event is dispatched when the connection is closed unexpectedly.
  // is_replace=true means the reset connection is going to be replaced by
  // another accepting connection with the same peer_addr, which currently only
  // happens under lossy policy when both sides wish to connect to each other.
  virtual void ms_handle_reset(ConnectionRef conn, bool is_replace) {}

  virtual void ms_handle_remote_reset(ConnectionRef conn) {}
};

} // namespace crimson::net
