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

#include "Fwd.h"

class AuthAuthorizer;

namespace crimson::net {

class Dispatcher {
 public:
  virtual ~Dispatcher() {}

  // Dispatchers are put into a chain as described by chain-of-responsibility
  // pattern. If any of the dispatchers claims this message, it returns a valid
  // future to prevent other dispatchers from processing it, and this is also
  // used to throttle the connection if it's too busy.
  virtual std::optional<seastar::future<>> ms_dispatch(ConnectionRef, MessageRef) = 0;

  // The connection is moving to the new_shard under accept/connect.
  // User should not operate conn in this shard thereafter.
  virtual void ms_handle_shard_change(
      ConnectionRef conn,
      seastar::shard_id new_shard,
      bool is_accept_or_connect) {}

  // The connection is accepted or recoverred(lossless), all the followup
  // events and messages will be dispatched to this shard.
  //
  // is_replace=true means the accepted connection has replaced
  // another connecting connection with the same peer_addr, which currently only
  // happens under lossy policy when both sides wish to connect to each other.
  virtual void ms_handle_accept(ConnectionRef conn, seastar::shard_id prv_shard, bool is_replace) {}

  // The connection is (re)connected, all the followup events and messages will
  // be dispatched to this shard.
  virtual void ms_handle_connect(ConnectionRef conn, seastar::shard_id prv_shard) {}

  // a reset event is dispatched when the connection is closed unexpectedly.
  //
  // is_replace=true means the reset connection is going to be replaced by
  // another accepting connection with the same peer_addr, which currently only
  // happens under lossy policy when both sides wish to connect to each other.
  virtual void ms_handle_reset(ConnectionRef conn, bool is_replace) {}

  virtual void ms_handle_remote_reset(ConnectionRef conn) {}
};

} // namespace crimson::net
