// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corp.
 *
 * Author: Yingxin Cheng <yingxincheng@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "Protocol.h"
#include "SocketConnection.h"

using namespace ceph::net;

seastar::future<>
Protocol::close() {
  ceph_assert(state);
  if (closed) {
    return seastar::now();
  }
  // unregister_conn() drops a reference, so hold another until completion
  auto cleanup = [this, conn_ref = ConnectionRef(&conn)] { closed = true; };
  state->trigger_close();
  return state->wait_execute().finally(std::move(cleanup));
}

void
Protocol::start_connect(const entity_addr_t& peer_addr, const entity_type_t& peer_type) {
  ceph_assert(!socket);
  s.peer_addr = peer_addr;
  s.peer_type = peer_type;
  do_start_connect();
}

void
Protocol::start_accept(seastar::connected_socket&& _socket, const entity_addr_t& peer_addr) {
  ceph_assert(!socket);
  s.peer_addr = peer_addr;
  socket.emplace(std::move(_socket));
  do_start_accept();
}
