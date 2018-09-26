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

#include <seastar/core/shared_future.hh>

#include "SocketConnection.h"

using namespace ceph::net;

SocketConnection::SocketConnection(Messenger *messenger,
                                   Dispatcher *disp,
                                   const entity_addr_t& my_addr,
                                   const entity_addr_t& peer_addr)
  : Connection(messenger),
    s(my_addr, peer_addr),
    workspace(&s, this, disp, messenger)
{
}

SocketConnection::SocketConnection(Messenger *messenger,
                                   Dispatcher *disp,
                                   const entity_addr_t& my_addr,
                                   const entity_addr_t& peer_addr,
                                   seastar::connected_socket&& fd)
  : Connection(messenger),
    s(my_addr, peer_addr, std::forward<seastar::connected_socket>(fd)),
    workspace(&s, this, disp, messenger)
{
}

SocketConnection::~SocketConnection()
{
  ceph_assert(s.protocol->state() == state_t::close);
}

bool SocketConnection::is_connected()
{
  return s.protocol->state() == state_t::open;
}

seastar::future<> SocketConnection::send(MessageRef msg)
{
  // chain the message after the last message is sent
  seastar::shared_future<> f = send_ready.then(
    [this, msg = std::move(msg)] {
      return seastar::repeat([this, msg=std::move(msg)] {
          return s.protocol->send(msg);
        });
    });

  // chain any later messages after this one completes
  send_ready = f.get_future();
  // allow the caller to wait on the same future
  return f.get_future();
}

seastar::future<> SocketConnection::keepalive()
{
  seastar::shared_future<> f = send_ready.then(
    [this] {
      return seastar::repeat([this] {
          return s.protocol->keepalive();
        });
    });
  send_ready = f.get_future();
  return f.get_future();
}

seastar::future<> SocketConnection::close()
{
  return s.protocol->close();
}

void SocketConnection::requeue_sent()
{
  s.out_seq -= s.sent.size();
  while (!s.sent.empty()) {
    auto m = s.sent.front();
    s.sent.pop();
    s.out_q.push(std::move(m));
  }
}
