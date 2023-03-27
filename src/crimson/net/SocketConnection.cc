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

#include "SocketConnection.h"

#include "ProtocolV2.h"
#include "SocketMessenger.h"

#ifdef UNIT_TESTS_BUILT
#include "Interceptor.h"
#endif

using std::ostream;
using crimson::common::local_conf;

namespace crimson::net {

SocketConnection::SocketConnection(SocketMessenger& messenger,
                                   ChainedDispatchers& dispatchers)
  : core(messenger.get_shard_id()),
    messenger(messenger)
{
  auto ret = create_handlers(dispatchers, *this);
  io_handler = std::move(ret.io_handler);
  protocol = std::move(ret.protocol);
#ifdef UNIT_TESTS_BUILT
  if (messenger.interceptor) {
    interceptor = messenger.interceptor;
    interceptor->register_conn(*this);
  }
#endif
}

SocketConnection::~SocketConnection() {}

bool SocketConnection::is_connected() const
{
  assert(seastar::this_shard_id() == shard_id());
  return io_handler->is_connected();
}

#ifdef UNIT_TESTS_BUILT
bool SocketConnection::is_closed() const
{
  assert(seastar::this_shard_id() == shard_id());
  return protocol->is_closed();
}

bool SocketConnection::is_closed_clean() const
{
  assert(seastar::this_shard_id() == shard_id());
  return protocol->is_closed_clean();
}

#endif
bool SocketConnection::peer_wins() const
{
  return (messenger.get_myaddr() > peer_addr || policy.server);
}

seastar::future<> SocketConnection::send(MessageURef msg)
{
  return seastar::smp::submit_to(
    shard_id(),
    [this, msg=std::move(msg)]() mutable {
      return io_handler->send(std::move(msg));
    });
}

seastar::future<> SocketConnection::send_keepalive()
{
  return seastar::smp::submit_to(
    shard_id(),
    [this] {
      return io_handler->send_keepalive();
    });
}

SocketConnection::clock_t::time_point
SocketConnection::get_last_keepalive() const
{
  return io_handler->get_last_keepalive();
}

SocketConnection::clock_t::time_point
SocketConnection::get_last_keepalive_ack() const
{
  return io_handler->get_last_keepalive_ack();
}

void SocketConnection::set_last_keepalive_ack(clock_t::time_point when)
{
  io_handler->set_last_keepalive_ack(when);
}

void SocketConnection::mark_down()
{
  assert(seastar::this_shard_id() == shard_id());
  io_handler->mark_down();
}

void
SocketConnection::start_connect(const entity_addr_t& _peer_addr,
                                const entity_name_t& _peer_name)
{
  protocol->start_connect(_peer_addr, _peer_name);
}

void
SocketConnection::start_accept(SocketRef&& sock,
                               const entity_addr_t& _peer_addr)
{
  protocol->start_accept(std::move(sock), _peer_addr);
}

seastar::future<>
SocketConnection::close_clean_yielded()
{
  return protocol->close_clean_yielded();
}

seastar::shard_id SocketConnection::shard_id() const {
  return core;
}

seastar::socket_address SocketConnection::get_local_address() const {
  return socket->get_local_address();
}

ConnectionRef
SocketConnection::get_local_shared_foreign_from_this()
{
  assert(seastar::this_shard_id() == shard_id());
  return make_local_shared_foreign(
      seastar::make_foreign(shared_from_this()));
}

void SocketConnection::set_socket(Socket *s) {
  socket = s;
}

void SocketConnection::print(ostream& out) const {
    out << (void*)this << " ";
    messenger.print(out);
    if (!socket) {
      out << " >> " << get_peer_name() << " " << peer_addr;
    } else if (socket->get_side() == Socket::side_t::acceptor) {
      out << " >> " << get_peer_name() << " " << peer_addr
          << "@" << socket->get_ephemeral_port();
    } else { // socket->get_side() == Socket::side_t::connector
      out << "@" << socket->get_ephemeral_port()
          << " >> " << get_peer_name() << " " << peer_addr;
    }
}

} // namespace crimson::net
