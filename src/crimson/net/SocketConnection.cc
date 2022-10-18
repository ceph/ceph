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
  : msgr_sid{messenger.get_shard_id()}, messenger(messenger)
{
  auto ret = create_handlers(dispatchers, *this);
  io_handler = std::move(ret.io_handler);
  protocol = std::move(ret.protocol);
#ifdef UNIT_TESTS_BUILT
  if (messenger.interceptor) {
    interceptor = messenger.interceptor;
    interceptor->register_conn(this->get_local_shared_foreign_from_this());
  }
#endif
}

SocketConnection::~SocketConnection() {}

bool SocketConnection::is_connected() const
{
  return io_handler->is_connected();
}

#ifdef UNIT_TESTS_BUILT
bool SocketConnection::is_protocol_ready() const
{
  assert(seastar::this_shard_id() == msgr_sid);
  return protocol->is_ready();
}

bool SocketConnection::is_protocol_standby() const {
  assert(seastar::this_shard_id() == msgr_sid);
  return protocol->is_standby();
}

bool SocketConnection::is_protocol_closed() const
{
  assert(seastar::this_shard_id() == msgr_sid);
  return protocol->is_closed();
}

bool SocketConnection::is_protocol_closed_clean() const
{
  assert(seastar::this_shard_id() == msgr_sid);
  return protocol->is_closed_clean();
}

#endif
bool SocketConnection::peer_wins() const
{
  assert(seastar::this_shard_id() == msgr_sid);
  return (messenger.get_myaddr() > peer_addr || policy.server);
}

seastar::future<> SocketConnection::send(MessageURef msg)
{
  return io_handler->send(std::move(msg));
}

seastar::future<> SocketConnection::send_keepalive()
{
  return io_handler->send_keepalive();
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
  io_handler->mark_down();
}

void
SocketConnection::start_connect(const entity_addr_t& _peer_addr,
                                const entity_name_t& _peer_name)
{
  assert(seastar::this_shard_id() == msgr_sid);
  protocol->start_connect(_peer_addr, _peer_name);
}

void
SocketConnection::start_accept(SocketFRef&& sock,
                               const entity_addr_t& _peer_addr)
{
  assert(seastar::this_shard_id() == msgr_sid);
  protocol->start_accept(std::move(sock), _peer_addr);
}

seastar::future<>
SocketConnection::close_clean_yielded()
{
  assert(seastar::this_shard_id() == msgr_sid);
  return protocol->close_clean_yielded();
}

seastar::socket_address SocketConnection::get_local_address() const {
  assert(seastar::this_shard_id() == msgr_sid);
  return socket->get_local_address();
}

ConnectionRef
SocketConnection::get_local_shared_foreign_from_this()
{
  assert(seastar::this_shard_id() == msgr_sid);
  return make_local_shared_foreign(
      seastar::make_foreign(shared_from_this()));
}

SocketMessenger &
SocketConnection::get_messenger() const
{
  assert(seastar::this_shard_id() == msgr_sid);
  return messenger;
}

seastar::shard_id
SocketConnection::get_messenger_shard_id() const
{
  return msgr_sid;
}

void SocketConnection::set_peer_type(entity_type_t peer_type) {
  assert(seastar::this_shard_id() == msgr_sid);
  // it is not allowed to assign an unknown value when the current
  // value is known
  assert(!(peer_type == 0 &&
           peer_name.type() != 0));
  // it is not allowed to assign a different known value when the
  // current value is also known.
  assert(!(peer_type != 0 &&
           peer_name.type() != 0 &&
           peer_type != peer_name.type()));
  peer_name._type = peer_type;
}

void SocketConnection::set_peer_id(int64_t peer_id) {
  assert(seastar::this_shard_id() == msgr_sid);
  // it is not allowed to assign an unknown value when the current
  // value is known
  assert(!(peer_id == entity_name_t::NEW &&
           peer_name.num() != entity_name_t::NEW));
  // it is not allowed to assign a different known value when the
  // current value is also known.
  assert(!(peer_id != entity_name_t::NEW &&
           peer_name.num() != entity_name_t::NEW &&
           peer_id != peer_name.num()));
  peer_name._num = peer_id;
}

void SocketConnection::set_features(uint64_t f) {
  assert(seastar::this_shard_id() == msgr_sid);
  features = f;
}

void SocketConnection::set_socket(Socket *s) {
  assert(seastar::this_shard_id() == msgr_sid);
  socket = s;
}

void SocketConnection::print(ostream& out) const {
  out << (void*)this << " ";
  messenger.print(out);
  if (seastar::this_shard_id() != msgr_sid) {
    out << " >> " << get_peer_name() << " " << peer_addr;
  } else if (!socket) {
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
