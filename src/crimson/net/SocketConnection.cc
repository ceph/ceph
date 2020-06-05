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

#include "ProtocolV1.h"
#include "ProtocolV2.h"
#include "SocketMessenger.h"

#ifdef UNIT_TESTS_BUILT
#include "Interceptor.h"
#endif

using namespace crimson::net;
using crimson::common::local_conf;

SocketConnection::SocketConnection(SocketMessenger& messenger,
                                   ChainedDispatchersRef& dispatcher,
                                   bool is_msgr2)
  : messenger(messenger)
{
  if (is_msgr2) {
    protocol = std::make_unique<ProtocolV2>(dispatcher, *this, messenger);
  } else {
    protocol = std::make_unique<ProtocolV1>(dispatcher, *this, messenger);
  }
#ifdef UNIT_TESTS_BUILT
  if (messenger.interceptor) {
    interceptor = messenger.interceptor;
    interceptor->register_conn(*this);
  }
#endif
}

SocketConnection::~SocketConnection() {}

crimson::net::Messenger*
SocketConnection::get_messenger() const {
  return &messenger;
}

bool SocketConnection::is_connected() const
{
  assert(seastar::this_shard_id() == shard_id());
  return protocol->is_connected();
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
  return protocol->is_closed_clean;
}

#endif
bool SocketConnection::peer_wins() const
{
  return (messenger.get_myaddr() > peer_addr || policy.server);
}

seastar::future<> SocketConnection::send(MessageRef msg)
{
  assert(seastar::this_shard_id() == shard_id());
  return protocol->send(std::move(msg));
}

seastar::future<> SocketConnection::keepalive()
{
  assert(seastar::this_shard_id() == shard_id());
  return protocol->keepalive();
}

void SocketConnection::mark_down()
{
  assert(seastar::this_shard_id() == shard_id());
  protocol->close(false);
}

bool SocketConnection::update_rx_seq(seq_num_t seq)
{
  if (seq <= in_seq) {
    if (HAVE_FEATURE(features, RECONNECT_SEQ) &&
        local_conf()->ms_die_on_old_message) {
      ceph_abort_msg("old msgs despite reconnect_seq feature");
    }
    return false;
  } else if (seq > in_seq + 1) {
    if (local_conf()->ms_die_on_skipped_message) {
      ceph_abort_msg("skipped incoming seq");
    }
    return false;
  } else {
    in_seq = seq;
    return true;
  }
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
SocketConnection::close_clean(bool dispatch_reset)
{
  return protocol->close_clean(dispatch_reset);
}

seastar::shard_id SocketConnection::shard_id() const {
  return messenger.shard_id();
}

void SocketConnection::print(ostream& out) const {
    messenger.print(out);
    if (!protocol->socket) {
      out << " >> " << get_peer_name() << " " << peer_addr;
    } else if (protocol->socket->get_side() == Socket::side_t::acceptor) {
      out << " >> " << get_peer_name() << " " << peer_addr
          << "@" << protocol->socket->get_ephemeral_port();
    } else { // protocol->socket->get_side() == Socket::side_t::connector
      out << "@" << protocol->socket->get_ephemeral_port()
          << " >> " << get_peer_name() << " " << peer_addr;
    }
}
