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

#include "Config.h"
#include "ProtocolV1.h"
#include "ProtocolV2.h"
#include "SocketMessenger.h"

using namespace ceph::net;

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_ms);
  }
}

SocketConnection::SocketConnection(SocketMessenger& messenger,
                                   Dispatcher& dispatcher,
                                   bool is_msgr2)
  : messenger(messenger)
{
  ceph_assert(&messenger.container().local() == &messenger);
  if (is_msgr2) {
    protocol = std::make_unique<ProtocolV2>(dispatcher, *this, messenger);
  } else {
    protocol = std::make_unique<ProtocolV1>(dispatcher, *this, messenger);
  }
}

SocketConnection::~SocketConnection() {}

ceph::net::Messenger*
SocketConnection::get_messenger() const {
  return &messenger;
}

seastar::future<bool> SocketConnection::is_connected()
{
  return seastar::smp::submit_to(shard_id(), [this] {
      return protocol->is_connected();
    });
}

seastar::future<> SocketConnection::send(MessageRef msg)
{
  logger().debug("{} --> {} === {}", messenger, get_peer_addr(), *msg);
  return seastar::smp::submit_to(shard_id(), [this, msg=std::move(msg)] {
    return protocol->send(std::move(msg));
  });
}

seastar::future<> SocketConnection::keepalive()
{
  return seastar::smp::submit_to(shard_id(), [this] {
    return protocol->keepalive();
  });
}

seastar::future<> SocketConnection::close()
{
  return seastar::smp::submit_to(shard_id(), [this] {
      return protocol->close();
    });
}

void SocketConnection::requeue_sent()
{
  out_seq -= sent.size();
  while (!sent.empty()) {
    auto m = sent.front();
    sent.pop_front();
    out_q.push_back(std::move(m));
  }
}

bool SocketConnection::update_rx_seq(seq_num_t seq)
{
  if (seq <= in_seq) {
    if (HAVE_FEATURE(features, RECONNECT_SEQ) &&
        conf.ms_die_on_old_message) {
      ceph_abort_msg("old msgs despite reconnect_seq feature");
    }
    return false;
  } else if (seq > in_seq + 1) {
    if (conf.ms_die_on_skipped_message) {
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
                                const entity_type_t& _peer_type)
{
  protocol->start_connect(_peer_addr, _peer_type);
}

void
SocketConnection::start_accept(SocketFRef&& sock,
                               const entity_addr_t& _peer_addr)
{
  protocol->start_accept(std::move(sock), _peer_addr);
}

seastar::shard_id SocketConnection::shard_id() const {
  return messenger.shard_id();
}

void SocketConnection::print(ostream& out) const {
    messenger.print(out);
    if (side == side_t::none) {
      out << " >> " << peer_addr;
    } else if (side == side_t::acceptor) {
      out << " >> " << peer_addr
          << "@" << socket_port;
    } else { // side == side_t::connector
      out << "@" << socket_port
          << " >> " << peer_addr;
    }
}
