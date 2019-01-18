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

#include "SocketMessenger.h"

#include <tuple>

#include "auth/Auth.h"
#include "Errors.h"
#include "Dispatcher.h"

using namespace ceph::net;

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_ms);
  }
}

SocketMessenger::SocketMessenger(const entity_name_t& myname,
                                 const std::string& logic_name,
                                 uint32_t nonce)
  : Messenger{myname}, logic_name{logic_name}, nonce{nonce}
{}

void SocketMessenger::set_myaddrs(const entity_addrvec_t& addrs)
{
  auto my_addrs = addrs;
  for (auto& addr : my_addrs.v) {
    addr.nonce = nonce;
  }
  // TODO: propagate to all the cores of the Messenger
  Messenger::set_myaddrs(my_addrs);
}

void SocketMessenger::bind(const entity_addrvec_t& addrs)
{
  // TODO: v2: listen on multiple addresses
  auto addr = addrs.legacy_addr();

  if (addr.get_family() != AF_INET) {
    throw std::system_error(EAFNOSUPPORT, std::generic_category());
  }

  set_myaddrs(addrs);

  seastar::socket_address address(addr.in4_addr());
  seastar::listen_options lo;
  lo.reuse_address = true;
  listener = seastar::listen(address, lo);
}

seastar::future<> SocketMessenger::start(Dispatcher *disp)
{
  dispatcher = disp;

  // start listening if bind() was called
  if (listener) {
    seastar::keep_doing([this] {
        return listener->accept()
          .then([this] (seastar::connected_socket socket,
                        seastar::socket_address paddr) {
            // allocate the connection
            entity_addr_t peer_addr;
            peer_addr.set_sockaddr(&paddr.as_posix_sockaddr());
            SocketConnectionRef conn = new SocketConnection(*this, *dispatcher);
            // don't wait before accepting another
            conn->start_accept(std::move(socket), peer_addr);
          });
      }).handle_exception_type([this] (const std::system_error& e) {
        // stop gracefully on connection_aborted
        if (e.code() != error::connection_aborted) {
          logger().error("{} unexpected error during accept: {}", *this, e);
        }
      });
  }

  return seastar::now();
}

ceph::net::ConnectionRef
SocketMessenger::connect(const entity_addr_t& peer_addr, const entity_type_t& peer_type)
{
  if (auto found = lookup_conn(peer_addr); found) {
    return found;
  }
  SocketConnectionRef conn = new SocketConnection(*this, *dispatcher);
  conn->start_connect(peer_addr, peer_type);
  return conn;
}

seastar::future<> SocketMessenger::shutdown()
{
  if (listener) {
    listener->abort_accept();
  }
  // close all connections
  return seastar::parallel_for_each(accepting_conns, [] (auto conn) {
      return conn->close();
    }).then([this] {
      ceph_assert(accepting_conns.empty());
      return seastar::parallel_for_each(connections, [] (auto conn) {
          return conn.second->close();
        });
    }).finally([this] {
      ceph_assert(connections.empty());
    });
}

void SocketMessenger::learned_addr(const entity_addr_t &peer_addr_for_me)
{
  if (!get_myaddr().is_blank_ip()) {
    // already learned or binded
    return;
  }

  // Only learn IP address if blank.
  entity_addr_t addr = get_myaddr();
  addr.u = peer_addr_for_me.u;
  addr.set_type(peer_addr_for_me.get_type());
  addr.set_port(get_myaddr().get_port());
  set_myaddrs(entity_addrvec_t{addr});
}

void SocketMessenger::set_default_policy(const SocketPolicy& p)
{
  policy_set.set_default(p);
}

void SocketMessenger::set_policy(entity_type_t peer_type,
				 const SocketPolicy& p)
{
  policy_set.set(peer_type, p);
}

void SocketMessenger::set_policy_throttler(entity_type_t peer_type,
					   Throttle* throttle)
{
  // only byte throttler is used in OSD
  policy_set.set_throttlers(peer_type, throttle, nullptr);
}

ceph::net::SocketConnectionRef SocketMessenger::lookup_conn(const entity_addr_t& addr)
{
  if (auto found = connections.find(addr);
      found != connections.end()) {
    return found->second;
  } else {
    return nullptr;
  }
}

void SocketMessenger::accept_conn(SocketConnectionRef conn)
{
  accepting_conns.insert(conn);
}

void SocketMessenger::unaccept_conn(SocketConnectionRef conn)
{
  accepting_conns.erase(conn);
}

void SocketMessenger::register_conn(SocketConnectionRef conn)
{
  auto [i, added] = connections.emplace(conn->get_peer_addr(), conn);
  std::ignore = i;
  ceph_assert(added);
}

void SocketMessenger::unregister_conn(SocketConnectionRef conn)
{
  ceph_assert(conn);
  auto found = connections.find(conn->get_peer_addr());
  ceph_assert(found != connections.end());
  ceph_assert(found->second == conn);
  connections.erase(found);
}
