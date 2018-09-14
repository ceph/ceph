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

#include <tuple>
#include "auth/Auth.h"
#include "SocketMessenger.h"
#include "SocketConnection.h"
#include "Dispatcher.h"
#include "msg/Message.h"

using namespace ceph::net;

SocketMessenger::SocketMessenger(const entity_name_t& myname)
  : Messenger{myname}
{}

void SocketMessenger::bind(const entity_addr_t& addr)
{
  if (addr.get_family() != AF_INET) {
    throw std::system_error(EAFNOSUPPORT, std::generic_category());
  }

  set_myaddr(addr);

  seastar::socket_address address(addr.in4_addr());
  seastar::listen_options lo;
  lo.reuse_address = true;
  listener = seastar::listen(address, lo);
}

seastar::future<> SocketMessenger::dispatch(ConnectionRef conn)
{
  auto [i, added] = connections.emplace(conn->get_peer_addr(), conn);
  std::ignore = i;
  ceph_assert(added);

  return seastar::keep_doing([=] {
      return conn->read_message()
        .then([=] (MessageRef msg) {
          // start dispatch, ignoring exceptions from the application layer
          seastar::with_gate(pending_dispatch, [=, msg = std::move(msg)] {
              return dispatcher->ms_dispatch(conn, std::move(msg))
                .handle_exception([] (std::exception_ptr eptr) {});
            });
          // return immediately to start on the next message
          return seastar::now();
        });
    }).handle_exception_type([=] (const std::system_error& e) {
      if (e.code() == error::connection_aborted ||
          e.code() == error::connection_reset) {
        return seastar::with_gate(pending_dispatch, [=] {
            return dispatcher->ms_handle_reset(conn);
          });
      } else if (e.code() == error::read_eof) {
        return seastar::with_gate(pending_dispatch, [=] {
            return dispatcher->ms_handle_remote_reset(conn);
          });
      } else {
        throw e;
      }
    });
}

seastar::future<> SocketMessenger::accept(seastar::connected_socket socket,
                                          seastar::socket_address paddr)
{
  // allocate the connection
  entity_addr_t peer_addr;
  peer_addr.set_type(entity_addr_t::TYPE_DEFAULT);
  peer_addr.set_sockaddr(&paddr.as_posix_sockaddr());
  ConnectionRef conn = new SocketConnection(this, get_myaddr(),
                                            peer_addr, std::move(socket));
  // initiate the handshake
  return conn->server_handshake()
    .handle_exception([conn] (std::exception_ptr eptr) {
      // close the connection before returning errors
      return seastar::make_exception_future<>(eptr)
        .finally([conn] { return conn->close(); });
    }).then([this, conn] {
      dispatcher->ms_handle_accept(conn);
      // dispatch messages until the connection closes or the dispatch
      // queue shuts down
      return dispatch(std::move(conn));
    });
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
            // start processing the connection
            accept(std::move(socket), paddr)
              .handle_exception([] (std::exception_ptr eptr) {});
            // don't wait before accepting another
          });
      }).handle_exception_type([this] (const std::system_error& e) {
        // stop gracefully on connection_aborted
        if (e.code() != error::connection_aborted) {
          throw e;
        }
      });
  }

  return seastar::now();
}

seastar::future<ceph::net::ConnectionRef>
SocketMessenger::connect(const entity_addr_t& addr, entity_type_t peer_type)
{
  if (auto found = lookup_conn(addr); found) {
    return seastar::make_ready_future<ceph::net::ConnectionRef>(found);
  }
  return seastar::connect(addr.in4_addr())
    .then([=] (seastar::connected_socket socket) {
      ConnectionRef conn = new SocketConnection(this, get_myaddr(), addr,
                                                std::move(socket));
      // complete the handshake before returning to the caller
      return conn->client_handshake(peer_type, get_myname().type())
        .handle_exception([conn] (std::exception_ptr eptr) {
          // close the connection before returning errors
          return seastar::make_exception_future<>(eptr)
            .finally([conn] { return conn->close(); });
	  // TODO: retry on fault
        }).then([=] {
          dispatcher->ms_handle_connect(conn);
          // dispatch replies on this connection
          dispatch(conn)
            .handle_exception([] (std::exception_ptr eptr) {});
          return conn;
        });
    });
}

seastar::future<> SocketMessenger::shutdown()
{
  if (listener) {
    listener->abort_accept();
  }
  // close all connections
  return seastar::parallel_for_each(connections.begin(), connections.end(),
    [this] (auto conn) {
      return conn.second->close();
    }).finally([this] {
      connections.clear();
      // closing connections will unblock any dispatchers that were waiting to
      // send(). wait for any pending calls to finish
      return pending_dispatch.close();
    });
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

ceph::net::ConnectionRef SocketMessenger::lookup_conn(const entity_addr_t& addr)
{
  if (auto found = connections.find(addr);
      found != connections.end()) {
    return found->second;
  } else {
    return nullptr;
  }
}

void SocketMessenger::unregister_conn(ConnectionRef conn)
{
  ceph_assert(conn);
  auto found = connections.find(conn->get_peer_addr());
  ceph_assert(found != connections.end());
  ceph_assert(found->second == conn);
  connections.erase(found);
}

seastar::future<msgr_tag_t, bufferlist>
SocketMessenger::verify_authorizer(peer_type_t peer_type,
				   auth_proto_t protocol,
				   bufferlist& auth)
{
  if (dispatcher) {
    return dispatcher->ms_verify_authorizer(peer_type, protocol, auth);
  } else {
    return seastar::make_ready_future<msgr_tag_t, bufferlist>(
        CEPH_MSGR_TAG_BADAUTHORIZER,
        bufferlist{});
  }
}

seastar::future<std::unique_ptr<AuthAuthorizer>>
SocketMessenger::get_authorizer(peer_type_t peer_type, bool force_new)
{
  if (dispatcher) {
    return dispatcher->ms_get_authorizer(peer_type, force_new);
  } else {
    return seastar::make_ready_future<std::unique_ptr<AuthAuthorizer>>(nullptr);
  }
}
