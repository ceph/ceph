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
#include <boost/functional/hash.hpp>

#include "auth/Auth.h"
#include "Errors.h"
#include "Dispatcher.h"
#include "Socket.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_ms);
  }
}

namespace crimson::net {

SocketMessenger::SocketMessenger(const entity_name_t& myname,
                                 const std::string& logic_name,
                                 uint32_t nonce,
                                 int master_sid)
  : Messenger{myname},
    master_sid{master_sid},
    sid{seastar::engine().cpu_id()},
    logic_name{logic_name},
    nonce{nonce}
{}

seastar::future<> SocketMessenger::set_myaddrs(const entity_addrvec_t& addrs)
{
  auto my_addrs = addrs;
  for (auto& addr : my_addrs.v) {
    addr.nonce = nonce;
  }
  return container().invoke_on_all([my_addrs](auto& msgr) {
      return msgr.Messenger::set_myaddrs(my_addrs);
    });
}

seastar::future<> SocketMessenger::bind(const entity_addrvec_t& addrs)
{
  ceph_assert(addrs.front().get_family() == AF_INET);
  auto my_addrs = addrs;
  for (auto& addr : my_addrs.v) {
    addr.nonce = nonce;
  }
  logger().info("{} listening on {}", *this, my_addrs.front().in4_addr());
  return container().invoke_on_all([my_addrs](auto& msgr) {
    msgr.do_bind(my_addrs);
  }).handle_exception_type([this] (const std::system_error& e) {
    if (e.code() == error::address_in_use) {
      throw e;
    } else {
      logger().error("{} bind: unexpected error {}", *this, e);
      ceph_abort();
    }
  });
}

seastar::future<>
SocketMessenger::try_bind(const entity_addrvec_t& addrs,
                          uint32_t min_port, uint32_t max_port)
{
  auto addr = addrs.front();
  if (addr.get_port() != 0) {
    return bind(addrs);
  }
  ceph_assert(min_port <= max_port);
  return seastar::do_with(uint32_t(min_port),
    [this, max_port, addr] (auto& port) {
      return seastar::repeat([this, max_port, addr, &port] {
          auto to_bind = addr;
          to_bind.set_port(port);
          return bind(entity_addrvec_t{to_bind})
            .then([this] {
              logger().info("{}: try_bind: done", *this);
              return stop_t::yes;
            }).handle_exception_type([this, max_port, &port] (const std::system_error& e) {
              ceph_assert(e.code() == error::address_in_use);
              logger().trace("{}: try_bind: {} already used", *this, port);
              if (port == max_port) {
                throw e;
              }
              ++port;
              return stop_t::no;
            });
        });
    });
}

seastar::future<> SocketMessenger::start(Dispatcher *disp) {
  return container().invoke_on_all([disp](auto& msgr) {
      return msgr.do_start(disp->get_local_shard());
    });
}

seastar::future<crimson::net::ConnectionXRef>
SocketMessenger::connect(const entity_addr_t& peer_addr, const entity_type_t& peer_type)
{
  // make sure we connect to a valid peer_addr
  ceph_assert(peer_addr.is_legacy() || peer_addr.is_msgr2());
  ceph_assert(peer_addr.get_port() > 0);

  auto shard = locate_shard(peer_addr);
  return container().invoke_on(shard, [peer_addr, peer_type](auto& msgr) {
      return msgr.do_connect(peer_addr, peer_type);
    }).then([](seastar::foreign_ptr<ConnectionRef>&& conn) {
      return seastar::make_lw_shared<seastar::foreign_ptr<ConnectionRef>>(std::move(conn));
    });
}

seastar::future<> SocketMessenger::stop()
{
  return do_shutdown();
}

seastar::future<> SocketMessenger::shutdown()
{
  return container().invoke_on_all([](auto& msgr) {
      return msgr.do_shutdown();
    }).finally([this] {
      return container().invoke_on_all([](auto& msgr) {
          msgr.shutdown_promise.set_value();
        });
    });
}

void SocketMessenger::do_bind(const entity_addrvec_t& addrs)
{
  // safe to discard an immediate ready future
  (void) Messenger::set_myaddrs(addrs);

  // TODO: v2: listen on multiple addresses
  seastar::socket_address address(addrs.front().in4_addr());
  seastar::listen_options lo;
  lo.reuse_address = true;
  listener = seastar::listen(address, lo);
}

seastar::future<> SocketMessenger::do_start(Dispatcher *disp)
{
  dispatcher = disp;
  started = true;

  // start listening if bind() was called
  if (listener) {
    // make sure we have already bound to a valid address
    ceph_assert(get_myaddr().is_legacy() || get_myaddr().is_msgr2());
    ceph_assert(get_myaddr().get_port() > 0);

    // forwarded to accepting_complete
    (void) seastar::keep_doing([this] {
        return Socket::accept(*listener)
          .then([this] (SocketFRef socket,
                        entity_addr_t peer_addr) {
            auto shard = locate_shard(peer_addr);
#warning fixme
            // we currently do dangerous i/o from a Connection core, different from the Socket core.
            return container().invoke_on(shard,
              [sock = std::move(socket), peer_addr, this](auto& msgr) mutable {
                SocketConnectionRef conn = seastar::make_shared<SocketConnection>(
                    msgr, *msgr.dispatcher, get_myaddr().is_msgr2());
                conn->start_accept(std::move(sock), peer_addr);
              });
          });
      }).handle_exception_type([this] (const std::system_error& e) {
        // stop gracefully on connection_aborted and invalid_argument
        if (e.code() != error::connection_aborted &&
            e.code() != error::invalid_argument) {
          logger().error("{} unexpected error during accept: {}", *this, e);
          ceph_abort();
        }
      }).handle_exception([this] (auto eptr) {
        logger().error("{} unexpected exception during accept: {}", *this, eptr);
        ceph_abort();
      }).then([this] () { return accepting_complete.set_value(); });
  } else {
    accepting_complete.set_value();
  }
  return seastar::now();
}

seastar::foreign_ptr<crimson::net::ConnectionRef>
SocketMessenger::do_connect(const entity_addr_t& peer_addr, const entity_type_t& peer_type)
{
  if (auto found = lookup_conn(peer_addr); found) {
    return seastar::make_foreign(found->shared_from_this());
  }
  SocketConnectionRef conn = seastar::make_shared<SocketConnection>(
      *this, *dispatcher, peer_addr.is_msgr2());
  conn->start_connect(peer_addr, peer_type);
  return seastar::make_foreign(conn->shared_from_this());
}

seastar::future<> SocketMessenger::do_shutdown()
{
  if (!started) {
    return seastar::now();
  }

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
    }).then([this] {
      return accepting_complete.get_shared_future();
    }).finally([this] {
      ceph_assert(connections.empty());
    });
}

seastar::future<> SocketMessenger::learned_addr(const entity_addr_t &peer_addr_for_me, const SocketConnection& conn)
{
  // make sure we there's no racing to learn address from peer
  return container().invoke_on(0, [peer_addr_for_me, &conn] (auto& msgr) {
    if (!msgr.need_addr) {
      if ((!msgr.get_myaddr().is_any() &&
           msgr.get_myaddr().get_type() != peer_addr_for_me.get_type()) ||
          msgr.get_myaddr().get_family() != peer_addr_for_me.get_family() ||
          !msgr.get_myaddr().is_same_host(peer_addr_for_me)) {
        logger().warn("{} peer_addr_for_me {} type/family/IP doesn't match myaddr {}",
                      conn, peer_addr_for_me, msgr.get_myaddr());
        throw std::system_error(
            make_error_code(crimson::net::error::bad_peer_address));
      }
      return seastar::now();
    }
    msgr.need_addr = false;

    if (msgr.get_myaddr().get_type() == entity_addr_t::TYPE_NONE) {
      // Not bound
      entity_addr_t addr = peer_addr_for_me;
      addr.set_type(entity_addr_t::TYPE_ANY);
      addr.set_port(0);
      return msgr.set_myaddrs(entity_addrvec_t{addr}
      ).then([&msgr, &conn, peer_addr_for_me] {
        logger().info("{} learned myaddr={} (unbound) from {}",
                      conn, msgr.get_myaddr(), peer_addr_for_me);
      });
    } else {
      // Already bound
      if (!msgr.get_myaddr().is_any() &&
          msgr.get_myaddr().get_type() != peer_addr_for_me.get_type()) {
        logger().warn("{} peer_addr_for_me {} type doesn't match myaddr {}",
                      conn, peer_addr_for_me, msgr.get_myaddr());
        throw std::system_error(
            make_error_code(crimson::net::error::bad_peer_address));
      }
      if (msgr.get_myaddr().get_family() != peer_addr_for_me.get_family()) {
        logger().warn("{} peer_addr_for_me {} family doesn't match myaddr {}",
                      conn, peer_addr_for_me, msgr.get_myaddr());
        throw std::system_error(
            make_error_code(crimson::net::error::bad_peer_address));
      }
      if (msgr.get_myaddr().is_blank_ip()) {
        entity_addr_t addr = peer_addr_for_me;
        addr.set_type(msgr.get_myaddr().get_type());
        addr.set_port(msgr.get_myaddr().get_port());
        return msgr.set_myaddrs(entity_addrvec_t{addr}
        ).then([&msgr, &conn, peer_addr_for_me] {
          logger().info("{} learned myaddr={} (blank IP) from {}",
                        conn, msgr.get_myaddr(), peer_addr_for_me);
        });
      } else if (!msgr.get_myaddr().is_same_host(peer_addr_for_me)) {
        logger().warn("{} peer_addr_for_me {} IP doesn't match myaddr {}",
                      conn, peer_addr_for_me, msgr.get_myaddr());
        throw std::system_error(
            make_error_code(crimson::net::error::bad_peer_address));
      } else {
        return seastar::now();
      }
    }
  });
}

SocketPolicy SocketMessenger::get_policy(entity_type_t peer_type) const
{
  return policy_set.get(peer_type);
}

SocketPolicy SocketMessenger::get_default_policy() const
{
  return policy_set.get_default();
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

seastar::shard_id SocketMessenger::locate_shard(const entity_addr_t& addr)
{
  ceph_assert(addr.get_family() == AF_INET);
  if (master_sid >= 0) {
    return master_sid;
  }
  std::size_t seed = 0;
  boost::hash_combine(seed, addr.u.sin.sin_addr.s_addr);
  //boost::hash_combine(seed, addr.u.sin.sin_port);
  //boost::hash_combine(seed, addr.nonce);
  return seed % seastar::smp::count;
}

crimson::net::SocketConnectionRef SocketMessenger::lookup_conn(const entity_addr_t& addr)
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
  if (master_sid >= 0) {
    ceph_assert(static_cast<int>(sid) == master_sid);
  }
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

seastar::future<uint32_t>
SocketMessenger::get_global_seq(uint32_t old)
{
  return container().invoke_on(0, [old] (auto& msgr) {
    if (old > msgr.global_seq) {
      msgr.global_seq = old;
    }
    return ++msgr.global_seq;
  });
}

} // namespace crimson::net
