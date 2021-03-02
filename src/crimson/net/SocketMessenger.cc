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
#include "Socket.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_ms);
  }
}

namespace crimson::net {

SocketMessenger::SocketMessenger(const entity_name_t& myname,
                                 const std::string& logic_name,
                                 uint32_t nonce)
  : Messenger{myname},
    master_sid{seastar::this_shard_id()},
    logic_name{logic_name},
    nonce{nonce}
{}

SocketMessenger::~SocketMessenger()
{
  ceph_assert(!listener);
}

seastar::future<> SocketMessenger::set_myaddrs(const entity_addrvec_t& addrs)
{
  assert(seastar::this_shard_id() == master_sid);
  auto my_addrs = addrs;
  for (auto& addr : my_addrs.v) {
    addr.nonce = nonce;
  }
  return Messenger::set_myaddrs(my_addrs);
}

SocketMessenger::bind_ertr::future<> SocketMessenger::do_bind(const entity_addrvec_t& addrs)
{
  assert(seastar::this_shard_id() == master_sid);
  ceph_assert(addrs.front().get_family() == AF_INET);
  return set_myaddrs(addrs).then([this] {
    if (!listener) {
      return FixedCPUServerSocket::create().then([this] (auto _listener) {
        listener = _listener;
      });
    } else {
      return seastar::now();
    }
  }).then([this] () -> bind_ertr::future<> {
    const entity_addr_t listen_addr = get_myaddr();
    logger().debug("{} do_bind: try listen {}...", *this, listen_addr);
    if (!listener) {
      logger().warn("{} do_bind: listener doesn't exist", *this);
      return bind_ertr::now();
    }
    return listener->listen(listen_addr);
  });
}

SocketMessenger::bind_ertr::future<>
SocketMessenger::bind(const entity_addrvec_t& addrs)
{
  return do_bind(addrs).safe_then([this] {
    logger().info("{} bind: done", *this);
  });
}

SocketMessenger::bind_ertr::future<>
SocketMessenger::try_bind(const entity_addrvec_t& addrs,
                          uint32_t min_port, uint32_t max_port)
{
  auto addr = addrs.front();
  if (addr.get_port() != 0) {
    return do_bind(addrs).safe_then([this] {
      logger().info("{} try_bind: done", *this);
    });
  }
  ceph_assert(min_port <= max_port);
  return seastar::do_with(uint32_t(min_port),
                          [this, max_port, addr] (auto& port) {
    return seastar::repeat_until_value([this, max_port, addr, &port] {
      auto to_bind = addr;
      to_bind.set_port(port);
      return do_bind(entity_addrvec_t{to_bind}
      ).safe_then([this] () -> seastar::future<std::optional<bool>> {
        logger().info("{} try_bind: done", *this);
        return seastar::make_ready_future<std::optional<bool>>(
            std::make_optional<bool>(true));
      }, bind_ertr::all_same_way([this, max_port, &port]
                                 (const std::error_code& e) mutable
                                 -> seastar::future<std::optional<bool>> {
        assert(e == std::errc::address_in_use);
        logger().trace("{} try_bind: {} already used", *this, port);
        if (port == max_port) {
          return seastar::make_ready_future<std::optional<bool>>(
              std::make_optional<bool>(false));
        }
        ++port;
        return seastar::make_ready_future<std::optional<bool>>();
      }));
    }).then([] (bool success) -> bind_ertr::future<> {
      if (success) {
        return bind_ertr::now();
      } else {
        return crimson::ct_error::address_in_use::make();
      }
    });
  });
}

seastar::future<> SocketMessenger::start(
    const dispatchers_t& _dispatchers) {
  assert(seastar::this_shard_id() == master_sid);

  dispatchers.assign(_dispatchers);
  if (listener) {
    // make sure we have already bound to a valid address
    ceph_assert(get_myaddr().is_msgr2());
    ceph_assert(get_myaddr().get_port() > 0);

    return listener->accept([this] (SocketRef socket, entity_addr_t peer_addr) {
      assert(seastar::this_shard_id() == master_sid);
      assert(get_myaddr().is_msgr2());
      SocketConnectionRef conn =
        seastar::make_shared<SocketConnection>(*this, dispatchers);
      conn->start_accept(std::move(socket), peer_addr);
      return seastar::now();
    });
  }
  return seastar::now();
}

crimson::net::ConnectionRef
SocketMessenger::connect(const entity_addr_t& peer_addr, const entity_name_t& peer_name)
{
  assert(seastar::this_shard_id() == master_sid);

  // make sure we connect to a valid peer_addr
  if (!peer_addr.is_msgr2()) {
    ceph_abort_msg("ProtocolV1 is no longer supported");
  }
  ceph_assert(peer_addr.get_port() > 0);

  if (auto found = lookup_conn(peer_addr); found) {
    logger().debug("{} connect to existing", *found);
    return found->shared_from_this();
  }
  SocketConnectionRef conn =
    seastar::make_shared<SocketConnection>(*this, dispatchers);
  conn->start_connect(peer_addr, peer_name);
  return conn->shared_from_this();
}

seastar::future<> SocketMessenger::shutdown()
{
  assert(seastar::this_shard_id() == master_sid);
  return seastar::futurize_invoke([this] {
    assert(dispatchers.empty());
    if (listener) {
      auto d_listener = listener;
      listener = nullptr;
      return d_listener->destroy();
    } else {
      return seastar::now();
    }
  // close all connections
  }).then([this] {
    return seastar::parallel_for_each(accepting_conns, [] (auto conn) {
      return conn->close_clean(false);
    });
  }).then([this] {
    ceph_assert(accepting_conns.empty());
    return seastar::parallel_for_each(connections, [] (auto conn) {
      return conn.second->close_clean(false);
    });
  }).then([this] {
    return seastar::parallel_for_each(closing_conns, [] (auto conn) {
      return conn->close_clean(false);
    });
  }).then([this] {
    ceph_assert(connections.empty());
    shutdown_promise.set_value();
  });
}

seastar::future<> SocketMessenger::learned_addr(const entity_addr_t &peer_addr_for_me, const SocketConnection& conn)
{
  assert(seastar::this_shard_id() == master_sid);
  if (!need_addr) {
    if ((!get_myaddr().is_any() &&
         get_myaddr().get_type() != peer_addr_for_me.get_type()) ||
        get_myaddr().get_family() != peer_addr_for_me.get_family() ||
        !get_myaddr().is_same_host(peer_addr_for_me)) {
      logger().warn("{} peer_addr_for_me {} type/family/IP doesn't match myaddr {}",
                    conn, peer_addr_for_me, get_myaddr());
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
    }
    return seastar::now();
  }

  if (get_myaddr().get_type() == entity_addr_t::TYPE_NONE) {
    // Not bound
    entity_addr_t addr = peer_addr_for_me;
    addr.set_type(entity_addr_t::TYPE_ANY);
    addr.set_port(0);
    need_addr = false;
    return set_myaddrs(entity_addrvec_t{addr}
    ).then([this, &conn, peer_addr_for_me] {
      logger().info("{} learned myaddr={} (unbound) from {}",
                    conn, get_myaddr(), peer_addr_for_me);
    });
  } else {
    // Already bound
    if (!get_myaddr().is_any() &&
        get_myaddr().get_type() != peer_addr_for_me.get_type()) {
      logger().warn("{} peer_addr_for_me {} type doesn't match myaddr {}",
                    conn, peer_addr_for_me, get_myaddr());
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
    }
    if (get_myaddr().get_family() != peer_addr_for_me.get_family()) {
      logger().warn("{} peer_addr_for_me {} family doesn't match myaddr {}",
                    conn, peer_addr_for_me, get_myaddr());
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
    }
    if (get_myaddr().is_blank_ip()) {
      entity_addr_t addr = peer_addr_for_me;
      addr.set_type(get_myaddr().get_type());
      addr.set_port(get_myaddr().get_port());
      need_addr = false;
      return set_myaddrs(entity_addrvec_t{addr}
      ).then([this, &conn, peer_addr_for_me] {
        logger().info("{} learned myaddr={} (blank IP) from {}",
                      conn, get_myaddr(), peer_addr_for_me);
      });
    } else if (!get_myaddr().is_same_host(peer_addr_for_me)) {
      logger().warn("{} peer_addr_for_me {} IP doesn't match myaddr {}",
                    conn, peer_addr_for_me, get_myaddr());
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
    } else {
      need_addr = false;
      return seastar::now();
    }
  }
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

void SocketMessenger::closing_conn(SocketConnectionRef conn)
{
  closing_conns.push_back(conn);
}

void SocketMessenger::closed_conn(SocketConnectionRef conn)
{
  for (auto it = closing_conns.begin();
       it != closing_conns.end();) {
    if (*it == conn) {
      it = closing_conns.erase(it);
    } else {
      it++;
    }
  }
}

seastar::future<uint32_t>
SocketMessenger::get_global_seq(uint32_t old)
{
  if (old > global_seq) {
    global_seq = old;
  }
  return seastar::make_ready_future<uint32_t>(++global_seq);
}

} // namespace crimson::net
