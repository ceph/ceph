// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <seastar/core/sleep.hh>

#include <tuple>
#include <boost/functional/hash.hpp>
#include <boost/range/irange.hpp>
#include <fmt/os.h>
#include <fmt/std.h>

#include "auth/Auth.h"
#include "crimson/common/config_proxy.h" // for local_conf()
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
                                 uint32_t nonce,
                                 bool dispatch_only_on_this_shard)
  : sid{seastar::this_shard_id()},
    logic_name{logic_name},
    nonce{nonce},
    dispatch_only_on_sid{dispatch_only_on_this_shard},
    my_name{myname}
{}

SocketMessenger::~SocketMessenger()
{
  logger().debug("~SocketMessenger: {}", logic_name);
  ceph_assert_always(seastar::this_shard_id() == sid);
  ceph_assert(!listener);
  ceph_assert(core_listeners.empty());
}

bool SocketMessenger::set_addr_unknowns(const entity_addrvec_t &addrs)
{
  assert(seastar::this_shard_id() == sid);
  bool ret = false;

  entity_addrvec_t newaddrs = my_addrs;
  for (auto& a : newaddrs.v) {
    if (a.is_blank_ip()) {
      int type = a.get_type();
      int port = a.get_port();
      uint32_t nonce = a.get_nonce();
      for (auto& b : addrs.v) {
       if (a.get_family() == b.get_family()) {
         logger().debug(" assuming my addr {} matches provided addr {}", a, b);
         a = b;
         a.set_nonce(nonce);
         a.set_type(type);
         a.set_port(port);
         ret = true;
         break;
       }
      }
    }
  }
  my_addrs = newaddrs;
  return ret;
}

void SocketMessenger::set_myaddrs(const entity_addrvec_t& addrs)
{
  assert(seastar::this_shard_id() == sid);
  my_addrs = addrs;
  for (auto& addr : my_addrs.v) {
    addr.nonce = nonce;
  }
}

crimson::net::listen_ertr::future<>
SocketMessenger::do_listen(const entity_addrvec_t& addrs)
{
  ceph_assert(addrs.front().get_family() == AF_INET);
  set_myaddrs(addrs);
  return seastar::futurize_invoke([this] {
    if (!listener) {
      return ShardedServerSocket::create(dispatch_only_on_sid
      ).then([this] (auto _listener) {
        listener = _listener;
      });
    } else {
      return seastar::now();
    }
  }).then([this] () -> listen_ertr::future<> {
    const entity_addr_t listen_addr = get_myaddr();
    logger().debug("{} do_listen: try listen {}...", *this, listen_addr);
    if (!listener) {
      logger().warn("{} do_listen: listener doesn't exist", *this);
      return listen_ertr::now();
    }
    return listener->listen(listen_addr);
  });
}

SocketMessenger::bind_ertr::future<>
SocketMessenger::try_bind(const entity_addrvec_t& addrs,
                          uint32_t min_port, uint32_t max_port)
{
  // the classical OSD iterates over the addrvec and tries to listen on each
  // addr. crimson doesn't need to follow as there is a consensus we need to
  // worry only about proto v2.
  assert(addrs.size() == 1);
  auto addr = addrs.msgr2_addr();
  if (addr.get_port() != 0) {
    return do_listen(addrs).safe_then([this] {
      logger().info("{} try_bind: done", *this);
    });
  }
  return listen_on_first_free_port(addr, min_port, max_port,
      [this](const entity_addr_t& to_bind) {
    return do_listen(entity_addrvec_t{to_bind});
  }).safe_then([this](uint32_t) {
    logger().info("{} try_bind: done", *this);
  });
}

listen_ertr::future<uint32_t>
SocketMessenger::listen_on_first_free_port(
    const entity_addr_t& addr,
    uint32_t min_port, uint32_t max_port,
    listen_func_t listen_fn)
{
  ceph_assert(min_port <= max_port);
  return seastar::do_with(uint32_t(min_port), std::move(listen_fn),
                          [this, max_port, addr] (auto& port, auto& listen_fn) {
    return seastar::repeat_until_value([this, max_port, addr, &port, &listen_fn] {
      auto to_bind = addr;
      to_bind.set_port(port);
      return listen_fn(to_bind
      ).safe_then([] () -> seastar::future<std::optional<std::error_code>> {
        return seastar::make_ready_future<std::optional<std::error_code>>(
          std::make_optional<std::error_code>(std::error_code{/* success! */}));
      }, listen_ertr::all_same_way([this, max_port, &port]
                                   (const std::error_code& e) mutable
                                   -> seastar::future<std::optional<std::error_code>> {
        logger().trace("{} try_bind: {} got error {}", *this, port, e);
        if (port == max_port) {
          return seastar::make_ready_future<std::optional<std::error_code>>(
            std::make_optional<std::error_code>(e));
        }
        ++port;
        return seastar::make_ready_future<std::optional<std::error_code>>(
          std::optional<std::error_code>{std::nullopt});
      }));
    }).then([&port] (const std::error_code e) -> listen_ertr::future<uint32_t> {
      if (!e) {
        return listen_ertr::make_ready_future<uint32_t>(port); // success!
      } else if (e == std::errc::address_in_use) {
        return crimson::ct_error::address_in_use::make();
      } else if (e == std::errc::address_not_available) {
        return crimson::ct_error::address_not_available::make();
      }
      ceph_abort();
    });
  });
}

SocketMessenger::bind_ertr::future<>
SocketMessenger::bind(const entity_addrvec_t& addrs)
{
  assert(seastar::this_shard_id() == sid);
  using crimson::common::local_conf;
  return seastar::do_with(int64_t{local_conf()->ms_bind_retry_count},
                          [this, addrs] (auto& count) {
    return seastar::repeat_until_value([this, addrs, &count] {
      assert(count >= 0);
      return try_bind(addrs,
                      local_conf()->ms_bind_port_min,
                      local_conf()->ms_bind_port_max)
      .safe_then([this] {
        logger().info("{} try_bind: done", *this);
        return seastar::make_ready_future<std::optional<std::error_code>>(
          std::make_optional<std::error_code>(std::error_code{/* success! */}));
      }, bind_ertr::all_same_way([this, &count] (const std::error_code error) {
        if (count-- > 0) {
	  logger().info("{} was unable to bind. Trying again in {} seconds",
                        *this, local_conf()->ms_bind_retry_delay);
          return seastar::sleep(
            std::chrono::seconds(local_conf()->ms_bind_retry_delay)
          ).then([] {
            // one more time, please
            return seastar::make_ready_future<std::optional<std::error_code>>(
              std::optional<std::error_code>{std::nullopt});
          });
        } else {
          logger().info("{} was unable to bind after {} attempts: {}",
                        *this, local_conf()->ms_bind_retry_count, error);
          return seastar::make_ready_future<std::optional<std::error_code>>(
            std::make_optional<std::error_code>(error));
        }
      }));
    }).then([] (const std::error_code error) -> bind_ertr::future<> {
      if (!error) {
        return bind_ertr::now(); // success!
      } else if (error == std::errc::address_in_use) {
        return crimson::ct_error::address_in_use::make();
      } else if (error == std::errc::address_not_available) {
        return crimson::ct_error::address_not_available::make();
      }
      ceph_abort();
    });
  });
}

SocketMessenger::bind_ertr::future<>
SocketMessenger::bind_core_listeners()
{
  assert(seastar::this_shard_id() == sid);
  // the core listeners are found via the main address, so bind() first
  ceph_assert(listener);
  ceph_assert(!dispatch_only_on_sid);
  ceph_assert(core_listeners.empty());
  auto cores = boost::irange<seastar::shard_id>(0, seastar::this_smp_shard_count());
  return seastar::do_for_each(cores.begin(), cores.end(),
      [this](seastar::shard_id core) {
    return ShardedServerSocket::create_fixed(core
    ).then([this](ShardedServerSocket* core_listener) {
      core_listeners.push_back(core_listener);
    });
  }).then([this]() -> bind_ertr::future<> {
    using crimson::common::local_conf;
    return crimson::do_for_each(core_listeners,
        [this](ShardedServerSocket* core_listener) {
      return listen_on_first_free_port(get_myaddr(),
          local_conf()->ms_bind_port_min,
          local_conf()->ms_bind_port_max,
          [core_listener](const entity_addr_t& to_bind) {
        return core_listener->listen(to_bind);
      }).safe_then([this](uint32_t port) {
        logger().info("{} bind_core_listeners: core {} listens on port {}",
                      *this, core_ports.size(), port);
        core_ports.push_back(port);
      });
    });
  });
}

entity_addrvec_t SocketMessenger::get_core_addr(seastar::shard_id core) const
{
  assert(seastar::this_shard_id() == sid);
  ceph_assert(core < core_ports.size());
  // derived from the main address, which may be learned after binding
  auto addr = get_myaddr();
  addr.set_port(core_ports[core]);
  return entity_addrvec_t{addr};
}

std::vector<entity_addrvec_t> SocketMessenger::get_core_addrs() const
{
  assert(seastar::this_shard_id() == sid);
  std::vector<entity_addrvec_t> core_addrs;
  core_addrs.reserve(core_ports.size());
  for (seastar::shard_id core = 0; core < core_ports.size(); ++core) {
    core_addrs.emplace_back(get_core_addr(core));
  }
  return core_addrs;
}

entity_addrvec_t SocketMessenger::get_myaddrs_via(
    std::optional<seastar::shard_id> listener_core) const
{
  assert(seastar::this_shard_id() == sid);
  return listener_core ? get_core_addr(*listener_core) : get_myaddrs();
}

seastar::future<> SocketMessenger::accept_on_primary(
    SocketRef _socket,
    entity_addr_t peer_addr,
    std::optional<seastar::shard_id> listener_core)
{
  assert(get_myaddr().is_msgr2());
  SocketFRef socket = seastar::make_foreign(std::move(_socket));
  if (seastar::this_shard_id() == sid) {
    return accept(std::move(socket), peer_addr, listener_core);
  }
  return seastar::smp::submit_to(sid,
      [this, peer_addr, listener_core, socket = std::move(socket)]() mutable {
    return accept(std::move(socket), peer_addr, listener_core);
  });
}

seastar::future<> SocketMessenger::accept(
    SocketFRef &&socket,
    const entity_addr_t &peer_addr,
    std::optional<seastar::shard_id> listener_core)
{
  assert(seastar::this_shard_id() == sid);
  SocketConnectionRef conn =
    seastar::make_shared<SocketConnection>(*this, dispatchers);
  conn->start_accept(std::move(socket), peer_addr, listener_core);
  return seastar::now();
}

seastar::future<> SocketMessenger::start(
    const dispatchers_t& _dispatchers) {
  assert(seastar::this_shard_id() == sid);

  dispatchers.assign(_dispatchers);
  if (listener) {
    // make sure we have already bound to a valid address
    ceph_assert(get_myaddr().is_msgr2());
    ceph_assert(get_myaddr().get_port() > 0);

    return listener->accept([this](SocketRef socket, entity_addr_t peer_addr) {
      return accept_on_primary(std::move(socket), peer_addr, std::nullopt);
    }).then([this] {
      auto cores = boost::irange<seastar::shard_id>(0, core_listeners.size());
      return seastar::parallel_for_each(cores, [this](seastar::shard_id core) {
        return core_listeners[core]->accept(
            [this, core](SocketRef socket, entity_addr_t peer_addr) {
          return accept_on_primary(std::move(socket), peer_addr, core);
        });
      });
    });
  }
  return seastar::now();
}

crimson::net::ConnectionRef
SocketMessenger::connect(const entity_addr_t& peer_addr, const entity_name_t& peer_name)
{
  assert(seastar::this_shard_id() == sid);

  // make sure we connect to a valid peer_addr
  if (!peer_addr.is_msgr2()) {
    ceph_abort_msg("ProtocolV1 is no longer supported");
  }
  ceph_assert(peer_addr.get_port() > 0);

  if (auto found = lookup_conn(peer_addr); found) {
    logger().debug("{} connect to existing", *found);
    return found->get_local_shared_foreign_from_this();
  }
  SocketConnectionRef conn =
    seastar::make_shared<SocketConnection>(*this, dispatchers);
  conn->start_connect(peer_addr, peer_name);
  return conn->get_local_shared_foreign_from_this();
}

seastar::future<> SocketMessenger::shutdown()
{
  assert(seastar::this_shard_id() == sid);
  return seastar::futurize_invoke([this] {
    assert(dispatchers.empty());
    auto d_core_listeners = std::move(core_listeners);
    core_listeners.clear();
    core_ports.clear();
    return seastar::parallel_for_each(std::move(d_core_listeners),
        [](ShardedServerSocket* core_listener) {
      return core_listener->shutdown_destroy();
    });
  }).then([this] {
    if (listener) {
      auto d_listener = listener;
      listener = nullptr;
      return d_listener->shutdown_destroy();
    } else {
      return seastar::now();
    }
  // close all connections
  }).then([this] {
    return seastar::parallel_for_each(accepting_conns, [] (auto conn) {
      return conn->close_clean_yielded();
    });
  }).then([this] {
    ceph_assert(accepting_conns.empty());
    return seastar::parallel_for_each(connections, [] (auto conn) {
      return conn.second->close_clean_yielded();
    });
  }).then([this] {
    return seastar::parallel_for_each(closing_conns, [] (auto conn) {
      return conn->close_clean_yielded();
    });
  }).then([this] {
    ceph_assert(connections.empty());
    shutdown_promise.set_value();
  });
}

static entity_addr_t choose_addr(
  const entity_addr_t &peer_addr_for_me,
  const SocketConnection& conn)
{
  using crimson::common::local_conf;
  // XXX: a syscall is here
  if (const auto local_addr = conn.get_local_address();
      local_conf()->ms_learn_addr_from_peer) {
    logger().info("{} peer {} says I am {} (socket says {})",
                  conn, conn.get_peer_socket_addr(), peer_addr_for_me,
                  local_addr);
    return peer_addr_for_me;
  } else {
    const auto local_addr_for_me = conn.get_local_address();
    logger().info("{} socket to {} says I am {} (peer says {})",
                  conn, conn.get_peer_socket_addr(),
                  local_addr, peer_addr_for_me);
    entity_addr_t addr;
    addr.set_sockaddr(&local_addr_for_me.as_posix_sockaddr());
    return addr;
  }
}

void SocketMessenger::learned_addr(
    const entity_addr_t &peer_addr_for_me,
    const SocketConnection& conn)
{
  assert(seastar::this_shard_id() == sid);
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
    return;
  }

  if (get_myaddr().get_type() == entity_addr_t::TYPE_NONE) {
    // Not bound
    auto addr = choose_addr(peer_addr_for_me, conn);
    addr.set_type(entity_addr_t::TYPE_ANY);
    addr.set_port(0);
    need_addr = false;
    set_myaddrs(entity_addrvec_t{addr});
    logger().info("{} learned myaddr={} (unbound)", conn, get_myaddr());
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
      auto addr = choose_addr(peer_addr_for_me, conn);
      addr.set_type(get_myaddr().get_type());
      addr.set_port(get_myaddr().get_port());
      need_addr = false;
      set_myaddrs(entity_addrvec_t{addr});
      logger().info("{} learned myaddr={} (blank IP)", conn, get_myaddr());
    } else if (!get_myaddr().is_same_host(peer_addr_for_me)) {
      logger().warn("{} peer_addr_for_me {} IP doesn't match myaddr {}",
                    conn, peer_addr_for_me, get_myaddr());
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
    } else {
      need_addr = false;
    }
  }
}

SocketPolicy SocketMessenger::get_policy(entity_type_t peer_type) const
{
  assert(seastar::this_shard_id() == sid);
  return policy_set.get(peer_type);
}

SocketPolicy SocketMessenger::get_default_policy() const
{
  assert(seastar::this_shard_id() == sid);
  return policy_set.get_default();
}

void SocketMessenger::set_default_policy(const SocketPolicy& p)
{
  assert(seastar::this_shard_id() == sid);
  policy_set.set_default(p);
}

void SocketMessenger::set_policy(entity_type_t peer_type,
				 const SocketPolicy& p)
{
  assert(seastar::this_shard_id() == sid);
  policy_set.set(peer_type, p);
}

void SocketMessenger::set_policy_throttler(entity_type_t peer_type,
					   Throttle* throttle)
{
  assert(seastar::this_shard_id() == sid);
  // only byte throttler is used in OSD
  policy_set.set_throttlers(peer_type, throttle, nullptr);
}

crimson::net::SocketConnectionRef SocketMessenger::lookup_conn(
    const entity_addr_t& addr,
    std::optional<seastar::shard_id> listener_core)
{
  assert(seastar::this_shard_id() == sid);
  if (auto found = connections.find({addr, listener_core});
      found != connections.end()) {
    return found->second;
  } else {
    return nullptr;
  }
}

void SocketMessenger::accept_conn(SocketConnectionRef conn)
{
  assert(seastar::this_shard_id() == sid);
  accepting_conns.insert(conn);
}

void SocketMessenger::unaccept_conn(SocketConnectionRef conn)
{
  assert(seastar::this_shard_id() == sid);
  accepting_conns.erase(conn);
}

void SocketMessenger::register_conn(SocketConnectionRef conn)
{
  assert(seastar::this_shard_id() == sid);
  auto [i, added] = connections.emplace(
      conn_key_t{conn->get_peer_addr(), conn->get_listener_core()}, conn);
  std::ignore = i;
  ceph_assert(added);
}

void SocketMessenger::unregister_conn(SocketConnectionRef conn)
{
  assert(seastar::this_shard_id() == sid);
  ceph_assert(conn);
  auto found = connections.find(
      conn_key_t{conn->get_peer_addr(), conn->get_listener_core()});
  ceph_assert(found != connections.end());
  ceph_assert(found->second == conn);
  connections.erase(found);
}

seastar::future<> SocketMessenger::mark_down(const entity_addr_t& a)
{
  assert(seastar::this_shard_id() == sid);
  // the main connection (nullopt sorts first), then the per-core ones
  std::vector<SocketConnectionRef> conns;
  for (auto i = connections.lower_bound({a, std::nullopt});
       i != connections.end() && i->first.first == a;
       ++i) {
    conns.push_back(i->second);
  }
  return seastar::parallel_for_each(std::move(conns),
      [](SocketConnectionRef conn) {
    return seastar::smp::submit_to(
      conn->get_shard_id(),
      [conn=conn.get()] {
      conn->mark_down();
      return seastar::now();
    }).then([conn] { return seastar::now(); });
  });
}

void SocketMessenger::closing_conn(SocketConnectionRef conn)
{
  assert(seastar::this_shard_id() == sid);
  closing_conns.push_back(conn);
}

void SocketMessenger::closed_conn(SocketConnectionRef conn)
{
  assert(seastar::this_shard_id() == sid);
  for (auto it = closing_conns.begin();
       it != closing_conns.end();) {
    if (*it == conn) {
      it = closing_conns.erase(it);
    } else {
      it++;
    }
  }
}

uint32_t SocketMessenger::get_global_seq(uint32_t old)
{
  assert(seastar::this_shard_id() == sid);
  if (old > global_seq) {
    global_seq = old;
  }
  return ++global_seq;
}

} // namespace crimson::net
