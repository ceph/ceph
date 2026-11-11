// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "Socket.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/do_with.hh>
#include <seastar/net/packet.hh>

#include <span>

#include "crimson/common/config_proxy.h" // for local_conf()
#include "crimson/common/log.h"
#include "include/random.h" // for ceph::util::generate_random_number()
#include "Errors.h"

using crimson::common::local_conf;

namespace crimson::net {

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_ms);
}

using tmp_buf = seastar::temporary_buffer<char>;
using packet = seastar::net::packet;

// an input_stream consumer that reads buffer segments into a bufferlist up to
// the given number of remaining bytes
struct bufferlist_consumer {
  bufferlist& bl;
  size_t& remaining;

  bufferlist_consumer(bufferlist& bl, size_t& remaining)
    : bl(bl), remaining(remaining) {}

  using consumption_result_type = typename seastar::input_stream<char>::consumption_result_type;

  // consume some or all of a buffer segment
  seastar::future<consumption_result_type> operator()(tmp_buf&& data) {
    if (remaining >= data.size()) {
      // consume the whole buffer
      remaining -= data.size();
      bl.append(buffer::create(std::move(data)));
      if (remaining > 0) {
        // return none to request more segments
        return seastar::make_ready_future<consumption_result_type>(
            seastar::continue_consuming{});
      } else {
        // return an empty buffer to singal that we're done
        return seastar::make_ready_future<consumption_result_type>(
            consumption_result_type::stop_consuming_type({}));
      }
    }
    if (remaining > 0) {
      // consume the front
      bl.append(buffer::create(data.share(0, remaining)));
      data.trim_front(remaining);
      remaining = 0;
    }
    // give the rest back to signal that we're done
    return seastar::make_ready_future<consumption_result_type>(
        consumption_result_type::stop_consuming_type{std::move(data)});
  };
};

seastar::future<> inject_delay()
{
  if (float delay_period = local_conf()->ms_inject_internal_delays;
      delay_period) {
    logger().debug("Socket::inject_delay: sleep for {}", delay_period);
    return seastar::sleep(
      std::chrono::milliseconds((int)(delay_period * 1000.0)));
  }
  return seastar::now();
}

void inject_failure()
{
  if (local_conf()->ms_inject_socket_failures) {
    uint64_t rand =
      ceph::util::generate_random_number<uint64_t>(1, RAND_MAX);
    if (rand % local_conf()->ms_inject_socket_failures == 0) {
      logger().warn("Socket::inject_failure: injecting socket failure");
      throw std::system_error(make_error_code(
        error::negotiation_failure));
    }
  }
}

} // anonymous namespace

Socket::Socket(
    seastar::connected_socket &&_socket,
    side_t _side,
    uint16_t e_port,
    construct_tag)
  : sid{seastar::this_shard_id()},
    socket(std::move(_socket)),
    in(socket.input()),
    // the default buffer size 8192 is too small that may impact our write
    // performance. see seastar::net::connected_socket::output()
    out(socket.output(65536)),
    socket_is_shutdown(false),
    side(_side),
    ephemeral_port(e_port)
{
  if (local_conf()->ms_tcp_nodelay) {
    socket.set_nodelay(true);
  }
}

Socket::~Socket()
{
  assert(seastar::this_shard_id() == sid);
#ifndef NDEBUG
  assert(closed);
#endif
}

seastar::future<bufferlist>
Socket::read(size_t bytes)
{
  assert(seastar::this_shard_id() == sid);
#ifdef UNIT_TESTS_BUILT
  return try_trap_pre(next_trap_read).then([bytes, this] {
#endif
    if (bytes == 0) {
      return seastar::make_ready_future<bufferlist>();
    }
    r.buffer.clear();
    r.remaining = bytes;
    return in.consume(bufferlist_consumer{r.buffer, r.remaining}).then([this] {
      if (r.remaining) { // throw on short reads
        throw std::system_error(make_error_code(error::read_eof));
      }
      inject_failure();
      return inject_delay().then([this] {
        return seastar::make_ready_future<bufferlist>(std::move(r.buffer));
      });
    });
#ifdef UNIT_TESTS_BUILT
  }).then([this](auto buf) {
    return try_trap_post(next_trap_read
    ).then([buf = std::move(buf)]() mutable {
      return std::move(buf);
    });
  });
#endif
}

seastar::future<bufferptr>
Socket::read_exactly(size_t bytes) {
  assert(seastar::this_shard_id() == sid);
#ifdef UNIT_TESTS_BUILT
  return try_trap_pre(next_trap_read).then([bytes, this] {
#endif
    if (bytes == 0) {
      return seastar::make_ready_future<bufferptr>();
    }
    return in.read_exactly(bytes).then([bytes](auto buf) {
      bufferptr ptr(buffer::create(buf.share()));
      if (ptr.length() < bytes) {
        throw std::system_error(make_error_code(error::read_eof));
      }
      inject_failure();
      return inject_delay(
      ).then([ptr = std::move(ptr)]() mutable {
        return seastar::make_ready_future<bufferptr>(std::move(ptr));
      });
    });
#ifdef UNIT_TESTS_BUILT
  }).then([this](auto ptr) {
    return try_trap_post(next_trap_read
    ).then([ptr = std::move(ptr)]() mutable {
      return std::move(ptr);
    });
  });
#endif
}

seastar::future<>
Socket::write(bufferlist buf)
{
  assert(seastar::this_shard_id() == sid);
#ifdef UNIT_TESTS_BUILT
  return try_trap_pre(next_trap_write
  ).then([buf = std::move(buf), this]() mutable {
#endif
    inject_failure();
    return inject_delay(
    ).then([buf = std::move(buf), this]() mutable {
      packet p(std::move(buf));
      return seastar::do_with(p.release(), [this](auto& frags) {
        return out.write(std::span<seastar::temporary_buffer<char>>(frags));
      });
    });
#ifdef UNIT_TESTS_BUILT
  }).then([this] {
    return try_trap_post(next_trap_write);
  });
#endif
}

seastar::future<>
Socket::flush()
{
  assert(seastar::this_shard_id() == sid);
  inject_failure();
  return inject_delay().then([this] {
    return out.flush();
  });
}

seastar::future<>
Socket::write_flush(bufferlist buf)
{
  assert(seastar::this_shard_id() == sid);
#ifdef UNIT_TESTS_BUILT
  return try_trap_pre(next_trap_write
  ).then([buf = std::move(buf), this]() mutable {
#endif
    inject_failure();
    return inject_delay(
    ).then([buf = std::move(buf), this]() mutable {
      packet p(std::move(buf));
      return seastar::do_with(p.release(), [this](auto& frags) {
        return out.write(std::span<seastar::temporary_buffer<char>>(frags));
      }).then([this] {
        return out.flush();
      });
    });
#ifdef UNIT_TESTS_BUILT
  }).then([this] {
    return try_trap_post(next_trap_write);
  });
#endif
}

void Socket::shutdown()
{
  assert(seastar::this_shard_id() == sid);
  socket_is_shutdown = true;
  socket.shutdown_input();
  socket.shutdown_output();
}

static inline seastar::future<>
close_and_handle_errors(seastar::output_stream<char>& out)
{
  return out.close().handle_exception_type([](const std::system_error& e) {
    if (e.code() != std::errc::broken_pipe &&
        e.code() != std::errc::connection_reset) {
      logger().error("Socket::close(): unexpected error {}", e.what());
      ceph_abort();
    }
    // can happen when out is already shutdown, ignore
  });
}

seastar::future<>
Socket::close()
{
  assert(seastar::this_shard_id() == sid);
#ifndef NDEBUG
  ceph_assert_always(!closed);
  closed = true;
#endif
  return seastar::when_all_succeed(
    inject_delay(),
    in.close(),
    close_and_handle_errors(out)
  ).then_unpack([] {
    return seastar::make_ready_future<>();
  }).handle_exception([](auto eptr) {
    const char *e_what;
    try {
      std::rethrow_exception(eptr);
    } catch (std::exception &e) {
      e_what = e.what();
    }
    logger().error("Socket::close(): unexpected exception {}", e_what);
    ceph_abort();
  });
}

seastar::future<SocketRef>
Socket::connect(const entity_addr_t &peer_addr)
{
  inject_failure();
  return inject_delay(
  ).then([peer_addr] {
    return seastar::connect(peer_addr.in4_addr());
  }).then([peer_addr](seastar::connected_socket socket) {
    auto ret = std::make_unique<Socket>(
      std::move(socket), side_t::connector, 0, construct_tag{});
    logger().debug("Socket::connect(): connected to {}, socket {}",
                   peer_addr, fmt::ptr(ret.get()));
    return ret;
  });
}

#ifdef UNIT_TESTS_BUILT
void Socket::set_trap(bp_type_t type, bp_action_t action, socket_blocker* blocker_) {
  assert(seastar::this_shard_id() == sid);
  blocker = blocker_;
  if (type == bp_type_t::READ) {
    ceph_assert_always(next_trap_read == bp_action_t::CONTINUE);
    next_trap_read = action;
  } else { // type == bp_type_t::WRITE
    if (next_trap_write == bp_action_t::CONTINUE) {
      next_trap_write = action;
    } else if (next_trap_write == bp_action_t::FAULT) {
      // do_sweep_messages() may combine multiple write events into one socket write
      ceph_assert_always(action == bp_action_t::FAULT || action == bp_action_t::CONTINUE);
    } else {
      ceph_abort();
    }
  }
}

seastar::future<>
Socket::try_trap_pre(bp_action_t& trap) {
  auto action = trap;
  trap = bp_action_t::CONTINUE;
  switch (action) {
   case bp_action_t::CONTINUE:
    break;
   case bp_action_t::FAULT:
    logger().info("[Test] got FAULT");
    throw std::system_error(make_error_code(error::negotiation_failure));
   case bp_action_t::BLOCK:
    logger().info("[Test] got BLOCK");
    return blocker->block();
   case bp_action_t::STALL:
    trap = action;
    break;
   default:
    ceph_abort_msg("unexpected action from trap");
  }
  return seastar::make_ready_future<>();
}

seastar::future<>
Socket::try_trap_post(bp_action_t& trap) {
  auto action = trap;
  trap = bp_action_t::CONTINUE;
  switch (action) {
   case bp_action_t::CONTINUE:
    break;
   case bp_action_t::STALL:
    logger().info("[Test] got STALL and block");
    force_shutdown();
    return blocker->block();
   default:
    ceph_abort_msg("unexpected action from trap");
  }
  return seastar::make_ready_future<>();
}
#endif

ShardedServerSocket::ShardedServerSocket(
    seastar::shard_id sid,
    bool dispatch_only_on_primary_sid,
    bool listen_on_primary_only,
    construct_tag)
  : primary_sid{sid},
    dispatch_only_on_primary_sid{dispatch_only_on_primary_sid},
    listen_on_primary_only{listen_on_primary_only}
{
}

ShardedServerSocket::~ShardedServerSocket()
{
  assert(!listener);
  // detect whether user have called destroy() properly
  ceph_assert_always(!service);
}

listen_ertr::future<>
ShardedServerSocket::listen(entity_addr_t addr,
                            std::optional<port_range_t> shard_port_range)
{
  ceph_assert_always(seastar::this_shard_id() == primary_sid);
  logger().debug("ShardedServerSocket({})::listen()...", addr);
  auto bind_on_shard = [addr](auto& ss) {
    if (ss.listener) {
      ss.listener->abort_accept();
      ss.listener.reset();
    }
    ss.listen_addr = addr;
    seastar::socket_address s_addr(addr.in4_addr());
    seastar::listen_options lo;
    lo.reuse_address = true;
    if (ss.dispatch_only_on_primary_sid) {
      lo.set_fixed_cpu(ss.primary_sid);
    }
    ss.listener = seastar::listen(s_addr, lo);
  };
  // The kernel listen socket lives on reactor 0. A per-reactor messenger
  // also registers on its own reactor so accepted connections are handed
  // there. Listening on every reactor races the other messengers and
  // exhausts ms_bind_port_min..max.
  seastar::future<> bound = seastar::now();
  if (listen_on_primary_only) {
    ceph_assert_always(dispatch_only_on_primary_sid);
    if (primary_sid == 0) {
      bound = this->container().invoke_on(0, bind_on_shard);
    } else {
      bound = seastar::when_all_succeed(
        this->container().invoke_on(0, bind_on_shard),
        this->container().invoke_on(primary_sid, bind_on_shard)
      ).discard_result();
    }
  } else {
    bound = this->container().invoke_on_all(bind_on_shard);
  }
  return bound.then([this] {
    // a retried listen() (e.g. the previous port was in use) starts over
    return this->container().invoke_on_all([](auto& ss) {
      ss.shard_listeners.clear();
      ss.shard_listener_ports.clear();
      ss.shard_ports.clear();
    });
  }).then([this, addr, shard_port_range] {
    // never fails: a reactor without a port is served via the main port
    return listen_shard_ports(addr, shard_port_range);
  }).then([] {
    return listen_ertr::now();
  }).handle_exception_type(
    [addr](const std::system_error& e) -> listen_ertr::future<> {
    if (e.code() == std::errc::address_in_use) {
      logger().debug("ShardedServerSocket({})::listen(): address in use", addr);
      return crimson::ct_error::address_in_use::make();
    } else if (e.code() == std::errc::address_not_available) {
      logger().debug("ShardedServerSocket({})::listen(): address not available",
                     addr);
      return crimson::ct_error::address_not_available::make();
    }
    logger().error("ShardedServerSocket({})::listen(): "
                   "got unexpeted error {}", addr, e.what());
    ceph_abort();
  });
}

std::vector<uint16_t>
ShardedServerSocket::bind_shard_ports(const entity_addr_t& addr,
                                      uint32_t min_port, uint32_t max_port)
{
  // shard 0: the posix stack creates kernel listen sockets only here
  ceph_assert_always(seastar::this_shard_id() == 0);
  const unsigned n = seastar::this_smp_shard_count();
  std::vector<uint16_t> ports(n, 0);
  uint32_t port = min_port;
  for (unsigned k = 0; k < n; ++k) {
    for (; port <= max_port && port <= 65535; ++port) {
      if (port == static_cast<uint32_t>(addr.get_port())) {
        continue;
      }
      auto shard_addr = addr;
      shard_addr.set_port(port);
      seastar::listen_options slo;
      slo.reuse_address = true;
      slo.set_fixed_cpu(k);
      try {
        shard_listeners.push_back(
          seastar::listen(seastar::socket_address(shard_addr.in4_addr()), slo));
        shard_listener_ports.push_back(port);
      } catch (const std::system_error& e) {
        if (e.code() == std::errc::address_in_use) {
          continue;   // taken (another daemon, or our cluster messenger)
        }
        // the address itself is unusable: every other port would fail too
        logger().error("ShardedServerSocket({})::listen(): reactor {} port {} "
                       "failed: {}; no further reactor ports", addr, k, port,
                       e.what());
        return ports;
      }
      ports[k] = port++;
      logger().info("ShardedServerSocket({})::listen(): reactor {} port {}",
                    addr, k, ports[k]);
      break;
    }
    if (ports[k] == 0) {
      // not fatal: clients use the main port for this reactor's PGs
      logger().error("ShardedServerSocket({})::listen(): no free port in "
                     "[{}, {}] for reactor {}", addr, min_port, max_port, k);
    }
  }
  return ports;
}

seastar::future<>
ShardedServerSocket::listen_shard_ports(entity_addr_t addr,
                                        std::optional<port_range_t> range)
{
  if (!range || dispatch_only_on_primary_sid) {
    return seastar::now();
  }
  return this->container().invoke_on(0, [addr, range=*range](auto& ss) {
    return ss.bind_shard_ports(addr, range.first, range.second);
  }).then([this, addr](std::vector<uint16_t> ports) {
    // each reactor opens its accept endpoint and reports whether it could
    return this->container().map([addr, ports](auto& ss) {
      return ss.listen_shard_endpoint(addr, ports);
    }).then([this, addr, ports](std::vector<bool> ok) mutable {
      // a reactor without an endpoint: shard 0 must not keep accepting for
      // it (fixed_cpu(k) would hand connections to a reactor that drops
      // them), and nobody may publish its port
      std::vector<uint16_t> failed;
      for (size_t k = 0; k < ports.size(); ++k) {
        if (ports[k] != 0 && !ok[k]) {
          failed.push_back(ports[k]);
          ports[k] = 0;
        }
      }
      return this->container().invoke_on(0, [failed](auto& ss) {
        for (auto port : failed) {
          ss.close_shard_listener(port);
        }
      }).then([this, ports] {
        return this->container().invoke_on_all([ports](auto& ss) {
          ss.shard_ports = ports;
        });
      });
    });
  });
}

bool ShardedServerSocket::listen_shard_endpoint(
  const entity_addr_t& addr, const std::vector<uint16_t>& ports)
{
  const auto k = seastar::this_shard_id();
  if (k == 0 || k >= ports.size() || ports[k] == 0) {
    return true;   // shard 0 already holds its kernel sockets
  }
  auto shard_addr = addr;
  shard_addr.set_port(ports[k]);
  seastar::listen_options slo;
  slo.reuse_address = true;
  slo.set_fixed_cpu(k);
  try {
    shard_listeners.push_back(
      seastar::listen(seastar::socket_address(shard_addr.in4_addr()), slo));
    shard_listener_ports.push_back(ports[k]);
    return true;
  } catch (const std::system_error& e) {
    // unexpected (the kernel socket is bound): the port is withdrawn
    logger().error("ShardedServerSocket({})::listen(): reactor {} "
                   "endpoint on port {} failed: {}; not publishing it",
                   addr, k, ports[k], e.what());
    return false;
  }
}

void ShardedServerSocket::close_shard_listener(uint16_t port)
{
  for (size_t i = 0; i < shard_listener_ports.size(); ++i) {
    if (shard_listener_ports[i] == port) {
      shard_listeners[i].abort_accept();
      shard_listeners.erase(shard_listeners.begin() + i);
      shard_listener_ports.erase(shard_listener_ports.begin() + i);
      return;
    }
  }
}

void ShardedServerSocket::arm_accept_loop(seastar::server_socket& sock)
{
  // gate accepting
  // shutdown_destroy() will drain the continuations in the gate,
  // so ignore the returned future
  std::ignore = seastar::with_gate(shutdown_gate, [this, &sock] {
    return seastar::keep_doing([this, &sock] {
      return sock.accept(
      ).then([this](seastar::accept_result accept_result) {
#ifndef NDEBUG
        if (dispatch_only_on_primary_sid) {
          // see seastar::listen_options::set_fixed_cpu()
          ceph_assert_always(seastar::this_shard_id() == primary_sid);
        }
#endif
        auto [socket, paddr] = std::move(accept_result);
        entity_addr_t peer_addr;
        peer_addr.set_sockaddr(&paddr.as_posix_sockaddr());
        peer_addr.set_type(listen_addr.get_type());
        SocketRef _socket = std::make_unique<Socket>(
            std::move(socket), Socket::side_t::acceptor,
            peer_addr.get_port(), Socket::construct_tag{});
        logger().debug("ShardedServerSocket({})::accept(): accepted peer {}, "
                       "socket {}, dispatch_only_on_primary_sid = {}",
                       listen_addr, peer_addr, fmt::ptr(_socket.get()),
                       dispatch_only_on_primary_sid);
        std::ignore = seastar::with_gate(
            shutdown_gate,
            [socket=std::move(_socket), peer_addr, this]() mutable {
          return fn_accept(std::move(socket), peer_addr
          ).handle_exception([this, peer_addr](auto eptr) {
            const char *e_what;
            try {
              std::rethrow_exception(eptr);
            } catch (std::exception &e) {
              e_what = e.what();
            }
            logger().error("ShardedServerSocket({})::accept(): "
                           "fn_accept(s, {}) got unexpected exception {}",
                           listen_addr, peer_addr, e_what);
            ceph_abort();
          });
        });
      });
    }).handle_exception_type([this](const std::system_error& e) {
      if (e.code() == std::errc::connection_aborted ||
          e.code() == std::errc::invalid_argument) {
        logger().debug("ShardedServerSocket({})::accept(): stopped ({})",
                       listen_addr, e.what());
      } else {
        throw;
      }
    }).handle_exception([this](auto eptr) {
      const char *e_what;
      try {
        std::rethrow_exception(eptr);
      } catch (std::exception &e) {
        e_what = e.what();
      }
      logger().error("ShardedServerSocket({})::accept(): "
                     "got unexpected exception {}", listen_addr, e_what);
      ceph_abort();
    });
  });
}

seastar::future<>
ShardedServerSocket::accept(accept_func_t &&_fn_accept)
{
  ceph_assert_always(seastar::this_shard_id() == primary_sid);
  logger().debug("ShardedServerSocket({})::accept()...", listen_addr);
  return this->container().invoke_on_all([_fn_accept](auto &ss) {
    if (ss.listen_on_primary_only &&
        seastar::this_shard_id() != 0 &&
        seastar::this_shard_id() != ss.primary_sid) {
      return;
    }
    assert(ss.listener);
    ss.fn_accept = _fn_accept;
    ss.arm_accept_loop(*ss.listener);
    for (auto& sock : ss.shard_listeners) {
      ss.arm_accept_loop(sock);
    }
  });
}

seastar::future<>
ShardedServerSocket::shutdown_destroy()
{
  assert(seastar::this_shard_id() == primary_sid);
  logger().debug("ShardedServerSocket({})::shutdown_destroy()...", listen_addr);
  // shutdown shards
  return this->container().invoke_on_all([](auto& ss) {
    if (ss.listener) {
      ss.listener->abort_accept();
    }
    for (auto& sock : ss.shard_listeners) {
      sock.abort_accept();
    }
    return ss.shutdown_gate.close();
  }).then([this] {
    // destroy shards
    return this->container().invoke_on_all([](auto& ss) {
      assert(ss.shutdown_gate.is_closed());
      ss.listen_addr = entity_addr_t();
      ss.listener.reset();
      ss.shard_listeners.clear();
      ss.shard_listener_ports.clear();
      ss.shard_ports.clear();
    });
  }).then([this] {
    // stop the sharded service: we should only construct/stop shards on #0
    return this->container().invoke_on(0, [](auto& ss) {
      assert(ss.service);
      return ss.service->stop().finally([cleanup = std::move(ss.service)] {});
    });
  });
}

seastar::future<ShardedServerSocket*>
ShardedServerSocket::create(bool dispatch_only_on_this_shard,
                             bool listen_on_primary_only)
{
  auto primary_sid = seastar::this_shard_id();
  // start the sharded service: we should only construct/stop shards on #0
  return seastar::smp::submit_to(0, [primary_sid, dispatch_only_on_this_shard,
                                     listen_on_primary_only] {
    auto service = std::make_unique<sharded_service_t>();
    return service->start(
        primary_sid, dispatch_only_on_this_shard, listen_on_primary_only,
        construct_tag{}
    ).then([service = std::move(service)]() mutable {
      auto p_shard = service.get();
      p_shard->local().service = std::move(service);
      return p_shard;
    });
  }).then([](auto p_shard) {
    return &p_shard->local();
  });
}

} // namespace crimson::net
