// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Socket.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/packet.hh>

#include "crimson/common/log.h"
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
      return out.write(std::move(p));
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
      return out.write(std::move(p)
      ).then([this] {
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
                   peer_addr, fmt::ptr(ret));
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
    ceph_abort("unexpected action from trap");
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
    ceph_abort("unexpected action from trap");
  }
  return seastar::make_ready_future<>();
}
#endif

ShardedServerSocket::ShardedServerSocket(
    seastar::shard_id sid,
    bool dispatch_only_on_primary_sid,
    construct_tag)
  : primary_sid{sid}, dispatch_only_on_primary_sid{dispatch_only_on_primary_sid}
{
}

ShardedServerSocket::~ShardedServerSocket()
{
  assert(!listener);
  // detect whether user have called destroy() properly
  ceph_assert_always(!service);
}

listen_ertr::future<>
ShardedServerSocket::listen(entity_addr_t addr)
{
  ceph_assert_always(seastar::this_shard_id() == primary_sid);
  logger().debug("ShardedServerSocket({})::listen()...", addr);
  return this->container().invoke_on_all([addr](auto& ss) {
    ss.listen_addr = addr;
    seastar::socket_address s_addr(addr.in4_addr());
    seastar::listen_options lo;
    lo.reuse_address = true;
    if (ss.dispatch_only_on_primary_sid) {
      lo.set_fixed_cpu(ss.primary_sid);
    }
    ss.listener = seastar::listen(s_addr, lo);
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

seastar::future<>
ShardedServerSocket::accept(accept_func_t &&_fn_accept)
{
  ceph_assert_always(seastar::this_shard_id() == primary_sid);
  logger().debug("ShardedServerSocket({})::accept()...", listen_addr);
  return this->container().invoke_on_all([_fn_accept](auto &ss) {
    assert(ss.listener);
    ss.fn_accept = _fn_accept;
    // gate accepting
    // ShardedServerSocket::shutdown() will drain the continuations in the gate
    // so ignore the returned future
    std::ignore = seastar::with_gate(ss.shutdown_gate, [&ss] {
      return seastar::keep_doing([&ss] {
        return ss.listener->accept(
        ).then([&ss](seastar::accept_result accept_result) {
#ifndef NDEBUG
          if (ss.dispatch_only_on_primary_sid) {
            // see seastar::listen_options::set_fixed_cpu()
            ceph_assert_always(seastar::this_shard_id() == ss.primary_sid);
          }
#endif
          auto [socket, paddr] = std::move(accept_result);
          entity_addr_t peer_addr;
          peer_addr.set_sockaddr(&paddr.as_posix_sockaddr());
          peer_addr.set_type(ss.listen_addr.get_type());
          SocketRef _socket = std::make_unique<Socket>(
              std::move(socket), Socket::side_t::acceptor,
              peer_addr.get_port(), Socket::construct_tag{});
          logger().debug("ShardedServerSocket({})::accept(): accepted peer {}, "
                         "socket {}, dispatch_only_on_primary_sid = {}",
                         ss.listen_addr, peer_addr, fmt::ptr(_socket),
                         ss.dispatch_only_on_primary_sid);
          std::ignore = seastar::with_gate(
              ss.shutdown_gate,
              [socket=std::move(_socket), peer_addr, &ss]() mutable {
            return ss.fn_accept(std::move(socket), peer_addr
            ).handle_exception([&ss, peer_addr](auto eptr) {
              const char *e_what;
              try {
                std::rethrow_exception(eptr);
              } catch (std::exception &e) {
                e_what = e.what();
              }
              logger().error("ShardedServerSocket({})::accept(): "
                             "fn_accept(s, {}) got unexpected exception {}",
                             ss.listen_addr, peer_addr, e_what);
              ceph_abort();
            });
          });
        });
      }).handle_exception_type([&ss](const std::system_error& e) {
        if (e.code() == std::errc::connection_aborted ||
            e.code() == std::errc::invalid_argument) {
          logger().debug("ShardedServerSocket({})::accept(): stopped ({})",
                         ss.listen_addr, e.what());
        } else {
          throw;
        }
      }).handle_exception([&ss](auto eptr) {
        const char *e_what;
        try {
          std::rethrow_exception(eptr);
        } catch (std::exception &e) {
          e_what = e.what();
        }
        logger().error("ShardedServerSocket({})::accept(): "
                       "got unexpected exception {}", ss.listen_addr, e_what);
        ceph_abort();
      });
    });
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
    return ss.shutdown_gate.close();
  }).then([this] {
    // destroy shards
    return this->container().invoke_on_all([](auto& ss) {
      assert(ss.shutdown_gate.is_closed());
      ss.listen_addr = entity_addr_t();
      ss.listener.reset();
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
ShardedServerSocket::create(bool dispatch_only_on_this_shard)
{
  auto primary_sid = seastar::this_shard_id();
  // start the sharded service: we should only construct/stop shards on #0
  return seastar::smp::submit_to(0, [primary_sid, dispatch_only_on_this_shard] {
    auto service = std::make_unique<sharded_service_t>();
    return service->start(
        primary_sid, dispatch_only_on_this_shard, construct_tag{}
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
