// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Socket.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>

#include "crimson/common/log.h"
#include "Errors.h"

using crimson::common::local_conf;

namespace crimson::net {

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_ms);
}

// an input_stream consumer that reads buffer segments into a bufferlist up to
// the given number of remaining bytes
struct bufferlist_consumer {
  bufferlist& bl;
  size_t& remaining;

  bufferlist_consumer(bufferlist& bl, size_t& remaining)
    : bl(bl), remaining(remaining) {}

  using tmp_buf = seastar::temporary_buffer<char>;
  using consumption_result_type = typename seastar::input_stream<char>::consumption_result_type;

  // consume some or all of a buffer segment
  seastar::future<consumption_result_type> operator()(tmp_buf&& data) {
    if (remaining >= data.size()) {
      // consume the whole buffer
      remaining -= data.size();
      bl.append(buffer::create_foreign(std::move(data)));
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
      bl.append(buffer::create_foreign(data.share(0, remaining)));
      data.trim_front(remaining);
      remaining = 0;
    }
    // give the rest back to signal that we're done
    return seastar::make_ready_future<consumption_result_type>(
        consumption_result_type::stop_consuming_type{std::move(data)});
  };
};

} // anonymous namespace

seastar::future<bufferlist> Socket::read(size_t bytes)
{
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
  }).then([this] (auto buf) {
    return try_trap_post(next_trap_read
    ).then([buf = std::move(buf)] () mutable {
      return std::move(buf);
    });
  });
#endif
}

seastar::future<seastar::temporary_buffer<char>>
Socket::read_exactly(size_t bytes) {
#ifdef UNIT_TESTS_BUILT
  return try_trap_pre(next_trap_read).then([bytes, this] {
#endif
    if (bytes == 0) {
      return seastar::make_ready_future<seastar::temporary_buffer<char>>();
    }
    return in.read_exactly(bytes).then([bytes](auto buf) {
      if (buf.size() < bytes) {
        throw std::system_error(make_error_code(error::read_eof));
      }
      inject_failure();
      return inject_delay(
      ).then([buf = std::move(buf)] () mutable {
        return seastar::make_ready_future<tmp_buf>(std::move(buf));
      });
    });
#ifdef UNIT_TESTS_BUILT
  }).then([this] (auto buf) {
    return try_trap_post(next_trap_read
    ).then([buf = std::move(buf)] () mutable {
      return std::move(buf);
    });
  });
#endif
}

void Socket::shutdown() {
  socket.shutdown_input();
  socket.shutdown_output();
}

static inline seastar::future<>
close_and_handle_errors(seastar::output_stream<char>& out)
{
  return out.close().handle_exception_type([] (const std::system_error& e) {
    if (e.code() != std::errc::broken_pipe &&
        e.code() != std::errc::connection_reset) {
      logger().error("Socket::close(): unexpected error {}", e);
      ceph_abort();
    }
    // can happen when out is already shutdown, ignore
  });
}

seastar::future<> Socket::close() {
#ifndef NDEBUG
  ceph_assert(!closed);
  closed = true;
#endif
  return seastar::when_all_succeed(
    inject_delay(),
    in.close(),
    close_and_handle_errors(out)
  ).then_unpack([] {
    return seastar::make_ready_future<>();
  }).handle_exception([] (auto eptr) {
    logger().error("Socket::close(): unexpected exception {}", eptr);
    ceph_abort();
  });
}

seastar::future<> Socket::inject_delay () {
  if (float delay_period = local_conf()->ms_inject_internal_delays;
      delay_period) {
    logger().debug("{}: sleep for {}",
                  __func__,
                  delay_period);
    return seastar::sleep(
      std::chrono::milliseconds((int)(delay_period * 1000.0)));
  }
  return seastar::now();
}

void Socket::inject_failure()
{
  if (local_conf()->ms_inject_socket_failures) {
    uint64_t rand =
      ceph::util::generate_random_number<uint64_t>(1, RAND_MAX);
      if (rand % local_conf()->ms_inject_socket_failures == 0) {
      if (true) {
        logger().warn("{} injecting socket failure", __func__);
	throw std::system_error(make_error_code(
	  crimson::net::error::negotiation_failure));
      }
    }
  }
}

#ifdef UNIT_TESTS_BUILT
seastar::future<> Socket::try_trap_pre(bp_action_t& trap) {
  auto action = trap;
  trap = bp_action_t::CONTINUE;
  switch (action) {
   case bp_action_t::CONTINUE:
    break;
   case bp_action_t::FAULT:
    logger().info("[Test] got FAULT");
    throw std::system_error(make_error_code(crimson::net::error::negotiation_failure));
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

seastar::future<> Socket::try_trap_post(bp_action_t& trap) {
  auto action = trap;
  trap = bp_action_t::CONTINUE;
  switch (action) {
   case bp_action_t::CONTINUE:
    break;
   case bp_action_t::STALL:
    logger().info("[Test] got STALL and block");
    shutdown();
    return blocker->block();
   default:
    ceph_abort("unexpected action from trap");
  }
  return seastar::make_ready_future<>();
}

void Socket::set_trap(bp_type_t type, bp_action_t action, socket_blocker* blocker_) {
  blocker = blocker_;
  if (type == bp_type_t::READ) {
    ceph_assert(next_trap_read == bp_action_t::CONTINUE);
    next_trap_read = action;
  } else { // type == bp_type_t::WRITE
    if (next_trap_write == bp_action_t::CONTINUE) {
      next_trap_write = action;
    } else if (next_trap_write == bp_action_t::FAULT) {
      // do_sweep_messages() may combine multiple write events into one socket write
      ceph_assert(action == bp_action_t::FAULT || action == bp_action_t::CONTINUE);
    } else {
      ceph_abort();
    }
  }
}
#endif

crimson::net::listen_ertr::future<>
FixedCPUServerSocket::listen(entity_addr_t addr)
{
  assert(seastar::this_shard_id() == cpu);
  logger().trace("FixedCPUServerSocket::listen({})...", addr);
  return container().invoke_on_all([addr] (auto& ss) {
    ss.addr = addr;
    seastar::socket_address s_addr(addr.in4_addr());
    seastar::listen_options lo;
    lo.reuse_address = true;
    lo.set_fixed_cpu(ss.cpu);
    ss.listener = seastar::listen(s_addr, lo);
  }).then([] {
    return listen_ertr::now();
  }).handle_exception_type(
    [addr] (const std::system_error& e) -> listen_ertr::future<> {
    if (e.code() == std::errc::address_in_use) {
      logger().trace("FixedCPUServerSocket::listen({}): address in use", addr);
      return crimson::ct_error::address_in_use::make();
    } else if (e.code() == std::errc::address_not_available) {
      logger().trace("FixedCPUServerSocket::listen({}): address not available",
                     addr);
      return crimson::ct_error::address_not_available::make();
    }
    logger().error("FixedCPUServerSocket::listen({}): "
                   "got unexpeted error {}", addr, e);
    ceph_abort();
  });
}

seastar::future<> FixedCPUServerSocket::shutdown()
{
  assert(seastar::this_shard_id() == cpu);
  logger().trace("FixedCPUServerSocket({})::shutdown()...", addr);
  return container().invoke_on_all([] (auto& ss) {
    if (ss.listener) {
      ss.listener->abort_accept();
    }
    return ss.shutdown_gate.close();
  }).then([this] {
    return reset();
  });
}

seastar::future<> FixedCPUServerSocket::destroy()
{
  assert(seastar::this_shard_id() == cpu);
  return shutdown().then([this] {
    // we should only construct/stop shards on #0
    return container().invoke_on(0, [] (auto& ss) {
      assert(ss.service);
      return ss.service->stop().finally([cleanup = std::move(ss.service)] {});
    });
  });
}

seastar::future<FixedCPUServerSocket*> FixedCPUServerSocket::create()
{
  auto cpu = seastar::this_shard_id();
  // we should only construct/stop shards on #0
  return seastar::smp::submit_to(0, [cpu] {
    auto service = std::make_unique<sharded_service_t>();
    return service->start(cpu, construct_tag{}
    ).then([service = std::move(service)] () mutable {
      auto p_shard = service.get();
      p_shard->local().service = std::move(service);
      return p_shard;
    });
  }).then([] (auto p_shard) {
    return &p_shard->local();
  });
}

} // namespace crimson::net
