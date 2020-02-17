// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/net/packet.hh>

#include "include/buffer.h"
#include "msg/msg_types.h"

#ifdef UNIT_TESTS_BUILT
#include "Interceptor.h"
#endif

namespace crimson::net {

class Socket;
using SocketFRef = seastar::foreign_ptr<std::unique_ptr<Socket>>;

class Socket
{
  const seastar::shard_id sid;
  seastar::connected_socket socket;
  seastar::input_stream<char> in;
  seastar::output_stream<char> out;

#ifndef NDEBUG
  bool closed = false;
#endif

  /// buffer state for read()
  struct {
    bufferlist buffer;
    size_t remaining;
  } r;

  struct construct_tag {};

 public:
  Socket(seastar::connected_socket&& _socket, construct_tag)
    : sid{seastar::engine().cpu_id()},
      socket(std::move(_socket)),
      in(socket.input()),
      // the default buffer size 8192 is too small that may impact our write
      // performance. see seastar::net::connected_socket::output()
      out(socket.output(65536)) {}

  ~Socket() {
#ifndef NDEBUG
    assert(closed);
#endif
  }

  Socket(Socket&& o) = delete;

  static seastar::future<SocketFRef>
  connect(const entity_addr_t& peer_addr) {
    return seastar::connect(peer_addr.in4_addr())
      .then([] (seastar::connected_socket socket) {
        return seastar::make_foreign(std::make_unique<Socket>(std::move(socket),
							      construct_tag{}));
      });
  }

  static seastar::future<SocketFRef, entity_addr_t>
  accept(seastar::server_socket& listener) {
    return listener.accept().then([] (seastar::accept_result accept_result) {
        auto [socket, paddr] = std::move(accept_result);
        entity_addr_t peer_addr;
        peer_addr.set_sockaddr(&paddr.as_posix_sockaddr());
        peer_addr.set_type(entity_addr_t::TYPE_ANY);
        return seastar::make_ready_future<SocketFRef, entity_addr_t>(
          seastar::make_foreign(std::make_unique<Socket>(std::move(socket),
							 construct_tag{})),
	  peer_addr);
      });
  }

  /// read the requested number of bytes into a bufferlist
  seastar::future<bufferlist> read(size_t bytes);
  using tmp_buf = seastar::temporary_buffer<char>;
  using packet = seastar::net::packet;
  seastar::future<tmp_buf> read_exactly(size_t bytes);

  seastar::future<> write(packet&& buf) {
#ifdef UNIT_TESTS_BUILT
    return try_trap_pre(next_trap_write).then([buf = std::move(buf), this] () mutable {
#endif
      return out.write(std::move(buf));
#ifdef UNIT_TESTS_BUILT
    }).then([this] {
      return try_trap_post(next_trap_write);
    });
#endif
  }
  seastar::future<> flush() {
    return out.flush();
  }
  seastar::future<> write_flush(packet&& buf) {
#ifdef UNIT_TESTS_BUILT
    return try_trap_pre(next_trap_write).then([buf = std::move(buf), this] () mutable {
#endif
      return out.write(std::move(buf)).then([this] { return out.flush(); });
#ifdef UNIT_TESTS_BUILT
    }).then([this] {
      return try_trap_post(next_trap_write);
    });
#endif
  }

  // preemptively disable further reads or writes, can only be shutdown once.
  void shutdown();

  /// Socket can only be closed once.
  seastar::future<> close();

  // shutdown input_stream only, for tests
  void force_shutdown_in() {
    socket.shutdown_input();
  }

  // shutdown output_stream only, for tests
  void force_shutdown_out() {
    socket.shutdown_output();
  }

#ifdef UNIT_TESTS_BUILT
 private:
  bp_action_t next_trap_read = bp_action_t::CONTINUE;
  bp_action_t next_trap_write = bp_action_t::CONTINUE;
  socket_blocker* blocker = nullptr;
  seastar::future<> try_trap_pre(bp_action_t& trap);
  seastar::future<> try_trap_post(bp_action_t& trap);

 public:
  void set_trap(bp_type_t type, bp_action_t action, socket_blocker* blocker_);
#endif
};

} // namespace crimson::net
