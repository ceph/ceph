// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/net/packet.hh>

#include "include/buffer.h"

namespace ceph::net {

class Socket
{
  const seastar::shard_id sid;
  seastar::connected_socket socket;
  seastar::input_stream<char> in;
  seastar::output_stream<char> out;

  /// buffer state for read()
  struct {
    bufferlist buffer;
    size_t remaining;
  } r;

 public:
  explicit Socket(seastar::connected_socket&& _socket)
    : sid{seastar::engine().cpu_id()},
      socket(std::move(_socket)),
      in(socket.input()),
      out(socket.output()) {}
  Socket(Socket&& o) = delete;

  /// read the requested number of bytes into a bufferlist
  seastar::future<bufferlist> read(size_t bytes);
  using tmp_buf = seastar::temporary_buffer<char>;
  using packet = seastar::net::packet;
  seastar::future<tmp_buf> read_exactly(size_t bytes);

  seastar::future<> write(packet&& buf) {
    return out.write(std::move(buf));
  }
  seastar::future<> flush() {
    return out.flush();
  }
  seastar::future<> write_flush(packet&& buf) {
    return out.write(std::move(buf)).then([this] { return out.flush(); });
  }

  /// Socket can only be closed once.
  seastar::future<> close() {
    return seastar::smp::submit_to(sid, [this] {
        return seastar::when_all(
          in.close(), out.close()).discard_result();
      });
  }
};

} // namespace ceph::net
