// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corp.
 *
 * Author: Yingxin Cheng <yingxincheng@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <memory>

#include <seastar/core/reactor.hh>
#include <seastar/net/packet.hh>

#include "include/buffer.h"
#include "Errors.h"

namespace ceph::net {

class Socket
{
  seastar::connected_socket socket;
  seastar::input_stream<char> in;
  seastar::output_stream<char> out;

  bool is_closed = false;
  bool allow_destruct = false;

  /// buffer state for read()
  struct Reader {
    bufferlist buffer;
    size_t remaining;
  } r;

 public:
  explicit Socket(seastar::connected_socket&& _socket)
    : socket(std::forward<seastar::connected_socket>(_socket)),
      in(socket.input()),
      out(socket.output()) {}
  Socket(Socket&& o)
    : socket(std::move(o.socket)),
      in(std::move(o.in)),
      out(std::move(o.out)),
      r(std::move(o.r)) {
    ceph_assert(!o.is_closed);
    o.allow_destruct = true;
  }
  ~Socket() { ceph_assert(allow_destruct); }

  seastar::future<bufferlist> read(size_t bytes);
  using tmp_buf = seastar::temporary_buffer<char>;
  using packet = seastar::net::packet;
  seastar::future<tmp_buf> read_exactly(size_t bytes) {
    return in.read_exactly(bytes);
  }

  seastar::future<> write(packet&& buf) {
    return out.write(std::forward<packet>(buf));
  }
  seastar::future<> flush() {
    return out.flush();
  }
  seastar::future<> write_flush(packet&& buf) {
    return out.write(std::forward<packet>(buf)).then([this] { return out.flush(); });
  }

  seastar::future<> close() {
    ceph_assert(!is_closed);
    is_closed = true;
    return seastar::when_all(in.close(), out.close())
      .discard_result().then([this] {
        allow_destruct = true;
      });
  }
};

} // namespace ceph::net
