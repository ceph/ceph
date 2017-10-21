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

#include <core/shared_future.hh>

#include "SocketConnection.h"

#include "msg/Message.h"

using namespace ceph::net;

SocketConnection::SocketConnection(Messenger *messenger,
                                   const entity_addr_t& my_addr,
                                   const entity_addr_t& peer_addr,
                                   seastar::connected_socket&& fd)
  : Connection(messenger, my_addr, peer_addr),
    socket(std::move(fd)),
    in(socket.input()),
    out(socket.output()),
    send_ready(seastar::now())
{
}

SocketConnection::~SocketConnection()
{
  // errors were reported to callers of send()
  assert(send_ready.available());
  send_ready.ignore_ready_future();
}

bool SocketConnection::is_connected()
{
  return !send_ready.failed();
}

// an input_stream consumer that reads buffer segments into a bufferlist up to
// the given number of remaining bytes
struct bufferlist_consumer {
  bufferlist& bl;
  size_t& remaining;

  bufferlist_consumer(bufferlist& bl, size_t& remaining)
    : bl(bl), remaining(remaining) {}

  using tmp_buf = seastar::temporary_buffer<char>;
  using unconsumed_remainder = std::experimental::optional<tmp_buf>;

  // consume some or all of a buffer segment
  seastar::future<unconsumed_remainder> operator()(tmp_buf&& data) {
    if (remaining >= data.size()) {
      // consume the whole buffer
      remaining -= data.size();
      bl.append(buffer::create_foreign(std::move(data)));
      if (remaining > 0) {
        // return none to request more segments
        return seastar::make_ready_future<unconsumed_remainder>();
      }
      // return an empty buffer to singal that we're done
      return seastar::make_ready_future<unconsumed_remainder>(tmp_buf{});
    }
    if (remaining > 0) {
      // consume the front
      bl.append(buffer::create_foreign(data.share(0, remaining)));
      data.trim_front(remaining);
      remaining = 0;
    }
    // give the rest back to signal that we're done
    return seastar::make_ready_future<unconsumed_remainder>(std::move(data));
  };
};

seastar::future<bufferlist> SocketConnection::read(size_t bytes)
{
  r.buffer.clear();
  r.remaining = bytes;
  return in.consume(bufferlist_consumer{r.buffer, r.remaining})
    .then([this] {
      if (r.remaining) { // throw on short reads
        throw std::system_error(make_error_code(error::read_eof));
      }
      return seastar::make_ready_future<bufferlist>(std::move(r.buffer));
    });
}

seastar::future<MessageRef> SocketConnection::read_message()
{
  return on_message.get_future()
    .then([this] {
      // read header
      return read(sizeof(m.header));
    }).then([this] (bufferlist bl) {
      auto p = bl.cbegin();
      ::decode(m.header, p);
    }).then([this] {
      // read front
      return read(m.header.front_len);
    }).then([this] (bufferlist bl) {
      m.front = std::move(bl);
      // read middle
      return read(m.header.middle_len);
    }).then([this] (bufferlist bl) {
      m.middle = std::move(bl);
      // read data
      return read(m.header.data_len);
    }).then([this] (bufferlist bl) {
      m.data = std::move(bl);
      // read footer
      return read(sizeof(m.footer));
    }).then([this] (bufferlist bl) {
      auto p = bl.begin();
      ::decode(m.footer, p);

      auto msg = ::decode_message(nullptr, 0, m.header, m.footer,
                                  m.front, m.middle, m.data, nullptr);
      constexpr bool add_ref = false; // Message starts with 1 ref
      return MessageRef{msg, add_ref};
    });
}

seastar::future<> SocketConnection::write_message(MessageRef msg)
{
  bufferlist bl;
  encode_message(msg.get(), 0, bl);
  // write as a seastar::net::packet
  return out.write(std::move(bl))
    .then([this] { return out.flush(); });
}

seastar::future<> SocketConnection::send(MessageRef msg)
{
  // chain the message after the last message is sent
  seastar::shared_future<> f = send_ready.then(
    [this, msg = std::move(msg)] {
      return write_message(std::move(msg));
    });

  // chain any later messages after this one completes
  send_ready = f.get_future();
  // allow the caller to wait on the same future
  return f.get_future();
}

seastar::future<> SocketConnection::close()
{
  return seastar::when_all(in.close(), out.close()).discard_result();
}

seastar::future<> SocketConnection::client_handshake()
{
  return seastar::now(); // TODO
}

seastar::future<> SocketConnection::server_handshake()
{
  return seastar::now(); // TODO
}
