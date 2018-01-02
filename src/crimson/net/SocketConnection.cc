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

#include <algorithm>
#include <core/shared_future.hh>

#include "SocketConnection.h"

#include "include/msgr.h"
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
    send_ready(h.promise.get_future())
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

void SocketConnection::read_tags_until_next_message()
{
  seastar::repeat([this] {
      // read the next tag
      return in.read_exactly(1)
        .then([this] (auto buf) {
          if (buf.empty()) {
            throw std::system_error(make_error_code(error::read_eof));
          }
          switch (buf[0]) {
          case CEPH_MSGR_TAG_MSG:
            // stop looping and notify read_header()
            return seastar::make_ready_future<seastar::stop_iteration>(
                seastar::stop_iteration::yes);

          case CEPH_MSGR_TAG_ACK:
            return in.read_exactly(sizeof(ceph_le64))
              .then([] (auto buf) {
                auto seq = reinterpret_cast<const ceph_le64*>(buf.get());
                std::cout << "ack " << *seq << std::endl;
                return seastar::stop_iteration::no;
              });

          case CEPH_MSGR_TAG_KEEPALIVE:
            break;

          case CEPH_MSGR_TAG_KEEPALIVE2:
            return in.read_exactly(sizeof(ceph_timespec))
              .then([] (auto buf) {
                auto t = reinterpret_cast<const ceph_timespec*>(buf.get());
                std::cout << "keepalive2 " << t->tv_sec << std::endl;
                // TODO: schedule ack
                return seastar::stop_iteration::no;
              });

          case CEPH_MSGR_TAG_KEEPALIVE2_ACK:
            return in.read_exactly(sizeof(ceph_timespec))
              .then([] (auto buf) {
                auto t = reinterpret_cast<const ceph_timespec*>(buf.get());
                std::cout << "keepalive2 ack " << t->tv_sec << std::endl;
                return seastar::stop_iteration::no;
              });

          case CEPH_MSGR_TAG_CLOSE:
            std::cout << "close" << std::endl;
            break;
          }
          return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::no);
        });
    }).then_wrapped([this] (auto fut) {
      // satisfy the message promise
      fut.forward_to(std::move(on_message));
      on_message = seastar::promise<>{};
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
      // resume background processing of tags
      read_tags_until_next_message();

      auto p = bl.cbegin();
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
  unsigned char tag = CEPH_MSGR_TAG_MSG;
  encode(tag, bl);
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

// handshake

/// store the banner in a non-const string for buffer::create_static()
static char banner[] = CEPH_BANNER;
constexpr size_t banner_size = sizeof(CEPH_BANNER)-1;

constexpr size_t client_header_size = banner_size + sizeof(ceph_entity_addr);
constexpr size_t server_header_size = banner_size + 2 * sizeof(ceph_entity_addr);

WRITE_RAW_ENCODER(ceph_msg_connect);
WRITE_RAW_ENCODER(ceph_msg_connect_reply);

std::ostream& operator<<(std::ostream& out, const ceph_msg_connect& c)
{
  return out << "connect{features=" << std::hex << c.features << std::dec
      << " host_type=" << c.host_type
      << " global_seq=" << c.global_seq
      << " connect_seq=" << c.connect_seq
      << " protocol_version=" << c.protocol_version
      << " authorizer_protocol=" << c.authorizer_protocol
      << " authorizer_len=" << c.authorizer_len
      << " flags=" << std::hex << static_cast<uint16_t>(c.flags) << std::dec << '}';
}

std::ostream& operator<<(std::ostream& out, const ceph_msg_connect_reply& r)
{
  return out << "connect_reply{tag=" << static_cast<uint16_t>(r.tag)
      << " features=" << std::hex << r.features << std::dec
      << " global_seq=" << r.global_seq
      << " connect_seq=" << r.connect_seq
      << " protocol_version=" << r.protocol_version
      << " authorizer_len=" << r.authorizer_len
      << " flags=" << std::hex << static_cast<uint16_t>(r.flags) << std::dec << '}';
}

// check that the buffer starts with a valid banner without requiring it to
// be contiguous in memory
static void validate_banner(bufferlist::const_iterator& p)
{
  auto b = std::cbegin(banner);
  auto end = b + banner_size;
  while (b != end) {
    const char *buf{nullptr};
    auto remaining = std::distance(b, end);
    auto len = p.get_ptr_and_advance(remaining, &buf);
    if (!std::equal(buf, buf + len, b)) {
      throw std::system_error(make_error_code(error::bad_connect_banner));
    }
    b += len;
  }
}

// make sure that we agree with the peer about its address
static void validate_peer_addr(const entity_addr_t& addr,
                               const entity_addr_t& expected)
{
  if (addr == expected) {
    return;
  }
  // ok if server bound anonymously, as long as port/nonce match
  if (addr.is_blank_ip() &&
      addr.get_port() == expected.get_port() &&
      addr.get_nonce() == expected.get_nonce()) {
    return;
  } else {
    throw std::system_error(make_error_code(error::bad_peer_address));
  }
}

/// return a static bufferptr to the given object
template <typename T>
bufferptr create_static(T& obj)
{
  return buffer::create_static(sizeof(obj), reinterpret_cast<char*>(&obj));
}

seastar::future<> SocketConnection::handle_connect()
{
  memset(&h.reply, 0, sizeof(h.reply));

  h.reply.protocol_version = CEPH_OSDC_PROTOCOL;
  h.reply.tag = CEPH_MSGR_TAG_READY;

  bufferlist bl;
  bl.append(create_static(h.reply));

  return out.write(std::move(bl))
    .then([this] { return out.flush(); });
}

seastar::future<> SocketConnection::handle_connect_reply()
{
  if (h.reply.tag != CEPH_MSGR_TAG_READY) {
    throw std::system_error(make_error_code(error::negotiation_failure));
  }
  return seastar::now();
}

seastar::future<> SocketConnection::client_handshake()
{
  // read server's handshake header
  return read(server_header_size)
    .then([this] (bufferlist headerbl) {
      auto p = headerbl.cbegin();
      validate_banner(p);
      entity_addr_t saddr, caddr;
      ::decode(saddr, p);
      ::decode(caddr, p);
      assert(p.end());
      validate_peer_addr(saddr, peer_addr);

      if (my_addr != caddr) {
        // take peer's address for me, but preserve my port/nonce
        caddr.set_port(my_addr.get_port());
        caddr.nonce = my_addr.nonce;
        my_addr = caddr;
      }
      // encode/send client's handshake header
      bufferlist bl;
      bl.append(buffer::create_static(banner_size, banner));
      ::encode(my_addr, bl, 0);

      // encode ceph_msg_connect
      memset(&h.connect, 0, sizeof(h.connect));
      h.connect.protocol_version = CEPH_OSDC_PROTOCOL;
      bl.append(create_static(h.connect));

      // TODO: append authorizer
      return out.write(std::move(bl))
        .then([this] { return out.flush(); });
    }).then([this] {
      // read the reply
      return read(sizeof(h.reply));
    }).then([this] (bufferlist bl) {
      auto p = bl.begin();
      ::decode(h.reply, p);
      // TODO: read authorizer
      assert(p.end());
      return handle_connect_reply();
    }).then([this] {
      // start background processing of tags
      read_tags_until_next_message();
    }).then_wrapped([this] (auto fut) {
      // satisfy the handshake's promise
      fut.forward_to(std::move(h.promise));
    });
}

seastar::future<> SocketConnection::server_handshake()
{
  // encode/send server's handshake header
  bufferlist bl;
  bl.append(buffer::create_static(banner_size, banner));
  ::encode(my_addr, bl, 0);
  ::encode(peer_addr, bl, 0);
  return out.write(std::move(bl))
    .then([this] { return out.flush(); })
    .then([this] {
      // read client's handshake header and connect request
      return read(client_header_size + sizeof(h.connect));
    }).then([this] (bufferlist bl) {
      auto p = bl.cbegin();
      validate_banner(p);
      entity_addr_t addr;
      ::decode(addr, p);
      ::decode(h.connect, p);
      assert(p.end());
      // TODO: read authorizer

      return handle_connect();
    }).then([this] {
      // start background processing of tags
      read_tags_until_next_message();
    }).then_wrapped([this] (auto fut) {
      // satisfy the handshake's promise
      fut.forward_to(std::move(h.promise));
    });
}
