// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/net/packet.hh>

#include "include/buffer.h"
#include "msg/msg_types.h"

namespace ceph::net {

class Socket;
using SocketFRef = seastar::foreign_ptr<std::unique_ptr<Socket>>;

class Socket : private seastar::net::input_buffer_factory
{
public:
  using read_buffer_t = seastar::temporary_buffer<char>;

private:
  const seastar::shard_id sid;
  seastar::connected_socket socket;
  seastar::input_stream<char> in;
  seastar::output_stream<char> out;

  // contiguous buffer we created basing on the hint from hint_read_chunk()
  // and passed to network stack with input_buffer_factory.
  ceph::bufferptr hinted_rxbuf;

  // sometimes Seastar is unable to fully utilize the large buffer donated
  // to it by ::create() and returns its tail. This happens when operating
  // system's procedure for reading-from-socket finishes with less data it
  // was requested to bring.
  read_buffer_t returned_rxbuf;

  // wrapping buffer. Used if read() or read_exactly() wants less than
  // S* gave us with seastar::input_stream::read().
  read_buffer_t wrapping_rxbuf;

  // reading output state for read() and read_exactly()
  struct {
    ceph::bufferlist sgl;
    read_buffer_t contiguous_buffer;
    size_t remaining;
  } r;

  // implementation of the input_buffer_factory interface
  input_buffer_factory::buffer_t create(
    seastar::compat::polymorphic_allocator<char>* const allocator) override;
  void return_unused(input_buffer_factory::buffer_t&& buf) override;

  struct construct_tag {};

 public:
  Socket(seastar::connected_socket&& _socket, construct_tag)
    : sid{seastar::engine().cpu_id()},
      socket(std::move(_socket)),
      in(socket.input(this)),
      // the default buffer size 8192 is too small that may impact our write
      // performance. see seastar::net::connected_socket::output()
      out(socket.output(65536)) {}

  Socket(Socket&& o) = delete;

  // typically when aligning multi-byte entity we still think just about
  // the base number (e.g. PAGE_SIZE) while implicitly assuming there is
  // no offset, and thus the beginning of such entity will be subject of
  // alignment. However, ProtocolV2 really wants to align in the middle
  // of bigger memory chunk (message) where the actual data starts from.
  struct alignment_t {
    size_t base = 0;
    off_t at = 0;
  };
  static constexpr alignment_t DEFAULT_ALIGNMENT{ 0, 0 };

  // for decoupling hint_read_chunk() and create(). Allows easy passing
  // the engine (CPU) boundary.
  struct read_hint_t {
    size_t further_bytes = 0;
    alignment_t alignment;
  } read_hint;

  // interface for Protocols to hint us about the size of next incoming
  // chunk we expect on-wire. For instance, in many cases it's possible
  // to determine that basing on a header we just read. This will allow
  // to tune rx buffer size for performance if particular network stack
  // implementation (we are getting ready for both DPDK and POSIX) does
  // support that.
  void hint_read_chunk(const size_t bytes, const alignment_t align) {
    read_hint = { /* .further_bytes =*/ bytes, /*.alignment =*/ align };
  }

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
    return listener.accept().then([] (seastar::connected_socket socket,
				      seastar::socket_address paddr) {
        entity_addr_t peer_addr;
        peer_addr.set_sockaddr(&paddr.as_posix_sockaddr());
        return seastar::make_ready_future<SocketFRef, entity_addr_t>(
          seastar::make_foreign(std::make_unique<Socket>(std::move(socket),
							 construct_tag{})),
	  peer_addr);
      });
  }

  /// read the requested number of bytes into a bufferlist
  // TODO: rename to read_scattered() or read_sgl()
  seastar::future<ceph::bufferlist> read(size_t bytes);
  // TODO: rename to read_contiguous()
  seastar::future<read_buffer_t> read_exactly(size_t bytes);

  using packet = seastar::net::packet;
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
        returned_rxbuf.release();
        wrapping_rxbuf.release();
        r.contiguous_buffer.release();
        r.sgl.clear();
        read_hint = { 0, { 0, 0 } };
        return seastar::when_all(
          in.close(), out.close()).discard_result();
      });
  }
};

} // namespace ceph::net
