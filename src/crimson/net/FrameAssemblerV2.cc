// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "FrameAssemblerV2.h"

#include "Errors.h"
#include "SocketConnection.h"

#ifdef UNIT_TESTS_BUILT
#include "Interceptor.h"
#endif

using ceph::msgr::v2::FrameAssembler;
using ceph::msgr::v2::FrameError;
using ceph::msgr::v2::preamble_block_t;
using ceph::msgr::v2::segment_t;
using ceph::msgr::v2::Tag;

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_ms);
}

} // namespace anonymous

namespace crimson::net {

FrameAssemblerV2::FrameAssemblerV2(SocketConnection &_conn)
  : conn{_conn}
{}

FrameAssemblerV2::~FrameAssemblerV2()
{
  if (has_socket()) {
    std::ignore = move_socket();
  }
}

#ifdef UNIT_TESTS_BUILT
// should be consistent to intercept() in ProtocolV2.cc
void FrameAssemblerV2::intercept_frame(Tag tag, bool is_write)
{
  assert(has_socket());
  if (conn.interceptor) {
    auto type = is_write ? bp_type_t::WRITE : bp_type_t::READ;
    auto action = conn.interceptor->intercept(
        conn, Breakpoint{tag, type});
    socket->set_trap(type, action, &conn.interceptor->blocker);
  }
}
#endif

void FrameAssemblerV2::set_is_rev1(bool _is_rev1)
{
  is_rev1 = _is_rev1;
  tx_frame_asm.set_is_rev1(_is_rev1);
  rx_frame_asm.set_is_rev1(_is_rev1);
}

void FrameAssemblerV2::create_session_stream_handlers(
  const AuthConnectionMeta &auth_meta,
  bool crossed)
{
  session_stream_handlers = ceph::crypto::onwire::rxtx_t::create_handler_pair(
      nullptr, auth_meta, is_rev1, crossed);
}

void FrameAssemblerV2::reset_handlers()
{
  session_stream_handlers = { nullptr, nullptr };
  session_comp_handlers = { nullptr, nullptr };
}

FrameAssemblerV2::mover_t
FrameAssemblerV2::to_replace()
{
  assert(is_socket_valid());
  return mover_t{
      move_socket(),
      std::move(session_stream_handlers),
      std::move(session_comp_handlers)};
}

seastar::future<> FrameAssemblerV2::replace_by(FrameAssemblerV2::mover_t &&mover)
{
  record_io = false;
  rxbuf.clear();
  txbuf.clear();
  session_stream_handlers = std::move(mover.session_stream_handlers);
  session_comp_handlers = std::move(mover.session_comp_handlers);
  if (has_socket()) {
    return replace_shutdown_socket(std::move(mover.socket));
  } else {
    set_socket(std::move(mover.socket));
    return seastar::now();
  }
}

void FrameAssemblerV2::start_recording()
{
  record_io = true;
  rxbuf.clear();
  txbuf.clear();
}

FrameAssemblerV2::record_bufs_t
FrameAssemblerV2::stop_recording()
{
  ceph_assert_always(record_io == true);
  record_io = false;
  return record_bufs_t{std::move(rxbuf), std::move(txbuf)};
}

bool FrameAssemblerV2::has_socket() const
{
  assert((socket && conn.socket) || (!socket && !conn.socket));
  return socket != nullptr;
}

bool FrameAssemblerV2::is_socket_valid() const
{
  return has_socket() && !socket->is_shutdown();
}

SocketRef FrameAssemblerV2::move_socket()
{
  assert(has_socket());
  conn.set_socket(nullptr);
  return std::move(socket);
}

void FrameAssemblerV2::set_socket(SocketRef &&new_socket)
{
  assert(!has_socket());
  socket = std::move(new_socket);
  conn.set_socket(socket.get());
  assert(is_socket_valid());
}

void FrameAssemblerV2::learn_socket_ephemeral_port_as_connector(uint16_t port)
{
  assert(has_socket());
  socket->learn_ephemeral_port_as_connector(port);
}

void FrameAssemblerV2::shutdown_socket()
{
  assert(is_socket_valid());
  socket->shutdown();
}

seastar::future<> FrameAssemblerV2::replace_shutdown_socket(SocketRef &&new_socket)
{
  assert(has_socket());
  assert(socket->is_shutdown());
  auto old_socket = move_socket();
  set_socket(std::move(new_socket));
  return old_socket->close(
  ).then([sock = std::move(old_socket)] {});
}

seastar::future<> FrameAssemblerV2::close_shutdown_socket()
{
  assert(has_socket());
  assert(socket->is_shutdown());
  return socket->close();
}

seastar::future<Socket::tmp_buf>
FrameAssemblerV2::read_exactly(std::size_t bytes)
{
  assert(has_socket());
  if (unlikely(record_io)) {
    return socket->read_exactly(bytes
    ).then([this](auto bl) {
      rxbuf.append(buffer::create(bl.share()));
      return bl;
    });
  } else {
    return socket->read_exactly(bytes);
  };
}

seastar::future<ceph::bufferlist>
FrameAssemblerV2::read(std::size_t bytes)
{
  assert(has_socket());
  if (unlikely(record_io)) {
    return socket->read(bytes
    ).then([this](auto buf) {
      rxbuf.append(buf);
      return buf;
    });
  } else {
    return socket->read(bytes);
  }
}

seastar::future<>
FrameAssemblerV2::write(ceph::bufferlist &&buf)
{
  assert(has_socket());
  if (unlikely(record_io)) {
    txbuf.append(buf);
  }
  return socket->write(std::move(buf));
}

seastar::future<>
FrameAssemblerV2::flush()
{
  assert(has_socket());
  return socket->flush();
}

seastar::future<>
FrameAssemblerV2::write_flush(ceph::bufferlist &&buf)
{
  assert(has_socket());
  if (unlikely(record_io)) {
    txbuf.append(buf);
  }
  return socket->write_flush(std::move(buf));
}

seastar::future<FrameAssemblerV2::read_main_t>
FrameAssemblerV2::read_main_preamble()
{
  rx_preamble.clear();
  return read_exactly(rx_frame_asm.get_preamble_onwire_len()
  ).then([this](auto bl) {
    try {
      rx_preamble.append(buffer::create(std::move(bl)));
      const Tag tag = rx_frame_asm.disassemble_preamble(rx_preamble);
#ifdef UNIT_TESTS_BUILT
      intercept_frame(tag, false);
#endif
      return read_main_t{tag, &rx_frame_asm};
    } catch (FrameError& e) {
      logger().warn("{} read_main_preamble: {}", conn, e.what());
      throw std::system_error(make_error_code(crimson::net::error::negotiation_failure));
    }
  });
}

seastar::future<FrameAssemblerV2::read_payload_t*>
FrameAssemblerV2::read_frame_payload()
{
  rx_segments_data.clear();
  return seastar::do_until(
    [this] {
      return rx_frame_asm.get_num_segments() == rx_segments_data.size();
    },
    [this] {
      // TODO: create aligned and contiguous buffer from socket
      const size_t seg_idx = rx_segments_data.size();
      if (uint16_t alignment = rx_frame_asm.get_segment_align(seg_idx);
          alignment != segment_t::DEFAULT_ALIGNMENT) {
        logger().trace("{} cannot allocate {} aligned buffer at segment desc index {}",
                       conn, alignment, rx_segments_data.size());
      }
      uint32_t onwire_len = rx_frame_asm.get_segment_onwire_len(seg_idx);
      // TODO: create aligned and contiguous buffer from socket
      return read_exactly(onwire_len
      ).then([this](auto tmp_bl) {
        logger().trace("{} RECV({}) frame segment[{}]",
                       conn, tmp_bl.size(), rx_segments_data.size());
        bufferlist segment;
        segment.append(buffer::create(std::move(tmp_bl)));
        rx_segments_data.emplace_back(std::move(segment));
      });
    }
  ).then([this] {
    return read_exactly(rx_frame_asm.get_epilogue_onwire_len());
  }).then([this](auto bl) {
    logger().trace("{} RECV({}) frame epilogue", conn, bl.size());
    bool ok = false;
    try {
      bufferlist rx_epilogue;
      rx_epilogue.append(buffer::create(std::move(bl)));
      ok = rx_frame_asm.disassemble_segments(rx_preamble, rx_segments_data.data(), rx_epilogue);
    } catch (FrameError& e) {
      logger().error("read_frame_payload: {} {}", conn, e.what());
      throw std::system_error(make_error_code(crimson::net::error::negotiation_failure));
    } catch (ceph::crypto::onwire::MsgAuthError&) {
      logger().error("read_frame_payload: {} bad auth tag", conn);
      throw std::system_error(make_error_code(crimson::net::error::negotiation_failure));
    }
    // we do have a mechanism that allows transmitter to start sending message
    // and abort after putting entire data field on wire. This will be used by
    // the kernel client to avoid unnecessary buffering.
    if (!ok) {
      ceph_abort("TODO");
    }
    return &rx_segments_data;
  });
}

void FrameAssemblerV2::log_main_preamble(const ceph::bufferlist &bl)
{
  const auto main_preamble =
    reinterpret_cast<const preamble_block_t*>(bl.front().c_str());
  logger().trace("{} SEND({}) frame: tag={}, num_segments={}, crc={}",
                 conn, bl.length(), (int)main_preamble->tag,
                 (int)main_preamble->num_segments, main_preamble->crc);
}

FrameAssemblerV2Ref FrameAssemblerV2::create(SocketConnection &conn)
{
  return std::make_unique<FrameAssemblerV2>(conn);
}

} // namespace crimson::net
