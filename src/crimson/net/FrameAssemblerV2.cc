// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "FrameAssemblerV2.h"

#include "Errors.h"
#include "SocketConnection.h"

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
  : conn{_conn}, sid{seastar::this_shard_id()}
{
  assert(seastar::this_shard_id() == conn.get_messenger_shard_id());
}

FrameAssemblerV2::~FrameAssemblerV2()
{
  assert(seastar::this_shard_id() == conn.get_messenger_shard_id());
  assert(seastar::this_shard_id() == sid);
  if (has_socket()) {
    std::ignore = move_socket();
  }
}

#ifdef UNIT_TESTS_BUILT
// should be consistent to intercept() in ProtocolV2.cc
seastar::future<> FrameAssemblerV2::intercept_frames(
    std::vector<Breakpoint> bps,
    bp_type_t type)
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  if (!conn.interceptor) {
    return seastar::now();
  }
  return conn.interceptor->intercept(conn, bps
  ).then([this, type](bp_action_t action) {
    return seastar::smp::submit_to(
        socket->get_shard_id(),
        [this, type, action] {
      socket->set_trap(type, action, &conn.interceptor->blocker);
    });
  });
}
#endif

void FrameAssemblerV2::set_is_rev1(bool _is_rev1)
{
  assert(seastar::this_shard_id() == sid);
  is_rev1 = _is_rev1;
  tx_frame_asm.set_is_rev1(_is_rev1);
  rx_frame_asm.set_is_rev1(_is_rev1);
}

void FrameAssemblerV2::create_session_stream_handlers(
  const AuthConnectionMeta &auth_meta,
  bool crossed)
{
  assert(seastar::this_shard_id() == sid);
  session_stream_handlers = ceph::crypto::onwire::rxtx_t::create_handler_pair(
      nullptr, auth_meta, is_rev1, crossed);
}

void FrameAssemblerV2::reset_handlers()
{
  assert(seastar::this_shard_id() == sid);
  session_stream_handlers = { nullptr, nullptr };
  session_comp_handlers = { nullptr, nullptr };
}

FrameAssemblerV2::mover_t
FrameAssemblerV2::to_replace()
{
  assert(seastar::this_shard_id() == sid);
  assert(is_socket_valid());

  clear();

  return mover_t{
      move_socket(),
      std::move(session_stream_handlers),
      std::move(session_comp_handlers)};
}

seastar::future<> FrameAssemblerV2::replace_by(FrameAssemblerV2::mover_t &&mover)
{
  assert(seastar::this_shard_id() == sid);

  clear();

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
  assert(seastar::this_shard_id() == sid);
  record_io = true;
  rxbuf.clear();
  txbuf.clear();
}

FrameAssemblerV2::record_bufs_t
FrameAssemblerV2::stop_recording()
{
  assert(seastar::this_shard_id() == sid);
  ceph_assert_always(record_io == true);
  record_io = false;
  return record_bufs_t{std::move(rxbuf), std::move(txbuf)};
}

bool FrameAssemblerV2::has_socket() const
{
  assert((socket && conn.socket) || (!socket && !conn.socket));
  return bool(socket);
}

bool FrameAssemblerV2::is_socket_valid() const
{
  assert(seastar::this_shard_id() == sid);
#ifndef NDEBUG
  if (has_socket() && socket->get_shard_id() == sid) {
    assert(socket->is_shutdown() == is_socket_shutdown);
  }
#endif
  return has_socket() && !is_socket_shutdown;
}

seastar::shard_id
FrameAssemblerV2::get_socket_shard_id() const
{
  assert(seastar::this_shard_id() == sid);
  assert(is_socket_valid());
  return socket->get_shard_id();
}

SocketFRef FrameAssemblerV2::move_socket()
{
  assert(has_socket());
  conn.set_socket(nullptr);
  return std::move(socket);
}

void FrameAssemblerV2::set_socket(SocketFRef &&new_socket)
{
  assert(seastar::this_shard_id() == sid);
  assert(!has_socket());
  assert(new_socket);
  socket = std::move(new_socket);
  conn.set_socket(socket.get());
  is_socket_shutdown = false;
  assert(is_socket_valid());
}

void FrameAssemblerV2::learn_socket_ephemeral_port_as_connector(uint16_t port)
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  // Note: may not invoke on the socket core
  socket->learn_ephemeral_port_as_connector(port);
}

template <bool may_cross_core>
void FrameAssemblerV2::shutdown_socket(crimson::common::Gated *gate)
{
  assert(seastar::this_shard_id() == sid);
  assert(is_socket_valid());
  is_socket_shutdown = true;
  if constexpr (may_cross_core) {
    assert(conn.get_messenger_shard_id() == sid);
    assert(gate);
    gate->dispatch_in_background("shutdown_socket", conn, [this] {
      return seastar::smp::submit_to(
          socket->get_shard_id(), [this] {
        socket->shutdown();
      });
    });
  } else {
    assert(socket->get_shard_id() == sid);
    assert(!gate);
    socket->shutdown();
  }
}
template void FrameAssemblerV2::shutdown_socket<true>(crimson::common::Gated *);
template void FrameAssemblerV2::shutdown_socket<false>(crimson::common::Gated *);

seastar::future<> FrameAssemblerV2::replace_shutdown_socket(SocketFRef &&new_socket)
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  assert(!is_socket_valid());
  auto old_socket = move_socket();
  auto old_socket_shard_id = old_socket->get_shard_id();
  set_socket(std::move(new_socket));
  return seastar::smp::submit_to(
      old_socket_shard_id,
      [old_socket = std::move(old_socket)]() mutable {
    return old_socket->close(
    ).then([sock = std::move(old_socket)] {});
  });
}

seastar::future<> FrameAssemblerV2::close_shutdown_socket()
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  assert(!is_socket_valid());
  return seastar::smp::submit_to(
      socket->get_shard_id(), [this] {
    return socket->close();
  });
}

template <bool may_cross_core>
seastar::future<ceph::bufferptr>
FrameAssemblerV2::read_exactly(std::size_t bytes)
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  if constexpr (may_cross_core) {
    assert(conn.get_messenger_shard_id() == sid);
    return seastar::smp::submit_to(
        socket->get_shard_id(), [this, bytes] {
      return socket->read_exactly(bytes);
    }).then([this](auto bptr) {
      if (record_io) {
        rxbuf.append(bptr);
      }
      return bptr;
    });
  } else {
    assert(socket->get_shard_id() == sid);
    return socket->read_exactly(bytes);
  }
}
template seastar::future<ceph::bufferptr> FrameAssemblerV2::read_exactly<true>(std::size_t);
template seastar::future<ceph::bufferptr> FrameAssemblerV2::read_exactly<false>(std::size_t);

template <bool may_cross_core>
seastar::future<ceph::bufferlist>
FrameAssemblerV2::read(std::size_t bytes)
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  if constexpr (may_cross_core) {
    assert(conn.get_messenger_shard_id() == sid);
    return seastar::smp::submit_to(
        socket->get_shard_id(), [this, bytes] {
      return socket->read(bytes);
    }).then([this](auto buf) {
      if (record_io) {
        rxbuf.append(buf);
      }
      return buf;
    });
  } else {
    assert(socket->get_shard_id() == sid);
    return socket->read(bytes);
  }
}
template seastar::future<ceph::bufferlist> FrameAssemblerV2::read<true>(std::size_t);
template seastar::future<ceph::bufferlist> FrameAssemblerV2::read<false>(std::size_t);

template <bool may_cross_core>
seastar::future<>
FrameAssemblerV2::write(ceph::bufferlist buf)
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  if constexpr (may_cross_core) {
    assert(conn.get_messenger_shard_id() == sid);
    if (record_io) {
      txbuf.append(buf);
    }
    return seastar::smp::submit_to(
        socket->get_shard_id(), [this, buf = std::move(buf)]() mutable {
      return socket->write(std::move(buf));
    });
  } else {
    assert(socket->get_shard_id() == sid);
    return socket->write(std::move(buf));
  }
}
template seastar::future<> FrameAssemblerV2::write<true>(ceph::bufferlist);
template seastar::future<> FrameAssemblerV2::write<false>(ceph::bufferlist);

template <bool may_cross_core>
seastar::future<>
FrameAssemblerV2::flush()
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  if constexpr (may_cross_core) {
    assert(conn.get_messenger_shard_id() == sid);
    return seastar::smp::submit_to(
        socket->get_shard_id(), [this] {
      return socket->flush();
    });
  } else {
    assert(socket->get_shard_id() == sid);
    return socket->flush();
  }
}
template seastar::future<> FrameAssemblerV2::flush<true>();
template seastar::future<> FrameAssemblerV2::flush<false>();

template <bool may_cross_core>
seastar::future<>
FrameAssemblerV2::write_flush(ceph::bufferlist buf)
{
  assert(seastar::this_shard_id() == sid);
  assert(has_socket());
  if constexpr (may_cross_core) {
    assert(conn.get_messenger_shard_id() == sid);
    if (unlikely(record_io)) {
      txbuf.append(buf);
    }
    return seastar::smp::submit_to(
        socket->get_shard_id(), [this, buf = std::move(buf)]() mutable {
      return socket->write_flush(std::move(buf));
    });
  } else {
    assert(socket->get_shard_id() == sid);
    return socket->write_flush(std::move(buf));
  }
}
template seastar::future<> FrameAssemblerV2::write_flush<true>(ceph::bufferlist);
template seastar::future<> FrameAssemblerV2::write_flush<false>(ceph::bufferlist);

template <bool may_cross_core>
seastar::future<FrameAssemblerV2::read_main_t>
FrameAssemblerV2::read_main_preamble()
{
  assert(seastar::this_shard_id() == sid);
  rx_preamble.clear();
  return read_exactly<may_cross_core>(
    rx_frame_asm.get_preamble_onwire_len()
  ).then([this](auto bptr) {
    rx_preamble.append(std::move(bptr));
    Tag tag;
    try {
      tag = rx_frame_asm.disassemble_preamble(rx_preamble);
    } catch (FrameError& e) {
      logger().warn("{} read_main_preamble: {}", conn, e.what());
      throw std::system_error(make_error_code(crimson::net::error::negotiation_failure));
    }
#ifdef UNIT_TESTS_BUILT
    return intercept_frame(tag, false
    ).then([this, tag] {
      return read_main_t{tag, &rx_frame_asm};
    });
#else
    return read_main_t{tag, &rx_frame_asm};
#endif
  });
}
template seastar::future<FrameAssemblerV2::read_main_t> FrameAssemblerV2::read_main_preamble<true>();
template seastar::future<FrameAssemblerV2::read_main_t> FrameAssemblerV2::read_main_preamble<false>();

template <bool may_cross_core>
seastar::future<FrameAssemblerV2::read_payload_t*>
FrameAssemblerV2::read_frame_payload()
{
  assert(seastar::this_shard_id() == sid);
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
      return read_exactly<may_cross_core>(onwire_len
      ).then([this](auto bptr) {
        logger().trace("{} RECV({}) frame segment[{}]",
                       conn, bptr.length(), rx_segments_data.size());
        bufferlist segment;
        segment.append(std::move(bptr));
        rx_segments_data.emplace_back(std::move(segment));
      });
    }
  ).then([this] {
    return read_exactly<may_cross_core>(rx_frame_asm.get_epilogue_onwire_len());
  }).then([this](auto bptr) {
    logger().trace("{} RECV({}) frame epilogue", conn, bptr.length());
    bool ok = false;
    try {
      bufferlist rx_epilogue;
      rx_epilogue.append(std::move(bptr));
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
template seastar::future<FrameAssemblerV2::read_payload_t*> FrameAssemblerV2::read_frame_payload<true>();
template seastar::future<FrameAssemblerV2::read_payload_t*> FrameAssemblerV2::read_frame_payload<false>();

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

void FrameAssemblerV2::clear()
{
  record_io = false;
  rxbuf.clear();
  txbuf.clear();
  rx_preamble.clear();
  rx_segments_data.clear();
}

} // namespace crimson::net
