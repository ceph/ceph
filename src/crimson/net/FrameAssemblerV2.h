// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/async/frames_v2.h"
#include "msg/async/crypto_onwire.h"
#include "msg/async/compression_onwire.h"

#include "crimson/net/Socket.h"

namespace crimson::net {

class SocketConnection;
class FrameAssemblerV2;
using FrameAssemblerV2Ref = std::unique_ptr<FrameAssemblerV2>;

class FrameAssemblerV2 {
public:
  FrameAssemblerV2(SocketConnection &conn);

  ~FrameAssemblerV2();

  FrameAssemblerV2(const FrameAssemblerV2 &) = delete;

  FrameAssemblerV2(FrameAssemblerV2 &&) = delete;

  void set_is_rev1(bool is_rev1);

  void create_session_stream_handlers(
      const AuthConnectionMeta &auth_meta,
      bool crossed);

  void reset_handlers();

  /*
   * replacing
   */

  struct mover_t {
    SocketRef socket;
    ceph::crypto::onwire::rxtx_t session_stream_handlers;
    ceph::compression::onwire::rxtx_t session_comp_handlers;
  };

  mover_t to_replace();

  seastar::future<> replace_by(mover_t &&);

  /*
   * auth signature interfaces
   */

  void start_recording();

  struct record_bufs_t {
    ceph::bufferlist rxbuf;
    ceph::bufferlist txbuf;
  };
  record_bufs_t stop_recording();

  /*
   * socket maintainence interfaces
   */

  // the socket exists and not shutdown
  bool is_socket_valid() const;

  void set_socket(SocketRef &&);

  void learn_socket_ephemeral_port_as_connector(uint16_t port);

  void shutdown_socket();

  seastar::future<> replace_shutdown_socket(SocketRef &&);

  seastar::future<> close_shutdown_socket();

  /*
   * socket read and write interfaces
   */

  seastar::future<Socket::tmp_buf> read_exactly(std::size_t bytes);

  seastar::future<ceph::bufferlist> read(std::size_t bytes);

  seastar::future<> write(ceph::bufferlist &&);

  seastar::future<> flush();

  seastar::future<> write_flush(ceph::bufferlist &&);

  /*
   * frame read and write interfaces
   */

  /// may throw negotiation_failure as fault
  struct read_main_t {
    ceph::msgr::v2::Tag tag;
    const ceph::msgr::v2::FrameAssembler *rx_frame_asm;
  };
  seastar::future<read_main_t> read_main_preamble();

  /// may throw negotiation_failure as fault
  using read_payload_t = ceph::msgr::v2::segment_bls_t;
  // FIXME: read_payload_t cannot be no-throw move constructible
  seastar::future<read_payload_t*> read_frame_payload();

  template <class F>
  ceph::bufferlist get_buffer(F &tx_frame) {
#ifdef UNIT_TESTS_BUILT
    intercept_frame(F::tag, true);
#endif
    auto bl = tx_frame.get_buffer(tx_frame_asm);
    log_main_preamble(bl);
    return bl;
  }

  template <class F>
  seastar::future<> write_flush_frame(F &tx_frame) {
    auto bl = get_buffer(tx_frame);
    return write_flush(std::move(bl));
  }

  static FrameAssemblerV2Ref create(SocketConnection &conn);

private:
  bool has_socket() const;

  SocketRef move_socket();

  void log_main_preamble(const ceph::bufferlist &bl);

#ifdef UNIT_TESTS_BUILT
  void intercept_frame(ceph::msgr::v2::Tag, bool is_write);
#endif

  SocketConnection &conn;

  SocketRef socket;

  /*
   * auth signature
   */

  bool record_io = false;

  ceph::bufferlist rxbuf;

  ceph::bufferlist txbuf;

  /*
   * frame data and handlers
   */

  ceph::crypto::onwire::rxtx_t session_stream_handlers = { nullptr, nullptr };

  // TODO
  ceph::compression::onwire::rxtx_t session_comp_handlers = { nullptr, nullptr };

  bool is_rev1 = false;

  ceph::msgr::v2::FrameAssembler tx_frame_asm{
    &session_stream_handlers, is_rev1, common::local_conf()->ms_crc_data,
    &session_comp_handlers};

  ceph::msgr::v2::FrameAssembler rx_frame_asm{
    &session_stream_handlers, is_rev1, common::local_conf()->ms_crc_data,
    &session_comp_handlers};

  ceph::bufferlist rx_preamble;

  read_payload_t rx_segments_data;
};

} // namespace crimson::net
