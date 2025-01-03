// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/async/frames_v2.h"
#include "msg/async/crypto_onwire.h"
#include "msg/async/compression_onwire.h"

#include "crimson/common/gated.h"
#include "crimson/net/Socket.h"

#ifdef UNIT_TESTS_BUILT
#include "Interceptor.h"
#endif

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

  void set_shard_id(seastar::shard_id _sid) {
    assert(seastar::this_shard_id() == sid);
    clear();
    sid = _sid;
  }

  seastar::shard_id get_shard_id() const {
    return sid;
  }

  void set_is_rev1(bool is_rev1);

  void create_session_stream_handlers(
      const AuthConnectionMeta &auth_meta,
      bool crossed);

  void reset_handlers();

  /*
   * replacing
   */

  struct mover_t {
    SocketFRef socket;
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

  seastar::shard_id get_socket_shard_id() const;

  void set_socket(SocketFRef &&);

  void learn_socket_ephemeral_port_as_connector(uint16_t port);

  // if may_cross_core == true, gate is required for cross-core shutdown
  template <bool may_cross_core>
  void shutdown_socket(crimson::common::Gated *gate);

  seastar::future<> replace_shutdown_socket(SocketFRef &&);

  seastar::future<> close_shutdown_socket();

  /*
   * socket read and write interfaces
   */

  template <bool may_cross_core = true>
  seastar::future<ceph::bufferptr> read_exactly(std::size_t bytes);

  template <bool may_cross_core = true>
  seastar::future<ceph::bufferlist> read(std::size_t bytes);

  template <bool may_cross_core = true>
  seastar::future<> write(ceph::bufferlist);

  template <bool may_cross_core = true>
  seastar::future<> flush();

  template <bool may_cross_core = true>
  seastar::future<> write_flush(ceph::bufferlist);

  /*
   * frame read and write interfaces
   */

  /// may throw negotiation_failure as fault
  struct read_main_t {
    ceph::msgr::v2::Tag tag;
    const ceph::msgr::v2::FrameAssembler *rx_frame_asm;
  };
  template <bool may_cross_core = true>
  seastar::future<read_main_t> read_main_preamble();

  /// may throw negotiation_failure as fault
  using read_payload_t = ceph::msgr::v2::segment_bls_t;
  // FIXME: read_payload_t cannot be no-throw move constructible
  template <bool may_cross_core = true>
  seastar::future<read_payload_t*> read_frame_payload();

  template <class F>
  ceph::bufferlist get_buffer(F &tx_frame) {
    assert(seastar::this_shard_id() == sid);
    auto bl = tx_frame.get_buffer(tx_frame_asm);
    log_main_preamble(bl);
    return bl;
  }

  template <class F, bool may_cross_core = true>
  seastar::future<> write_flush_frame(F &tx_frame) {
    assert(seastar::this_shard_id() == sid);
    auto bl = get_buffer(tx_frame);
#ifdef UNIT_TESTS_BUILT
    return intercept_frame(F::tag, true
    ).then([this, bl=std::move(bl)]() mutable {
      return write_flush<may_cross_core>(std::move(bl));
    });
#else
    return write_flush<may_cross_core>(std::move(bl));
#endif
  }

  static FrameAssemblerV2Ref create(SocketConnection &conn);

#ifdef UNIT_TESTS_BUILT
  seastar::future<> intercept_frames(
      std::vector<ceph::msgr::v2::Tag> tags,
      bool is_write) {
    auto type = is_write ? bp_type_t::WRITE : bp_type_t::READ;
    std::vector<Breakpoint> bps;
    for (auto &tag : tags) {
      bps.emplace_back(Breakpoint{tag, type});
    }
    return intercept_frames(bps, type);
  }

  seastar::future<> intercept_frame(
      ceph::msgr::v2::Tag tag,
      bool is_write) {
    auto type = is_write ? bp_type_t::WRITE : bp_type_t::READ;
    std::vector<Breakpoint> bps;
    bps.emplace_back(Breakpoint{tag, type});
    return intercept_frames(bps, type);
  }

  seastar::future<> intercept_frame(
      custom_bp_t bp,
      bool is_write) {
    auto type = is_write ? bp_type_t::WRITE : bp_type_t::READ;
    std::vector<Breakpoint> bps;
    bps.emplace_back(Breakpoint{bp});
    return intercept_frames(bps, type);
  }
#endif

private:
#ifdef UNIT_TESTS_BUILT
  seastar::future<> intercept_frames(
      std::vector<Breakpoint> bps,
      bp_type_t type);
#endif

  bool has_socket() const;

  SocketFRef move_socket();

  void clear();

  void log_main_preamble(const ceph::bufferlist &bl);

  SocketConnection &conn;

  SocketFRef socket;

  // checking Socket::is_shutdown() synchronously is impossible when sid is
  // different from the socket sid.
  bool is_socket_shutdown = false;

  // the current working shard, can be messenger or socket shard.
  // if is messenger shard, should call interfaces with may_cross_core = true.
  seastar::shard_id sid;

  /*
   * auth signature
   *
   * only in the messenger core
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

  // in the messenger core during handshake,
  // and in the socket core during open,
  // must be cleaned before switching cores.

  ceph::bufferlist rx_preamble;

  read_payload_t rx_segments_data;
};

} // namespace crimson::net
