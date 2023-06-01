// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/util/later.hh>

#include "crimson/common/gated.h"
#include "Fwd.h"
#include "SocketConnection.h"
#include "FrameAssemblerV2.h"

namespace crimson::net {

/**
 * io_handler_state
 *
 * It is required to populate the states from IOHandler to ProtocolV2
 * asynchronously.
 */
struct io_handler_state {
  seq_num_t in_seq;
  bool is_out_queued;
  bool has_out_sent;

  bool is_out_queued_or_sent() const {
    return is_out_queued || has_out_sent;
  }

  /*
   * should be consistent with the accroding interfaces in IOHandler
   */

  void reset_session(bool full) {
    in_seq = 0;
    if (full) {
      is_out_queued = false;
      has_out_sent = false;
    }
  }

  void reset_peer_state() {
    in_seq = 0;
    is_out_queued = is_out_queued_or_sent();
    has_out_sent = false;
  }

  void requeue_out_sent_up_to() {
    // noop since the information is insufficient
  }

  void requeue_out_sent() {
    if (has_out_sent) {
      has_out_sent = false;
      is_out_queued = true;
    }
  }
};

/**
 * HandshakeListener
 *
 * The interface class for IOHandler to notify the ProtocolV2.
 *
 * The notifications may be cross-core and asynchronous.
 */
class HandshakeListener {
public:
  virtual ~HandshakeListener() = default;

  HandshakeListener(const HandshakeListener&) = delete;
  HandshakeListener(HandshakeListener &&) = delete;
  HandshakeListener &operator=(const HandshakeListener &) = delete;
  HandshakeListener &operator=(HandshakeListener &&) = delete;

  virtual void notify_out() = 0;

  virtual void notify_out_fault(
      const char *where,
      std::exception_ptr,
      io_handler_state) = 0;

  virtual void notify_mark_down() = 0;

protected:
  HandshakeListener() = default;
};

/**
 * IOHandler
 *
 * Implements the message read and write paths after the handshake, and also be
 * responsible to dispatch events. It is supposed to be working on the same
 * core with the underlying socket and the FrameAssemblerV2 class.
 */
class IOHandler final : public ConnectionHandler {
public:
  IOHandler(ChainedDispatchers &,
            SocketConnection &);

  ~IOHandler() final;

  IOHandler(const IOHandler &) = delete;
  IOHandler(IOHandler &&) = delete;
  IOHandler &operator=(const IOHandler &) = delete;
  IOHandler &operator=(IOHandler &&) = delete;

/*
 * as ConnectionHandler
 */
private:
  seastar::shard_id get_shard_id() const final {
    return sid;
  }

  bool is_connected() const final {
    ceph_assert_always(seastar::this_shard_id() == sid);
    return protocol_is_connected;
  }

  seastar::future<> send(MessageFRef msg) final;

  seastar::future<> send_keepalive() final;

  clock_t::time_point get_last_keepalive() const final {
    ceph_assert_always(seastar::this_shard_id() == sid);
    return last_keepalive;
  }

  clock_t::time_point get_last_keepalive_ack() const final {
    ceph_assert_always(seastar::this_shard_id() == sid);
    return last_keepalive_ack;
  }

  void set_last_keepalive_ack(clock_t::time_point when) final {
    ceph_assert_always(seastar::this_shard_id() == sid);
    last_keepalive_ack = when;
  }

  void mark_down() final;

/*
 * as IOHandler to be called by ProtocolV2 handshake
 *
 * The calls may be cross-core and asynchronous
 */
public:
  void set_handshake_listener(HandshakeListener &hl) {
    ceph_assert_always(handshake_listener == nullptr);
    handshake_listener = &hl;
  }

  io_handler_state get_states() const {
    return {in_seq, is_out_queued(), has_out_sent()};
  }

  struct io_stat_printer {
    const IOHandler &io_handler;
  };
  void print_io_stat(std::ostream &out) const;

  seastar::future<> close_io(bool is_dispatch_reset, bool is_replace);

  /**
   * io_state_t
   *
   * The io_state is changed with the protocol state, to control the
   * io behavior accordingly.
   */
  enum class io_state_t : uint8_t {
    none,  // no IO is possible as the connection is not available to the user yet.
    delay, // IO is delayed until open.
    open,  // Dispatch In and Out concurrently.
    drop   // Drop IO as the connection is closed.
  };
  friend class fmt::formatter<io_state_t>;

  void set_io_state(
      io_state_t new_state,
      FrameAssemblerV2Ref fa = nullptr,
      bool set_notify_out = false);

  struct exit_dispatching_ret {
    FrameAssemblerV2Ref frame_assembler;
    io_handler_state io_states;
  };
  seastar::future<exit_dispatching_ret> wait_io_exit_dispatching();

  void reset_session(bool full);

  void reset_peer_state();

  void requeue_out_sent_up_to(seq_num_t seq);

  void requeue_out_sent();

  void dispatch_accept();

  void dispatch_connect();

 private:
  seastar::future<> do_send(MessageFRef msg);

  seastar::future<> do_send_keepalive();

  void dispatch_reset(bool is_replace);

  void dispatch_remote_reset();

  bool is_out_queued() const {
    return (!out_pending_msgs.empty() ||
            ack_left > 0 ||
            need_keepalive ||
            next_keepalive_ack.has_value());
  }

  bool has_out_sent() const {
    return !out_sent_msgs.empty();
  }

  void reset_in();

  void reset_out();

  void discard_out_sent();

  seastar::future<> do_out_dispatch();

  ceph::bufferlist sweep_out_pending_msgs_to_sent(
      bool require_keepalive,
      std::optional<utime_t> maybe_keepalive_ack,
      bool require_ack);

  void maybe_notify_out_dispatch();

  void notify_out_dispatch();

  void ack_out_sent(seq_num_t seq);

  seastar::future<> read_message(
      utime_t throttle_stamp,
      std::size_t msg_size);

  void do_in_dispatch();

private:
  seastar::shard_id sid;

  ChainedDispatchers &dispatchers;

  SocketConnection &conn;

  // core local reference for dispatching, valid until reset/close
  ConnectionRef conn_ref;

  HandshakeListener *handshake_listener = nullptr;

  crimson::common::Gated gate;

  FrameAssemblerV2Ref frame_assembler;

  bool protocol_is_connected = false;

  bool need_dispatch_reset = true;

  io_state_t io_state = io_state_t::none;

  // wait until current io_state changed
  seastar::promise<> io_state_changed;

  /*
   * out states for writing
   */

  bool out_dispatching = false;

  std::optional<seastar::promise<>> out_exit_dispatching;

  /// the seq num of the last transmitted message
  seq_num_t out_seq = 0;

  // messages to be resent after connection gets reset
  std::deque<MessageFRef> out_pending_msgs;

  // messages sent, but not yet acked by peer
  std::deque<MessageFRef> out_sent_msgs;

  bool need_keepalive = false;

  std::optional<utime_t> next_keepalive_ack = std::nullopt;

  uint64_t ack_left = 0;

  bool need_notify_out = false;

  /*
   * in states for reading
   */

  std::optional<seastar::promise<>> in_exit_dispatching;

  /// the seq num of the last received message
  seq_num_t in_seq = 0;

  clock_t::time_point last_keepalive;

  clock_t::time_point last_keepalive_ack;
};

inline std::ostream& operator<<(
    std::ostream& out, IOHandler::io_stat_printer stat) {
  stat.io_handler.print_io_stat(out);
  return out;
}

} // namespace crimson::net

template <>
struct fmt::formatter<crimson::net::io_handler_state> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(crimson::net::io_handler_state state, FormatContext& ctx) {
    return fmt::format_to(
        ctx.out(),
        "io(in_seq={}, is_out_queued={}, has_out_sent={})",
        state.in_seq,
        state.is_out_queued,
        state.has_out_sent);
  }
};

template <>
struct fmt::formatter<crimson::net::IOHandler::io_state_t>
  : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(crimson::net::IOHandler::io_state_t state, FormatContext& ctx) {
    using enum crimson::net::IOHandler::io_state_t;
    std::string_view name;
    switch (state) {
    case none:
      name = "none";
      break;
    case delay:
      name = "delay";
      break;
    case open:
      name = "open";
      break;
    case drop:
      name = "drop";
      break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::net::IOHandler::io_stat_printer> : fmt::ostream_formatter {};
#endif
