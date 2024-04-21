// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>

#include <seastar/util/later.hh>

#include "crimson/common/gated.h"
#include "crimson/common/smp_helpers.h"
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
 * The notifications may be cross-core and must be sent to
 * SocketConnection::get_messenger_shard_id()
 */
class HandshakeListener {
public:
  using proto_crosscore_ordering_t = smp_crosscore_ordering_t<crosscore_type_t::ONE>;
  using cc_seq_t = proto_crosscore_ordering_t::seq_t;

  virtual ~HandshakeListener() = default;

  HandshakeListener(const HandshakeListener&) = delete;
  HandshakeListener(HandshakeListener &&) = delete;
  HandshakeListener &operator=(const HandshakeListener &) = delete;
  HandshakeListener &operator=(HandshakeListener &&) = delete;

  virtual seastar::future<> notify_out(
      cc_seq_t cc_seq) = 0;

  virtual seastar::future<> notify_out_fault(
      cc_seq_t cc_seq,
      const char *where,
      std::exception_ptr,
      io_handler_state) = 0;

  virtual seastar::future<> notify_mark_down(
      cc_seq_t cc_seq) = 0;

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
  using io_crosscore_ordering_t = smp_crosscore_ordering_t<crosscore_type_t::N_ONE>;
  using proto_crosscore_ordering_t = smp_crosscore_ordering_t<crosscore_type_t::ONE>;
  using cc_seq_t = proto_crosscore_ordering_t::seq_t;

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
public:
  seastar::shard_id get_shard_id() const final {
    return shard_states->get_shard_id();
  }

  bool is_connected() const final {
    ceph_assert_always(seastar::this_shard_id() == get_shard_id());
    return protocol_is_connected;
  }

  seastar::future<> send(MessageURef msg) final;

  seastar::future<> send_keepalive() final;

  clock_t::time_point get_last_keepalive() const final {
    ceph_assert_always(seastar::this_shard_id() == get_shard_id());
    return last_keepalive;
  }

  clock_t::time_point get_last_keepalive_ack() const final {
    ceph_assert_always(seastar::this_shard_id() == get_shard_id());
    return last_keepalive_ack;
  }

  void set_last_keepalive_ack(clock_t::time_point when) final {
    ceph_assert_always(seastar::this_shard_id() == get_shard_id());
    last_keepalive_ack = when;
  }

  void mark_down() final;

/*
 * as IOHandler to be called by ProtocolV2 handshake
 *
 * The calls may be cross-core and asynchronous
 */
public:
  /*
   * should not be called cross-core
   */

  void set_handshake_listener(HandshakeListener &hl) {
    assert(seastar::this_shard_id() == get_shard_id());
    ceph_assert_always(handshake_listener == nullptr);
    handshake_listener = &hl;
  }

  io_handler_state get_states() const {
    // might be called from prv_sid during wait_io_exit_dispatching()
    return {in_seq, is_out_queued(), has_out_sent()};
  }

  struct io_stat_printer {
    const IOHandler &io_handler;
  };
  void print_io_stat(std::ostream &out) const;

  seastar::future<> set_accepted_sid(
      cc_seq_t cc_seq,
      seastar::shard_id sid,
      ConnectionFRef conn_fref);

  /*
   * may be called cross-core
   */

  seastar::future<> close_io(
      cc_seq_t cc_seq,
      bool is_dispatch_reset,
      bool is_replace);

  /**
   * io_state_t
   *
   * The io_state is changed with the protocol state, to control the
   * io behavior accordingly.
   */
  enum class io_state_t : uint8_t {
    none,    // no IO is possible as the connection is not available to the user yet.
    delay,   // IO is delayed until open.
    open,    // Dispatch In and Out concurrently.
    drop,    // Drop IO as the connection is closed.
    switched // IO is switched to a different core
             // (is moved to maybe_prv_shard_states)
  };
  friend class fmt::formatter<io_state_t>;

  seastar::future<> set_io_state(
      cc_seq_t cc_seq,
      io_state_t new_state,
      FrameAssemblerV2Ref fa,
      bool set_notify_out);

  struct exit_dispatching_ret {
    FrameAssemblerV2Ref frame_assembler;
    io_handler_state io_states;
  };
  seastar::future<exit_dispatching_ret>
  wait_io_exit_dispatching(
      cc_seq_t cc_seq);

  seastar::future<> reset_session(
      cc_seq_t cc_seq,
      bool full);

  seastar::future<> reset_peer_state(
      cc_seq_t cc_seq);

  seastar::future<> requeue_out_sent_up_to(
      cc_seq_t cc_seq,
      seq_num_t msg_seq);

  seastar::future<> requeue_out_sent(
      cc_seq_t cc_seq);

  seastar::future<> dispatch_accept(
      cc_seq_t cc_seq,
      seastar::shard_id new_sid,
      ConnectionFRef,
      bool is_replace);

  seastar::future<> dispatch_connect(
      cc_seq_t cc_seq,
      seastar::shard_id new_sid,
      ConnectionFRef);

 private:
  class shard_states_t;
  using shard_states_ref_t = std::unique_ptr<shard_states_t>;

  class shard_states_t {
  public:
    shard_states_t(seastar::shard_id _sid, io_state_t state)
      : sid{_sid}, io_state{state} {}

    seastar::shard_id get_shard_id() const {
      return sid;
    }

    io_state_t get_io_state() const {
      assert(seastar::this_shard_id() == sid);
      return io_state;
    }

    void set_io_state(io_state_t new_state) {
      assert(seastar::this_shard_id() == sid);
      assert(io_state != new_state);
      pr_io_state_changed.set_value();
      pr_io_state_changed = seastar::promise<>();
      if (io_state == io_state_t::open) {
        // from open
        if (out_dispatching) {
          ceph_assert_always(!out_exit_dispatching.has_value());
          out_exit_dispatching = seastar::promise<>();
        }
      }
      io_state = new_state;
    }

    seastar::future<> wait_state_change() {
      assert(seastar::this_shard_id() == sid);
      return pr_io_state_changed.get_future();
    }

    template <typename Func>
    void dispatch_in_background(
        const char *what, SocketConnection &who, Func &&func) {
      assert(seastar::this_shard_id() == sid);
      ceph_assert_always(!gate.is_closed());
      gate.dispatch_in_background(what, who, std::move(func));
    }

    void enter_in_dispatching() {
      assert(seastar::this_shard_id() == sid);
      assert(io_state == io_state_t::open);
      ceph_assert_always(!in_exit_dispatching.has_value());
      in_exit_dispatching = seastar::promise<>();
    }

    void exit_in_dispatching() {
      assert(seastar::this_shard_id() == sid);
      assert(io_state != io_state_t::open);
      ceph_assert_always(in_exit_dispatching.has_value());
      in_exit_dispatching->set_value();
      in_exit_dispatching = std::nullopt;
    }

    bool try_enter_out_dispatching() {
      assert(seastar::this_shard_id() == sid);
      if (out_dispatching) {
        // already dispatching out
        return false;
      }
      switch (io_state) {
      case io_state_t::open:
        [[fallthrough]];
      case io_state_t::delay:
        out_dispatching = true;
        return true;
      case io_state_t::drop:
        [[fallthrough]];
      case io_state_t::switched:
        // do not dispatch out
        return false;
      default:
        ceph_abort("impossible");
      }
    }

    void notify_out_dispatching_stopped(
        const char *what, SocketConnection &conn);

    void exit_out_dispatching(
        const char *what, SocketConnection &conn) {
      assert(seastar::this_shard_id() == sid);
      ceph_assert_always(out_dispatching);
      out_dispatching = false;
      notify_out_dispatching_stopped(what, conn);
    }

    seastar::future<> wait_io_exit_dispatching();

    seastar::future<> close() {
      assert(seastar::this_shard_id() == sid);
      assert(!gate.is_closed());
      return gate.close();
    }

    bool assert_closed_and_exit() const {
      assert(seastar::this_shard_id() == sid);
      if (gate.is_closed()) {
        ceph_assert_always(io_state == io_state_t::drop ||
                           io_state == io_state_t::switched);
        ceph_assert_always(!out_dispatching);
        ceph_assert_always(!out_exit_dispatching);
        ceph_assert_always(!in_exit_dispatching);
        return true;
      } else {
        return false;
      }
    }

    static shard_states_ref_t create(
        seastar::shard_id sid, io_state_t state) {
      return std::make_unique<shard_states_t>(sid, state);
    }

    static shard_states_ref_t create_from_previous(
        shard_states_t &prv_states, seastar::shard_id new_sid);

  private:
    const seastar::shard_id sid;
    io_state_t io_state;

    crimson::common::Gated gate;
    seastar::promise<> pr_io_state_changed;
    bool out_dispatching = false;
    std::optional<seastar::promise<>> out_exit_dispatching;
    std::optional<seastar::promise<>> in_exit_dispatching;
  };

  void do_set_io_state(
      io_state_t new_state,
      std::optional<cc_seq_t> cc_seq = std::nullopt,
      FrameAssemblerV2Ref fa = nullptr,
      bool set_notify_out = false);

  io_state_t get_io_state() const {
    return shard_states->get_io_state();
  }

  void do_requeue_out_sent();

  void do_requeue_out_sent_up_to(seq_num_t seq);

  void assign_frame_assembler(FrameAssemblerV2Ref);

  seastar::future<> send_recheck_shard(cc_seq_t, core_id_t, MessageFRef);

  seastar::future<> do_send(cc_seq_t, core_id_t, MessageFRef);

  seastar::future<> send_keepalive_recheck_shard(cc_seq_t, core_id_t);

  seastar::future<> do_send_keepalive(cc_seq_t, core_id_t);

  seastar::future<> to_new_sid(
      cc_seq_t cc_seq,
      seastar::shard_id new_sid,
      ConnectionFRef,
      std::optional<bool> is_replace);

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

  seastar::future<> do_out_dispatch(shard_states_t &ctx);

#ifdef UNIT_TESTS_BUILT
  struct sweep_ret {
    ceph::bufferlist bl;
    std::vector<ceph::msgr::v2::Tag> tags;
  };
  sweep_ret
#else
  ceph::bufferlist
#endif
  sweep_out_pending_msgs_to_sent(
      bool require_keepalive,
      std::optional<utime_t> maybe_keepalive_ack,
      bool require_ack);

  void maybe_notify_out_dispatch();

  void notify_out_dispatch();

  void ack_out_sent(seq_num_t seq);

  seastar::future<> read_message(
      shard_states_t &ctx,
      utime_t throttle_stamp,
      std::size_t msg_size);

  void do_in_dispatch();

  seastar::future<> cleanup_prv_shard(seastar::shard_id prv_sid);

private:
  shard_states_ref_t shard_states;

  proto_crosscore_ordering_t proto_crosscore;

  io_crosscore_ordering_t io_crosscore;

  // drop was happening in the previous sid
  std::optional<seastar::shard_id> maybe_dropped_sid;

  // the remaining states in the previous sid for cleanup, see to_new_sid()
  shard_states_ref_t maybe_prv_shard_states;

  ChainedDispatchers &dispatchers;

  SocketConnection &conn;

  // core local reference for dispatching, valid until reset/close
  ConnectionRef conn_ref;

  HandshakeListener *handshake_listener = nullptr;

  FrameAssemblerV2Ref frame_assembler;

  bool protocol_is_connected = false;

  bool need_dispatch_reset = true;

  /*
   * out states for writing
   */

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
    case switched:
      name = "switched";
      break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::net::IOHandler::io_stat_printer> : fmt::ostream_formatter {};
#endif
