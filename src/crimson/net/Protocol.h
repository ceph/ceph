// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/later.hh>

#include "crimson/common/gated.h"
#include "crimson/common/log.h"
#include "Fwd.h"
#include "SocketConnection.h"
#include "FrameAssemblerV2.h"

namespace crimson::net {

class Protocol {
// public to SocketConnection
 public:
  Protocol(Protocol&&) = delete;
  virtual ~Protocol();

  virtual bool is_connected() const = 0;

  virtual void close() = 0;

  virtual seastar::future<> close_clean_yielded() = 0;

#ifdef UNIT_TESTS_BUILT
  virtual bool is_closed_clean() const = 0;

  virtual bool is_closed() const = 0;

#endif
  virtual void start_connect(const entity_addr_t& peer_addr,
                             const entity_name_t& peer_name) = 0;

  virtual void start_accept(SocketRef&& socket,
                            const entity_addr_t& peer_addr) = 0;

  virtual void print_conn(std::ostream&) const = 0;

 protected:
  Protocol(ChainedDispatchers& dispatchers,
           SocketConnection& conn);

  virtual ceph::bufferlist do_sweep_messages(
      const std::deque<MessageURef>& msgs,
      size_t num_msgs,
      bool require_keepalive,
      std::optional<utime_t> maybe_keepalive_ack,
      bool require_ack) = 0;

  virtual void notify_out() = 0;

// the write state-machine
 public:
  using clock_t = seastar::lowres_system_clock;

  seastar::future<> send(MessageURef msg);

  seastar::future<> send_keepalive();

  clock_t::time_point get_last_keepalive() const {
    return last_keepalive;
  }

  clock_t::time_point get_last_keepalive_ack() const {
    return last_keepalive_ack;
  }

  void set_last_keepalive_ack(clock_t::time_point when) {
    last_keepalive_ack = when;
  }

  struct io_stat_printer {
    const Protocol &protocol;
  };
  void print_io_stat(std::ostream &out) const {
    out << "io_stat("
        << "in_seq=" << in_seq
        << ", out_seq=" << out_seq
        << ", out_pending_msgs_size=" << out_pending_msgs.size()
        << ", out_sent_msgs_size=" << out_sent_msgs.size()
        << ", need_ack=" << (ack_left > 0)
        << ", need_keepalive=" << need_keepalive
        << ", need_keepalive_ack=" << bool(next_keepalive_ack)
        << ")";
  }

// TODO: encapsulate a SessionedSender class
 protected:
  seastar::future<> close_out() {
    assert(!gate.is_closed());
    return gate.close();
  }

  /**
   * out_state_t
   *
   * The out_state is changed with protocol state atomically, indicating the
   * out behavior of the according protocol state.
   */
  enum class out_state_t : uint8_t {
    none,
    delay,
    open,
    drop
  };

  friend class fmt::formatter<out_state_t>;
  void set_out_state(const out_state_t& state) {
    if (out_state == out_state_t::open &&
        state != out_state_t::open &&
        out_dispatching) {
      out_exit_dispatching = seastar::shared_promise<>();
    }
    out_state = state;
    out_state_changed.set_value();
    out_state_changed = seastar::shared_promise<>();
  }

  seastar::future<> wait_out_exit_dispatching() {
    if (out_exit_dispatching) {
      return out_exit_dispatching->get_shared_future();
    }
    return seastar::now();
  }

  void notify_keepalive_ack(utime_t keepalive_ack);

  void notify_ack();

  void requeue_out_sent_up_to(seq_num_t seq);

  void requeue_out_sent();

  void reset_out();

  void reset_in() {
    in_seq = 0;
  }

  bool is_out_queued_or_sent() const {
    return is_out_queued() || !out_sent_msgs.empty();
  }

  void ack_out_sent(seq_num_t seq);

  void set_last_keepalive(clock_t::time_point when) {
    last_keepalive = when;
  }

  seq_num_t get_in_seq() const {
    return in_seq;
  }

  void set_in_seq(seq_num_t _in_seq) {
    in_seq = _in_seq;
  }

  seq_num_t increment_out_seq() {
    return ++out_seq;
  }

  ChainedDispatchers& dispatchers;

  SocketConnection &conn;

  FrameAssemblerV2 frame_assembler;

 private:
  bool is_out_queued() const {
    return (!out_pending_msgs.empty() ||
            ack_left > 0 ||
            need_keepalive ||
            next_keepalive_ack.has_value());
  }

  seastar::future<stop_t> try_exit_out_dispatch();

  seastar::future<> do_out_dispatch();

  ceph::bufferlist sweep_out_pending_msgs_to_sent(
      size_t num_msgs,
      bool require_keepalive,
      std::optional<utime_t> maybe_keepalive_ack,
      bool require_ack);

  void notify_out_dispatch();

  crimson::common::Gated gate;

  /*
   * out states for writing
   */

  out_state_t out_state = out_state_t::none;

  // wait until current out_state changed
  seastar::shared_promise<> out_state_changed;

  bool out_dispatching = false;

  // If another continuation is trying to close or replace socket when
  // out_dispatching is true and out_state is open, it needs to wait for
  // out_exit_dispatching until writing is stopped or failed.
  std::optional<seastar::shared_promise<>> out_exit_dispatching;

  /// the seq num of the last transmitted message
  seq_num_t out_seq = 0;

  // messages to be resent after connection gets reset
  std::deque<MessageURef> out_pending_msgs;

  // messages sent, but not yet acked by peer
  std::deque<MessageURef> out_sent_msgs;

  bool need_keepalive = false;

  std::optional<utime_t> next_keepalive_ack = std::nullopt;

  uint64_t ack_left = 0;

  /*
   * in states for reading
   */

  /// the seq num of the last received message
  seq_num_t in_seq = 0;

  clock_t::time_point last_keepalive;

  clock_t::time_point last_keepalive_ack;
};

inline std::ostream& operator<<(std::ostream& out, const Protocol& proto) {
  proto.print_conn(out);
  return out;
}

inline std::ostream& operator<<(
    std::ostream& out, Protocol::io_stat_printer stat) {
  stat.protocol.print_io_stat(out);
  return out;
}

} // namespace crimson::net

template <>
struct fmt::formatter<crimson::net::Protocol::out_state_t>
  : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(crimson::net::Protocol::out_state_t state, FormatContext& ctx) {
    using enum crimson::net::Protocol::out_state_t;
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
template <> struct fmt::formatter<crimson::net::Protocol> : fmt::ostream_formatter {};
#endif
