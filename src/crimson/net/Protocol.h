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

namespace crimson::net {

class Protocol {
 public:
  Protocol(Protocol&&) = delete;
  virtual ~Protocol();

  virtual bool is_connected() const = 0;

#ifdef UNIT_TESTS_BUILT
  bool is_closed_clean = false;
  bool is_closed() const { return closed; }
#endif

  // Reentrant closing
  void close(bool dispatch_reset, std::optional<std::function<void()>> f_accept_new=std::nullopt);
  seastar::future<> close_clean(bool dispatch_reset) {
    // yield() so that close(dispatch_reset) can be called *after*
    // close_clean() is applied to all connections in a container using
    // seastar::parallel_for_each(). otherwise, we could erase a connection in
    // the container when seastar::parallel_for_each() is still iterating in
    // it. that'd lead to a segfault.
    return seastar::yield(
    ).then([this, dispatch_reset, conn_ref = conn.shared_from_this()] {
      close(dispatch_reset);
      // it can happen if close_clean() is called inside Dispatcher::ms_handle_reset()
      // which will otherwise result in deadlock
      assert(close_ready.valid());
      return close_ready.get_future();
    });
  }

  virtual void start_connect(const entity_addr_t& peer_addr,
                             const entity_name_t& peer_name) = 0;

  virtual void start_accept(SocketRef&& socket,
                            const entity_addr_t& peer_addr) = 0;

  virtual void print_conn(std::ostream&) const = 0;

 protected:
  Protocol(ChainedDispatchers& dispatchers,
           SocketConnection& conn);

  virtual void trigger_close() = 0;

  virtual ceph::bufferlist do_sweep_messages(
      const std::deque<MessageURef>& msgs,
      size_t num_msgs,
      bool require_keepalive,
      std::optional<utime_t> keepalive_ack,
      bool require_ack) = 0;

  virtual void notify_write() {};

  virtual void on_closed() {}
 
 private:
  ceph::bufferlist sweep_messages_and_move_to_sent(
      size_t num_msgs,
      bool require_keepalive,
      std::optional<utime_t> keepalive_ack,
      bool require_ack); 

 protected:
  ChainedDispatchers& dispatchers;
  SocketConnection &conn;

 private:
  bool closed = false;
  // become valid only after closed == true
  seastar::shared_future<> close_ready;

// the write state-machine
 public:
  using clock_t = seastar::lowres_system_clock;

  seastar::future<> send(MessageURef msg);

  seastar::future<> keepalive();

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
        << ", out_q_size=" << out_q.size()
        << ", sent_size=" << sent.size()
        << ", need_ack=" << (ack_left > 0)
        << ", need_keepalive=" << need_keepalive
        << ", need_keepalive_ack=" << bool(keepalive_ack)
        << ")";
  }

// TODO: encapsulate a SessionedSender class
 protected:
  // write_state is changed with state atomically, indicating the write
  // behavior of the according state.
  enum class write_state_t : uint8_t {
    none,
    delay,
    open,
    drop
  };

  friend class fmt::formatter<write_state_t>;
  void set_write_state(const write_state_t& state) {
    if (write_state == write_state_t::open &&
        state != write_state_t::open &&
        write_dispatching) {
      exit_open = seastar::shared_promise<>();
    }
    write_state = state;
    state_changed.set_value();
    state_changed = seastar::shared_promise<>();
  }

  seastar::future<> wait_write_exit() {
    if (exit_open) {
      return exit_open->get_shared_future();
    }
    return seastar::now();
  }

  void notify_keepalive_ack(utime_t keepalive_ack);

  void notify_ack();

  void requeue_up_to(seq_num_t seq);

  void requeue_sent();

  void reset_write();

  void reset_read() {
    in_seq = 0;
  }

  bool is_queued() const {
    return (!out_q.empty() ||
            ack_left > 0 ||
            need_keepalive ||
            keepalive_ack.has_value());
  }

  bool is_queued_or_sent() const {
    return is_queued() || !sent.empty();
  }

  void ack_writes(seq_num_t seq);

  void set_last_keepalive(clock_t::time_point when) {
    last_keepalive = when;
  }

  seq_num_t get_in_seq() const {
    return in_seq;
  }

  void set_in_seq(seq_num_t _in_seq) {
    in_seq = _in_seq;
  }

  seq_num_t increment_out() {
    return ++out_seq;
  }

  crimson::common::Gated gate;

 private:
  write_state_t write_state = write_state_t::none;

  // wait until current state changed
  seastar::shared_promise<> state_changed;

  /// the seq num of the last transmitted message
  seq_num_t out_seq = 0;

  // messages to be resent after connection gets reset
  std::deque<MessageURef> out_q;

  // messages sent, but not yet acked by peer
  std::deque<MessageURef> sent;

  bool need_keepalive = false;
  std::optional<utime_t> keepalive_ack = std::nullopt;
  uint64_t ack_left = 0;
  bool write_dispatching = false;
  // If another continuation is trying to close or replace socket when
  // write_dispatching is true and write_state is open,
  // it needs to wait for exit_open until writing is stopped or failed.
  std::optional<seastar::shared_promise<>> exit_open;

  /// the seq num of the last received message
  seq_num_t in_seq = 0;

  clock_t::time_point last_keepalive;

  clock_t::time_point last_keepalive_ack;

  seastar::future<stop_t> try_exit_sweep();
  seastar::future<> do_write_dispatch_sweep();
  void write_event();
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
struct fmt::formatter<crimson::net::Protocol::write_state_t>
  : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(crimson::net::Protocol::write_state_t state, FormatContext& ctx) {
    using enum crimson::net::Protocol::write_state_t;
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
