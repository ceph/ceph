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

  virtual seastar::future<> close_clean_yielded() = 0;

#ifdef UNIT_TESTS_BUILT
  virtual bool is_closed_clean() const = 0;

  virtual bool is_closed() const = 0;

#endif
  virtual void start_connect(const entity_addr_t& peer_addr,
                             const entity_name_t& peer_name) = 0;

  virtual void start_accept(SocketRef&& socket,
                            const entity_addr_t& peer_addr) = 0;

 protected:
  Protocol(ChainedDispatchers& dispatchers,
           SocketConnection& conn);

  virtual void notify_out() = 0;

  virtual void notify_out_fault(const char *where, std::exception_ptr) = 0;

  virtual void notify_mark_down() = 0;

// the write state-machine
 public:
  using clock_t = seastar::lowres_system_clock;

  bool is_connected() const {
    return protocol_is_connected;
  }

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

  void mark_down();

  struct io_stat_printer {
    const Protocol &protocol;
  };
  void print_io_stat(std::ostream &out) const;

// TODO: encapsulate a SessionedSender class
 protected:
  seastar::future<> close_io(
      bool is_dispatch_reset,
      bool is_replace) {
    ceph_assert_always(io_state == io_state_t::drop);

    if (is_dispatch_reset) {
      dispatch_reset(is_replace);
    }
    assert(!gate.is_closed());
    return gate.close();
  }

  /**
   * io_state_t
   *
   * The io_state is changed with protocol state atomically, indicating the
   * IOHandler behavior of the according protocol state.
   */
  enum class io_state_t : uint8_t {
    none,
    delay,
    open,
    drop
  };
  friend class fmt::formatter<io_state_t>;

  void set_io_state(const io_state_t &new_state, FrameAssemblerV2Ref fa=nullptr);

  seastar::future<FrameAssemblerV2Ref> wait_io_exit_dispatching();

  void reset_session(bool full);

  void requeue_out_sent_up_to(seq_num_t seq);

  void requeue_out_sent();

  bool is_out_queued_or_sent() const {
    return is_out_queued() || !out_sent_msgs.empty();
  }

  seq_num_t get_in_seq() const {
    return in_seq;
  }

  void dispatch_accept();

  void dispatch_connect();

 private:
  void dispatch_reset(bool is_replace);

  void dispatch_remote_reset();

  bool is_out_queued() const {
    return (!out_pending_msgs.empty() ||
            ack_left > 0 ||
            need_keepalive ||
            next_keepalive_ack.has_value());
  }

  void reset_out();

  seastar::future<stop_t> try_exit_out_dispatch();

  seastar::future<> do_out_dispatch();

  ceph::bufferlist sweep_out_pending_msgs_to_sent(
      bool require_keepalive,
      std::optional<utime_t> maybe_keepalive_ack,
      bool require_ack);

  void notify_out_dispatch();

  void ack_out_sent(seq_num_t seq);

  seastar::future<> read_message(utime_t throttle_stamp, std::size_t msg_size);

  void do_in_dispatch();

private:
  ChainedDispatchers &dispatchers;

  SocketConnection &conn;

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
  std::deque<MessageURef> out_pending_msgs;

  // messages sent, but not yet acked by peer
  std::deque<MessageURef> out_sent_msgs;

  bool need_keepalive = false;

  std::optional<utime_t> next_keepalive_ack = std::nullopt;

  uint64_t ack_left = 0;

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
    std::ostream& out, Protocol::io_stat_printer stat) {
  stat.protocol.print_io_stat(out);
  return out;
}

} // namespace crimson::net

template <>
struct fmt::formatter<crimson::net::Protocol::io_state_t>
  : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(crimson::net::Protocol::io_state_t state, FormatContext& ctx) {
    using enum crimson::net::Protocol::io_state_t;
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
