// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include "crimson/common/gated.h"
#include "crimson/common/log.h"
#include "Fwd.h"
#include "SocketConnection.h"

namespace crimson::net {

class Protocol {
 public:
  enum class proto_t {
    none,
    v1,
    v2
  };

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
    close(dispatch_reset);
    // it can happen if close_clean() is called inside Dispatcher::ms_handle_reset()
    // which will otherwise result in deadlock
    assert(close_ready.valid());
    return close_ready.get_future();
  }

  virtual void start_connect(const entity_addr_t& peer_addr,
                             const entity_name_t& peer_name) = 0;

  virtual void start_accept(SocketRef&& socket,
                            const entity_addr_t& peer_addr) = 0;

  virtual void print(std::ostream&) const = 0;
 protected:
  Protocol(proto_t type,
           ChainedDispatchersRef& dispatcher,
           SocketConnection& conn);

  virtual void trigger_close() = 0;

  virtual ceph::bufferlist do_sweep_messages(
      const std::deque<MessageRef>& msgs,
      size_t num_msgs,
      bool require_keepalive,
      std::optional<utime_t> keepalive_ack,
      bool require_ack) = 0;

  virtual void notify_write() {};

 public:
  const proto_t proto_type;
  SocketRef socket;

 protected:
  ChainedDispatchersRef dispatcher;
  SocketConnection &conn;

  AuthConnectionMetaRef auth_meta;

 private:
  bool closed = false;
  // become valid only after closed == true
  seastar::shared_future<> close_ready;

// the write state-machine
 public:
  seastar::future<> send(MessageRef msg);
  seastar::future<> keepalive();

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

  static const char* get_state_name(write_state_t state) {
    uint8_t index = static_cast<uint8_t>(state);
    static const char *const state_names[] = {"none",
                                              "delay",
                                              "open",
                                              "drop"};
    assert(index < std::size(state_names));
    return state_names[index];
  }

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

  bool is_queued() const {
    return (!conn.out_q.empty() ||
            ack_left > 0 ||
            need_keepalive ||
            keepalive_ack.has_value());
  }

  void ack_writes(seq_num_t seq);
  crimson::common::Gated gate;

 private:
  write_state_t write_state = write_state_t::none;
  // wait until current state changed
  seastar::shared_promise<> state_changed;

  bool need_keepalive = false;
  std::optional<utime_t> keepalive_ack = std::nullopt;
  uint64_t ack_left = 0;
  bool write_dispatching = false;
  // If another continuation is trying to close or replace socket when
  // write_dispatching is true and write_state is open,
  // it needs to wait for exit_open until writing is stopped or failed.
  std::optional<seastar::shared_promise<>> exit_open;

  seastar::future<stop_t> try_exit_sweep();
  seastar::future<> do_write_dispatch_sweep();
  void write_event();
};

inline std::ostream& operator<<(std::ostream& out, const Protocol& proto) {
  proto.print(out);
  return out;
}


} // namespace crimson::net
