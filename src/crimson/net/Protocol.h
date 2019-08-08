// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include "Fwd.h"
#include "SocketConnection.h"

namespace ceph::net {

class Protocol {
 public:
  enum class proto_t {
    none,
    v1,
    v2
  };

  Protocol(Protocol&&) = delete;
  virtual ~Protocol();

  bool is_connected() const;

  // Reentrant closing
  seastar::future<> close();

  virtual void start_connect(const entity_addr_t& peer_addr,
                             const entity_type_t& peer_type) = 0;

  virtual void start_accept(SocketFRef&& socket,
                            const entity_addr_t& peer_addr) = 0;

 protected:
  Protocol(proto_t type,
           Dispatcher& dispatcher,
           SocketConnection& conn);

  virtual void trigger_close() = 0;

  virtual ceph::bufferlist do_sweep_messages(
      const std::deque<MessageRef>& msgs,
      size_t num_msgs,
      bool require_keepalive,
      std::optional<utime_t> keepalive_ack,
      bool require_ack) = 0;

 public:
  const proto_t proto_type;

 protected:
  Dispatcher &dispatcher;
  SocketConnection &conn;

  SocketFRef socket;
  seastar::gate pending_dispatch;
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
  enum class write_state_t {
    none,
    delay,
    open,
    drop
  };
  void set_write_state(const write_state_t& state) {
    if (write_state == write_state_t::open &&
        state == write_state_t::delay) {
      if (open_write) {
        exit_open = seastar::shared_promise<>();
      }
    }
    if (state == write_state_t::drop && exit_open) {
      exit_open->set_value();
      exit_open = std::nullopt;
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

 private:
  write_state_t write_state = write_state_t::none;
  // wait until current state changed
  seastar::shared_promise<> state_changed;

  bool need_keepalive = false;
  std::optional<utime_t> keepalive_ack = std::nullopt;
  uint64_t ack_left = 0;
  bool write_dispatching = false;
  // Indicate if we are in the middle of writing.
  bool open_write = false;
  // If another continuation is trying to close or replace socket when
  // open_write is true, it needs to wait for exit_open until writing is
  // stopped or failed.
  std::optional<seastar::shared_promise<>> exit_open;

  seastar::future<stop_t> do_write_dispatch_sweep();
  void write_event();
};

} // namespace ceph::net
