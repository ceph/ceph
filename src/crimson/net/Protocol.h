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
      std::optional<utime_t> keepalive_ack) = 0;

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
    write_state = state;
    state_changed.set_value();
    state_changed = seastar::shared_promise<>();
  }

  void notify_keepalive_ack(utime_t keepalive_ack);

  bool is_queued() const {
    return (!conn.out_q.empty() ||
            need_keepalive ||
            keepalive_ack.has_value());
  }

 private:
  write_state_t write_state = write_state_t::none;
  // wait until current state changed
  seastar::shared_promise<> state_changed;

  bool need_keepalive = false;
  std::optional<utime_t> keepalive_ack = std::nullopt;
  bool write_dispatching = false;
  seastar::future<stop_t> do_write_dispatch_sweep();
  void write_event();
};

} // namespace ceph::net
