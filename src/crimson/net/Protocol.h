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

  seastar::future<> send(MessageRef msg);

  seastar::future<> keepalive();

  virtual void start_connect(const entity_addr_t& peer_addr,
                             const entity_type_t& peer_type) = 0;

  virtual void start_accept(SocketFRef&& socket,
                            const entity_addr_t& peer_addr) = 0;

 protected:
  Protocol(proto_t type,
           Dispatcher& dispatcher,
           SocketConnection& conn);

  virtual void trigger_close() = 0;

  // encode/write a message
  virtual seastar::future<> write_message(MessageRef msg) = 0;

  virtual seastar::future<> do_keepalive() = 0;

  virtual seastar::future<> do_keepalive_ack() = 0;

 public:
  const proto_t proto_type;

 protected:
  Dispatcher &dispatcher;
  SocketConnection &conn;

  SocketFRef socket;
  seastar::gate pending_dispatch;
  AuthConnectionMetaRef auth_meta;

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

  void notify_keepalive_ack();

 private:
  write_state_t write_state = write_state_t::none;
  // wait until current state changed
  seastar::shared_promise<> state_changed;

  bool closed = false;
  // become valid only after closed == true
  seastar::shared_future<> close_ready;

  bool need_keepalive = false;
  bool need_keepalive_ack = false;
  bool write_dispatching = false;
  void write_event();
};

} // namespace ceph::net
