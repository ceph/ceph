// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corp.
 *
 * Author: Yingxin Cheng <yingxincheng@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "Dispatcher.h"
#include "Socket.h"

#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

namespace ceph::net {

class SocketConnection;
using SocketConnectionRef = boost::intrusive_ptr<SocketConnection>;
class SocketMessenger;
class Session;

class Protocol {
 protected:
  using seastop = seastar::stop_iteration;

  ////////////////////////// Protocol:States //////////////////////////
  class State
  {
    seastar::shared_future<> execute_ready;
    Protocol &_proto;
    const bool gated;

    /// execute protocol logic of the current state
    void execute() {
      ceph_assert(this != _proto.state);
      _proto.state = this;
      if (gated) {
        seastar::with_gate(_proto.dispatch_gate, [this] {
            execute_ready = do_execute()
              .handle_exception([this] (std::exception_ptr eptr) {
                  // we should not leak exceptions in do_execute()
                  ceph_assert(false);
                });
            return wait_execute();
          });
      } else {
        execute_ready = do_execute()
          .handle_exception([this] (std::exception_ptr eptr) {
              // we should not leak exceptions in do_execute()
              ceph_assert(false);
            });
      }
    }

   protected:
    Session &s;
    SocketConnection &conn;
    Dispatcher &dispatcher;
    SocketMessenger &messenger;

    State(Protocol &p, bool _gated=true)
      : _proto(p), gated(_gated), s(p.s), conn(p.conn), dispatcher(p.disp), messenger(p.msgr) {};

    /// trigger state transition to the next state
    void trigger(State *const state) {
      ceph_assert(this == _proto.state);
      state->execute();
    }

    /// state main execution, no except
    virtual seastar::future<> do_execute() = 0;

   public:
    virtual ~State() {};

    virtual bool is_connected() const = 0;
    /// try send message
    virtual seastar::future<seastop> send(MessageRef msg) = 0;
    /// try send keepalive
    virtual seastar::future<seastop> keepalive() = 0;

    /// trigger close state and unregister conn
    virtual void trigger_close() = 0;

    /// wait for state execution
    seastar::future<> wait_execute() {
      return execute_ready.get_future();
    }
  };

 ////////////////////////////// Protocol //////////////////////////////
 private:
  Session &s;
  SocketConnection &conn;
  Dispatcher &disp;
  SocketMessenger &msgr;

  State *state;
  seastar::future<> send_ready = seastar::now();
  bool closed = false;

 protected:
  seastar::gate dispatch_gate;
  std::optional<Socket> socket;

  Protocol(Session &_s,
           SocketConnection &_conn,
           Dispatcher &_disp,
           SocketMessenger &_msgr,
           State *_state)
    : s(_s), conn(_conn), disp(_disp), msgr(_msgr), state(_state) { }

  virtual void do_start_connect() = 0;
  virtual void do_start_accept() = 0;

 public:
  virtual ~Protocol() {
    ceph_assert(closed);
    ceph_assert(dispatch_gate.is_closed());
    // socket will check itself.
  };

  void start_connect(const entity_addr_t&, const entity_type_t&);

  void start_accept(seastar::connected_socket&&, const entity_addr_t&);

  bool is_connected() const {
    ceph_assert(state);
    return state->is_connected();
  }

  seastar::future<> send(MessageRef msg) {
    ceph_assert(state);
    // chain the message after the last message is sent
    seastar::shared_future<> f = send_ready.then(
      [this, msg = std::move(msg)] {
        return seastar::repeat([this, msg=std::move(msg)] {
            return state->send(msg);
          });
      });
    // chain any later messages after this one completes
    send_ready = f.get_future();
    // allow the caller to wait on the same future
    return f.get_future();
  }

  seastar::future<> keepalive() {
    ceph_assert(state);
    seastar::shared_future<> f = send_ready.then(
      [this] {
        return seastar::repeat([this] {
            return state->keepalive();
          });
      });
    send_ready = f.get_future();
    return f.get_future();
  }

  // Reentrant closing
  seastar::future<> close();
};

} // namespace ceph::net
