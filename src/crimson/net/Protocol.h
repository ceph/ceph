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

#include "Fwd.h"
#include "Session.h"
#include "Messenger.h"

#include <seastar/core/shared_future.hh>

namespace ceph::net {

enum class state_t {
  none,
  accept,
  connect,
  open,
  // standby,
  // wait,
  close,
  LAST
};

class Protocol
{
  Protocol *const *const protocols;
  seastar::shared_future<> execute_ready;

 protected:
  Session *const s;
  Connection *const managed_conn;
  Dispatcher *const dispatcher;
  Messenger *const messenger;

  /// trigger state transition to the next state
  Protocol *const execute_next(state_t state, bool gated=true) {
    ceph_assert(this == s->protocol);
    int index = static_cast<int>(state);
    Protocol *const &p = protocols[index];
    ceph_assert(p->state()==state);
    p->execute(gated);
    return p;
  }

  /// execute protocol logic of the current state
  void execute(bool gated) {
    ceph_assert(this != s->protocol);
    s->protocol = this;
    if (gated) {
      seastar::with_gate(s->dispatch_gate, [this] {
          execute_ready = do_execute()
            .handle_exception([this] (std::exception_ptr eptr) {
                // we should not leak exceptions in do_execute()
                ceph_assert(false);
              });
          return wait();
        });
    } else {
      execute_ready = do_execute()
        .handle_exception([this] (std::exception_ptr eptr) {
            // we should not leak exceptions in do_execute()
            ceph_assert(false);
          });
    }
  }

  /// do execute protocol logic, no exception allowed
  virtual seastar::future<> do_execute() = 0;

 public:
  Protocol(Protocol *const *protos,
           Session *_s,
           Connection *conn,
           Dispatcher *disp,
           Messenger *msgr)
    : protocols(protos), s(_s), managed_conn(conn), dispatcher(disp), messenger(msgr) {};
  virtual ~Protocol() {};

  virtual const state_t& state() const = 0;
  /// try send message
  virtual seastar::future<seastar::stop_iteration> send(MessageRef msg) = 0;
  /// try send keepalive
  virtual seastar::future<seastar::stop_iteration> keepalive() = 0;
  /// try replaceing
  virtual bool replace(const Protocol *newp) = 0;

  /// wait for execution
  seastar::future<> wait() {
    return execute_ready.get_future();
  }

  seastar::future<> close() {
    // unregister_conn() drops a reference, so hold another until completion
    auto cleanup = [conn = ConnectionRef(managed_conn)] {};
    if (s->protocol->state() == state_t::accept) {
      messenger->unaccept_conn(managed_conn);
      execute_next(state_t::close, false);
    } else if (s->protocol->state() >= state_t::connect &&
             s->protocol->state() <= state_t::open) {
      messenger->unregister_conn(managed_conn);
      execute_next(state_t::close, false);
    } else if (s->protocol->state() == state_t::close) {
      //already closed
    } else {
      ceph_assert(false);
    }
    return s->protocol->wait().finally(std::move(cleanup));
  }
};

} // namespace ceph::net
