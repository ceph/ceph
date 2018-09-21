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
  Protocol *const execute_next(state_t state) {
    ceph_assert(this == s->protocol);
    int index = static_cast<int>(state);
    Protocol *const &p = protocols[index];
    ceph_assert(p->state()==state);
    p->execute();
    return p;
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
  virtual seastar::future<bool> send(MessageRef msg) = 0;
  /// try send keepalive
  virtual seastar::future<bool> keepalive() = 0;
  /// try replaceing
  virtual bool replace(const Protocol *newp) = 0;

  /// execute protocol logic of the current state
  void execute() {
    ceph_assert(this != s->protocol);
    s->protocol = this;
    seastar::with_gate(s->dispatch_gate, [this] {
        execute_ready = do_execute()
          .handle_exception([this] (std::exception_ptr eptr) {
              // we should not leak exceptions in do_execute()
              ceph_assert(false);
            });
        return wait();
      });
  }
  /// wait for execution
  seastar::future<> wait() {
    return execute_ready.get_future();
  }
};

} // namespace ceph::net
