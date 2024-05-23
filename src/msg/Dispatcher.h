// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_DISPATCHER_H
#define CEPH_DISPATCHER_H

#include <memory>
#include "include/buffer_fwd.h"
#include "include/ceph_assert.h"
#include "include/common_fwd.h"
#include "msg/MessageRef.h"

class Messenger;
class Connection;
class CryptoKey;
class KeyStore;

class Dispatcher {
public:
  /* Ordering of dispatch for a list of Dispatchers. */
  using priority_t = uint32_t;
  static constexpr priority_t PRIORITY_HIGH = std::numeric_limits<priority_t>::max() / 4;
  static constexpr priority_t PRIORITY_DEFAULT = std::numeric_limits<priority_t>::max() / 2;
  static constexpr priority_t PRIORITY_LOW = (std::numeric_limits<priority_t>::max() / 4) * 3;

  explicit Dispatcher(CephContext *cct_)
    : cct(cct_)
  {
  }
  virtual ~Dispatcher() = default;

  /**
   * The Messenger calls this function to query if you are capable
   * of "fast dispatch"ing a message. Indicating that you can fast
   * dispatch it requires that you:
   * 1) Handle the Message quickly and without taking long-term contended
   * locks. (This function is likely to be called in-line with message
   * receipt.)
   * 2) Be able to accept the Message even if you have not yet received
   * an ms_handle_accept() notification for the Connection it is associated
   * with, and even if you *have* called mark_down() or received an
   * ms_handle_reset() (or similar) call on the Connection. You will
   * not receive more than one dead "message" (and should generally be
   * prepared for that circumstance anyway, since the normal dispatch can begin,
   * then trigger Connection failure before it's percolated through your system).
   * We provide ms_handle_fast_[connect|accept] calls if you need them, under
   * similar speed and state constraints as fast_dispatch itself.
   * 3) Be able to make a determination on fast_dispatch without relying
   * on particular system state -- the ms_can_fast_dispatch() call might
   * be called multiple times on a single message; the state might change between
   * calling ms_can_fast_dispatch and ms_fast_dispatch; etc.
   *
   * @param m The message we want to fast dispatch.
   * @returns True if the message can be fast dispatched; false otherwise.
   */
  virtual bool ms_can_fast_dispatch(const Message *m) const { return false; }
  virtual bool ms_can_fast_dispatch2(const MessageConstRef& m) const {
    return ms_can_fast_dispatch(m.get());
  }
  /**
   * This function determines if a dispatcher is included in the
   * list of fast-dispatch capable Dispatchers.
   * @returns True if the Dispatcher can handle any messages via
   * fast dispatch; false otherwise.
   */
  virtual bool ms_can_fast_dispatch_any() const { return false; }
  /**
   * Perform a "fast dispatch" on a given message. See
   * ms_can_fast_dispatch() for the requirements.
   *
   * @param m The Message to fast dispatch.
   */
  virtual void ms_fast_dispatch(Message *m) { ceph_abort(); }

  /* ms_fast_dispatch2 because otherwise the child must define both */
  virtual void ms_fast_dispatch2(const MessageRef &m) {
    /* allow old style dispatch handling that expects a Message * with a floating ref */
    return ms_fast_dispatch(MessageRef(m).detach()); /* XXX N.B. always consumes ref */
  }

  /**
   * Let the Dispatcher preview a Message before it is dispatched. This
   * function is called on *every* Message, prior to the fast/regular dispatch
   * decision point, but it is only used on fast-dispatch capable systems. An
   * implementation of ms_fast_preprocess must be essentially lock-free in the
   * same way as the ms_fast_dispatch function is (in particular, ms_fast_preprocess
   * may be called while the Messenger holds internal locks that prevent progress from
   * other threads, so any locks it takes must be at the very bottom of the hierarchy).
   * Messages are delivered in receipt order within a single Connection, but there are
   * no guarantees across Connections. This makes it useful for some limited
   * coordination between Messages which can be fast_dispatch'ed and those which must
   * go through normal dispatch.
   *
   * @param m A message which has been received
   */
  virtual void ms_fast_preprocess(Message *m) {}

  /* ms_fast_preprocess2 because otherwise the child must define both */
  virtual void ms_fast_preprocess2(const MessageRef &m) {
    /* allow old style dispatch handling that expects a Message* */
    return ms_fast_preprocess(m.get());
  }

  /**
   * The Messenger calls this function to deliver a single message.
   *
   * @param m The message being delivered. You (the Dispatcher)
   * are given a single reference count on it.
   */
  virtual bool ms_dispatch(Message *m) {
    ceph_abort();
  }

  /* ms_dispatch2 because otherwise the child must define both */
  virtual bool ms_dispatch2(const MessageRef &m) {
    /* allow old style dispatch handling that expects a Message * with a floating ref */
    MessageRef mr(m);
    if (ms_dispatch(mr.get())) {
      mr.detach(); /* dispatcher consumed ref */
      return true;
    }
    return false;
  }

  /**
   * This function will be called whenever a Connection is newly-created
   * or reconnects in the Messenger.
   *
   * @param con The new Connection which has been established. You are not
   * granted a reference to it -- take one if you need one!
   */
  virtual void ms_handle_connect(Connection *con) {}

  /**
   * This function will be called synchronously whenever a Connection is
   * newly-created or reconnects in the Messenger, if you support fast
   * dispatch. It is guaranteed to be called before any messages are
   * dispatched.
   *
   * @param con The new Connection which has been established. You are not
   * granted a reference to it -- take one if you need one!
   */
  virtual void ms_handle_fast_connect(Connection *con) {}

  /**
   * Callback indicating we have accepted an incoming connection.
   *
   * @param con The (new or existing) Connection associated with the session
   */
  virtual void ms_handle_accept(Connection *con) {}

  /**
   * Callback indicating we have accepted an incoming connection, if you
   * support fast dispatch. It is guaranteed to be called before any messages
   * are dispatched.
   *
   * @param con The (new or existing) Connection associated with the session
   */
  virtual void ms_handle_fast_accept(Connection *con) {}

  /*
   * this indicates that the ordered+reliable delivery semantics have
   * been violated.  Messages may have been lost due to a fault
   * in the network connection.
   * Only called on lossy Connections.
   *
   * @param con The Connection which broke. You are not granted
   * a reference to it.
   */
  virtual bool ms_handle_reset(Connection *con) = 0;

  /**
   * This indicates that the ordered+reliable delivery semantics
   * have been violated because the remote somehow reset.
   * It implies that incoming messages were dropped, and
   * probably some of our previous outgoing messages were too.
   *
   * @param con The Connection which broke. You are not granted
   * a reference to it.
   */
  virtual void ms_handle_remote_reset(Connection *con) = 0;

  /**
   * This indicates that the connection is both broken and further
   * connection attempts are failing because other side refuses
   * it.
   *
   * @param con The Connection which broke. You are not granted
   * a reference to it.
   */
  virtual bool ms_handle_refused(Connection *con) = 0;

  /**
   * @defgroup Authentication
   * @{
   */

  /**
   * handle successful authentication (msgr2)
   *
   * Authenticated result/state will be attached to the Connection. This is
   * called via the MonClient.
   *
   * Do not acquire locks in this method! It is considered "fast" delivery.
   *
   * return 1 for success
   * return 0 for no action (let another Dispatcher handle it)
   * return <0 for failure (failure to parse caps, for instance)
   */
  virtual int ms_handle_fast_authentication(Connection *con) {
    return 0;
  }

  /**
   * @} //Authentication
   */

protected:
  CephContext *cct;
private:
  explicit Dispatcher(const Dispatcher &rhs);
  Dispatcher& operator=(const Dispatcher &rhs);
};

#endif
