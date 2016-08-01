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

#include "include/assert.h"
#include "include/buffer_fwd.h"
#include "include/assert.h"

class Messenger;
class Message;
class Connection;
class AuthAuthorizer;
class CryptoKey;
class CephContext;

class Dispatcher {
public:
  explicit Dispatcher(CephContext *cct_)
    : cct(cct_)
  {
  }
  virtual ~Dispatcher() { }

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
  virtual bool ms_can_fast_dispatch(Message *m) const { return false;}
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
  virtual void ms_fast_dispatch(Message *m) { assert(0); }
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
  /**
   * The Messenger calls this function to deliver a single message.
   *
   * @param m The message being delivered. You (the Dispatcher)
   * are given a single reference count on it.
   */
  virtual bool ms_dispatch(Message *m) = 0;

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
   * @defgroup Authentication
   * @{
   */
  /**
   * Retrieve the AuthAuthorizer for the given peer type. It might not
   * provide one if it knows there is no AuthAuthorizer for that type.
   *
   * @param dest_type The peer type we want the authorizer for.
   * @param a Double pointer to an AuthAuthorizer. The Dispatcher will fill
   * in *a with the correct AuthAuthorizer, if it can. Make sure that you have
   * set *a to NULL before calling in.
   * @param force_new Force the Dispatcher to wait for a new set of keys before
   * returning the authorizer.
   *
   * @return True if this function call properly filled in *a, false otherwise.
   */
  virtual bool ms_get_authorizer(int dest_type, AuthAuthorizer **a, bool force_new) { return false; }
  /**
   * Verify the authorizer for a new incoming Connection.
   *
   * @param con The new incoming Connection
   * @param peer_type The type of the endpoint which initiated this Connection
   * @param protocol The ID of the protocol in use (at time of writing, cephx or none)
   * @param authorizer The authorization string supplied by the remote
   * @param authorizer_reply Output param: The string we should send back to
   * the remote to authorize ourselves. Only filled in if isvalid
   * @param isvalid Output param: True if authorizer is valid, false otherwise
   *
   * @return True if we were able to prove or disprove correctness of
   * authorizer, false otherwise.
   */
  virtual bool ms_verify_authorizer(Connection *con,
				    int peer_type,
				    int protocol,
				    ceph::bufferlist& authorizer,
				    ceph::bufferlist& authorizer_reply,
				    bool& isvalid,
				    CryptoKey& session_key) { return false; }
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
