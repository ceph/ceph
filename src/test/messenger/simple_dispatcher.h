// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef SIMPLEDISPATCHER_H_
#define SIMPLEDISPATCHER_H_

#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

class SimpleDispatcher: public Dispatcher {
private:
  bool active;
  Messenger *messenger;
  uint64_t dcount;
public:
  explicit SimpleDispatcher(Messenger *msgr);
  virtual ~SimpleDispatcher();

  uint64_t get_dcount() { return dcount; }

  void set_active() {
    active = true;
  };

  // how i receive messages
  virtual bool ms_dispatch(Message *m);

  /**
   * This function will be called whenever a new Connection is made to the
   * Messenger.
   *
   * @param con The new Connection which has been established. You are not
   * granted a reference to it -- take one if you need one!
   */
  virtual void ms_handle_connect(Connection *con) { };

  /**
   * Callback indicating we have accepted an incoming connection.
   *
   * @param con The (new or existing) Connection associated with the session
   */
  virtual void ms_handle_accept(Connection *con) { };

  /*
   * this indicates that the ordered+reliable delivery semantics have
   * been violated.  Messages may have been lost due to a fault
   * in the network connection.
   * Only called on lossy Connections or those you've
   * designated mark_down_on_empty().
   *
   * @param con The Connection which broke. You are not granted
   * a reference to it.
   */
  virtual bool ms_handle_reset(Connection *con);

  /**
   * This indicates that the ordered+reliable delivery semantics
   * have been violated because the remote somehow reset.
   * It implies that incoming messages were dropped, and
   * probably some of our previous outgoing messages were too.
   *
   * @param con The Connection which broke. You are not granted
   * a reference to it.
   */
  virtual void ms_handle_remote_reset(Connection *con);

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
  virtual bool ms_get_authorizer(int dest_type, AuthAuthorizer **a,
				 bool force_new) { return false; };

  /**
   * Verify the authorizer for a new incoming Connection.
   *
   * @param con The new incoming Connection
   * @param peer_type The type of the endpoint which initiated this Connection
   * @param protocol The ID of the protocol in use (at time of writing, cephx
   *  or none)
   * @param authorizer The authorization string supplied by the remote
   * @param authorizer_reply Output param: The string we should send back to
   * the remote to authorize ourselves. Only filled in if isvalid
   * @param isvalid Output param: True if authorizer is valid, false otherwise
   *
   * @return True if we were able to prove or disprove correctness of
   * authorizer, false otherwise.
   */
  virtual bool ms_verify_authorizer(Connection *con, int peer_type,
				    int protocol, bufferlist& authorizer,
				    bufferlist& authorizer_reply,
				    bool& isvalid, CryptoKey& session_key) {
    /* always succeed */
    isvalid = true;
    return true;
  };

};

#endif /* SIMPLEDISPATCHER_H_ */
