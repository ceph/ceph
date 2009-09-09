// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __AUTHCLIENTHANDLER_H
#define __AUTHCLIENTHANDLER_H


#include "auth/Auth.h"

#include "common/Mutex.h"
#include "common/Cond.h"

class MAuthReply;

class AuthClientHandler {
  Mutex lock;
  Cond cond;

  uint32_t want;
  uint32_t have;

  // session state
  int request_state;
  int response_state;

  int status;

  int cephx_request_state;
  int cephx_response_state;

  EntityName name;
  entity_addr_t addr;

  /* envelope protocol parameters */
  uint64_t server_challenge;

  /* ceph-x protocol */
  utime_t auth_ts;
  AuthTicketsManager tickets;

  CryptoKey secret;

  int generate_cephx_protocol_request(bufferlist& bl);
  int handle_cephx_protocol_response(bufferlist::iterator& indata);

  void _reset() {
    request_state = 0;
    response_state = 0;
    status = 0;
    cephx_request_state = 0;
    cephx_response_state = 0;
  }


public:
  AuthClientHandler() : lock("AuthClientHandler::lock"),
			want(0), have(0) {
    _reset();
  }
  
  void set_want_keys(__u32 keys) {
    Mutex::Locker l(lock);
    want = keys;
  }
  bool have_keys(__u32 k) {
    Mutex::Locker l(lock);
    return (k & have) == have;
  }
  bool have_keys() {
    Mutex::Locker l(lock);
    return (want & have) == have;
  }
  bool wait_for_keys(double timeout) {
    Mutex::Locker l(lock);
    utime_t t;
    t += timeout;
    while ((want & have) != have)
      cond.WaitInterval(lock, t);
    return (want & have) == have;
  }

  void start_session();
  void handle_auth_reply(MAuthReply *m);
  void tick();
};


#endif

