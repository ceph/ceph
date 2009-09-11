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
#include "auth/AuthClient.h"

#include "common/Mutex.h"
#include "common/Cond.h"

#include "common/Timer.h"

class MAuthReply;
class Message;
class AuthClient;

class AuthorizeContextMap {
  map<int, AuthorizeContext> m;

  Mutex lock;
  int max_id;

public:
  AuthorizeContextMap() : lock("AuthorizeMap") {}
  AuthorizeContext& create();
  void remove(int id);
  AuthorizeContext *get(int id);
};

class AuthClientHandler {
  Mutex lock;
  Cond keys_cond;
  Cond *cur_request_cond;
  Context *timeout_event;

  uint32_t want;
  uint32_t have;

  // session state
  int request_state;
  int response_state;

  int status;

  int cephx_request_state;
  int cephx_response_state;

  bool got_response;
  bool got_timeout;

  EntityName name;
  entity_addr_t addr;

  /* envelope protocol parameters */
  uint64_t server_challenge;

  /* ceph-x protocol */
  utime_t auth_ts;
  AuthTicketsManager tickets;

  CryptoKey secret;

  AuthClient *client;

  AuthorizeContextMap context_map;

  bool request_pending();
  Message *build_request();

  int generate_request(bufferlist& bl);
  int handle_response(Message *response);

  /* cephx requests */
  int generate_cephx_authenticate_request(bufferlist& bl);
  int generate_cephx_authorize_request(uint32_t service_id, bufferlist& bl);

  /* cephx responses */
  int handle_cephx_response(bufferlist::iterator& indata);

  void _reset() {
    request_state = 0;
    response_state = 0;
    status = 0;
    cephx_request_state = 0;
    cephx_response_state = 0;
    got_response = false;
    got_timeout = false;
    timeout_event = NULL;
    cur_request_cond = NULL;
  }

  SafeTimer timer;

 class C_OpTimeout : public Context {
  protected:
    AuthClientHandler *client_handler;
    double timeout;
  public:
    C_OpTimeout(AuthClientHandler *handler, double to) :
                                        client_handler(handler), timeout(to) {
    }
    void finish(int r) {
      if (r >= 0) client_handler->_request_timeout(timeout);
    }
  };

  void _request_timeout(double timeout);
  int _do_request(double timeout);

public:
  AuthClientHandler() : lock("AuthClientHandler::lock"),
			want(0), have(0), client(NULL), timer(lock) {
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
      keys_cond.WaitInterval(lock, t);
    return (want & have) == have;
  }

  int start_session(AuthClient *client, double timeout);
  int authorize(uint32_t service_id);
  void handle_auth_reply(MAuthReply *m);
  void tick();
};


#endif

