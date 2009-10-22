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

class Message;
class AuthClient;

class AuthClientHandler;

class AuthClientProtocolHandler {
 class C_OpTimeout : public Context {
  protected:
    AuthClientProtocolHandler *client;
    double timeout;
  public:
    C_OpTimeout(AuthClientProtocolHandler *handler, double to) :
                                        client(handler), timeout(to) {
    }
    void finish(int r) {
      if (r >= 0) client->_request_timeout(timeout);
    }
  };

protected:
  AuthClientHandler *client;
  Message *msg;
  bool got_response;
  bool got_timeout;
  Context *timeout_event;
  uint32_t id;
  Mutex lock;
  AuthAuthorizer authorizer;

  // session state
  int status;

  virtual void _reset() {}

  Cond cond;

  virtual int _handle_response(int ret, bufferlist::iterator& iter) = 0;
  virtual int _build_request() = 0;
  virtual Message *_get_new_msg() = 0;
  virtual bufferlist& _get_msg_bl(Message *m) = 0;

  void _request_timeout(double timeout);
public:
  AuthClientProtocolHandler(AuthClientHandler *ch);
  virtual ~AuthClientProtocolHandler();
  int build_request();

  int handle_response(int ret, bufferlist::iterator& iter);

  void reset() {
    status = 0;
    _reset();
  }

 int do_async_request(double timeout);
};

class AuthClientAuthenticateHandler : public AuthClientProtocolHandler {
  int request_state;
  int response_state;

  int cephx_request_state;
  int cephx_response_state;

  /* envelope protocol parameters */
  uint64_t server_challenge;

  /* envelope protocol */
  int generate_authenticate_request(bufferlist& bl);
  /* auth protocol */
  int generate_cephx_authenticate_request(bufferlist& bl);
  int handle_cephx_response(bufferlist::iterator& indata);

  uint32_t want;
  uint32_t have;

protected:
  void _reset() {
    request_state = 0;
    response_state = 0;
    cephx_request_state = 0;
    cephx_response_state = 0;
    timeout_event = NULL;
  }

  bool request_pending();

  int _build_request();
  int _handle_response(int ret, bufferlist::iterator& iter);
  Message *_get_new_msg();
  bufferlist& _get_msg_bl(Message *m);
public:
  AuthClientAuthenticateHandler(AuthClientHandler *client, uint32_t _want, uint32_t _have) :
             AuthClientProtocolHandler(client), want(_want), have(_have) { reset(); }
  void set_want_keys(__u32 keys) {
    want = keys;
  }
  void add_want_keys(__u32 keys) {
    want |= keys;
  }
};

class AuthClientAuthorizeHandler : public AuthClientProtocolHandler {
  uint32_t service_id;
protected:
  int _build_request();
  int _handle_response(int ret, bufferlist::iterator& iter);
  Message *_get_new_msg();
  bufferlist& _get_msg_bl(Message *m);
public:
  AuthClientAuthorizeHandler(AuthClientHandler *client, uint32_t sid) : AuthClientProtocolHandler(client), service_id(sid) {}
};

class AuthClientHandler {
  friend class AuthClientProtocolHandler;

  Mutex lock;
  Cond keys_cond;


  /* ceph-x protocol */
  utime_t auth_ts;

  AuthClient *client;

  Message *build_authenticate_request();


  SafeTimer timer;

  uint32_t max_proto_handlers;
  map<uint32_t, AuthClientProtocolHandler *> handlers_map;

  AuthClientProtocolHandler *_get_proto_handler(uint32_t id);
  uint32_t _add_proto_handler(AuthClientProtocolHandler *handler);

  void _authenticate_request_timeout(double timeout);
  int _do_authenticate_request(double timeout);
public:
  EntityName name;
  entity_addr_t addr;
  uint32_t want;
  uint32_t have;
  CryptoKey secret;

  AuthTicketManager tickets;

  AuthClientHandler() : lock("AuthClientHandler::lock"),
			client(NULL), timer(lock), max_proto_handlers(0) { }
  void init(EntityName& n) { name = n; }
  
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

  int handle_response(int trans_id, Message *response);

  int send_session_request(AuthClient *client, AuthClientProtocolHandler *handler, double timeout);
  void tick();

  int build_authorizer(uint32_t service_id, AuthAuthorizer& authorizer);
};


#endif

