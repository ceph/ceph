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
class AuthClient;
class AuthClientHandler;

class AuthClientProtocolHandler {
protected:
  AuthAuthorizer authorizer;
  AuthClientHandler *client;

public:
  AuthClientProtocolHandler(AuthClientHandler *c) : client(c) {}
  virtual ~AuthClientProtocolHandler() {}

  virtual int get_protocol() = 0;

  virtual void reset() = 0;
  virtual int build_request(bufferlist& bl) = 0;
  virtual int handle_response(int ret, bufferlist::iterator& iter) = 0;
};



// ----------------------------------------

class AuthClientHandler {
  friend class AuthClientProtocolHandler;

  Mutex lock;

  AuthClient *client;
  AuthClientProtocolHandler *proto;

  AuthClientProtocolHandler *get_protocol_handler(int id);

public:
  EntityName name;
  uint32_t want;
  uint32_t have;

  AuthTicketManager tickets;

  AuthClientHandler(AuthClient *c) : lock("AuthClientHandler::lock"),
				     client(c), proto(NULL),
				     want(0), have(0) { }

  void init(EntityName& n) { name = n; }
  
  void set_want_keys(__u32 keys) {
    Mutex::Locker l(lock);
    want = keys;
  }
  void add_want_keys(__u32 keys) {
    Mutex::Locker l(lock);
    want |= keys;
  }   

  bool have_keys(__u32 k) {
    Mutex::Locker l(lock);
    return (k & have) == have;
  }
  bool have_keys() {
    Mutex::Locker l(lock);
    return (want & have) == have;
  }

  void reset() {
    if (proto)
      proto->reset();
  }
  int send_request();
  int handle_response(MAuthReply *m);

  void tick();

  bool build_authorizer(uint32_t service_id, AuthAuthorizer& authorizer);
};


#endif

