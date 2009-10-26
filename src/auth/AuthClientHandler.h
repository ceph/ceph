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

#include "common/Timer.h"

class MAuthReply;
class AuthClientHandler;

class AuthClientHandler {
protected:
  EntityName name;
  uint32_t want;
  uint32_t have;
  uint32_t need;

public:
  AuthClientHandler() : want(CEPH_ENTITY_TYPE_AUTH), have(0), need(0) {}
  virtual ~AuthClientHandler() {}

  void init(EntityName& n) { name = n; }
  
  void set_want_keys(__u32 keys) {
    want = keys | CEPH_ENTITY_TYPE_AUTH;
    validate_tickets();
  }
  void add_want_keys(__u32 keys) {
    want |= keys;
    validate_tickets();
  }   

  bool have_keys(__u32 k) {
    validate_tickets();
    return (k & have) == have;
  }
  bool have_keys() {
    validate_tickets();
    return (want & have) == have;
  }


  virtual int get_protocol() = 0;

  virtual void reset() = 0;
  virtual int build_request(bufferlist& bl) = 0;
  virtual int handle_response(int ret, bufferlist::iterator& iter) = 0;
  virtual void build_rotating_request(bufferlist& bl) = 0;

  virtual void tick() = 0;

  virtual AuthAuthorizer *build_authorizer(uint32_t service_id) = 0;

  virtual void validate_tickets() = 0;
  virtual bool need_tickets() = 0;
};


extern AuthClientHandler *get_auth_client_handler(int proto);

#endif

