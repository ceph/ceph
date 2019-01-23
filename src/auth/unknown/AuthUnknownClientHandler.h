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

#ifndef CEPH_AUTHUNKNOWNCLIENTHANDLER_H
#define CEPH_AUTHUNKNOWNCLIENTHANDLER_H

#include "auth/AuthClientHandler.h"
#include "AuthUnknownProtocol.h"

class CephContext;

class AuthUnknownClientHandler : public AuthClientHandler {
public:
  AuthUnknownClientHandler(CephContext *cct_, RotatingKeyRing *rkeys) 
    : AuthClientHandler(cct_) {}

  void reset() { }

  void prepare_build_request() {}
  int build_request(bufferlist& bl) const { return 0; }
  int handle_response(int ret, bufferlist::iterator& iter,
		      CryptoKey *session_key,
		      std::string *connection_secret) { return 0; }
  bool build_rotating_request(bufferlist& bl) const { return false; }

  int get_protocol() const { return CEPH_AUTH_UNKNOWN; }
  
  AuthAuthorizer *build_authorizer(uint32_t service_id) const {
    RWLock::RLocker l(lock);
    AuthUnknownAuthorizer *auth = new AuthUnknownAuthorizer();
    if (auth) {
      auth->build_authorizer(cct->_conf->name, global_id);
    }
    return auth;
  }

  bool need_tickets() { return false; }

  void set_global_id(uint64_t id) {
    RWLock::WLocker l(lock);
    global_id = id;
  }
private:
  void validate_tickets() { }
};

#endif
