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

#ifndef CEPH_AUTHNONECLIENTHANDLER_H
#define CEPH_AUTHNONECLIENTHANDLER_H

#include "auth/AuthClientHandler.h"
#include "AuthNoneProtocol.h"
#include "common/ceph_context.h"
#include "common/config.h"

template<LockPolicy lock_policy>
class AuthNoneClientHandler : public AuthClientHandler<lock_policy> {
  using AuthClientHandler<lock_policy>::cct;
  using AuthClientHandler<lock_policy>::global_id;
  using AuthClientHandler<lock_policy>::lock;

public:
  AuthNoneClientHandler(CephContext *cct_,
			RotatingKeyRing<lock_policy> *rkeys)
    : AuthClientHandler<lock_policy>(cct_) {}

  void reset() override { }

  void prepare_build_request() override {}
  int build_request(bufferlist& bl) const override { return 0; }
  int handle_response(int ret, bufferlist::const_iterator& iter) override { return 0; }
  bool build_rotating_request(bufferlist& bl) const override { return false; }

  int get_protocol() const override { return CEPH_AUTH_NONE; }
  
  AuthAuthorizer *build_authorizer(uint32_t service_id) const override {
    std::shared_lock l{lock};
    AuthNoneAuthorizer *auth = new AuthNoneAuthorizer();
    if (auth) {
      auth->build_authorizer(cct->_conf->name, global_id);
    }
    return auth;
  }

  bool need_tickets() override { return false; }

  void set_global_id(uint64_t id) override {
    std::unique_lock l{lock};
    global_id = id;
  }
private:
  void validate_tickets() override {}
};

#endif
