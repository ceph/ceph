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

#ifndef CEPH_AUTHNONEPROTOCOL_H
#define CEPH_AUTHNONEPROTOCOL_H

#include "../Auth.h"

struct AuthNoneAuthorizer : public AuthAuthorizer {
  AuthNoneAuthorizer() : AuthAuthorizer(CEPH_AUTH_NONE) { }
  bool build_authorizer(const EntityName &ename, uint64_t global_id) {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(ename, bl);
    ::encode(global_id, bl);
    return 0;
  }
  bool verify_reply(bufferlist::iterator& reply) { return true; }
};

#endif
