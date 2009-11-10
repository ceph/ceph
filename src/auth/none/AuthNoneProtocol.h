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

#ifndef __AUTHNONEPROTOCOL_H
#define __AUTHNONEPROTOCOL_H

#include "../Auth.h"

struct AuthNoneAuthorizer : public AuthAuthorizer {
  uint64_t global_id;

  AuthNoneAuthorizer() : AuthAuthorizer(CEPH_AUTH_NONE) { }
  bool build_authorizer() {
    ::encode(*g_conf.entity_name, bl);
    ::encode(global_id, bl);
    return 0;
  }
  bool verify_reply(bufferlist::iterator& reply) { return true; }
};

#endif
