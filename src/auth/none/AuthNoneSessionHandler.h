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

#include "../AuthSessionHandler.h"
#include "../Auth.h"

#define dout_subsys ceph_subsys_auth

class CephContext;

class AuthNoneSessionHandler  : public AuthSessionHandler {
public:
  AuthNoneSessionHandler(CephContext *cct_, CryptoKey session_key)
    : AuthSessionHandler(cct_, CEPH_AUTH_NONE, session_key) {}
  ~AuthNoneSessionHandler() {}
  
  bool no_security() {
    return true;
  }

  // The None suite neither signs nor encrypts messages, so these functions just return success.
  // Since nothing was signed or encrypted, don't increment the stats.  PLR

  int sign_message(Message *m) {
    return 0;
  }

  int check_message_signature(Message *m) {
    return 0;
  }

  int encrypt_message(Message *m) {
    return 0;
  }

  int decrypt_message(Message *m) {
    return 0;
  }

};

