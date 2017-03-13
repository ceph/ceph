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


#include "auth/AuthSessionHandler.h"
#include "auth/Auth.h"

class CephContext;
class Message;

class CephxSessionHandler  : public AuthSessionHandler {
  uint64_t features;

public:
  CephxSessionHandler(CephContext *cct_, CryptoKey session_key, uint64_t features)
    : AuthSessionHandler(cct_, CEPH_AUTH_CEPHX, session_key),
      features(features) {}
  ~CephxSessionHandler() override {}
  
  bool no_security() override {
    return false;
  }

  int _calc_signature(Message *m, uint64_t *psig);

  int sign_message(Message *m) override;
  int check_message_signature(Message *m) override ;

  // Cephx does not currently encrypt messages, so just return 0 if called.  PLR

  int encrypt_message(Message *m) override {
    return 0;
  }

  int decrypt_message(Message *m) override {
    return 0;
  }

};

