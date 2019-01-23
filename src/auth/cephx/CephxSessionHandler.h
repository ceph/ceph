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
  CephContext *cct;
  int protocol;
  CryptoKey key;                // per mon authentication
  uint64_t features;

  int _calc_signature(Message *m, uint64_t *psig);

public:
  CephxSessionHandler(CephContext *cct,
		      const CryptoKey& session_key,
		      const uint64_t features)
    : cct(cct),
      protocol(CEPH_AUTH_CEPHX),
      key(session_key),
      features(features) {
  }
  ~CephxSessionHandler() override = default;

  int sign_message(Message *m) override;
  int check_message_signature(Message *m) override ;
};

