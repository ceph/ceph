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


#ifndef CEPH_AUTHSESSIONHANDLER_H
#define CEPH_AUTHSESSIONHANDLER_H

#include "include/types.h"
#include "Auth.h"

#define SESSION_SIGNATURE_FAILURE -1

// Defines the security applied to ongoing messages in a session, once the session is established. PLR

class CephContext;
class Message;

struct AuthSessionHandler {
  virtual ~AuthSessionHandler() = default;
  virtual int sign_message(Message *message) = 0;
  virtual int check_message_signature(Message *message) = 0;
};

struct DummyAuthSessionHandler : AuthSessionHandler {
  int sign_message(Message*) final {
    return 0;
  }
  int check_message_signature(Message*) final {
    return 0;
  }
};

// TODO: make this a static member of AuthSessionHandler.
extern AuthSessionHandler *get_auth_session_handler(
  CephContext *cct, int protocol,
  const CryptoKey& key,
  uint64_t features);


struct AuthStreamHandler {
  virtual ~AuthStreamHandler() = default;
  //virtual ceph::bufferlist authenticated_encrypt(ceph::bufferlist& in) = 0;
  //virtual ceph::bufferlist authenticated_decrypt(ceph::bufferlist& in) = 0;

  // TODO: kill the dummies
  int encrypt_bufferlist(bufferlist &in, bufferlist &out) {
    return 0;
  }
  int decrypt_bufferlist(bufferlist &in, bufferlist &out) {
    return 0;
  }

  static std::unique_ptr<AuthStreamHandler> create_stream_handler(
    CephContext* ctx,
    const class AuthConnectionMeta& auth_meta);
};

#endif
