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
protected:
  CephContext *cct;
  int protocol;
  CryptoKey key;                  // per mon authentication
  std::string connection_secret;  // per connection

public:
  explicit AuthSessionHandler(CephContext *cct_) : cct(cct_), protocol(CEPH_AUTH_UNKNOWN) {}

  AuthSessionHandler(CephContext *cct_, int protocol_,
		     const CryptoKey& key_,
		     const std::string& cs_)
    : cct(cct_),
      protocol(protocol_),
      key(key_),
      connection_secret(cs_) {}
  virtual ~AuthSessionHandler() { }

  virtual bool no_security() = 0;
  virtual int sign_message(Message *message) = 0;
  virtual int check_message_signature(Message *message) = 0;
  virtual int encrypt_message(Message *message) = 0;
  virtual int decrypt_message(Message *message) = 0;

  virtual int sign_bufferlist(bufferlist &in, bufferlist &out) {
    return 0;
  };
  virtual int encrypt_bufferlist(bufferlist &in, bufferlist &out) {
    return 0;
  }
  virtual int decrypt_bufferlist(bufferlist &in, bufferlist &out) {
    return 0;
  }

  int get_protocol() {return protocol;}
  CryptoKey get_key() {return key;}

};

extern AuthSessionHandler *get_auth_session_handler(
  CephContext *cct, int protocol,
  const CryptoKey& key,
  const std::string& connection_secret,
  uint64_t features);

#endif
