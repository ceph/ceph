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
#include "common/config.h"
#include "msg/Message.h"
#include "Auth.h"

#define SESSION_SIGNATURE_FAILURE -1

// Defines the security applied to ongoing messages in a session, once the session is established. PLR

class CephContext;
class KeyServer;

struct AuthSessionHandler {
protected:
  CephContext *cct;
  int protocol;
  CryptoKey key;

public:
  // Keep stats on how many messages were signed, how many messages were encrypted, how many 
  // signatures were properly checked, and how many messages were decrypted. PLR
  int messages_signed;
  int signatures_checked;
  int signatures_matched;
  int signatures_failed;
  int messages_encrypted;
  int messages_decrypted;

  explicit AuthSessionHandler(CephContext *cct_) : cct(cct_), protocol(CEPH_AUTH_UNKNOWN), messages_signed(0),
    signatures_checked(0), signatures_matched(0), signatures_failed(0), messages_encrypted(0),
    messages_decrypted(0) {}

  AuthSessionHandler(CephContext *cct_, int protocol_, CryptoKey key_) : cct(cct_), 
    protocol(protocol_), key(key_), messages_signed(0), signatures_checked(0), signatures_matched(0), 
    signatures_failed(0), messages_encrypted(0), messages_decrypted(0) {}
  virtual ~AuthSessionHandler() { }

  void print_auth_session_handler_stats() ;

  virtual bool no_security() = 0;
  virtual int sign_message(Message *message) = 0;
  virtual int check_message_signature(Message *message) = 0;
  virtual int encrypt_message(Message *message) = 0;
  virtual int decrypt_message(Message *message) = 0;

  int get_protocol() {return protocol;}
  CryptoKey get_key() {return key;}

};

extern AuthSessionHandler *get_auth_session_handler(CephContext *cct, int protocol, CryptoKey key,
						    uint64_t features);

#endif
