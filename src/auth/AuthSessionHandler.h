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

struct SHA256SignatureError : public std::exception {
  sha256_digest_t sig1;
  sha256_digest_t sig2;
  std::string reason;

  SHA256SignatureError(const char *sig1, const char *sig2)
      : sig1((const unsigned char *)sig1), sig2((const unsigned char *)sig2) {
    std::stringstream ss;
    ss << " signature mismatch: calc signature=" << this->sig1
       << " msg signature=" << this->sig2;
    reason = ss.str();
  }

  const char *what() const throw() { return reason.c_str(); }
};

struct DecryptionError : public std::exception {};

struct AuthStreamHandler {
  virtual ~AuthStreamHandler() = default;
  //virtual ceph::bufferlist authenticated_encrypt(ceph::bufferlist& in) = 0;
  //virtual ceph::bufferlist authenticated_decrypt(ceph::bufferlist& in) = 0;
  virtual void authenticated_encrypt(ceph::bufferlist& payload) = 0;
  virtual void authenticated_decrypt(char* payload, uint32_t& length) = 0;
  virtual std::size_t calculate_payload_size(std::size_t length) = 0;

  struct rxtx_t {
    //rxtx_t(rxtx_t&& r) : rx(std::move(rx)), tx(std::move(tx)) {}
    // Each peer can use different handlers.
    // Hmm, isn't that too much flexbility?
    std::unique_ptr<AuthStreamHandler> rx;
    std::unique_ptr<AuthStreamHandler> tx;
  };
  static rxtx_t create_stream_handler_pair(
    CephContext* ctx,
    const class AuthConnectionMeta& auth_meta);
};

// TODO: make this a static member of AuthSessionHandler.
extern AuthSessionHandler *get_auth_session_handler(
  CephContext *cct, int protocol,
  const CryptoKey& key,
  uint64_t features);

#endif
