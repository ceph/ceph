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

// TODO: make this a static member of AuthSessionHandler.
extern AuthSessionHandler *get_auth_session_handler(
  CephContext *cct, int protocol,
  const CryptoKey& key,
  uint64_t features);


namespace ceph::math {

template <typename T>
class always_aligned_t {
  T val;

  template <class... Args>
  always_aligned_t(Args&&... args)
    : val(std::forward<Args>(args)...) {
  }
};

} // namespace ceph::math

namespace ceph::crypto::onwire {

struct TxHandler {
  virtual ~TxHandler() = default;

  virtual std::uint32_t calculate_segment_size(std::uint32_t size) = 0;

  // Instance of TxHandler must be reset before doing any encrypt-update
  // step. This applies also to situation when encrypt-final was already
  // called and another round of update-...-update-final will take place.
  //
  // The input parameter informs implementation how the -update sequence
  // is fragmented and allows to make concious decision about allocation
  // or reusage of provided memory. One implementation could do in-place
  // encryption while other might prefer one huge output buffer.
  //
  // It's undefined what will happen if client doesn't follow the order.
  //
  // TODO: switch to always_aligned_t
  virtual void reset_tx_handler(
    std::initializer_list<std::uint32_t> update_size_sequence) = 0;

  // Reserve n bytes in the bufferlist being crafted by TxHandler.
  // TODO: this will be dropped altogether with new frame format
  virtual ceph::bufferlist::contiguous_filler reserve(std::uint32_t) = 0;

  // Perform encryption. Client gives full ownership right to provided
  // bufferlist. The method MUST NOT be called after _final() if there
  // was no call to _reset().
  virtual void authenticated_encrypt_update(
    ceph::bufferlist&& plaintext) = 0;

  // Generates authentication signature and returns bufferlist crafted
  // basing on plaintext from preceding call to _update().
  virtual ceph::bufferlist authenticated_encrypt_final() = 0;
};

class RxHandler {
public:
  virtual ~RxHandler() = default;

  // Instance of RxHandler must be reset before doing any decrypt-update
  // step. This applies also to situation when decrypt-final was already
  // called and another round of update-...-update-final will take place.
  virtual void reset_rx_handler() = 0;

  // Perform decryption ciphertext must be ALWAYS aligned to 16 bytes.
  // TODO: switch to always_aligned_t
  virtual ceph::bufferlist authenticated_decrypt_update(
    ceph::bufferlist&& ciphertext,
    std::uint32_t alignment) = 0;

  // Perform decryption of last cipertext's portion and verify signature
  // for overall decryption sequence.
  // Throws on integrity/authenticity checks
  virtual ceph::bufferlist authenticated_decrypt_update_final(
    ceph::bufferlist&& ciphertext,
    std::uint32_t alignment) = 0;
};

struct rxtx_t {
  //rxtx_t(rxtx_t&& r) : rx(std::move(rx)), tx(std::move(tx)) {}
  // Each peer can use different handlers.
  // Hmm, isn't that too much flexbility?
  std::unique_ptr<RxHandler> rx;
  std::unique_ptr<TxHandler> tx;

  static rxtx_t create_handler_pair(
    CephContext* ctx,
    const class AuthConnectionMeta& auth_meta,
    bool crossed);
};


} // namespace ceph::crypto::onwire

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

#endif
