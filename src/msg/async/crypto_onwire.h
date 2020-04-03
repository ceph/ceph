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


#ifndef CEPH_CRYPTO_ONWIRE_H
#define CEPH_CRYPTO_ONWIRE_H

#include <cstdint>
#include <memory>

#include "auth/Auth.h"
#include "include/buffer.h"

namespace ceph::math {

// TODO
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

struct MsgAuthError : public std::runtime_error {
  MsgAuthError()
    : runtime_error("message signature mismatch") {
  }
};

struct TxHandlerError : public std::runtime_error {
  TxHandlerError(const char* what)
    : std::runtime_error(std::string("tx handler error: ") + what) {}
};

struct TxHandler {
  virtual ~TxHandler() = default;

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
  virtual void reset_tx_handler(const uint32_t* first,
                                const uint32_t* last) = 0;

  void reset_tx_handler(std::initializer_list<uint32_t> update_size_sequence) {
    if (update_size_sequence.size() > 0) {
      const uint32_t* first = &*update_size_sequence.begin();
      reset_tx_handler(first, first + update_size_sequence.size());
    } else {
      reset_tx_handler(nullptr, nullptr);
    }
  }

  // Perform encryption. Client gives full ownership right to provided
  // bufferlist. The method MUST NOT be called after _final() if there
  // was no call to _reset().
  virtual void authenticated_encrypt_update(
    const ceph::bufferlist& plaintext) = 0;

  // Generates authentication signature and returns bufferlist crafted
  // basing on plaintext from preceding call to _update().
  virtual ceph::bufferlist authenticated_encrypt_final() = 0;
};

class RxHandler {
public:
  virtual ~RxHandler() = default;

  // Transmitter can append extra bytes of ciphertext at the -final step.
  // This method return how much was added, and thus let client translate
  // plaintext size into ciphertext size to grab from wire.
  virtual std::uint32_t get_extra_size_at_final() = 0;

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

#endif // CEPH_CRYPTO_ONWIRE_H
