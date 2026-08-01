/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Mirantis, Inc.
 *
 * Author: Adam Kupczyk <akupczyk@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CRYPTO_ACCEL_H
#define CRYPTO_ACCEL_H
#include <cstddef>
#include "include/Context.h"

class optional_yield;

class CryptoAccel;
typedef std::shared_ptr<CryptoAccel> CryptoAccelRef;

class CryptoAccel {
 public:
  CryptoAccel() {}
  CryptoAccel(const size_t chunk_size, const size_t max_requests) {}
  virtual ~CryptoAccel() {}

  static const int AES_256_IVSIZE = 128/8;
  static const int AES_256_KEYSIZE = 256/8;

  /**
   * GCM constants (IV size is 12 bytes; distinct from CBC's 16-byte IV).
   * NIST SP 800-38D recommends 96-bit (12-byte) IVs for GCM.
   */
  static const int AES_GCM_IV_SIZE = 96/8;  // 12 bytes
  static const int AES_GCM_TAGSIZE = 16;       // 128-bit auth tag

  virtual bool cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   optional_yield y) = 0;
  virtual bool cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   optional_yield y) = 0;
  virtual bool cbc_encrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char iv[][AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   optional_yield y) = 0;
  virtual bool cbc_decrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char iv[][AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   optional_yield y) = 0;

  virtual bool gcm_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_GCM_IV_SIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   const unsigned char* aad, size_t aad_len,
                   unsigned char* tag,
                   optional_yield y) = 0;
  virtual bool gcm_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_GCM_IV_SIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   const unsigned char* aad, size_t aad_len,
                   const unsigned char* tag,
                   optional_yield y) = 0;
  virtual bool gcm_encrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char iv[][AES_GCM_IV_SIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   const unsigned char* const aad[], const size_t aad_len[],
                   unsigned char tag[][AES_GCM_TAGSIZE],
                   optional_yield y) = 0;
  virtual bool gcm_decrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char iv[][AES_GCM_IV_SIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   const unsigned char* const aad[], const size_t aad_len[],
                   const unsigned char tag[][AES_GCM_TAGSIZE],
                   optional_yield y) = 0;
};
#endif
