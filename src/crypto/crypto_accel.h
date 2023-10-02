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
};
#endif
