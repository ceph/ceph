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

#ifndef ISAL_CRYPTO_ACCEL_H
#define ISAL_CRYPTO_ACCEL_H
#include "crypto/crypto_accel.h"
#include "common/async/yield_context.h"

class ISALCryptoAccel : public CryptoAccel {
 public:
  ISALCryptoAccel() {}
  virtual ~ISALCryptoAccel() {}

  bool cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   optional_yield y) override;
  bool cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   optional_yield y) override;
  bool cbc_encrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char iv[][AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   optional_yield y) override { return false; }
  bool cbc_decrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char iv[][AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE],
                   optional_yield y) override { return false; }
};
#endif
