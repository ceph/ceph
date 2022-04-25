/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Intel Corporation
 *
 * Author: Qiaowei Ren <qiaowei.ren@intel.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef OPENSSL_CRYPTO_ACCEL_H
#define OPENSSL_CRYPTO_ACCEL_H

#include "crypto/crypto_accel.h"
#include "common/async/yield_context.h"

class OpenSSLCryptoAccel : public CryptoAccel {
 public:
  OpenSSLCryptoAccel() {}
  virtual ~OpenSSLCryptoAccel() {}

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
