/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Intel Corporation
 *
 * Author: Hui Han <hui.han@intel.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef IPP_CRYPTO_ACCEL_H
#define IPP_CRYPTO_ACCEL_H
#include "crypto/crypto_accel.h"
#include "ippcrypto.h"

class IppCryptoAccel : public CryptoAccel {
 public:
  IppCryptoAES ippcrypto;
  IppCryptoAccel() {}
  virtual ~IppCryptoAccel() {}

  bool cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE]) override;
  bool cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE]) override;
};
#endif
