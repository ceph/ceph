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

#include "crypto/ipp/ipp_crypto_accel.h"


bool IppCryptoAccel::cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE])
{
  if ((size % AES_256_IVSIZE) != 0) {
    return false;
  }

  return ippcrypto.ipp_enc(out, in, size, const_cast<unsigned char*>(&iv[0]), const_cast<unsigned char*>(&key[0]));
}

bool IppCryptoAccel::cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE])
{
  if ((size % AES_256_IVSIZE) != 0) {
    return false;
  }

  return ippcrypto.ipp_dec(out, in, size, const_cast<unsigned char*>(&iv[0]), const_cast<unsigned char*>(&key[0]));
}
