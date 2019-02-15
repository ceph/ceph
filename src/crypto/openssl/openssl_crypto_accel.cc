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

#include "crypto/openssl/openssl_crypto_accel.h"
#include <openssl/aes.h>

bool OpenSSLCryptoAccel::cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE])
{
  if ((size % AES_256_IVSIZE) != 0) {
    return false;
  }

  AES_KEY aes_key;
  if (AES_set_encrypt_key(const_cast<unsigned char*>(&key[0]), 256, &aes_key) < 0)
    return false;

  AES_cbc_encrypt(const_cast<unsigned char*>(in), out, size, &aes_key,
                  const_cast<unsigned char*>(&iv[0]), AES_ENCRYPT);
  return true;
}
bool OpenSSLCryptoAccel::cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE])
{
  if ((size % AES_256_IVSIZE) != 0) {
    return false;
  }

  AES_KEY aes_key;
  if (AES_set_decrypt_key(const_cast<unsigned char*>(&key[0]), 256, &aes_key) < 0)
    return false;

  AES_cbc_encrypt(const_cast<unsigned char*>(in), out, size, &aes_key,
                  const_cast<unsigned char*>(&iv[0]), AES_DECRYPT);
  return true;
}
