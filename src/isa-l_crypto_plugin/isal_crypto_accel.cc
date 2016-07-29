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

#include "../isa-l_crypto_plugin/isal_crypto_accel.h"

#include <isa-l_crypto/include/aes_cbc.h>

bool ISALCryptoAccel::cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE])
{
  if ((size % AES_256_IVSIZE) != 0) {
    return false;
  }
  struct cbc_key_data *keys_blk = (struct cbc_key_data*) memalign(16, sizeof(struct cbc_key_data));
  aes_cbc_precomp(const_cast<unsigned char*>(&key[0]), AES_256_KEYSIZE, keys_blk);
  aes_cbc_enc_256(const_cast<unsigned char*>(in),
                  const_cast<unsigned char*>(&iv[0]), keys_blk->enc_keys, out, size);
  free(keys_blk);
  return true;
}
bool ISALCryptoAccel::cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE])
{
  if ((size % AES_256_IVSIZE) != 0) {
    return false;
  }
  struct cbc_key_data *keys_blk = (struct cbc_key_data*) memalign(16, sizeof(struct cbc_key_data));
  aes_cbc_precomp(const_cast<unsigned char*>(&key[0]), AES_256_KEYSIZE, keys_blk);
  aes_cbc_dec_256(const_cast<unsigned char*>(in), const_cast<unsigned char*>(&iv[0]), keys_blk->dec_keys, out, size);
  free(keys_blk);
  return true;
}
