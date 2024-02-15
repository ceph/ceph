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
#include <openssl/evp.h>
#include <openssl/engine.h>
#include "common/debug.h"

// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_crypto
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream&
_prefix(std::ostream* _dout)
{
  return *_dout << "OpensslCryptoAccel: ";
}
// -----------------------------------------------------------------------------

#define EVP_SUCCESS 1
#define AES_ENCRYPT 1
#define AES_DECRYPT 0

bool evp_transform(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char* iv,
                   const unsigned char* key,
                   ENGINE* engine,
                   const EVP_CIPHER* const type,
                   const int encrypt)
{
  using pctx_t = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;
  pctx_t pctx{ EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free };

  if (!pctx) {
    derr << "failed to create evp cipher context" << dendl;
    return false;
  }

  if (EVP_CipherInit_ex(pctx.get(), type, engine, key, iv, encrypt) != EVP_SUCCESS) {
    derr << "EVP_CipherInit_ex failed" << dendl;
    return false;
  }

  if (EVP_CIPHER_CTX_set_padding(pctx.get(), 0) != EVP_SUCCESS) {
    derr << "failed to disable PKCS padding" << dendl;
    return false;
  }

  int len_update = 0;
  if (EVP_CipherUpdate(pctx.get(), out, &len_update, in, size) != EVP_SUCCESS) {
    derr << "EVP_CipherUpdate failed" << dendl;
    return false;
  }
  
  int len_final = 0;
  if (EVP_CipherFinal_ex(pctx.get(), out + len_update, &len_final) != EVP_SUCCESS) {
    derr << "EVP_CipherFinal_ex failed" << dendl;
    return false;
  }

  ceph_assert(len_final == 0);
  return (len_update + len_final) == static_cast<int>(size);
}
                        
bool OpenSSLCryptoAccel::cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE],
                             optional_yield y)
{
  if (unlikely((size % AES_256_IVSIZE) != 0)) {
    return false;
  }

  return evp_transform(out, in, size, const_cast<unsigned char*>(&iv[0]),
                       const_cast<unsigned char*>(&key[0]),
                       nullptr, // Hardware acceleration engine can be used in the future
                       EVP_aes_256_cbc(), AES_ENCRYPT);
}
                             
bool OpenSSLCryptoAccel::cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE],
                             optional_yield y)
{
  if (unlikely((size % AES_256_IVSIZE) != 0)) {
    return false;
  }

  return evp_transform(out, in, size, const_cast<unsigned char*>(&iv[0]),
                       const_cast<unsigned char*>(&key[0]),
                       nullptr, // Hardware acceleration engine can be used in the future
                       EVP_aes_256_cbc(), AES_DECRYPT);
}
