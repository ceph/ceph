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

bool OpenSSLCryptoAccel::gcm_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                                      const unsigned char (&iv)[AES_GCM_IV_SIZE],
                                      const unsigned char (&key)[AES_256_KEYSIZE],
                                      const unsigned char* aad, size_t aad_len,
                                      unsigned char* tag,
                                      optional_yield y)
{
  using pctx_t = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;
  pctx_t pctx{ EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free };

  if (!pctx) {
    derr << "failed to create evp cipher context for GCM encrypt" << dendl;
    return false;
  }

  int outlen;

  if (EVP_EncryptInit_ex(pctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != EVP_SUCCESS) {
    derr << "EVP_EncryptInit_ex failed for GCM" << dendl;
    return false;
  }

  if (EVP_CIPHER_CTX_ctrl(pctx.get(), EVP_CTRL_GCM_SET_IVLEN, AES_GCM_IV_SIZE, nullptr) != EVP_SUCCESS) {
    derr << "failed to set GCM IV length" << dendl;
    return false;
  }

  if (EVP_EncryptInit_ex(pctx.get(), nullptr, nullptr, &key[0], &iv[0]) != EVP_SUCCESS) {
    derr << "failed to set GCM key/IV" << dendl;
    return false;
  }

  // Add AAD (pass nullptr for out to process AAD only)
  if (aad_len > 0) {
    if (EVP_EncryptUpdate(pctx.get(), nullptr, &outlen, aad,
                          static_cast<int>(aad_len)) != EVP_SUCCESS) {
      derr << "failed to set GCM AAD" << dendl;
      return false;
    }
  }

  if (EVP_EncryptUpdate(pctx.get(), out, &outlen, in,
                        static_cast<int>(size)) != EVP_SUCCESS) {
    derr << "EVP_EncryptUpdate failed for GCM" << dendl;
    return false;
  }

  int final_len;
  if (EVP_EncryptFinal_ex(pctx.get(), out + outlen, &final_len) != EVP_SUCCESS) {
    derr << "EVP_EncryptFinal_ex failed for GCM" << dendl;
    return false;
  }

  if (EVP_CIPHER_CTX_ctrl(pctx.get(), EVP_CTRL_GCM_GET_TAG, AES_GCM_TAGSIZE, tag) != EVP_SUCCESS) {
    derr << "failed to get GCM tag" << dendl;
    return false;
  }

  return true;
}

bool OpenSSLCryptoAccel::gcm_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                                      const unsigned char (&iv)[AES_GCM_IV_SIZE],
                                      const unsigned char (&key)[AES_256_KEYSIZE],
                                      const unsigned char* aad, size_t aad_len,
                                      const unsigned char* tag,
                                      optional_yield y)
{
  using pctx_t = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;
  pctx_t pctx{ EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free };

  if (!pctx) {
    derr << "failed to create evp cipher context for GCM decrypt" << dendl;
    return false;
  }

  int outlen;

  if (EVP_DecryptInit_ex(pctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != EVP_SUCCESS) {
    derr << "EVP_DecryptInit_ex failed for GCM" << dendl;
    return false;
  }

  if (EVP_CIPHER_CTX_ctrl(pctx.get(), EVP_CTRL_GCM_SET_IVLEN, AES_GCM_IV_SIZE, nullptr) != EVP_SUCCESS) {
    derr << "failed to set GCM IV length" << dendl;
    return false;
  }

  if (EVP_DecryptInit_ex(pctx.get(), nullptr, nullptr, &key[0], &iv[0]) != EVP_SUCCESS) {
    derr << "failed to set GCM key/IV" << dendl;
    return false;
  }

  if (EVP_CIPHER_CTX_ctrl(pctx.get(), EVP_CTRL_GCM_SET_TAG, AES_GCM_TAGSIZE,
                          const_cast<unsigned char*>(tag)) != EVP_SUCCESS) {
    derr << "failed to set GCM expected tag" << dendl;
    return false;
  }

  // Add AAD (must match encryption)
  if (aad_len > 0) {
    if (EVP_DecryptUpdate(pctx.get(), nullptr, &outlen, aad,
                          static_cast<int>(aad_len)) != EVP_SUCCESS) {
      derr << "failed to set GCM AAD" << dendl;
      return false;
    }
  }

  if (EVP_DecryptUpdate(pctx.get(), out, &outlen, in,
                        static_cast<int>(size)) != EVP_SUCCESS) {
    derr << "EVP_DecryptUpdate failed for GCM" << dendl;
    return false;
  }

  int final_len;
  if (EVP_DecryptFinal_ex(pctx.get(), out + outlen, &final_len) != EVP_SUCCESS) {
    // Authentication failure - tag mismatch
    derr << "GCM authentication failed - tag mismatch" << dendl;
    memset(out, 0, size);  // Clear output on auth failure
    return false;
  }

  return true;
}
