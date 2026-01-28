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

#include "crypto/isa-l/isal_crypto_accel.h"

#include "include/compat.h"

#include "crypto/isa-l/isa-l_crypto/include/aes_cbc.h"
#include "crypto/isa-l/isa-l_crypto/include/aes_gcm.h"

#include <cstdint>
#include <cstring>
#include <limits>

bool ISALCryptoAccel::cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE],
                             optional_yield y)
{
  if (unlikely((size % AES_256_IVSIZE) != 0)) {
    return false;
  }
  alignas(16) struct cbc_key_data keys_blk;
  aes_cbc_precomp(const_cast<unsigned char*>(&key[0]), AES_256_KEYSIZE, &keys_blk);
  aes_cbc_enc_256(const_cast<unsigned char*>(in),
                  const_cast<unsigned char*>(&iv[0]), keys_blk.enc_keys, out, size);
  return true;
}
bool ISALCryptoAccel::cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE],
                             optional_yield y)
{
  if (unlikely((size % AES_256_IVSIZE) != 0)) {
    return false;
  }
  alignas(16) struct cbc_key_data keys_blk;
  aes_cbc_precomp(const_cast<unsigned char*>(&key[0]), AES_256_KEYSIZE, &keys_blk);
  aes_cbc_dec_256(const_cast<unsigned char*>(in), const_cast<unsigned char*>(&iv[0]), keys_blk.dec_keys, out, size);
  return true;
}

// Constant-time byte comparison (best-effort): avoids early-exit timing from memcmp.
static inline bool ct_memeq(const unsigned char* a, const unsigned char* b, size_t len)
{
  volatile unsigned char diff = 0;
  for (size_t i = 0; i < len; ++i) {
    diff |= static_cast<unsigned char>(a[i] ^ b[i]);
  }
  return diff == 0;
}

// Cache AES-GCM key schedule per-thread to avoid re-running aes_gcm_pre_256() for repeated keys.
// The cached material is wiped on key change and when the thread exits.
static inline const gcm_key_data* get_cached_gcm_key(const unsigned char* key)
{
  struct gcm_key_cache_t {
    bool valid = false;
    unsigned char last_key[CryptoAccel::AES_256_KEYSIZE];
    alignas(16) gcm_key_data cached_gkey;

    void purge()
    {
      if (valid) {
        ceph_memzero_s(last_key, sizeof(last_key), sizeof(last_key));
        ceph_memzero_s(&cached_gkey, sizeof(cached_gkey), sizeof(cached_gkey));
        valid = false;
      }
    }

    ~gcm_key_cache_t()
    {
      purge();
    }
  };

  static thread_local gcm_key_cache_t cache;

  if (!cache.valid || !ct_memeq(cache.last_key, key, CryptoAccel::AES_256_KEYSIZE)) {
    cache.purge();
    aes_gcm_pre_256(key, &cache.cached_gkey);
    memcpy(cache.last_key, key, CryptoAccel::AES_256_KEYSIZE);
    cache.valid = true;
  }

  return &cache.cached_gkey;
}

bool ISALCryptoAccel::gcm_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                                  const unsigned char (&iv)[AES_GCM_IVSIZE],
                                  const unsigned char (&key)[AES_256_KEYSIZE],
                                  unsigned char* tag,
                                  optional_yield y)
{
  if (!out || !in || !tag) {
    return false;
  }
  if (unlikely(size > static_cast<size_t>(std::numeric_limits<uint64_t>::max()))) {
    return false;
  }

  const gcm_key_data* gkey = get_cached_gcm_key(&key[0]);

  alignas(16) struct gcm_context_data gctx;
  uint8_t iv_copy[AES_GCM_IVSIZE];
  memcpy(iv_copy, &iv[0], AES_GCM_IVSIZE);

  aes_gcm_enc_256(gkey, &gctx,
                  reinterpret_cast<uint8_t*>(out),
                  reinterpret_cast<const uint8_t*>(in),
                  static_cast<uint64_t>(size),
                  iv_copy,
                  nullptr, 0,
                  reinterpret_cast<uint8_t*>(tag),
                  CryptoAccel::AES_GCM_TAGSIZE);
  return true;
}

bool ISALCryptoAccel::gcm_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                                  const unsigned char (&iv)[AES_GCM_IVSIZE],
                                  const unsigned char (&key)[AES_256_KEYSIZE],
                                  unsigned char* tag,
                                  optional_yield y)
{
  if (!out || !in || !tag) {
    return false;
  }
  if (unlikely(size > static_cast<size_t>(std::numeric_limits<uint64_t>::max()))) {
    return false;
  }

  const gcm_key_data* gkey = get_cached_gcm_key(&key[0]);
  alignas(16) struct gcm_context_data gctx;

  uint8_t iv_copy[AES_GCM_IVSIZE];
  memcpy(iv_copy, &iv[0], AES_GCM_IVSIZE);

  unsigned char computed_tag[CryptoAccel::AES_GCM_TAGSIZE];
  aes_gcm_dec_256(gkey, &gctx,
                  reinterpret_cast<uint8_t*>(out),
                  reinterpret_cast<const uint8_t*>(in),
                  static_cast<uint64_t>(size),
                  iv_copy,
                  nullptr, 0,
                  reinterpret_cast<uint8_t*>(computed_tag),
                  CryptoAccel::AES_GCM_TAGSIZE);

  if (!ct_memeq(computed_tag, tag, CryptoAccel::AES_GCM_TAGSIZE)) {
    memset(out, 0, size);
    return false;
  }

  return true;
}
