
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/config.h"
#include "common/ceph_crypto.h"
#include "include/ceph_assert.h"

#include "client/FSCrypt.h"

#include <string.h>


using ceph::crypto::HMACSHA512;

static int calc_hmac_sha512(const char *key, int key_len,
                             const char *msg, int msg_len,
                             char *dest, int dest_len)
{
  char hash_sha512[CEPH_CRYPTO_HMACSHA512_DIGESTSIZE];

  HMACSHA512 hmac((const unsigned char *)key, key_len);
  hmac.Update((const unsigned char *)msg, msg_len);
  hmac.Final((unsigned char *)hash_sha512);

  auto len = std::min(dest_len, CEPH_CRYPTO_HMACSHA512_DIGESTSIZE);

  memcpy(dest, hash_sha512, len);

  return len;
}

#define SALT_LEN_DEFAULT 32

static char default_salt[SALT_LEN_DEFAULT] = { 0 };

static int hkdf_extract(const char *_salt, int salt_len,
                         const char *ikm, int ikm_len,
                         char *dest, int dest_len) {
  const char *salt = _salt;
  if (!_salt) {
    salt = default_salt;
    salt_len = SALT_LEN_DEFAULT;
  }

  return calc_hmac_sha512(salt, salt_len, ikm, ikm_len, dest, dest_len);
}

static int hkdf_expand(const char *data, int data_len,
                       const char *info, int info_len,
                       char *dest, int dest_len)
{
  int total_len = 0;

  char info_buf[info_len + 16];
  memcpy(info_buf, info, info_len);

  char *p = dest;

  for (char i = 1; total_len < dest_len; i++) {
    *(char *)(info_buf + info_len) =  i;

    int r = calc_hmac_sha512(data, data_len,
                         info_buf, info_len  + 1,
                         p, dest_len - total_len);
    if (r < 0) {
      return r;
    }
    if (r == 0) {
      return -EINVAL;
    }

    total_len += r;
  }

  return total_len;
}


int fscrypt_calc_hkdf(const char *salt, int salt_len,
                      const char *key, int key_len,
                      char *dest, int dest_len)
{
  char extract_buf[CEPH_CRYPTO_HMACSHA512_DIGESTSIZE];
  int r = hkdf_extract(salt, salt_len,
                       key, key_len,
                       extract_buf, sizeof(extract_buf));
  if (r < 0) {
    return r;
  }

  int extract_len = r;

#define FSCRYPT_INFO_STR "fscrypt\x00\x01"

  return hkdf_expand(extract_buf, extract_len,
                     FSCRYPT_INFO_STR, sizeof(FSCRYPT_INFO_STR) - 1,
                     dest, dest_len);
}

int FSCryptKey::init(const char *k, int klen) {
  int r = fscrypt_calc_hkdf(nullptr, 0,
                            (const char *)k, klen,
                            identifier.raw, sizeof(identifier.raw));
  if (r < 0) {
    return r;
  }

  key.append_hole(klen);
  memcpy(key.c_str(), k, klen);

  return 0;
}

int ceph_fscrypt_key_identifier::init(const char *k, int klen) {
  if (klen != sizeof(raw)) {
    return -EINVAL;
  }
  memcpy(raw, k, klen);

  return 0;
}

int ceph_fscrypt_key_identifier::init(const struct fscrypt_key_specifier& k) {
  if (k.type != FSCRYPT_KEY_SPEC_TYPE_IDENTIFIER) {
    return -ENOTSUP;
  }

  return init((const char *)k.u.identifier, sizeof(k.u.identifier));
}

bool ceph_fscrypt_key_identifier::operator<(const struct ceph_fscrypt_key_identifier& r) const {
  return (memcmp(raw, r.raw, sizeof(raw)) < 0);
}

int FSCryptKeyStore::create(const char *k, int klen, FSCryptKeyRef& key)
{
  key = std::make_shared<FSCryptKey>();

  int r = key->init(k, klen);
  if (r < 0) {
    return r;
  }

  std::unique_lock wl{lock};

  const auto& id = key->get_identifier();
  
  auto iter = m.find(id);
  if (iter != m.end()) {
    return -EEXIST;
  }

  m[id] = key;

  return 0;
}

int FSCryptKeyStore::find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyRef& key)
{
  std::shared_lock rl{lock};

  auto iter = m.find(id);
  if (iter == m.end()) {
    return -ENOENT;
  }

  key = iter->second;

  return 0;
}

int FSCryptKeyStore::remove(const struct ceph_fscrypt_key_identifier& id)
{
  std::unique_lock rl{lock};

  m.erase(id);

  return 0;
}
