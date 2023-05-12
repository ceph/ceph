
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
