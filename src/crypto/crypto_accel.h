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

#ifndef CRYPTO_ACCEL_H
#define CRYPTO_ACCEL_H
#include <cstddef>
#include "include/Context.h"

class CryptoAccel;
typedef std::shared_ptr<CryptoAccel> CryptoAccelRef;

class CryptoEngine {
 public:
  static const int AES_256_KEYSIZE = 256 / 8;
  unsigned char m_key[AES_256_KEYSIZE];

  CryptoEngine() {}
  CryptoEngine(const unsigned char(&key)[AES_256_KEYSIZE]) {
    memcpy(m_key, key, sizeof(key));
  }
  virtual ~CryptoEngine() {}
};

class CryptoAccel {
 public:
  CryptoAccel() {}
  virtual ~CryptoAccel() {}

  static const int AES_256_IVSIZE = 128/8;
  static const int AES_256_KEYSIZE = 256/8;
  virtual bool cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE]) = 0;
  virtual bool cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE]) = 0;

  /*
  * Operate on multiple buffers in parallel and flush at the end, aim to promote performance.
  * Need to configure ipsec as the accelerator, refer to:
  *  - option: plugin_crypto_accelerator
  *  - value: crypto_ipsec
  *
  * Others like openssl/isa-l/QAT don't have multi-buffer functionality, so switch to traditional interface
  */
  virtual CryptoEngine* get_engine(const unsigned char(&key)[AES_256_KEYSIZE]) { return new CryptoEngine(key); }
  virtual void put_engine(CryptoEngine* eng) { delete eng; }

  virtual bool flush(CryptoEngine* engine) { return true; }
  virtual bool cbc_encrypt_mb(CryptoEngine* engine, unsigned char* out, const unsigned char* in, size_t size,
    const unsigned char(&iv)[AES_256_IVSIZE]) {
    if (!engine) return false;
    return cbc_encrypt(out, in, size, iv, engine->m_key);
  };
  virtual bool cbc_decrypt_mb(CryptoEngine* engine, unsigned char* out, const unsigned char* in, size_t size,
    const unsigned char(&iv)[AES_256_IVSIZE]) {
    if (!engine) return false;
    return cbc_decrypt(out, in, size, iv, engine->m_key);
  };
};
#endif
