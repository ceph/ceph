/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Intel Corporation
 *
 * Author: Liang Fang <liang.a.fang@intel.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef IPSEC_CRYPTO_ACCEL_H
#define IPSEC_CRYPTO_ACCEL_H
#include <atomic>
#include "crypto/crypto_accel.h"
#include "crypto/ipsec_mb/intel-ipsec-mb/lib/intel-ipsec-mb.h"

class IPSECCryptoEngine : public CryptoEngine {
 public:
  alignas(16) uint32_t enc_keys[15 * 4];
  alignas(16) uint32_t dec_keys[15 * 4];

  IMB_MGR* m_mgr = NULL;
  std::atomic_flag in_use;
 private:
  bool detect_arch_and_init_mgr();
 public:
  IPSECCryptoEngine();
  IPSECCryptoEngine(const unsigned char(&key)[AES_256_KEYSIZE]);
  virtual ~IPSECCryptoEngine();
  void set_key(const unsigned char(&key)[AES_256_KEYSIZE]);
};

class IPSECCryptoAccel : public CryptoAccel {
 public:
  IPSECCryptoAccel() {}
  virtual ~IPSECCryptoAccel() {}

  bool cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE]) override;
  bool cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                   const unsigned char (&iv)[AES_256_IVSIZE],
                   const unsigned char (&key)[AES_256_KEYSIZE]) override;

  /*
  * Below is the multi-buffer version interface. Comparing with traditional interface listed above:
  * - In the platform that CPU contains AVX512 instruction set, there's about 6 times
  *   performance boost.
  * - In the platform with lasted CPU (e.g. Intel Icelake Xeon) that contains VAES instruction set,
  *   there's about 10 times performance boost.
  */
  CryptoEngine* get_engine(const unsigned char(&key)[AES_256_KEYSIZE]) override;
  void put_engine(CryptoEngine* eng) override;
  bool flush(CryptoEngine* engine) override;
  bool cbc_encrypt_mb(CryptoEngine* engine, unsigned char* out, const unsigned char* in, size_t size,
    const unsigned char(&iv)[AES_256_IVSIZE]) override;
  bool cbc_decrypt_mb(CryptoEngine* engine, unsigned char* out, const unsigned char* in, size_t size,
    const unsigned char(&iv)[AES_256_IVSIZE]) override;
};
#endif
