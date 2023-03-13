/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Intel Corporation
 *
 * Author: Qiaowei Ren <qiaowei.ren@intel.com>
 * Author: Ganesh Mahalingam <ganesh.mahalingam@intel.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef QAT_CRYPTO_ACCEL_H
#define QAT_CRYPTO_ACCEL_H

#include "crypto/crypto_accel.h"
#include "crypto/qat/qcccrypto.h"
#include "common/async/yield_context.h"

class QccCryptoAccel : public CryptoAccel {
  public:
    QccCrypto qcccrypto;
    QccCryptoAccel(const size_t chunk_size, const size_t max_requests):qcccrypto() { qcccrypto.init(chunk_size, max_requests); };
    ~QccCryptoAccel() { qcccrypto.destroy(); };

    bool cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
        const unsigned char (&iv)[AES_256_IVSIZE],
        const unsigned char (&key)[AES_256_KEYSIZE],
        optional_yield y) override { return false; }
    bool cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
        const unsigned char (&iv)[AES_256_IVSIZE],
        const unsigned char (&key)[AES_256_KEYSIZE],
        optional_yield y) override { return false; }
    bool cbc_encrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
        const unsigned char iv[][AES_256_IVSIZE],
        const unsigned char (&key)[AES_256_KEYSIZE],
        optional_yield y) override;
    bool cbc_decrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
        const unsigned char iv[][AES_256_IVSIZE],
        const unsigned char (&key)[AES_256_KEYSIZE],
        optional_yield y) override;
};
#endif
