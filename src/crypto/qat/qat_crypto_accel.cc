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

#include "crypto/qat/qat_crypto_accel.h"




bool QccCryptoAccel::cbc_encrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
        const unsigned char iv[][AES_256_IVSIZE],
        const unsigned char (&key)[AES_256_KEYSIZE],
        optional_yield y) {
  if (unlikely((size % AES_256_IVSIZE) != 0)) {
    return false;
  }

  return qcccrypto.perform_op_batch(out, in, size,
      const_cast<unsigned char *>(&iv[0][0]),
      const_cast<unsigned char *>(&key[0]),
      CPA_CY_SYM_CIPHER_DIRECTION_ENCRYPT, y);
}

bool QccCryptoAccel::cbc_decrypt_batch(unsigned char* out, const unsigned char* in, size_t size,
        const unsigned char iv[][AES_256_IVSIZE],
        const unsigned char (&key)[AES_256_KEYSIZE],
        optional_yield y) {
  if (unlikely((size % AES_256_IVSIZE) != 0)) {
    return false;
  }

  return qcccrypto.perform_op_batch(out, in, size,
      const_cast<unsigned char *>(&iv[0][0]),
      const_cast<unsigned char *>(&key[0]),
      CPA_CY_SYM_CIPHER_DIRECTION_DECRYPT, y);
}
