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

#ifndef QAT_CRYPTO_PLUGIN_H
#define QAT_CRYPTO_PLUGIN_H

#include "crypto/crypto_plugin.h"
#include "crypto/qat/qat_crypto_accel.h"


class QccCryptoPlugin : public CryptoPlugin {
  static std::mutex qat_init;

public:

  explicit QccCryptoPlugin(CephContext* cct) : CryptoPlugin(cct)
  {}
  ~QccCryptoPlugin()
  {}
  virtual int factory(CryptoAccelRef *cs, std::ostream *ss, const size_t chunk_size, const size_t max_requests)
  {
    std::lock_guard<std::mutex> l(qat_init);
    if (cryptoaccel == nullptr)
      cryptoaccel = CryptoAccelRef(new QccCryptoAccel(chunk_size, max_requests));

    *cs = cryptoaccel;
    return 0;
  }
};
#endif
