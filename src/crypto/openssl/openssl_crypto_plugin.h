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

#ifndef ISAL_CRYPTO_PLUGIN_H
#define ISAL_CRYPTO_PLUGIN_H

#include "crypto/crypto_plugin.h"
#include "crypto/openssl/openssl_crypto_accel.h"


class OpenSSLCryptoPlugin : public CryptoPlugin {

  CryptoAccelRef cryptoaccel;
public:
  explicit OpenSSLCryptoPlugin(CephContext* cct) : CryptoPlugin(cct)
  {}
  int factory(CryptoAccelRef *cs, ostream *ss) override {
    if (cryptoaccel == nullptr)
      cryptoaccel = CryptoAccelRef(new OpenSSLCryptoAccel);

    *cs = cryptoaccel;
    return 0;
  }
};
#endif
