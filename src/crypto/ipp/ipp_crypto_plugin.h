/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Intel Corporation
 *
 * Author: Hui Han <hui.han@intel.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef IPP_CRYPTO_PLUGIN_H
#define IPP_CRYPTO_PLUGIN_H
// -----------------------------------------------------------------------------
#include "crypto/crypto_plugin.h"
#include "crypto/ipp/ipp_crypto_accel.h"
#include "arch/intel.h"
#include "arch/probe.h"
// -----------------------------------------------------------------------------


class IppCryptoPlugin : public CryptoPlugin {

public:

  explicit IppCryptoPlugin(CephContext* cct) : CryptoPlugin(cct)
  {}
  ~IppCryptoPlugin()
  {}
  virtual int factory(CryptoAccelRef *cs,
                      std::ostream *ss)
  {
    if (cryptoaccel == nullptr)
    {
      ceph_arch_probe();
      if (ceph_arch_intel_aesni && ceph_arch_intel_sse41) {
        cryptoaccel = CryptoAccelRef(new IppCryptoAccel);
      }
    }
    *cs = cryptoaccel;
    return 0;
  }
};
#endif
