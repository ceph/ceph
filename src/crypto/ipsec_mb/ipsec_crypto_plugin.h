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

#ifndef IPSEC_CRYPTO_PLUGIN_H
#define IPSEC_CRYPTO_PLUGIN_H
// -----------------------------------------------------------------------------
#include "crypto/crypto_plugin.h"
#include "crypto/ipsec_mb/ipsec_crypto_accel.h"
#include "arch/intel.h"
#include "arch/probe.h"
// -----------------------------------------------------------------------------


class IPSECCryptoPlugin : public CryptoPlugin {

public:

  explicit IPSECCryptoPlugin(CephContext* cct) : CryptoPlugin(cct)
  {}
  ~IPSECCryptoPlugin()
  {}
  virtual int factory(CryptoAccelRef *cs,
                      std::ostream *ss)
  {
    if (cryptoaccel == nullptr)
    {
      ceph_arch_probe();
      if (ceph_arch_intel_aesni && ceph_arch_intel_sse41) {
        cryptoaccel = CryptoAccelRef(new IPSECCryptoAccel);
      }
    }
    *cs = cryptoaccel;
    return 0;
  }
};
#endif
