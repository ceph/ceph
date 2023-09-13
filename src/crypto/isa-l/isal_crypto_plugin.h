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

#ifndef ISAL_CRYPTO_PLUGIN_H
#define ISAL_CRYPTO_PLUGIN_H
// -----------------------------------------------------------------------------
#include "crypto/crypto_plugin.h"
#include "crypto/isa-l/isal_crypto_accel.h"
#include "arch/intel.h"
#include "arch/probe.h"
// -----------------------------------------------------------------------------


class ISALCryptoPlugin : public CryptoPlugin {

public:

  explicit ISALCryptoPlugin(CephContext* cct) : CryptoPlugin(cct)
  {}
  ~ISALCryptoPlugin()
  {}
  virtual int factory(CryptoAccelRef *cs,
                      std::ostream *ss,
                      const size_t chunk_size,
                      const size_t max_requests)
  {
    if (cryptoaccel == nullptr)
    {
      ceph_arch_probe();
      if (ceph_arch_intel_aesni && ceph_arch_intel_sse41) {
        cryptoaccel = CryptoAccelRef(new ISALCryptoAccel);
      }
    }
    *cs = cryptoaccel;
    return 0;
  }
};
#endif
