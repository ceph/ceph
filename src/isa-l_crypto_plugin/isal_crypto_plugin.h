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
#include "../isa-l_crypto_plugin/crypto_plugin.h"
#include "../isa-l_crypto_plugin/isal_crypto_accel.h"
// -----------------------------------------------------------------------------


class ISALCryptoPlugin : public CryptoPlugin {

  CryptoAccelRef cryptoaccel;
public:

  explicit ISALCryptoPlugin(CephContext* cct) : CryptoPlugin(cct)
  {}
  ~ISALCryptoPlugin()
  {}
  virtual int factory(CryptoAccelRef *cs,
                      ostream *ss)
  {
    if (cryptoaccel == nullptr)
    {
      cryptoaccel = CryptoAccelRef(new ISALCryptoAccel);
    }
    *cs = cryptoaccel;
    return 0;
  }
};
#endif
