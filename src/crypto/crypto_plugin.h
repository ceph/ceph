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

#ifndef CRYPTO_PLUGIN_H
#define CRYPTO_PLUGIN_H

// -----------------------------------------------------------------------------
#include "common/PluginRegistry.h"
#include "ostream"

#include "crypto/crypto_accel.h"
#include <boost/asio/io_context.hpp>
// -----------------------------------------------------------------------------

class CryptoPlugin : public ceph::Plugin {

public:
  CryptoAccelRef cryptoaccel;
  explicit CryptoPlugin(CephContext* cct) : Plugin(cct)
  {}
  ~CryptoPlugin()
  {}
  virtual int factory(CryptoAccelRef *cs,
                      std::ostream *ss,
                      const size_t chunk_size,
                      const size_t max_requests) = 0;
};
#endif
