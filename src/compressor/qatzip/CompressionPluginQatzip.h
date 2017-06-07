/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Alyona Kiseleva <akiselyova@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_COMPRESSION_PLUGIN_QATZIP_H
#define CEPH_COMPRESSION_PLUGIN_QATZIP_H

// -----------------------------------------------------------------------------
#include "compressor/CompressionPlugin.h"
#include "QatzipCompressor.h"
// -----------------------------------------------------------------------------

class CompressionPluginQatzip : public CompressionPlugin {

public:

  explicit CompressionPluginQatzip(CephContext* cct) : CompressionPlugin(cct)
  {}

  int factory(CompressorRef *cs,
                      std::ostream *ss) override
  {
    if (compressor == 0) {
      QatzipCompressor *interface = new SnappyCompressor();
      compressor = CompressorRef(interface);
    }
    *cs = compressor;
    return 0;
  }
};

#endif
