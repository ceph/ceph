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

#ifndef CEPH_COMPRESSION_PLUGIN_ZLIB_H
#define CEPH_COMPRESSION_PLUGIN_ZLIB_H

// -----------------------------------------------------------------------------
#include "arch/probe.h"
#include "arch/intel.h"
#include "arch/arm.h"
#include "common/config.h"
#include "compressor/CompressionPlugin.h"
#include "ZlibCompressor.h"

// -----------------------------------------------------------------------------

class CompressionPluginZlib : public CompressionPlugin {
public:
  bool has_isal = false;

  explicit CompressionPluginZlib(CephContext *cct) : CompressionPlugin(cct)
  {}

  virtual int factory(CompressorRef *cs,
                      std::ostream *ss)
  {
    bool isal;
    if (cct->_conf->compressor_zlib_isal) {
      ceph_arch_probe();
      isal = (ceph_arch_intel_pclmul && ceph_arch_intel_sse41);
    } else {
      isal = false;
    }
    if (compressor == 0 || has_isal != isal) {
      compressor = CompressorRef(new ZlibCompressor(isal));
      has_isal = isal;
    }
    *cs = compressor;
    return 0;
  }
};

#endif
