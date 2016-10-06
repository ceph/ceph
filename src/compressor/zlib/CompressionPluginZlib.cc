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


// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "arch/probe.h"
#include "arch/intel.h"
#include "arch/arm.h"
#include "compressor/CompressionPlugin.h"
#include "ZlibCompressor.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_mon
// -----------------------------------------------------------------------------

class CompressionPluginZlib : public CompressionPlugin {
public:
  bool has_isal = false;

  explicit CompressionPluginZlib(CephContext *cct) : CompressionPlugin(cct)
  {}

  virtual int factory(CompressorRef *cs,
                      ostream *ss)
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

// -----------------------------------------------------------------------------

const char *__ceph_plugin_version()
{
  return CEPH_GIT_NICE_VER;
}

// -----------------------------------------------------------------------------

int __ceph_plugin_init(CephContext *cct,
                       const std::string& type,
                       const std::string& name)
{
  PluginRegistry *instance = cct->get_plugin_registry();

  return instance->add(type, name, new CompressionPluginZlib(cct));
}
