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
#include "compressor/CompressionPlugin.h"
#include "SnappyCompressor.h"
// -----------------------------------------------------------------------------

class CompressionPluginSnappy : public CompressionPlugin {

public:

  explicit CompressionPluginSnappy(CephContext* cct) : CompressionPlugin(cct)
  {}

  virtual int factory(CompressorRef *cs,
                      ostream *ss)
  {
    if (compressor == 0) {
      SnappyCompressor *interface = new SnappyCompressor();
      compressor = CompressorRef(interface);
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

  return instance->add(type, name, new CompressionPluginSnappy(cct));
}
