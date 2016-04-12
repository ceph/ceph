// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
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

#include <unistd.h>

#include "ceph_ver.h"
#include "compressor/CompressionPlugin.h"
#include "compressor_example.h"

class CompressorPluginExample : public CompressionPlugin {
public:

  explicit CompressorPluginExample(CephContext* cct) : CompressionPlugin(cct)
  {}

  virtual int factory(CompressorRef *cs,
		      ostream *ss)
  {
    if (compressor == 0) {
      CompressorExample *interface = new CompressorExample();
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

  return instance->add(type, name, new CompressorPluginExample(cct));
}
