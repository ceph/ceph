// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Alyona Kiseleva <akiselyova@mirantis.com>
 *
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef COMPRESSION_PLUGIN_H
#define COMPRESSION_PLUGIN_H

#include <iosfwd>
#include <iostream>

#include "common/PluginRegistry.h"
#include "include/common_fwd.h"
#include "Compressor.h"

namespace ceph {

  class CompressionPlugin :  public Plugin {
  public:
    TOPNSPC::CompressorRef compressor;

    explicit CompressionPlugin(CephContext *cct)
      : Plugin(cct)
    {}
    
    ~CompressionPlugin() override {}

    virtual int factory(TOPNSPC::CompressorRef *cs,
			std::ostream *ss) = 0;

    virtual const char* name() {return "CompressionPlugin";}
  };

}

#endif
