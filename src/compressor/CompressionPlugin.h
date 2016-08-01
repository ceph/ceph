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

#include "common/Mutex.h"
#include "common/PluginRegistry.h"
#include "Compressor.h"

namespace ceph {

  class CompressionPlugin :  public Plugin {
  public:
    CompressorRef compressor;

    explicit CompressionPlugin(CephContext *cct) : Plugin(cct),
                                          compressor(0) 
    {}
    
    virtual ~CompressionPlugin() {}

    virtual int factory(CompressorRef *cs,
			                  ostream *ss) = 0;

    virtual const char* name() {return "CompressionPlugin";}
  };

}

#endif
