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

#ifndef CEPH_COMPRESSOR_EXAMPLE_H
#define CEPH_COMPRESSOR_EXAMPLE_H

#include <unistd.h>
#include <errno.h>
#include <algorithm>
#include <sstream>

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "compressor/Compressor.h"

class CompressorExample : public Compressor {
public:
  virtual ~CompressorExample() {}

  virtual int compress(const bufferlist &in, bufferlist &out)
  {
    out = in;
    return 0;
  }

  virtual int decompress(const bufferlist &in, bufferlist &out)
  {
    out = in;
    return 0;
  }

  virtual const char* get_method_name()
  {
    return "example";
  }

};

#endif
