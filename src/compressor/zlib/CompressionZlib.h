// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_COMPRESSION_ZLIB_H
#define CEPH_COMPRESSION_ZLIB_H

#include "compressor/Compressor.h"

class CompressionZlib : public Compressor {
  const char version = '1';

public:
  CompressionZlib() : Compressor("zlib") {}
  int compress(const bufferlist &in, bufferlist &out) override;
  int decompress(const bufferlist &in, bufferlist &out) override;
  int decompress(bufferlist::iterator &p, size_t compressed_len, bufferlist &out) override;
 };


#endif
