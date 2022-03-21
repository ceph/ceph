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

#include "common/config.h"
#include "compressor/Compressor.h"

class ZlibCompressor : public Compressor {
  bool isal_enabled;
  CephContext *const cct;
public:
  ZlibCompressor(CephContext *cct, bool isal)
    : Compressor(COMP_ALG_ZLIB, "zlib"), isal_enabled(isal), cct(cct) {
#ifdef HAVE_QATZIP
    if (cct->_conf->qat_compressor_enabled && qat_accel.init("zlib"))
      qat_enabled = true;
    else
      qat_enabled = false;
#endif
  }

  int compress(const ceph::buffer::list &in, ceph::buffer::list &out, std::optional<int32_t> &compressor_message) override;
  int decompress(const ceph::buffer::list &in, ceph::buffer::list &out, std::optional<int32_t> compressor_message) override;
  int decompress(ceph::buffer::list::const_iterator &p, size_t compressed_len, ceph::buffer::list &out, std::optional<int32_t> compressor_message) override;
private:
  int zlib_compress(const ceph::buffer::list &in, ceph::buffer::list &out, std::optional<int32_t> &compressor_message);
  int isal_compress(const ceph::buffer::list &in, ceph::buffer::list &out, std::optional<int32_t> &compressor_message);
 };


#endif
