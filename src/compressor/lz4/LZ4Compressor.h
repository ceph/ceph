// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LZ4COMPRESSOR_H
#define CEPH_LZ4COMPRESSOR_H

#include <optional>
#include <lz4.h>

#include "compressor/Compressor.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/config.h"

class QatAccel;

class LZ4Compressor : public Compressor {
#ifdef HAVE_QATZIP
  bool qat_enabled;
  static QatAccel qat_accel;
#endif

 public:
  explicit LZ4Compressor(CephContext* cct);

  int compress(const ceph::buffer::list &src,
               ceph::buffer::list &dst,
               std::optional<int32_t> &compressor_message) override;

  int decompress(const ceph::buffer::list &src,
                 ceph::buffer::list &dst,
                 std::optional<int32_t> compressor_message) override;

  int decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 std::optional<int32_t> compressor_message) override;
};

#endif
