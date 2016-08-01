// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMPRESSOR_H
#define CEPH_COMPRESSOR_H

#include <string>
#include "include/memory.h"
#include "include/buffer.h"

class Compressor;
typedef shared_ptr<Compressor> CompressorRef;

class Compressor {
  std::string type;
 public:
  Compressor(std::string t) : type(t) {}
  virtual ~Compressor() {}
  const std::string& get_type() const {
    return type;
  }
  virtual int compress(const bufferlist &in, bufferlist &out) = 0;
  virtual int decompress(const bufferlist &in, bufferlist &out) = 0;
  // this is a bit weird but we need non-const iterator to be in
  // alignment with decode methods
  virtual int decompress(bufferlist::iterator &p, size_t compressed_len, bufferlist &out) = 0;

  static CompressorRef create(CephContext *cct, const std::string &type);

  enum CompressionAlgorithm {
    COMP_ALG_NONE = 0,
    COMP_ALG_SNAPPY = 1,
    COMP_ALG_ZLIB = 2,
  };

  static const char * get_comp_alg_name(int a) {
    switch (a) {
    case COMP_ALG_NONE: return "none";
    case COMP_ALG_SNAPPY: return "snappy";
    case COMP_ALG_ZLIB: return "zlib";
    default: return "???";
    }
  }

  static int get_comp_alg_type(const std::string &s) {
    if (s == "snappy")
      return COMP_ALG_SNAPPY;
    if (s == "zlib")
      return COMP_ALG_ZLIB;

    return COMP_ALG_NONE;
  }

};

#endif
