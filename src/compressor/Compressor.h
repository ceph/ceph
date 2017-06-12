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


#include <memory>
#include <string>
#include <boost/optional.hpp>
#include "include/assert.h"	// boost clobbers this
#include "include/buffer.h"
#include "include/int_types.h"

class Compressor;
typedef std::shared_ptr<Compressor> CompressorRef;
class CephContext;

class Compressor {
public:
  enum CompressionAlgorithm {
    COMP_ALG_NONE = 0,
    COMP_ALG_SNAPPY = 1,
    COMP_ALG_ZLIB = 2,
    COMP_ALG_ZSTD = 3,
    COMP_ALG_LZ4 = 4,
    COMP_ALG_LAST	//the last value for range checks
  };
  // compression options
  enum CompressionMode {
    COMP_NONE,                  ///< compress never
    COMP_PASSIVE,               ///< compress if hinted COMPRESSIBLE
    COMP_AGGRESSIVE,            ///< compress unless hinted INCOMPRESSIBLE
    COMP_FORCE                  ///< compress always
  };

  static const char * get_comp_alg_name(int a);
  static boost::optional<CompressionAlgorithm> get_comp_alg_type(const std::string &s);

  static const char *get_comp_mode_name(int m);
  static boost::optional<CompressionMode> get_comp_mode_type(const std::string &s);

  Compressor(CompressionAlgorithm a, const char* t) : alg(a), type(t) {
  }
  virtual ~Compressor() {}
  const std::string& get_type_name() const {
    return type;
  }
  CompressionAlgorithm get_type() const {
    return alg;
  }
  virtual int compress(const ceph::bufferlist &in, ceph::bufferlist &out) = 0;
  virtual int decompress(const ceph::bufferlist &in, ceph::bufferlist &out) = 0;
  // this is a bit weird but we need non-const iterator to be in
  // alignment with decode methods
  virtual int decompress(ceph::bufferlist::iterator &p, size_t compressed_len, ceph::bufferlist &out) = 0;

  static CompressorRef create(CephContext *cct, const std::string &type);
  static CompressorRef create(CephContext *cct, int alg);

protected:
  CompressionAlgorithm alg;
  std::string type;

};

#endif
