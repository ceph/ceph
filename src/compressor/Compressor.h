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

#include "include/int_types.h"
#include "include/Context.h"

class Compressor;
typedef shared_ptr<Compressor> CompressorRef;

class Compressor {
protected:
  string type;
public:
  Compressor(string t) : type(t) {}
  virtual ~Compressor() {}
  const string& get_type() const {
    return type;
  }
  virtual int compress(const bufferlist &in, bufferlist &out) = 0;
  virtual int decompress(const bufferlist &in, bufferlist &out) = 0;
  // this is a bit weird but we need non-const iterator to be in
  // alignment with decode methods
  virtual int decompress(bufferlist::iterator &p, size_t compressed_len, bufferlist &out) = 0;

  static CompressorRef create(CephContext *cct, const string &type);
};

#endif
