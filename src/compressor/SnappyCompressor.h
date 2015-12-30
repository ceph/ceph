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

#ifndef CEPH_SNAPPYCOMPRESSOR_H
#define CEPH_SNAPPYCOMPRESSOR_H

#include <snappy.h>
#include <snappy-sinksource.h>
#include "include/buffer.h"
#include "Compressor.h"

class BufferlistSource : public snappy::Source {
  list<bufferptr>::const_iterator pb;
  size_t pb_off;
  size_t left;

 public:
  BufferlistSource(bufferlist &data): pb(data.buffers().begin()), pb_off(0), left(data.length()) {}
  virtual ~BufferlistSource() {}
  virtual size_t Available() const { return left; }
  virtual const char* Peek(size_t* len) {
    if (left) {
      *len = pb->length() - pb_off;
      return pb->c_str() + pb_off;
    } else {
      *len = 0;
      return NULL;
    }
  }
  virtual void Skip(size_t n) {
    if (n + pb_off == pb->length()) {
      ++pb;
      pb_off = 0;
    } else {
      pb_off += n;
    }
    left -= n;
  }
};

class SnappyCompressor : public Compressor {
 public:
  virtual ~SnappyCompressor() {}
  virtual int compress(bufferlist &src, bufferlist &dst) {
    BufferlistSource source(src);
    bufferptr ptr(snappy::MaxCompressedLength(src.length()));
    snappy::UncheckedByteArraySink sink(ptr.c_str());
    snappy::Compress(&source, &sink);
    dst.append(ptr, 0, sink.CurrentDestination()-ptr.c_str());
    return 0;
  }
  virtual int decompress(bufferlist &src, bufferlist &dst) {
    BufferlistSource source(src);
    size_t res_len = 0;
    // Trick, decompress only need first 32bits buffer
    if (!snappy::GetUncompressedLength(src.get_contiguous(0, 8), 8, &res_len))
      return -1;
    bufferptr ptr(res_len);
    if (snappy::RawUncompress(&source, ptr.c_str())) {
      dst.append(ptr);
      return 0;
    }
    return -1;
  }
};

#endif
