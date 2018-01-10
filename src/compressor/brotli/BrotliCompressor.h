/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 BI SHUN KE <aionshun@livemail.tw>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BROTLICOMPRESSOR_H
#define CEPH_BROTLICOMPRESSOR_H


#include "include/buffer.h"
#include "compressor/Compressor.h"

class BrotliCompressor : public Compressor 
{
  public:
  BrotliCompressor() : Compressor(COMP_ALG_BROTLI, "brotli") {}
  
  int compress(const bufferlist &in, bufferlist &out) override;
  int decompress(const bufferlist &in, bufferlist &out) override;
  int decompress(bufferlist::iterator &p, size_t compressed_len, bufferlist &out) override;
};

#endif //CEPH_BROTLICOMPRESSOR_H

