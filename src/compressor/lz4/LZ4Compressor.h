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

#include <lz4.h>

#include "compressor/Compressor.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/Tub.h"


class LZ4Compressor : public Compressor {
 public:
  LZ4Compressor() : Compressor(COMP_ALG_LZ4, "lz4") {}

  int compress(const bufferlist &src, bufferlist &dst) override {
    bufferptr outptr = buffer::create_page_aligned(
      LZ4_compressBound(src.length()));
    LZ4_stream_t lz4_stream;
    LZ4_resetStream(&lz4_stream);

    auto p = src.begin();
    size_t left = src.length();
    int pos = 0;
    const char *data;
    unsigned num = src.get_num_buffers();
    uint32_t origin_len;
    uint32_t compressed_len;
    ::encode((uint32_t)num, dst);
    while (left) {
      origin_len = p.get_ptr_and_advance(left, &data);
      compressed_len = LZ4_compress_fast_continue(
        &lz4_stream, data, outptr.c_str()+pos, origin_len,
        outptr.length()-pos, 1);
      if (compressed_len <= 0)
        return -1;
      pos += compressed_len;
      left -= origin_len;
      ::encode(origin_len, dst);
      ::encode(compressed_len, dst);
    }
    assert(p.end());

    dst.append(outptr, 0, pos);
    return 0;
  }

  int decompress(const bufferlist &src, bufferlist &dst) override {
    bufferlist::iterator i = const_cast<bufferlist&>(src).begin();
    return decompress(i, src.length(), dst);
  }

  int decompress(bufferlist::iterator &p,
		 size_t compressed_len,
		 bufferlist &dst) override {
    uint32_t count;
    std::vector<std::pair<uint32_t, uint32_t> > compressed_pairs;
    ::decode(count, p);
    compressed_pairs.resize(count);
    uint32_t total_origin = 0;
    for (unsigned i = 0; i < count; ++i) {
      ::decode(compressed_pairs[i].first, p);
      ::decode(compressed_pairs[i].second, p);
      total_origin += compressed_pairs[i].first;
    }
    compressed_len -= (sizeof(uint32_t) + sizeof(uint32_t) * count * 2);

    bufferptr dstptr(total_origin);
    LZ4_streamDecode_t lz4_stream_decode;
    LZ4_setStreamDecode(&lz4_stream_decode, nullptr, 0);

    bufferptr cur_ptr = p.get_current_ptr();
    bufferptr *ptr = &cur_ptr;
    Tub<bufferptr> data_holder;
    if (compressed_len != cur_ptr.length()) {
      data_holder.construct(compressed_len);
      p.copy_deep(compressed_len, *data_holder);
      ptr = data_holder.get();
    }

    char *c_in = ptr->c_str();
    char *c_out = dstptr.c_str();
    for (unsigned i = 0; i < count; ++i) {
      int r = LZ4_decompress_safe_continue(
          &lz4_stream_decode, c_in, c_out, compressed_pairs[i].second, compressed_pairs[i].first);
      if (r == (int)compressed_pairs[i].first) {
        c_in += compressed_pairs[i].second;
        c_out += compressed_pairs[i].first;
      } else if (r < 0) {
        return -1;
      } else if (r != (int)compressed_pairs[i].first) {
        return -2;
      }
    }
    dst.push_back(std::move(dstptr));
    return 0;
  }
};

#endif
