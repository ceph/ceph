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
#include "common/config.h"
#include "common/Tub.h"


class LZ4Compressor : public Compressor {
 public:
  LZ4Compressor(CephContext* cct) : Compressor(COMP_ALG_LZ4, "lz4") {
#ifdef HAVE_QATZIP
    if (cct->_conf->qat_compressor_enabled && qat_accel.init("lz4"))
      qat_enabled = true;
    else
      qat_enabled = false;
#endif
  }

  int compress(const bufferlist &src, bufferlist &dst) override {
    // older versions of liblz4 introduce bit errors when compressing
    // fragmented buffers.  this was fixed in lz4 commit
    // af127334670a5e7b710bbd6adb71aa7c3ef0cd72, which first
    // appeared in v1.8.2.
    //
    // workaround: rebuild if not contiguous.
    if (!src.is_contiguous()) {
      bufferlist new_src = src;
      new_src.rebuild();
      return compress(new_src, dst);
    }

#ifdef HAVE_QATZIP
    if (qat_enabled)
      return qat_accel.compress(src, dst);
#endif
    bufferptr outptr = buffer::create_small_page_aligned(
      LZ4_compressBound(src.length()));
    LZ4_stream_t lz4_stream;
    LZ4_resetStream(&lz4_stream);

    auto p = src.begin();
    size_t left = src.length();
    int pos = 0;
    const char *data;
    unsigned num = src.get_num_buffers();
    encode((uint32_t)num, dst);
    while (left) {
      uint32_t origin_len = p.get_ptr_and_advance(left, &data);
      int compressed_len = LZ4_compress_fast_continue(
        &lz4_stream, data, outptr.c_str()+pos, origin_len,
        outptr.length()-pos, 1);
      if (compressed_len <= 0)
        return -1;
      pos += compressed_len;
      left -= origin_len;
      encode(origin_len, dst);
      encode((uint32_t)compressed_len, dst);
    }
    ceph_assert(p.end());

    dst.append(outptr, 0, pos);
    return 0;
  }

  int decompress(const bufferlist &src, bufferlist &dst) override {
#ifdef HAVE_QATZIP
    if (qat_enabled)
      return qat_accel.decompress(src, dst);
#endif
    auto i = std::cbegin(src);
    return decompress(i, src.length(), dst);
  }

  int decompress(bufferlist::const_iterator &p,
		 size_t compressed_len,
		 bufferlist &dst) override {
#ifdef HAVE_QATZIP
    if (qat_enabled)
      return qat_accel.decompress(p, compressed_len, dst);
#endif
    uint32_t count;
    std::vector<std::pair<uint32_t, uint32_t> > compressed_pairs;
    decode(count, p);
    compressed_pairs.resize(count);
    uint32_t total_origin = 0;
    for (unsigned i = 0; i < count; ++i) {
      decode(compressed_pairs[i].first, p);
      decode(compressed_pairs[i].second, p);
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
      } else {
        return -2;
      }
    }
    dst.push_back(std::move(dstptr));
    return 0;
  }
};

#endif
