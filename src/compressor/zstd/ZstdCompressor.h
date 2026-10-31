// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#ifndef CEPH_ZSTDCOMPRESSOR_H
#define CEPH_ZSTDCOMPRESSOR_H

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <limits>
#include <memory>
#include "include/buffer.h"
#include "include/encoding.h"
#include "compressor/Compressor.h"

class ZstdCompressor : public Compressor {
 public:
  ZstdCompressor(CephContext *cct) : Compressor(COMP_ALG_ZSTD, "zstd"), cct(cct) {}

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message) override {
    // ZSTD_freeCCtx accepts NULL.
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> s(
      ZSTD_createCCtx(), &ZSTD_freeCCtx);
    if (s.get() == nullptr) {
      return -ENOMEM;
    }

    if (ZSTD_isError(ZSTD_CCtx_setParameter(s.get(), ZSTD_c_compressionLevel, cct->_conf->compressor_zstd_level))) {
      return -EINVAL;
    }
    if (ZSTD_isError(ZSTD_CCtx_setPledgedSrcSize(s.get(), src.length()))) {
      return -EINVAL;
    }
    auto p = src.begin();
    size_t left = src.length();

    // the decompressed length is encoded as a uint32_t prefix below
    static_assert(std::numeric_limits<decltype(src.length())>::max() <=
                  std::numeric_limits<uint32_t>::max());

    size_t const out_max = ZSTD_compressBound(left);
    if (ZSTD_isError(out_max)) {
      return -EINVAL;
    }
    ceph::buffer::ptr outptr = ceph::buffer::create_small_page_aligned(out_max);
    ZSTD_outBuffer_s outbuf;
    outbuf.dst = outptr.c_str();
    outbuf.size = outptr.length();
    outbuf.pos = 0;

    size_t r = 0;
    while (left) {
      ceph_assert(!p.end());
      struct ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(left, (const char**)&inbuf.src);
      left -= inbuf.size;
      ZSTD_EndDirective const zed = (left==0) ? ZSTD_e_end : ZSTD_e_continue;
      // zstd doesn't guarantee consuming all input (or, for ZSTD_e_end,
      // completing the frame) in one call, so loop until it does
      while (true) {
        size_t const prev_in_pos = inbuf.pos;
        size_t const prev_out_pos = outbuf.pos;
        r = ZSTD_compressStream2(s.get(), &outbuf, &inbuf, zed);
        if (ZSTD_isError(r)) {
          return -EINVAL;
        }
        if (inbuf.pos == inbuf.size && (zed != ZSTD_e_end || r == 0)) {
          break;
        }
        if (inbuf.pos == prev_in_pos && outbuf.pos == prev_out_pos) {
          // no progress: outbuf is full, which ZSTD_compressBound() should prevent
          return -EINVAL;
        }
      }
    }
    ceph_assert(p.get_remaining() == 0);

    // prefix with decompressed length
    ceph::encode((uint32_t)src.length(), dst);
    dst.append(outptr, 0, outbuf.pos);
    return 0;
  }

  int decompress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> compressor_message) override {
    auto i = std::cbegin(src);
    return decompress(i, src.length(), dst, compressor_message);
  }

  int decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 std::optional<int32_t> compressor_message) override {
    if (compressed_len < 4) {
      return -EINVAL;
    }
    compressed_len -= 4;
    uint32_t dst_len;
    ceph::decode(dst_len, p);

    ceph::buffer::ptr dstptr(dst_len);
    ZSTD_outBuffer_s outbuf;
    outbuf.dst = dstptr.c_str();
    outbuf.size = dstptr.length();
    outbuf.pos = 0;

    // ZSTD_freeDStream accepts NULL.
    std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)> s(
      ZSTD_createDStream(), &ZSTD_freeDStream);
    if (s.get() == nullptr) {
      return -ENOMEM;
    }

    // stays 0 for an empty payload, which is what compress() emits for empty
    // input (just the length prefix, no zstd frame)
    size_t r = 0;
    while (compressed_len > 0) {
      if (p.end()) {
        // compressed_len exceeds the buffer
        return -EINVAL;
      }
      ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(compressed_len,
					 (const char**)&inbuf.src);
      compressed_len -= inbuf.size;

      while (inbuf.pos < inbuf.size) {
        size_t const prev_in_pos = inbuf.pos;
        size_t const prev_out_pos = outbuf.pos;
        r = ZSTD_decompressStream(s.get(), &outbuf, &inbuf);
        if (ZSTD_isError(r)) {
          return -EINVAL;
        }
        if (r == 0) {
          // the frame ended; there must be no trailing data
          if (inbuf.pos != inbuf.size || compressed_len > 0) {
            return -EINVAL;
          }
          break;
        }
        if (inbuf.pos == prev_in_pos && outbuf.pos == prev_out_pos) {
          // no progress: outbuf is full, so dst_len understated the size
          return -EINVAL;
        }
      }
    }

    // r != 0: truncated frame; short output: dst_len overstated the size
    if (r != 0) {
      return -EINVAL;
    }
    if (outbuf.pos != dst_len) {
      return -EINVAL;
    }

    dst.append(dstptr, 0, outbuf.pos);
    return 0;
  }
 private:
  CephContext *const cct;
};

#endif
