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

#include "include/buffer.h"
#include "include/encoding.h"
#include "compressor/Compressor.h"

class ZstdCompressor : public Compressor {
 public:
  ZstdCompressor(CephContext *cct) : Compressor(COMP_ALG_ZSTD, "zstd"), cct(cct) {}

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message) override {
    // the decompressed length is stored as a uint32_t prefix, so a source
    // larger than that cannot be round-tripped
    if (src.length() > std::numeric_limits<uint32_t>::max()) {
      return -EINVAL;
    }
    ZSTD_CCtx *s = ZSTD_createCCtx();
    if (!s) {
      return -ENOMEM;
    }
    size_t res = ZSTD_CCtx_reset(s, ZSTD_reset_session_and_parameters);
    if (ZSTD_isError(res)) {
      ZSTD_freeCCtx(s);
      return -EINVAL;
    }
    res = ZSTD_CCtx_setParameter(s, ZSTD_c_compressionLevel, cct->_conf->compressor_zstd_level);
    if (ZSTD_isError(res)) {
      ZSTD_freeCCtx(s);
      return -EINVAL;
    }
    res = ZSTD_CCtx_setPledgedSrcSize(s, src.length());
    if (ZSTD_isError(res)) {
      ZSTD_freeCCtx(s);
      return -EINVAL;
    }
    auto p = src.begin();
    size_t left = src.length();

    size_t const out_max = ZSTD_compressBound(left);
    ceph::buffer::ptr outptr = ceph::buffer::create_small_page_aligned(out_max);
    ZSTD_outBuffer_s outbuf;
    outbuf.dst = outptr.c_str();
    outbuf.size = outptr.length();
    outbuf.pos = 0;

    while (left) {
      ceph_assert(!p.end());
      struct ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(left, (const char**)&inbuf.src);
      left -= inbuf.size;
      ZSTD_EndDirective const zed = (left==0) ? ZSTD_e_end : ZSTD_e_continue;
      size_t r = ZSTD_compressStream2(s, &outbuf, &inbuf, zed);
      if (ZSTD_isError(r)) {
        ZSTD_freeCCtx(s);
        return -EINVAL;
      }
    }
    ceph_assert(p.get_remaining() == 0);

    ZSTD_freeCCtx(s);

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
    ZSTD_DStream *s = ZSTD_createDStream();
    if (!s) {
      return -ENOMEM;
    }
    size_t res = ZSTD_initDStream(s);
    if (ZSTD_isError(res)) {
      ZSTD_freeDStream(s);
      return -EINVAL;
    }
    // tracks the return of the last ZSTD_decompressStream() call; a non-zero
    // value after all input is consumed means the frame was truncated
    size_t left_in_frame = 0;
    while (compressed_len > 0) {
      if (p.end()) {
	ZSTD_freeDStream(s);
	return -EINVAL;
      }
      ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(compressed_len,
					 (const char**)&inbuf.src);
      compressed_len -= inbuf.size;
      size_t r = ZSTD_decompressStream(s, &outbuf, &inbuf);
      if (ZSTD_isError(r)) {
	ZSTD_freeDStream(s);
	return -EINVAL;
      }
      // zstd must consume the whole input chunk. If it stopped early the
      // output buffer filled up, which means the decoded-length prefix
      // understated the real size: a corrupt or inconsistent frame.
      if (inbuf.pos != inbuf.size) {
	ZSTD_freeDStream(s);
	return -EINVAL;
      }
      left_in_frame = r;
    }
    ZSTD_freeDStream(s);

    // the frame must have ended exactly when the input ran out, and must have
    // produced exactly the number of bytes promised by the length prefix
    if (left_in_frame != 0 || outbuf.pos != dst_len) {
      return -EINVAL;
    }

    dst.append(dstptr, 0, outbuf.pos);
    return 0;
  }
 private:
  CephContext *const cct;
};

#endif
