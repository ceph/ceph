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

#include <memory>
#include "include/buffer.h"
#include "include/encoding.h"
#include "compressor/Compressor.h"

class ZstdCompressor : public Compressor {
 public:
  ZstdCompressor(CephContext *cct) : Compressor(COMP_ALG_ZSTD, "zstd"), cct(cct) {}

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message) override {
    // RAII wrapper so every error path frees the stream (no manual frees).
    // ZSTD_freeCCtx is documented to accept NULL in case creation fails.
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> s(
      ZSTD_createCCtx(), &ZSTD_freeCCtx);
    if (s.get() == nullptr) {
      // It's not documented when ZSTD_createCCtx() returns NULL but it can happen in the case of a malloc failure.
      return -ENOMEM;
    }

    // ZSTD_CCtx_reset is not needed because we create a fresh context every time (see zstd streaming_compressio.c example)

    if (ZSTD_isError(ZSTD_CCtx_setParameter(s.get(), ZSTD_c_compressionLevel, cct->_conf->compressor_zstd_level))) {
      return -EINVAL;
    }
    if (ZSTD_isError(ZSTD_CCtx_setPledgedSrcSize(s.get(), src.length()))) {
      return -EINVAL;
    }
    auto p = src.begin();
    size_t left = src.length();

    // The on-disk format prefixes the compressed payload with the decompressed
    // length as a uint32_t (ceph::encode below); ensure it does not overflow.
    if (left > std::numeric_limits<uint32_t>::max()) {
      return -EFBIG;
    }

    size_t const out_max = ZSTD_compressBound(left);
    if (ZSTD_isError(out_max)) {
      return -EINVAL;
    }
    ceph::buffer::ptr outptr = ceph::buffer::create_small_page_aligned(out_max);
    ZSTD_outBuffer_s outbuf;
    outbuf.dst = outptr.c_str();
    outbuf.size = outptr.length();
    outbuf.pos = 0;

    // Tracks the most recent ZSTD_compressStream2 return value. After the final
    // ZSTD_e_end flush, 0 means the frame was fully completed and flushed.
    size_t r = 0;
    while (left) {
      ceph_assert(!p.end());
      struct ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(left, (const char**)&inbuf.src);
      left -= inbuf.size;
      ZSTD_EndDirective const zed = (left==0) ? ZSTD_e_end : ZSTD_e_continue;
      // ZSTD_compressStream is not guaranteed to finish immediately, so loop until it does.
      // From the docs:
      //   Note that the function may not consume the entire input, for example, because
      //   the output buffer is already full, in which case `input.pos < input.size`.
      //   The caller must check if input has been entirely consumed.
      //   note: ZSTD_e_continue is guaranteed to make some forward progress when called,
      //   but doesn't guarantee maximal forward progress. This is especially relevant
      do {
        r = ZSTD_compressStream2(s.get(), &outbuf, &inbuf, zed);
        if (ZSTD_isError(r)) {
          return -EINVAL;
        }
      } while (inbuf.pos < inbuf.size || (zed == ZSTD_e_end && r != 0));
    }
    ceph_assert(p.get_remaining() == 0);

    // The final ZSTD_e_end call must have returned 0, i.e. the frame epilogue
    // was fully flushed. Otherwise we'd emit a truncated, undecodable frame.
    if (r != 0) {
      return -EINVAL;
    }

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

    // RAII wrapper so every error path frees the stream (no manual frees).
    // ZSTD_freeDStream is documented to accept NULL in case creation fails.
    std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)> s(
      ZSTD_createDStream(), &ZSTD_freeDStream);
    if (s.get() == nullptr) {
      // It's not documented when ZSTD_createDStream() returns NULL but it can happen in the case of a malloc failure.
      return -ENOMEM;
    }

    // Tracks the most recent ZSTD_decompressStream return value; 0 means the
    // frame completed cleanly.
    size_t r = 0;
    while (compressed_len > 0) {
      if (p.end()) {
        // Truncated input: compressed_len claims more data than the buffer
        // actually contains.
        return -EINVAL;
      }
      ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(compressed_len,
					 (const char**)&inbuf.src);
      compressed_len -= inbuf.size;

      // Drive zstd until it has consumed this whole input chunk. zstd may stop
      // consuming input before the chunk is exhausted (e.g. if the output
      // buffer fills because the encoded dst_len prefix understated the real
      // size); looping on inbuf.pos ensures we don't silently drop the
      // remaining bytes of the chunk.
      while (inbuf.pos < inbuf.size) {
        size_t const prev_in_pos = inbuf.pos;
        size_t const prev_out_pos = outbuf.pos;
        r = ZSTD_decompressStream(s.get(), &outbuf, &inbuf);
        if (ZSTD_isError(r)) {
          // Corrupt input, etc.
          return -EINVAL;
        }
        if (r == 0) {
          // Frame ended.
          break;
        }
        if (inbuf.pos == prev_in_pos && outbuf.pos == prev_out_pos) {
          // No forward progress on either input or output, yet input remains
          // and the frame has not ended. This happens when the output buffer
          // is full but zstd still has more to emit, i.e. dst_len understated
          // the real decompressed size. Treat as corrupt rather than silently
          // truncating (and avoid spinning forever). Note: zstd legitimately
          // consumes input without producing output (and vice versa), so we
          // only bail when *neither* advances.
          return -EINVAL;
        }
      }
    }

    // Verify the frame actually completed and produced exactly the advertised
    // number of bytes. r != 0 means zstd is still expecting more input (a
    // truncated frame); a short output means dst_len overstated the size.
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
