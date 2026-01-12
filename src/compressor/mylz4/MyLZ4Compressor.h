// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MyLZ4COMPRESSOR_H
#define CEPH_MyLZ4COMPRESSOR_H

#include <optional>
// 【修正1】必须引用真正的系统头文件
#include <lz4.h>

#include "compressor/Compressor.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/config.h"

class MyLZ4Compressor : public Compressor {
 public:
  // 【修正2】COMP_ALG_MyLZ4 不存在，暂时借用 LZ4 的 ID，但名字叫 mylz4
  MyLZ4Compressor(CephContext* cct) : Compressor(Compressor::COMP_ALG_LZ4, "mylz4") {
#ifdef HAVE_QATZIP
    if (cct->_conf->qat_compressor_enabled && qat_accel.init("mylz4"))
      qat_enabled = true;
    else
      qat_enabled = false;
#endif
  }

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, boost::optional<int32_t> &compressor_message) override {
    if (!src.is_contiguous()) {
      ceph::buffer::list new_src = src;
      new_src.rebuild();
      return compress(new_src, dst, compressor_message);
    }

#ifdef HAVE_QATZIP
    if (qat_enabled)
      return qat_accel.compress(src, dst, compressor_message);
#endif
    // 【修正3】函数调用必须改回 LZ4_ 开头，否则链接时报错
    ceph::buffer::ptr outptr = ceph::buffer::create_small_page_aligned(
      LZ4_compressBound(src.length()));
    LZ4_stream_t lz4_stream;
    LZ4_resetStream(&lz4_stream);

    using ceph::encode;

    auto p = src.begin();
    size_t left = src.length();
    int pos = 0;
    const char *data;
    unsigned num = src.get_num_buffers();
    encode((uint32_t)num, dst);
    while (left) {
      uint32_t origin_len = p.get_ptr_and_advance(left, &data);
      // 【修正3】函数调用必须改回 LZ4_ 开头
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

  int decompress(const ceph::buffer::list &src, ceph::buffer::list &dst, boost::optional<int32_t> compressor_message) override {
#ifdef HAVE_QATZIP
    if (qat_enabled)
      return qat_accel.decompress(src, dst, compressor_message);
#endif
    auto i = std::cbegin(src);
    return decompress(i, src.length(), dst, compressor_message);
  }

  int decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 boost::optional<int32_t> compressor_message) override {
#ifdef HAVE_QATZIP
    if (qat_enabled)
      return qat_accel.decompress(p, compressed_len, dst, compressor_message);
#endif
    using ceph::decode;
    uint32_t count;
    decode(count, p);
    std::vector<std::pair<uint32_t, uint32_t> > compressed_pairs(count);
    uint32_t total_origin = 0;
    for (auto& [dst_size, src_size] : compressed_pairs) {
      decode(dst_size, p);
      decode(src_size, p);
      total_origin += dst_size;
    }
    compressed_len -= (sizeof(uint32_t) + sizeof(uint32_t) * count * 2);

    ceph::buffer::ptr dstptr(total_origin);
    // 【修正3】函数调用必须改回 LZ4_ 开头
    LZ4_streamDecode_t lz4_stream_decode;
    LZ4_setStreamDecode(&lz4_stream_decode, nullptr, 0);

    ceph::buffer::ptr cur_ptr = p.get_current_ptr();
    ceph::buffer::ptr *ptr = &cur_ptr;
    std::optional<ceph::buffer::ptr> data_holder;
    if (compressed_len != cur_ptr.length()) {
      data_holder.emplace(compressed_len);
      p.copy_deep(compressed_len, *data_holder);
      ptr = &*data_holder;
    }

    char *c_in = ptr->c_str();
    char *c_out = dstptr.c_str();
    for (unsigned i = 0; i < count; ++i) {
      // 【修正3】函数调用必须改回 LZ4_ 开头
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