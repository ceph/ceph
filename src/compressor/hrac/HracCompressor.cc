#include "HracCompressor.h"
#include "common/debug.h"
#include "common/ceph_context.h"
#include "include/buffer.h" 
#include <immintrin.h> 
#include <x86gprintrin.h> 
#include <stdlib.h> // for aligned_alloc

#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix *_dout << "hrac: "

const uint32_t HRAC_BLK = 64;
const uint32_t HRAC_INNER = 16384;

// 宏定义复刻
#define dtype uint8_t
#define diff_e diff_e8
#define diff_d diff_d8
#define BW 8
#define LBW 3
#define sig_len(x) nonzero_count_u8[x]

// 辅助宏：将数值向上取整到 256 的倍数
#define ROUND_UP_256(x) (((x) + 255) & ~255)

uint32_t fits_kdecomp_u8(const uint8_t* in, uint32_t iLen, dtype* out, uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);
uint32_t fits_kcomp_u8(const dtype* in, uint32_t iLen, uint8_t* out, uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);

int HracCompressor::compress(const ceph::buffer::list &src, ceph::buffer::list &dst, boost::optional<int32_t> &compressor_message) {
  ldout(cct, 0) << "HRAC_DEBUG: compress() called! input_size=" << src.length() << dendl;

  uint32_t origin_len = src.length();
  
  if (origin_len < HRAC_BLK) {
    return -1; 
  }

  ceph::buffer::list src_contig = src;
  if (!src.is_contiguous()) {
    src_contig.rebuild();
  }
  const uint8_t* input_ptr = (const uint8_t*)src_contig.c_str();

  size_t max_out_len = (size_t)(origin_len * 1.2) + 2048;
  ceph::buffer::ptr out_ptr = ceph::buffer::create_small_page_aligned(max_out_len);
  uint8_t* output_raw = (uint8_t*)out_ptr.c_str();

  uint32_t compressed_len = fits_kcomp_u8(
      input_ptr, //const uint8_t* input_ptr = (const uint8_t*)src_contig.c_str();
      origin_len, //uint32_t origin_len = src.length();
      output_raw, //uint8_t* output_raw = (uint8_t*)out_ptr.c_str();
      max_out_len, //size_t max_out_len = (size_t)(origin_len * 1.2) + 2048;
      HRAC_BLK, //64
      256,//origin_len, 
      HRAC_INNER //1
  );

  ldout(cct, 0) << "HRAC_DEBUG: Compressed len=" << compressed_len << dendl;

  if (compressed_len == 0 || compressed_len >= origin_len) {
    return -1; 
  }

  ceph::encode(origin_len, dst);
  dst.append(out_ptr, 0, compressed_len);

  return 0;
}

int HracCompressor::decompress(const ceph::buffer::list &src, ceph::buffer::list &dst, boost::optional<int32_t> compressor_message) {
  ldout(cct, 0) << "HRAC_DEBUG: decompress() called! src_len=" << src.length() << dendl;

  if (src.length() <= 4) {
    return -1;
  }

  auto iter = src.cbegin();
  uint32_t origin_len;
  try {
    ceph::decode(origin_len, iter);
  } catch (...) {
    return -1;
  }

  ldout(cct, 0) << "HRAC_DEBUG: origin_len=" << origin_len << dendl;

  if (origin_len > 100 * 1024 * 1024) { 
      ldout(cct, 0) << "HRAC_ERROR: origin_len too big!" << dendl;
      return -1;
  }

  uint32_t compressed_len = src.length() - 4;
  
  size_t alloc_in_size = ROUND_UP_256(compressed_len + 64);
  void* aligned_input_buffer = aligned_alloc(256, alloc_in_size);
  
  if (!aligned_input_buffer) {
      ldout(cct, 0) << "HRAC_ERROR: Alloc failed for input" << dendl;
      return -1;
  }
  
  memset(aligned_input_buffer, 0, alloc_in_size);

  iter.copy(compressed_len, (char*)aligned_input_buffer);
  const uint8_t* input_raw = (const uint8_t*)aligned_input_buffer;

  size_t alloc_out_size = ROUND_UP_256(origin_len + 64);
  void* aligned_output_buffer = aligned_alloc(256, alloc_out_size);
  
  if (!aligned_output_buffer) {
      ldout(cct, 0) << "HRAC_ERROR: Alloc failed for output" << dendl;
      free(aligned_input_buffer);
      return -1;
  }
  memset(aligned_output_buffer, 0, alloc_out_size);
  uint8_t* output_raw = static_cast<uint8_t*>(aligned_output_buffer);

  ldout(cct, 0) << "HRAC_DEBUG: Calling fits_kdecomp_u8..." << dendl;
  
  fits_kdecomp_u8(
      input_raw,
      compressed_len,
      output_raw,
      origin_len, 
      HRAC_BLK,
      256,//origin_len, 
      HRAC_INNER
  );

  ldout(cct, 0) << "HRAC_DEBUG: Decompression done." << dendl;

  //dst.append(ceph::buffer::claim_char(origin_len, (char*)aligned_output_buffer));
  dst.append((char*)aligned_output_buffer, origin_len);
  free(aligned_input_buffer);

  return 0;
}