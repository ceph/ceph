// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "Compression.h"
#include "BlueStore.h"
#include "include/intarith.h"
#include <limits>


template <typename Char>
struct basic_ostream_formatter : fmt::formatter<std::basic_string_view<Char>, Char> {
  template <typename T, typename OutputIt>
  auto format(const T& value, fmt::basic_format_context<OutputIt, Char>& ctx) const
      -> OutputIt {
    std::basic_stringstream<Char> ss;
    ss << value;
    return fmt::formatter<std::basic_string_view<Char>, Char>::format(
        ss.view(), ctx);
  }
};
using ostream_formatter = basic_ostream_formatter<char>;

template <> struct fmt::formatter<BlueStore::Blob::printer>
  : ostream_formatter {};
template <> struct fmt::formatter<BlueStore::Extent::printer>
  : ostream_formatter {};

#define dout_context bluestore->cct
#define dout_subsys ceph_subsys_bluestore_compression
#undef  dout_prefix
#define dout_prefix *_dout << "bluecompr "

using Estimator = BlueStore::Estimator;
using P = BlueStore::printer;

void Estimator::reset()
{
  new_size = 0;
  uncompressed_size = 0;
  compressed_occupied = 0;
  compressed_size = 0;
  total_uncompressed_size = 0;
  total_compressed_occupied = 0;
  total_compressed_size = 0;
  actual_compressed = 0;
  actual_compressed_plus_pad = 0;
  extra_recompress.clear();
}
inline void Estimator::batch(const BlueStore::Extent* e, uint32_t gain)
{
  const Blob *h_Blob = &(*e->blob);
  const bluestore_blob_t &h_bblob = h_Blob->get_blob();
  if (h_bblob.is_compressed()) {
    compressed_size += e->length * h_bblob.get_compressed_payload_length() / h_bblob.get_logical_length();
    compressed_occupied += gain;
  } else {
    uncompressed_size += e->length;
  }
  dout(20) << fmt::format("Estimator::batch {} gain {:#x}", e->print(P::NICK), gain) << dendl;
  dout(20) << fmt::format("Estimator::batch non-compr={:#x} compr-occup={:#x} compr-size?={:#x}",
    uncompressed_size, compressed_occupied, compressed_size) << dendl;
}

inline bool Estimator::is_worth()
{
  uint32_t cost = uncompressed_size * expected_compression_factor +
                  compressed_size * expected_recompression_error;
  uint32_t gain = uncompressed_size + compressed_occupied;
  double need_ratio = bluestore->cct->_conf->bluestore_recompression_min_gain;
  bool take = gain > cost * need_ratio;
  if (take) {
    total_uncompressed_size += uncompressed_size;
    total_compressed_occupied += compressed_occupied;
    total_compressed_size += compressed_size;
  }
  uncompressed_size = 0;
  compressed_occupied = 0;
  compressed_size = 0;
  return take;
}

inline bool Estimator::is_worth(const BlueStore::Extent* e)
{
  const Blob *h_Blob = &(*e->blob);
  const bluestore_blob_t &h_bblob = h_Blob->get_blob();
  ceph_assert(!h_bblob.is_compressed());
  ceph_assert(!h_bblob.is_shared());
  // for now assume it always worth
  total_uncompressed_size += e->length;
  return true;
}

inline void Estimator::mark_recompress(const BlueStore::Extent* e)
{
  ceph_assert(!extra_recompress.contains(e->logical_offset));
  dout(25) << "recompress: " << e->print(P::NICK + P::JUSTID) << dendl;
  extra_recompress.emplace(e->logical_offset, e->length);
}

inline void Estimator::mark_main(uint32_t location, uint32_t length)
{
  ceph_assert(!extra_recompress.contains(location));
  dout(25) << "main data compress: " << std::hex
    << location << "~" << length << std::dec << dendl;
  extra_recompress.emplace(location, length);
  new_size = length;
}

void Estimator::get_regions(std::vector<region_t>& regions)
{
  constexpr uint32_t unset = std::numeric_limits<uint32_t>::max();
  // walk extents to form continous regions
  region_t* r;
  uint32_t end = unset;
  auto i = extra_recompress.begin();
  while (i != extra_recompress.end()) {
    dout(25) << std::hex << i->first
      << "~" << i->second << dendl;
    if (end == unset) {
      regions.emplace_back();
      r = &regions.back();
      r->offset = i->first;
      r->length = i->second;
      end = i->first + i->second;
    } else {
      if (i->first == end) {
        r->length += i->second;
        end = i->first + i->second;
      }
    }
    ++i;
    if (i == extra_recompress.end() || i->first != end) {
      end = unset;
    }
  }
}

int32_t Estimator::split_and_compress(
  CompressorRef compr,
  uint32_t max_blob_size,
  ceph::buffer::list& data_bl,
  Writer::blob_vec& bd)
{
  uint32_t au_size = bluestore->min_alloc_size;
  uint32_t size = data_bl.length();
  ceph_assert(size > 0);
  uint32_t blobs = (size + max_blob_size - 1) / max_blob_size;
  uint32_t blob_size = p2roundup(size / blobs, au_size);
  std::vector<uint32_t> blob_sizes(blobs);
  for (auto& i: blob_sizes) {
    i = std::min(size, blob_size);
    size -= i;
  }
  int32_t disk_needed = 0;
  uint32_t bl_src_off = 0;
  for (auto& i: blob_sizes) {
    bd.emplace_back();
    bd.back().real_length = i;
    bd.back().compressed_length = 0;
    bd.back().object_data.substr_of(data_bl, bl_src_off, i);
    bl_src_off += i;
    // FIXME: memory alignment here is bad
    bufferlist t;
    std::optional<int32_t> compressor_message;
    int r = compr->compress(bd.back().object_data, t, compressor_message);
    ceph_assert(r == 0);
    bluestore_compression_header_t chdr;
    chdr.type = compr->get_type();
    chdr.length = t.length();
    chdr.compressor_message = compressor_message;
    encode(chdr, bd.back().disk_data);
    bd.back().disk_data.claim_append(t);
    uint32_t len = bd.back().disk_data.length();
    bd.back().compressed_length = len;
    uint32_t rem = p2nphase(len, au_size);
    if (rem > 0) {
      bd.back().disk_data.append_zero(rem);
    }
    actual_compressed += len;
    actual_compressed_plus_pad += len + rem;
    disk_needed += len + rem;
  }
  return disk_needed;
}

void Estimator::finish()
{
  dout(25) << "new_size=" << new_size
          << " unc_size=" << total_uncompressed_size
          << " comp_cost=" << total_compressed_size << dendl;
  uint32_t sum = new_size + total_uncompressed_size + total_compressed_size;
  double expected =
    (new_size + total_uncompressed_size) * expected_compression_factor +
    total_compressed_size * expected_recompression_error;
  double size_misprediction = double(expected - actual_compressed) / actual_compressed;
  double size_misprediction_weighted = 1.0 / sum * size_misprediction * 0.01;
  expected_compression_factor -= (new_size + total_uncompressed_size) * size_misprediction_weighted;
  expected_recompression_error -= total_compressed_size * size_misprediction_weighted;

  double expected_pad = actual_compressed * expected_pad_expansion;
  double pad_misprediction = (expected_pad - actual_compressed_plus_pad) / actual_compressed;
  expected_pad_expansion -= pad_misprediction * 0.01;
  dout(25) << "exp_comp_factor=" << expected_compression_factor
           << " exp_recomp_err=" << expected_recompression_error
           << " exp_pad_exp=" << expected_pad_expansion << dendl;
}
