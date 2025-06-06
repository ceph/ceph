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

using Extent = BlueStore::Extent;
using ExtentMap = BlueStore::ExtentMap;
using Blob = BlueStore::Blob;
using exmp_cit = BlueStore::extent_map_t::const_iterator;
using exmp_it = BlueStore::extent_map_t::iterator;
using Scanner = BlueStore::Scanner;
using Scan = BlueStore::Scanner::Scan;
using P = BlueStore::printer;
using Estimator = BlueStore::Estimator;
using P = BlueStore::printer;

void Estimator::cleanup()
{
  wctx = nullptr;
  new_size = 0;
  uncompressed_size = 0;
  compressed_occupied = 0;
  compressed_size = 0;
  compressed_area = 0;
  total_uncompressed_size = 0;
  total_compressed_occupied = 0;
  total_compressed_size = 0;
  actual_compressed = 0;
  actual_compressed_plus_pad = 0;
  extra_recompress.clear();
  single_compressed_blob = true;
  last_blob = nullptr;
}

void Estimator::set_wctx(const WriteContext* wctx)
{
  this->wctx = wctx;
}

inline void Estimator::batch(const BlueStore::Extent* e, uint32_t gain)
{
  const Blob *h_Blob = &(*e->blob);
  const bluestore_blob_t &h_bblob = h_Blob->get_blob();
  if (h_bblob.is_compressed()) {
    if (last_blob) {
      if (h_Blob != last_blob) {
        single_compressed_blob = false;
      }
    } else {
      last_blob = h_Blob;
    }
    compressed_size += e->length * h_bblob.get_compressed_payload_length() / h_bblob.get_logical_length();
    compressed_area += e->length;
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
  if (uncompressed_size == 0 && single_compressed_blob) {
    // The special case if all extents are from compressed blobs.
    // We want to avoid the case of recompressing into exactly the same.
    // The cost should increase proportionally to blob size;
    // the rationale is that recompressing small blob is likely to provide gain,
    // but recompressing whole large blob isn't.
    uint64_t padding_size = p2nphase<uint64_t>(compressed_size, bluestore->min_alloc_size);
    uint32_t split_tax = padding_size * compressed_area / wctx->target_blob_size;
    cost += split_tax;
  }
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
  ceph::buffer::list& data_bl,
  Writer::blob_vec& bd)
{
  uint32_t au_size = bluestore->min_alloc_size;
  uint32_t size = data_bl.length();
  ceph_assert(size > 0);
  uint32_t blobs = (size + wctx->target_blob_size - 1) / wctx->target_blob_size;
  uint32_t blob_size = p2roundup((size + blobs - 1) / blobs, au_size);
  // dividing 'size' to 'blobs'
  // blobs[*] = blob_size; blobs[last] = whatever remains from 'size'
  std::vector<uint32_t> blob_sizes(blobs, blob_size);
  blob_sizes.back() = size - blob_size * (blobs - 1);
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
    int r = wctx->compressor->compress(bd.back().object_data, t, compressor_message);
    ceph_assert(r == 0);
    bluestore_compression_header_t chdr;
    chdr.type = wctx->compressor->get_type();
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
  cleanup();
}

void Estimator::dump(Formatter *f) const {
  f->dump_float("expected_compression_now", expected_compression_factor);
  f->dump_float("expected_recompression_error", expected_recompression_error);
  f->dump_float("expected_pad_expansion", expected_pad_expansion);
}

Estimator* BlueStore::create_estimator()
{
  return new Estimator(this);
}

struct scan_blob_element_t {
  uint32_t blob_use_size = 0;    // how many bytes in object logical mapping are covered by this blob
  uint32_t visited_size = 0;     // updated during scanning, will be either commited to consumed or reverted from consumed
  uint32_t consumed_size = 0;    // updated during merging
  uint32_t consumed_saved_size = 0;
  uint32_t blob_left = 0;        // beginning of blob in object logical space
  uint32_t blob_right = 0;       // end of blob in object logical space
  exmp_cit leftmost_extent;      // leftmost extent that refers the blob
  exmp_cit rightmost_extent;     // rightmost extent that refers the blob
  bool     rejected = false;     // blob checked and rejected for recompression
  scan_blob_element_t(
    uint32_t blob_use_size,
    uint32_t visited_size,
    uint32_t consumed_size,
    uint32_t consumed_saved_size,
    uint32_t blob_left,
    uint32_t blob_right,
    exmp_cit leftmost_extent,
    exmp_cit rightmost_extent,
    bool rejected = false)
    : blob_use_size(blob_use_size)
    , visited_size(visited_size)
    , consumed_size(consumed_size)
    , consumed_saved_size(consumed_saved_size)
    , blob_left(blob_left)
    , blob_right(blob_right)
    , leftmost_extent(leftmost_extent)
    , rightmost_extent(rightmost_extent)
    , rejected(rejected) {}
};
using object_scan_info_t = std::map<const Blob*, scan_blob_element_t>;

class BlueStore::Scanner::Scan {
public:
  BlueStore* bluestore;
  BlueStore::Onode* onode;
  ExtentMap* extent_map;
  Estimator* estimator;
  Scan(
    BlueStore* bluestore,
    BlueStore::Onode* onode,
    Estimator* estimator)
  : bluestore(bluestore)
  , onode(onode)
  , extent_map(&onode->extent_map)
  , estimator(estimator) {}
  object_scan_info_t scanned_blobs;
  void on_write_start(
    uint32_t offset, uint32_t length,
    uint32_t left_limit, uint32_t right_limit);
  struct exmp_logical_offset {
    const Scan &ow;
    const exmp_cit &cit;
    exmp_logical_offset(const Scan& ow, const exmp_cit& cit)
    : ow(ow), cit(cit) {}
  };
  friend std::ostream &operator<<(std::ostream &out,
                                  const exmp_logical_offset &lo);

private:
  bool is_end(const exmp_cit &it) const {
    return it == extent_map->extent_map.end();
  }
  // range that has aready been processed
  uint32_t done_left;
  uint32_t done_right;
  // range that is allowed for processing
  uint32_t limit_left;
  uint32_t limit_right;
  // range that has already been scanned for blobs
  uint32_t scanned_left;
  uint32_t scanned_right;
  exmp_cit scanned_left_it;  // <- points to first in scanned range
  exmp_cit scanned_right_it; // <- points to first outside scanned range
  void scan_for_compressed_blobs(exmp_cit start, exmp_cit finish);
  bool maybe_expand_scan_range(const exmp_cit &it, uint32_t &left, uint32_t &right);
  void remove_consumed_blobs();
  void move_visited_to_consumed();
  void remove_fully_consumed_blobs();
  void save_consumed();
  void restore_consumed();
  object_scan_info_t::iterator find_least_expanding();
  void candidate(const Extent *e);
  void estimate_gain_left(exmp_cit left, uint32_t right);
  void estimate_gain_right(uint32_t left, exmp_cit right);
  void add_compress_left(exmp_cit left, uint32_t right);
  void add_compress_right(uint32_t left, exmp_cit right);
  void expand_left(exmp_cit &it);
  void expand_right(exmp_cit &it);
  exmp_logical_offset lo(const exmp_cit &it) {
    return exmp_logical_offset(*this, it);
  }
};
template <> struct fmt::formatter<BlueStore::Scanner::Scan::exmp_logical_offset>
  : ostream_formatter {};

void Scanner::write_lookaround(
  BlueStore::Onode* onode,
  uint32_t offset, uint32_t length,
  uint32_t left_limit, uint32_t right_limit,
  Estimator* estimator)
{
  Scan on_write(bluestore, onode, estimator);
  on_write.on_write_start(offset, length, left_limit, right_limit);
}

std::ostream& operator<<(
  std::ostream& out, const scan_blob_element_t& b)
{
  out << fmt::format("blob=[{:#x}-{:x}] extent=[{:#x}-{:x}] {:#x}/{:x}/{:x}({:x}){}",
    b.blob_left, b.blob_right,
    b.leftmost_extent->logical_offset, b.rightmost_extent->logical_offset,
    b.blob_use_size, b.visited_size, b.consumed_size, b.consumed_saved_size,
    b.rejected?" rejected":"");
  return out;
}

std::ostream& operator<<(
  std::ostream& out, const object_scan_info_t& scanned_blobs)
{
  out << "[";
  for (const auto& it : scanned_blobs) {
    out << std::endl;
    out << it.first->print(P::NICK + P::JUSTID) << " ==> " << it.second;
  }
  out << "]";
  return out;
}

std::ostream& operator<<(
  std::ostream& out,
  const Scan::exmp_logical_offset& lo)
{
  if (lo.ow.is_end(lo.cit)) {
    out << "end";
  } else {
    out << std::hex << lo.cit->logical_offset << std::dec;
  }
  return out;
}

// Action to do after initial scan_for_compressed_blobs on write region.
// The compressed blobs that are completely visited are removed, because
// those blob will no longer exist.
// The visited size becomes consumed size, because relevant extents will be
// overwritten with new data.
void Scan::remove_consumed_blobs()
{
  for (auto i = scanned_blobs.begin(); i != scanned_blobs.end();) {
    if (i->second.visited_size == i->second.blob_use_size) {
      i = scanned_blobs.erase(i);
    } else {
      i->second.consumed_size = i->second.visited_size;
      ++i;
    }
  }
}

inline void Scan::save_consumed()
{
  for (auto& i : scanned_blobs) {
    i.second.consumed_saved_size = i.second.consumed_size;
  }
};

inline void Scan::restore_consumed()
{
  for (auto& i : scanned_blobs) {
    i.second.consumed_size = i.second.consumed_saved_size;
  }
};

// Scan to take info on compressed blobs in specified range.
// It can be used for initial cover for punch_hole() region,
// or as subsequent scan to fully cover compressed blobs that
// we intend to become candidates for recompression.
// start - first extent of region to scan
// finish - the first extent outside scan region
void Scan::scan_for_compressed_blobs(
  exmp_cit start, exmp_cit finish)
{
  dout(20) << "scan.compr range " << lo(start) << "-" << lo(finish) << dendl;
  for (auto it = start; it != finish; ++it) {
    const Blob* h_Blob = &(*it->blob);
    const bluestore_blob_t& h_bblob = h_Blob->get_blob();
    if (h_bblob.is_shared()) {
      // Do not analyze shared blobs, even compressed ones.
      // It seems improbable that recompressing it would yield benefits.
      dout(20) << "scan.compr " << h_Blob->print(P::NICK | P::JUSTID) << " shared, rejected" << dendl;
      continue;
    }
    if (!h_bblob.is_compressed()) {
      dout(20) << "scan.compr " << h_Blob->print(P::NICK | P::JUSTID) << " non-compressed, rejected" << dendl;
      continue;
    }
    auto bit = scanned_blobs.find(h_Blob);
    if (bit == scanned_blobs.end()) {
      // first time we see this blob
      dout(20) << fmt::format("scan.compr {} new, range [{:#x}-{:x}]",
        h_Blob->print(P::NICK | P::JUSTID), it->logical_offset, it->logical_end()) << dendl;
      scanned_blobs.emplace(
          h_Blob,
          scan_blob_element_t(
              h_Blob->get_blob_use_tracker().get_referenced_bytes(),
              it->length, 0, 0, it->blob_start(), it->blob_end(), it, it));
      // Do not care that blob_start() or blob_end() might be outside limit_left
      // or limit_right. It is possible that no extent is on the outside and we
      // still can fully scan blob.
    } else {
      // we are already processing this blob, just update
      scan_blob_element_t &info = bit->second;
      info.visited_size += it->length;
      if (it->logical_offset > info.rightmost_extent->logical_offset) {
        info.rightmost_extent = it;
      }
      if (it->logical_offset < info.leftmost_extent->logical_offset) {
        info.leftmost_extent = it;
      }
      dout(20) << fmt::format("scan.compr {} updated range [{:#x}-{:x}]",
        h_Blob->print(P::NICK | P::JUSTID),
        info.leftmost_extent->logical_offset, info.rightmost_extent->logical_end()) << dendl;
      ceph_assert(info.blob_left == it->blob_start());
      ceph_assert(info.blob_right == it->blob_end());
    }
  }
  dout(30) << "scan.compr done" << dendl;
}

// Takes a compressed extent `it` and expands scan range
// so it would surely encompass all extents that refer to the same blob.
bool Scan::maybe_expand_scan_range(
  const exmp_cit& it, uint32_t& left, uint32_t& right)
{
  bool expanded = false;
  const Blob *h_Blob = &(*it->blob);
  const bluestore_blob_t &h_bblob = h_Blob->get_blob();
  // we made left_walk up to here, it must be compressed blob
  if (h_bblob.is_shared()) {
    dout(30) << "maybe_expand rejecting shared " << h_Blob->print(P::NICK + P::JUSTID) << dendl;
  } else {
    ceph_assert(h_bblob.is_compressed());
    auto i = scanned_blobs.find(h_Blob);
    if (i == scanned_blobs.end() || i->second.rejected == false) {
      // We only accept blobs we have not seen before,
      // or ones that are not fully scanned yet.
      left = std::min(left, it->blob_start());
      left = std::max(limit_left, left);
      right = std::max(right, it->blob_end());
      right = std::min(limit_right, right);
      dout(20) << fmt::format("maybe_expand take {} range=[{:#x}-{:x}]",
        h_Blob->print(P::NICK + P::SDISK + P::SUSE + P::SCHK), left, right) << dendl;
      expanded = true;
    } else {
      dout(30) << "maybe_expand blob aready scanned and rejected" << dendl;
    }
  }
  return expanded;
}

// Calculate gain and cost of recompressing an extent
void Scan::candidate(const Extent* e)
{
  const Blob *h_Blob = &(*e->blob);
  const bluestore_blob_t &h_bblob = h_Blob->get_blob();
  uint32_t gain = 0;
  dout(30) << "candidate " << e->print(P::NICK + P::JUSTID) << dendl;
  if (h_bblob.is_shared()) {
    dout(30) << "candidate ignoring shared " << h_Blob->print(P::NICK + P::JUSTID) << dendl;
  } else if (h_bblob.is_compressed()) {
    auto bit = scanned_blobs.find(h_Blob);
    if (bit != scanned_blobs.end()) {
      bit->second.consumed_size += e->length;
      if (bit->second.consumed_size == bit->second.blob_use_size) {
        // Seen all extents of the blob, can take gain.
        gain = h_bblob.get_ondisk_length();
      }
      estimator->batch(e, gain);
    } else {
      derr << "candidate blob not scanned before! " << h_Blob->print(P::NICK + P::SUSE) << dendl;
      // This blob was seen, but removed as completely consumed?
      // Is it even possible?
      ceph_assert(false);
    }
  } else {
    // not compressed
    gain = e->length;
    estimator->batch(e, gain);
  }
};

// left side variant of estimate
// 'left' is extent that is the leftmost extent to be analyzed.
// 'right' is the leftmost logical offset of area already marked for recompression
void Scan::estimate_gain_left(
  exmp_cit left, uint32_t right)
{
  auto i = left;
  uint32_t last_offset = i->logical_offset;
  for (; i != extent_map->extent_map.end() && i->logical_offset < right; ++i) {
    ceph_assert(i->logical_offset >= last_offset);
    if (last_offset < i->logical_offset) {
      // Here we have a hole.
      // TODO something has to be done with it....
      // Does it influence compression gain/cost?
    }
    candidate(&(*i));
    last_offset = i->logical_end();
  }
  if (last_offset < right) {
    // Here we have a hole
    // TODO somehow factor it in compression gain/cost.
  }
};

// right side variant of estimate
// 'left' is the first byte after rightmost logical offset that has already marked for recompression
// 'right' is extent that is the last that should be analyzed.
void Scan::estimate_gain_right(
  uint32_t left, exmp_cit right)
{
  uint32_t last_offset = left;
  for (auto i = extent_map->seek_lextent(left);;) {
    ceph_assert(i->logical_offset >= last_offset);
    if (i->logical_offset > last_offset) {
      // here we have hole
      // TODO something has to be done with it....
    }
    candidate(&(*i));
    last_offset = i->logical_end();
    if (i == right)
      break; // stop once right is processed
    ++i;
  };
};

// Add extents to recompress. Left side variant.
// 'left' is extent that is the leftmost extent to add.
// 'right' is the leftmost logical offset of area already marked for recompression
void Scan::add_compress_left(
  exmp_cit left, uint32_t right)
{
  ceph_assert(left != extent_map->extent_map.end());
  dout(30) << fmt::format("add_compress_left extents {:#x}(include) .. {:#x}",
    left->logical_offset, right) << dendl;
  ceph_assert(!left->blob->get_blob().is_shared());
  for (auto i = left; i != extent_map->extent_map.end() && i->logical_offset < right; ++i) {
    if (!i->blob->get_blob().is_shared()) {
      // we only process not shared blobs
      estimator->mark_recompress(&(*i));
    }
  }
}

// Add extents to recompress. Right side variant.
// 'left' is the first byte after rightmost logical offset 
//        that has already been marked for recompression
// 'right' is the rightmost extent that should be added
void Scan::add_compress_right(
  uint32_t left, exmp_cit right) {
  ceph_assert(right != extent_map->extent_map.end());
  dout(30) << fmt::format("add_compress_right {:#x} .. {:#x}(include)",
    left, right->logical_offset) << dendl;
  ceph_assert(!right->blob->get_blob().is_shared());
  for (auto i = extent_map->seek_lextent(left);;) {
    ceph_assert(i != extent_map->extent_map.end());
    if (!i->blob->get_blob().is_shared()) {
      // we only process not shared blobs
      estimator->mark_recompress(&(*i));
    }
    if (i == right)
      break; // stop once right is processed
    ++i;
  }
}

// Argument `it` points to extent that has already been processed
// Returned `it` points to extent that has been processed
void Scan::expand_left(exmp_cit& it)
{
  dout(30) << "expand_left start it=" << lo(it) << dendl;
  uint16_t mode = P::NICK + P::SDISK + P::SUSE;
  for (; it != extent_map->extent_map.begin();) {
    --it;
    const Blob *h_Blob = &(*it->blob);
    const bluestore_blob_t &h_bblob = h_Blob->get_blob();
    if (it->logical_offset < limit_left || h_bblob.is_shared() || h_bblob.is_compressed()) {
      dout(30) << "expand_left stops at " << it->print(mode) << dendl;
      ++it; //back out
      break;
    }
    if (estimator->is_worth(&(*it))) {
      dout(20) << "expand_left take " << it->print(mode) << dendl;
      estimator->mark_recompress(&(*it));
      done_left = it->logical_offset;
    } else {
      dout(20) << "expand_left rejected " << it->print(mode) << dendl;
      break;
    }
  }
  scanned_left_it = it;
  scanned_left = it->logical_offset;
  dout(30) << "expand_left done it=" << lo(it) << dendl;
}

// Argument `it` points to extent that is a candidate
// Returned `it` points to extent that has been rejected
void Scan::expand_right(exmp_cit& it)
{
  uint16_t mode = P::NICK + P::SDISK + P::SUSE;
  dout(30) << "expand_right start it=" << lo(it) << dendl;
  for (; !is_end(it); ++it) {
    const Blob *h_Blob = &(*it->blob);
    const bluestore_blob_t &h_bblob = h_Blob->get_blob();
    if (it->logical_end() > limit_right || h_bblob.is_shared() || h_bblob.is_compressed()) {
      dout(30) << "expand_right stops at " << it->print(mode) << dendl;
      break;
    }
    if (estimator->is_worth(&(*it))) {
      dout(20) << "expand_right take " << it->print(mode) << dendl;
      estimator->mark_recompress(&(*it));
      done_right = it->logical_end();
    } else {
      dout(20) << "expand_right rejected " << it->print(mode) << dendl;
      break;
    }
  }
  scanned_right_it = it;
  scanned_right = is_end(it) ? limit_right : it->logical_offset;
  dout(30) << "expand_right done it=" << lo(it) << dendl;
}

// Searches in `scanned_blobs` for a blob that causes least expansion of recompress range.
// Never selects "rejected" blobs.
// Uses `done_left` and `done_right` to measure expansion ratio.
object_scan_info_t::iterator Scanner::Scan::find_least_expanding()
{
  auto expansion_size = [&](object_scan_info_t::const_iterator i) -> uint32_t {
    return done_left - std::min(i->second.leftmost_extent->logical_offset, done_left) +
           std::max(i->second.rightmost_extent->logical_end(), done_right) - done_right;
  };
  if (scanned_blobs.empty()) {
    return scanned_blobs.end();
  }
  // find first non-rejected
  auto imin = scanned_blobs.begin();
  for (; imin != scanned_blobs.end() && imin->second.rejected; ++imin) {
  }
  if (imin != scanned_blobs.end()) {
    uint32_t vmin = expansion_size(imin);
    auto i = imin;
    ++i;
    for (; i != scanned_blobs.end(); ++i) {
      if (!i->second.rejected) {
        uint32_t v = expansion_size(i);
        if (v < vmin) {
          vmin = v;
          imin = i;
        }
      }
    }
  }
  return imin;
};

// 1. Scan write region for compressed blobs
// 2. Some compressed blobs might stick out of write region, calculate range that covers them
// 3. Scan expanded region to make sure compressed blobs are fully scanned
//              WRITEWRITE
//           AAaaAaAA  CCCCccCC
//             BB E        DD
//     FFFFFFF
// A, B, C blobs partialy scanned during 1)
// E blob fully scanned during 1)
// A - E blobs fully scanned during 3)
// F blob partially scanned during 3)
//
// 4. Order observed compressed blobs from smallest to largest.
//    Check if melding gives space benefits. Meld.
// NOTE - partially scanned blobs from step 3 are never necessary for
// recompression of write region
// Proof:
// Notice that compressed blobs cannot cross each other
// AAABBBAAABBB <- not possible, A or B has to be written first:
// AAABBBBBBBBB or AAAAAAAAABBB <- only possible overlaps
// So, when we find outermost extents of compressed blobs:
//              WRITEWRITE
//           A BB   A  C       C
//    F       F            D  D
//            ^impossible  ^possible
//            (FAFA)       (CDDC)
// In the result if we find additional compressed blobs during step 3
// it will be fully inside range calculated in step 2.
// 5. When we consumed all compressed blob candidates, 
//    try to expand on previously non-compressed extents.
// 6. If blob on left and right are compressed, 
//    expand scan region and goto 3
void Scan::on_write_start(
  uint32_t offset, uint32_t length,
  uint32_t _limit_left, uint32_t _limit_right)
{
  estimator->mark_main(offset, length);
  done_left = offset;
  done_right = offset + length;
  limit_left = _limit_left;
  limit_right = _limit_right;
  dout(10) << fmt::format("on_write #1 [{:#x}-{:x}] limit=({:#x}-{:x})",
    done_left, done_right, limit_left, limit_right) << dendl;
  dout(20) << "on_write #1 on: "
    << onode->print(P::NICK + P::SDISK + P::SUSE, limit_left, limit_right) << dendl;

  ceph_assert(limit_left <= done_left && done_right <= limit_right);

  exmp_cit start = extent_map->maybe_split_at(done_left);
  exmp_cit finish = extent_map->maybe_split_at(done_right);
  
  // STEP 1.
  // Scan blobs in write punch_hole() region.
  scan_for_compressed_blobs(start, finish);
  // The region under write will release previous content.
  // Remove those blobs that will not exist after punch_hole().
  remove_consumed_blobs();

  scanned_left_it = start;
  scanned_right_it = finish;
  scanned_left = done_left;
  scanned_right = done_right;
  // Note. It is possible that:
  // scanned_left_it->logical_offset != scanned_left
  // scanned_right_it->logical_offset != scanned_right
  // STEP 2.
  dout(15) << fmt::format("on_write #2 scan=[{:#x}-{:x}] it={}-{}",
    scanned_left, scanned_right, lo(scanned_left_it), lo(scanned_right_it)) << dendl;

  uint32_t left = std::numeric_limits<uint32_t>::max();
  uint32_t right = std::numeric_limits<uint32_t>::min();
  // Find maximum reach of blobs we scanned.
  for (auto& i: scanned_blobs) {
    ceph_assert(i.second.blob_use_size != i.second.consumed_size);
    left = std::min(left, i.second.blob_left);
    right = std::max(right, i.second.blob_right);
  }
  // Keep scan range withing limit.
  left = std::max(left, limit_left);
  right = std::min(right, limit_right);
  // STEP 3.
  // `left`, `right` cannot be iterators, because original begin/end of compressed blob
  // could be overwritten, so there would be no extent that left_it->logical_offset == left.
  step3:
  if (left < scanned_left || scanned_right < right) {
    dout(15) << fmt::format("on_write #3 expand [{:#x}-{:x}] to [{:#x}-{:x}]",
      scanned_left, scanned_right, left, right) << dendl;
    if (left < scanned_left) {
      exmp_cit s = extent_map->seek_nextent(left); //get precisely or next
      scan_for_compressed_blobs(s, scanned_left_it);
      scanned_left = left;
      scanned_left_it = s;
    }
    // scan right side
    if (scanned_right < right) {
      //exmp_cit s = extent_map->seek_lextent(done_right);
      exmp_cit f = extent_map->seek_nextent(right);
      scan_for_compressed_blobs(scanned_right_it, f);
      scanned_right = right;
      scanned_right_it = f;
    }
    // Now, compressed blobs in write range should be fully visible,
    // as should compressed blobs from step 6.
    // The exception is if some extents fell outside processing limit.

    // STEP 4.
    // We are ready to take blobs in expanding order.
    // There can be 2 blobs that are same least expanding.
    // But it is impossible that 2 extents start or end at the same position.
    dout(20) << "on_write #4 scanned_blobs=" << scanned_blobs << dendl;
    object_scan_info_t::iterator b_it;
    while (true) {
      b_it = find_least_expanding();
      if (b_it == scanned_blobs.end()) {
        break;
      }
      dout(25) << "on_write #4 test expand " << b_it->first->print(P::NICK | P::JUSTID)
        << ", " << b_it->second << dendl;
      save_consumed();
      // now check if we want to merge this blob that requires least expanding
      if (b_it->second.leftmost_extent->logical_offset < done_left) {
        estimate_gain_left(b_it->second.leftmost_extent, done_left);
      }
      if (done_right < b_it->second.rightmost_extent->logical_end()) {
        estimate_gain_right(done_right, b_it->second.rightmost_extent);
      }
      if (estimator->is_worth()) {
        // meld this new blob into re-write region
        if (b_it->second.leftmost_extent->logical_offset < done_left) {
          add_compress_left(b_it->second.leftmost_extent, done_left);
          done_left = b_it->second.leftmost_extent->logical_offset;
        }
        if (done_right < b_it->second.rightmost_extent->logical_end()) {
          add_compress_right(done_right, b_it->second.rightmost_extent);
          done_right = b_it->second.rightmost_extent->logical_end();
        }
        scanned_blobs.erase(b_it);
      } else {
        // We do not want to recompress this blob yet.
        restore_consumed();
        // Do not select this blob again.
        b_it->second.rejected = true;
        // It might get recompressed as a part of larger recompress though.
      }
    }
  } else {
    dout(15) << fmt::format("on_write #3 scanned=[{:#x}-{:x}]", scanned_left, scanned_right) << dendl;
  }
  // STEP 5.
  // All compressed blobs are within the range we already scanned.
  // We are free to expand onto uncompressed blobs.
  exmp_cit right_it = extent_map->seek_nextent(done_right);
  exmp_cit left_it = extent_map->seek_nextent(done_left);
  dout(15) << "on_write #5 left=" << lo(left_it) << " right=" << lo(right_it) << dendl;
  if (!is_end(right_it)) {
    expand_right(right_it);
  }
  if (left_it != extent_map->extent_map.begin()) {
    expand_left(left_it);
  }
  
  // STEP 6.
  // now left_it & right_it are either compressed or at allowed limit
  // attempt to expand scan range
  dout(15) << "on_write #6 left=" << lo(left_it) << " right=" << lo(right_it) << dendl;
  bool has_expanded = false;
  if (right_it != extent_map->extent_map.end()) {
    if (right_it->logical_end() < limit_right) {
      dout(30) << "right maybe expand" << dendl;
      has_expanded |= maybe_expand_scan_range(right_it, left, right);
    }
  }
  if (left_it != extent_map->extent_map.begin()) {
    --left_it; // left_walk points to processes extent
    if (limit_left <= left_it->logical_offset) {
      dout(30) << "left maybe expand" << dendl;
      has_expanded |= maybe_expand_scan_range(left_it, left, right);
    }
    ++left_it; //give it back
  }
  dout(15) << "on_write #6 expanded=" << has_expanded << dendl;
  if (has_expanded) {
    goto step3;
  }
  dout(10) << "on_write done" << dendl;
}
