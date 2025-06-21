// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <map>
#include <ostream>
#include <set>
#include <string>

#include "erasure-code/ErasureCodeInterface.h"
#include "include/buffer_fwd.h"
#include "include/ceph_assert.h"
#include "include/encoding.h"
#include "common/interval_map.h"
#include "common/mini_flat_map.h"

#include "osd_types.h"

// Must be a power of 2.
static inline constexpr uint64_t EC_ALIGN_SIZE = 4096;
static inline constexpr uint64_t EC_ALIGN_MASK = ~(EC_ALIGN_SIZE - 1);

/// If someone wants these types, but not ExtentCache, move to another file
struct bl_split_merge {
  ceph::buffer::list split(
      uint64_t offset,
      uint64_t length,
      ceph::buffer::list &bl) const {
    ceph::buffer::list out;
    out.substr_of(bl, offset, length);
    return out;
  }

  bool can_merge(const ceph::buffer::list &left, const ceph::buffer::list &right) const {
    return true;
  }

  ceph::buffer::list merge(ceph::buffer::list &&left, ceph::buffer::list &&right) const {
    ceph::buffer::list bl{std::move(left)};
    bl.claim_append(right);
    return bl;
  }

  uint64_t length(const ceph::buffer::list &b) const { return b.length(); }
};

using extent_set = interval_set<uint64_t, boost::container::flat_map, false>;
using extent_map = interval_map<uint64_t, ceph::buffer::list, bl_split_merge,
                                boost::container::flat_map>;

/* Slice iterator.  This looks for contiguous buffers which are common
 * across all shards in the out_set.
 *
 * It is a template, but essentially:
 * K must a key suitable for a mini_flat_map.
 * T must be either an extent map or a reference to an extent map.
 */
template <typename K, typename T>
class slice_iterator {
  mini_flat_map<K, T> &input;
  uint64_t offset = std::numeric_limits<uint64_t>::max();
  uint64_t length = std::numeric_limits<uint64_t>::max();
  uint64_t start = std::numeric_limits<uint64_t>::max();
  uint64_t end = std::numeric_limits<uint64_t>::max();
  shard_id_map<std::pair<extent_map::const_iterator,
                         bufferlist::const_iterator>> iters;
  shard_id_map<bufferptr> in;
  shard_id_map<bufferptr> out;
  const shard_id_set &out_set;

  void advance() {
    in.clear();
    out.clear();
    offset = start;
    end = std::numeric_limits<uint64_t>::max();

    if (iters.empty()) {
      return;
    }

    // First we find the last buffer in the list
    for (auto &&[shard, iters] : iters) {
      auto &&[emap_iter, bl_iter] = iters;
      uint64_t iter_offset = emap_iter.get_off() + bl_iter.get_off();
      ceph_assert(iter_offset >= start);
      // If this iterator is after the current offset, then we will ignore
      // it for this buffer ptr. The end must move to or before this point.
      if (iter_offset > start && iter_offset < end) {
        end = iter_offset;
        continue;
      }

      uint64_t iter_end = iter_offset + bl_iter.get_current_ptr().length();
      if (iter_end < end) {
        end = iter_end;
      }
    }

    for (auto &&iter = iters.begin(); iter != iters.end();) {
      auto shard = iter->first;
      auto &&[emap_iter, bl_iter] = iter->second;
      uint64_t iter_offset = emap_iter.get_off() + bl_iter.get_off();
      bool erase = false;

      // Ignore any blank buffers.
      if (iter_offset == start) {
        ceph_assert(iter_offset == start);

        // Create a new buffer pointer for the result. We don't want the client
        // manipulating the ptr.
        if (out_set.contains(shard)) {
          out.emplace(
            shard, bufferptr(bl_iter.get_current_ptr(), 0, end - start));
        } else {
          in.emplace(
            shard, bufferptr(bl_iter.get_current_ptr(), 0, end - start));
        }

        // Now we need to move on the iterators.
        bl_iter += end - start;

        // If we have reached the end of the extent, we need to move that on too.
        if (bl_iter == emap_iter.get_val().end()) {
          ++emap_iter;
          if (emap_iter == input[shard].end()) {
            erase = true;
          } else {
            if (out_set.contains(shard)) {
              bufferlist bl = emap_iter.get_val();
              bl.invalidate_crc();
            }
            iters.at(shard).second = emap_iter.get_val().begin();
          }
        }
      } else
        ceph_assert(iter_offset > start);

      if (erase) {
        iter = iters.erase(iter);
      } else {
        ++iter;
      }
    }

    // We can now move the offset on.
    length = end - start;
    start = end;

    /* This can arise in two ways:
     * 1. We can generate an empty buffer out of a gap, so just skip over.
     * 2. Only the inputs contain any interesting data.  We don't need
     *    to perform a decode/encode on a slice in that case.
     */
    if (out.empty()) {
      advance();
    }
  }

public:
  slice_iterator(mini_flat_map<K, T> &_input, const shard_id_set &out_set) :
    input(_input),
    iters(input.max_size()),
    in(input.max_size()),
    out(input.max_size()),
    out_set(out_set) {
    for (auto &&[shard, emap] : input) {
      auto emap_iter = emap.begin();
      auto bl_iter = emap_iter.get_val().begin();
      auto p = std::make_pair(std::move(emap_iter), std::move(bl_iter));
      iters.emplace(shard, std::move(p));

      if (emap_iter.get_off() < start) {
        start = emap_iter.get_off();
      }
    }

    advance();
  }

  shard_id_map<bufferptr> &get_in_bufferptrs() { return in; }
  shard_id_map<bufferptr> &get_out_bufferptrs() { return out; }
  uint64_t get_offset() const { return offset; }
  uint64_t get_length() const { return length; }
  bool is_end() const { return in.empty() && out.empty(); }

  bool is_page_aligned() const {
    for (auto &&[_, ptr] : in) {
      uintptr_t p = (uintptr_t)ptr.c_str();
      if (p & ~EC_ALIGN_MASK) return false;
      if ((p + ptr.length()) & ~EC_ALIGN_MASK) return false;
    }

    for (auto &&[_, ptr] : out) {
      uintptr_t p = (uintptr_t)ptr.c_str();
      if (p & ~EC_ALIGN_MASK) return false;
      if ((p + ptr.length()) & ~EC_ALIGN_MASK) return false;
    }

    return true;
  }

  slice_iterator &operator++() {
    advance();
    return *this;
  }
};

namespace ECUtil {
class shard_extent_map_t;

struct shard_extent_set_t {
  // The following boilerplate is just to make this look like a map.
  shard_id_map<extent_set> map;

  shard_extent_set_t(short max_shards) : map(max_shards) {}

  bool contains(shard_id_t shard) const { return map.contains(shard); }
  bool empty() const { return map.empty(); }
  void swap(shard_extent_set_t &other) noexcept { map.swap(other.map); }
  void clear() { map.clear(); }
  auto erase(shard_id_t shard) { return map.erase(shard); }

  auto erase(shard_id_map<extent_set>::iterator &iter) {
    return map.erase(iter);
  }

  void erase_stripe(uint64_t offset, uint64_t length) {
    for (auto it = map.begin(); it != map.end();) {
      it->second.erase(offset, length);
      if (it->second.empty()) it = map.erase(it);
      else ++it;
    }
  }

  auto begin() const { return map.cbegin(); }
  auto begin() { return map.begin(); }
  auto end() const { return map.cend(); }
  auto end() { return map.end(); }

  void emplace(shard_id_t shard, extent_set &&set) {
    map.emplace(shard, std::move(set));
  }

  size_t shard_count() const { return map.size(); }
  extent_set &at(shard_id_t shard) { return map.at(shard); }
  const extent_set &at(shard_id_t shard) const { return map.at(shard); }

  extent_set get(shard_id_t shard) const {
    if (!map.contains(shard)) {
      return extent_set();
    }
    return at(shard);
  }

  extent_set &operator[](shard_id_t shard) { return map[shard]; }

  bool operator==(shard_extent_set_t const &other) const {
    return map == other.map;
  }

  friend std::ostream &operator<<(std::ostream &lhs,
                                  const shard_extent_set_t &rhs) {
    lhs << rhs.map;
    return lhs;
  }

  void get_extent_superset(extent_set &eset) const {
    for (auto &&[_, e] : map) {
      eset.union_of(e);
    }
  }

  extent_set get_extent_superset() const {
    extent_set eset;
    get_extent_superset(eset);
    return eset;
  }

  /* Return the extent set which is common across all populated shards. */
  extent_set get_extent_common_set() const {
    extent_set eset;
    bool first = true;
    for (auto &&[_, e] : map) {
      if (first) {
        eset.insert(e);
        first = false;
      } else {
        eset.intersection_of(e);
      }
    }
    return eset;
  }

  void align(uint64_t a) {
    for (auto &&[_, e] : map) {
      e.align(a);
    }
  }

  size_t get_max_shards() const { return map.max_size(); }

  void subtract(const shard_extent_set_t &set);
  void intersection_of(const shard_extent_set_t &set);
  void insert(const shard_extent_set_t &set);

  /** return the sum of extent_set.size */
  uint64_t size() const {
    uint64_t size = 0;
    for (auto &&[_, e] : map) {
      size += e.size();
    }

    return size;
  }

  void populate_shard_id_set(shard_id_set &set) const {
    map.populate_bitset_set(set);
  }

  shard_id_set get_shard_id_set() const {
    shard_id_set r;
    map.populate_bitset_set(r);
    return r;
  }
};

inline uint64_t align_next(uint64_t val) {
  return p2roundup(val, EC_ALIGN_SIZE);
}

inline uint64_t align_prev(uint64_t val) {
  return p2align(val, EC_ALIGN_SIZE);
}

class stripe_info_t {
  friend class shard_extent_map_t;

  const uint64_t stripe_width;
  const uint64_t plugin_flags;
  const uint64_t chunk_size;
  const pg_pool_t *pool;
  const unsigned int k;
  // Can be calculated with a division from above. Better to cache.
  const unsigned int m;
  const std::vector<shard_id_t> chunk_mapping;
  const std::vector<raw_shard_id_t> chunk_mapping_reverse;
  const shard_id_set data_shards;
  const shard_id_set parity_shards;
  const shard_id_set all_shards;

private:
  void ro_range_to_shards(
      uint64_t ro_offset,
      uint64_t ro_size,
      ECUtil::shard_extent_set_t *shard_extent_set,
      extent_set *extent_superset,
      buffer::list *bl,
      shard_extent_map_t *shard_extent_map) const;

  static std::vector<shard_id_t> complete_chunk_mapping(
      const std::vector<shard_id_t> &_chunk_mapping, unsigned int n) {
    unsigned int size = (int)_chunk_mapping.size();
    std::vector<shard_id_t> chunk_mapping(n);
    for (unsigned int i = 0; i < n; i++) {
      if (size > i) {
        chunk_mapping.at(i) = _chunk_mapping.at(i);
      } else {
        chunk_mapping.at(i) = static_cast<int>(i);
      }
    }
    return chunk_mapping;
  }

  static std::vector<raw_shard_id_t> reverse_chunk_mapping(
      const std::vector<shard_id_t> &chunk_mapping) {
    size_t size = chunk_mapping.size();
    std::vector<raw_shard_id_t> reverse(size);
    shard_id_set used;
    for (raw_shard_id_t raw_shard; raw_shard < size; ++raw_shard) {
      shard_id_t shard = chunk_mapping[int(raw_shard)];
      // Mapping must be a bijection and a permutation
      ceph_assert(!used.contains(shard));
      used.insert(shard);
      reverse.at(int(shard)) = raw_shard;
    }
    return reverse;
  }

  static shard_id_set calc_shards(raw_shard_id_t start,
                                  int count,
                                  const std::vector<shard_id_t> &chunk_mapping) {
    shard_id_set data_shards;
    for (raw_shard_id_t raw_shard = start;
         raw_shard < int(start) + count;
         ++raw_shard) {
      shard_id_t shard = chunk_mapping[int(raw_shard)];
      data_shards.insert(shard);
    }
    return data_shards;
  }

  static shard_id_set calc_all_shards(int k_plus_m) {
    shard_id_set all_shards;
    all_shards.insert_range(shard_id_t(), k_plus_m);
    return all_shards;
  }


public:
  stripe_info_t(const ErasureCodeInterfaceRef &ec_impl, const pg_pool_t *pool,
                uint64_t stripe_width
    )
    : stripe_width(stripe_width),
      plugin_flags(ec_impl->get_supported_optimizations()),
      chunk_size(stripe_width / ec_impl->get_data_chunk_count()),
      pool(pool),
      k(ec_impl->get_data_chunk_count()),
      m(ec_impl->get_coding_chunk_count()),
      chunk_mapping(
        complete_chunk_mapping(ec_impl->get_chunk_mapping(), k + m)),
      chunk_mapping_reverse(reverse_chunk_mapping(chunk_mapping)),
      data_shards(calc_shards(raw_shard_id_t(), k, chunk_mapping)),
      parity_shards(calc_shards(raw_shard_id_t(k), m, chunk_mapping)),
      all_shards(calc_all_shards(k + m)) {
    ceph_assert(stripe_width != 0);
    ceph_assert(stripe_width % k == 0);
  }

  // Simpler constructors for unit tests
  stripe_info_t(unsigned int k, unsigned int m, uint64_t stripe_width)
    : stripe_width(stripe_width),
      plugin_flags(0xFFFFFFFFFFFFFFFFul),
      // Everything enabled for test harnesses.
      chunk_size(stripe_width / k),
      pool(nullptr),
      k(k),
      m(m),
      chunk_mapping(complete_chunk_mapping(std::vector<shard_id_t>(), k + m)),
      chunk_mapping_reverse(reverse_chunk_mapping(chunk_mapping)),
      data_shards(calc_shards(raw_shard_id_t(), k, chunk_mapping)),
      parity_shards(calc_shards(raw_shard_id_t(k), m, chunk_mapping)) {
    ceph_assert(stripe_width != 0);
    ceph_assert(stripe_width % k == 0);
  }

  stripe_info_t(unsigned int k, unsigned int m, uint64_t stripe_width,
                const std::vector<shard_id_t> &_chunk_mapping)
    : stripe_width(stripe_width),
      plugin_flags(0xFFFFFFFFFFFFFFFFul),
      // Everything enabled for test harnesses.
      chunk_size(stripe_width / k),
      pool(nullptr),
      k(k),
      m(m),
      chunk_mapping(complete_chunk_mapping(_chunk_mapping, k + m)),
      chunk_mapping_reverse(reverse_chunk_mapping(chunk_mapping)),
      data_shards(calc_shards(raw_shard_id_t(), k, chunk_mapping)),
      parity_shards(calc_shards(raw_shard_id_t(k), m, chunk_mapping)) {
    ceph_assert(stripe_width != 0);
    ceph_assert(stripe_width % k == 0);
  }

  stripe_info_t(unsigned int k, unsigned int m, uint64_t stripe_width,
                const pg_pool_t *pool, const std::vector<shard_id_t> &_chunk_mapping)
    : stripe_width(stripe_width),
      plugin_flags(0xFFFFFFFFFFFFFFFFul),
      // Everything enabled for test harnesses.
      chunk_size(stripe_width / k),
      pool(pool),
      k(k),
      m(m),
      chunk_mapping(complete_chunk_mapping(_chunk_mapping, k + m)),
      chunk_mapping_reverse(reverse_chunk_mapping(chunk_mapping)),
      data_shards(calc_shards(raw_shard_id_t(), k, chunk_mapping)),
      parity_shards(calc_shards(raw_shard_id_t(k), m, chunk_mapping)) {
    ceph_assert(stripe_width != 0);
    ceph_assert(stripe_width % k == 0);
  }

  stripe_info_t(unsigned int k, unsigned int m, uint64_t stripe_width,
                const pg_pool_t *pool)
    : stripe_width(stripe_width),
      plugin_flags(0xFFFFFFFFFFFFFFFFul),
      // Everything enabled for test harnesses.
      chunk_size(stripe_width / k),
      pool(pool),
      k(k),
      m(m),
      chunk_mapping(complete_chunk_mapping(std::vector<shard_id_t>(), k + m)),
      chunk_mapping_reverse(reverse_chunk_mapping(chunk_mapping)),
      data_shards(calc_shards(raw_shard_id_t(), k, chunk_mapping)),
      parity_shards(calc_shards(raw_shard_id_t(k), m, chunk_mapping)) {
    ceph_assert(stripe_width != 0);
    ceph_assert(stripe_width % k == 0);
  }

  uint64_t object_size_to_shard_size(const uint64_t size, shard_id_t shard) const {
    uint64_t remainder = size % get_stripe_width();
    uint64_t shard_size = (size - remainder) / k;
    raw_shard_id_t raw_shard = get_raw_shard(shard);
    if (raw_shard >= get_k()) {
      // coding parity shards have same size as data shard 0
      raw_shard = 0;
    }
    if (remainder > uint64_t(raw_shard) * get_chunk_size()) {
      remainder -= uint64_t(raw_shard) * get_chunk_size();
      if (remainder > get_chunk_size()) {
        remainder = get_chunk_size();
      }
      shard_size += remainder;
    }
    return align_next(shard_size);
  }

  uint64_t ro_offset_to_shard_offset(uint64_t ro_offset,
                                     const raw_shard_id_t raw_shard) const {
    uint64_t full_stripes = (ro_offset / stripe_width) * chunk_size;
    int offset_shard = (ro_offset / chunk_size) % k;

    if (int(raw_shard) == offset_shard) {
      return full_stripes + ro_offset % chunk_size;
    }
    if (raw_shard < offset_shard) {
      return full_stripes + chunk_size;
    }
    return full_stripes;
  }

  /**
   * Return true if shard does not require metadata updates
   */
  bool is_nonprimary_shard(const shard_id_t shard) const {
    return pool->is_nonprimary_shard(shard);
  }

  bool supports_ec_overwrites() const {
    return pool->allows_ecoverwrites();
  }

  bool supports_sub_chunks() const {
    return (plugin_flags &
      ErasureCodeInterface::FLAG_EC_PLUGIN_REQUIRE_SUB_CHUNKS) != 0;
  }

  bool get_is_hinfo_required() const {
    return !supports_ec_overwrites();
  }

  bool supports_partial_reads() const {
    return (plugin_flags &
      ErasureCodeInterface::FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION) != 0;
  }

  bool supports_partial_writes() const {
    return (plugin_flags &
      ErasureCodeInterface::FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION) != 0;
  }

  bool supports_parity_delta_writes() const {
    return (plugin_flags &
      ErasureCodeInterface::FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION) != 0;
  }

  uint64_t get_stripe_width() const {
    return stripe_width;
  }

  uint64_t get_chunk_size() const {
    return chunk_size;
  }

  unsigned int get_m() const {
    return m;
  }

  unsigned int get_k() const {
    return k;
  }

  unsigned int get_k_plus_m() const {
    return k + m;
  }

  const shard_id_t get_shard(const raw_shard_id_t raw_shard) const {
    return chunk_mapping[int(raw_shard)];
  }

  raw_shard_id_t get_raw_shard(shard_id_t shard) const {
    return chunk_mapping_reverse.at(int(shard));
  }

  /* Return a "span" - which can be iterated over */
  auto get_data_shards() const {
    return data_shards;
  }

  auto get_parity_shards() const {
    return parity_shards;
  }

  auto get_all_shards() const {
    return all_shards;
  }


  uint64_t ro_offset_to_prev_chunk_offset(uint64_t offset) const {
    return (offset / stripe_width) * chunk_size;
  }

  uint64_t ro_offset_to_next_chunk_offset(uint64_t offset) const {
    return ((offset + stripe_width - 1) / stripe_width) * chunk_size;
  }

  uint64_t ro_offset_to_prev_stripe_ro_offset(uint64_t offset) const {
    return offset - (offset % stripe_width);
  }

  uint64_t ro_offset_to_next_stripe_ro_offset(uint64_t offset) const {
    return ((offset % stripe_width)
              ? (offset - (offset % stripe_width) + stripe_width)
              : offset);
  }

  uint64_t aligned_ro_offset_to_chunk_offset(uint64_t offset) const {
    ceph_assert(offset % stripe_width == 0);
    return (offset / stripe_width) * chunk_size;
  }

  uint64_t chunk_aligned_ro_offset_to_chunk_offset(uint64_t offset) const {
    [[maybe_unused]] const auto residue_in_stripe = offset % stripe_width;
    ceph_assert(residue_in_stripe % chunk_size == 0);
    ceph_assert(stripe_width % chunk_size == 0);
    // this rounds down
    return (offset / stripe_width) * chunk_size;
  }

  uint64_t chunk_aligned_ro_length_to_shard_length(uint64_t len) const {
    // this rounds up
    return ((len + stripe_width - 1) / stripe_width) * chunk_size;
  }

  uint64_t chunk_aligned_shard_offset_to_ro_offset(uint64_t offset) const {
    ceph_assert(offset % chunk_size == 0);
    return (offset / chunk_size) * stripe_width;
  }

  std::pair<uint64_t, uint64_t> chunk_aligned_ro_range_to_shard_ro_range(
      uint64_t off, uint64_t len) const;

  std::pair<uint64_t, uint64_t> ro_offset_len_to_stripe_ro_offset_len(
      uint64_t _off, uint64_t _len) const {
    uint64_t off = ro_offset_to_prev_stripe_ro_offset(_off);
    uint64_t len = ro_offset_to_next_stripe_ro_offset(
      (_off - off) + _len);
    return std::make_pair(off, len);
  }

  std::pair<uint64_t, uint64_t> ro_range_to_chunk_ro_range(
      const std::pair<uint64_t, uint64_t> &in) const {
    uint64_t off = in.first - (in.first % chunk_size);
    uint64_t tmp_len = (in.first - off) + in.second;
    uint64_t len = ((tmp_len % chunk_size)
                      ? (tmp_len - (tmp_len % chunk_size) + chunk_size)
                      : tmp_len);
    return std::make_pair(off, len);
  }

  void ro_range_to_shard_extent_set(
      uint64_t ro_offset,
      uint64_t ro_size,
      ECUtil::shard_extent_set_t &shard_extent_set) const {
    ro_range_to_shards(ro_offset, ro_size, &shard_extent_set, nullptr, nullptr, nullptr);
  }

  void ro_range_to_shard_extent_set(
      uint64_t ro_offset,
      uint64_t ro_size,
      ECUtil::shard_extent_set_t &shard_extent_set,
      extent_set &extent_superset) const {
    ro_range_to_shards(ro_offset, ro_size, &shard_extent_set, &extent_superset,
                       nullptr,
                       nullptr);
  }

  void ro_range_to_shard_extent_set_with_parity(
      uint64_t ro_offset,
      uint64_t ro_size,
      ECUtil::shard_extent_set_t &shard_extent_set) const {
    extent_set parity;
    ro_range_to_shards(ro_offset, ro_size, &shard_extent_set, &parity, nullptr,
                       nullptr);

    if (parity.empty()) return;

    for (shard_id_t shard : get_parity_shards()) {
      shard_extent_set[shard].union_of(parity);
    }
  }

  void ro_range_to_shard_extent_set_with_superset(
      uint64_t ro_offset,
      uint64_t ro_size,
      ECUtil::shard_extent_set_t &shard_extent_set,
      extent_set &superset) const {
    ro_range_to_shards(ro_offset, ro_size, &shard_extent_set, &superset, nullptr,
                       nullptr);
  }

  void ro_range_to_shard_extent_map(
      uint64_t ro_offset,
      uint64_t ro_size,
      buffer::list &bl,
      shard_extent_map_t &shard_extent_map) const {
    ro_range_to_shards(ro_offset, ro_size, nullptr, nullptr, &bl, &shard_extent_map);
  }

  void trim_shard_extent_set_for_ro_offset(uint64_t ro_offset,
                                           ECUtil::shard_extent_set_t &
                                           shard_extent_set) const;

  void ro_size_to_stripe_aligned_read_mask(
      uint64_t ro_size,
      ECUtil::shard_extent_set_t &shard_extent_set) const;

  void ro_size_to_read_mask(
      uint64_t ro_size,
      ECUtil::shard_extent_set_t &shard_extent_set) const;

  void ro_size_to_zero_mask(
      uint64_t ro_size,
      ECUtil::shard_extent_set_t &shard_extent_set) const;
};

class HashInfo {
  uint64_t total_chunk_size = 0;
  std::vector<uint32_t> cumulative_shard_hashes;

public:
  HashInfo() {}

  explicit HashInfo(unsigned num_chunks) :
    cumulative_shard_hashes(num_chunks, -1) {}

  void append(uint64_t old_size, shard_id_map<bufferptr> &to_append);

  void clear() {
    total_chunk_size = 0;
    cumulative_shard_hashes = std::vector<uint32_t>(
      cumulative_shard_hashes.size(),
      -1);
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<HashInfo*> &o);

  uint32_t get_chunk_hash(shard_id_t shard) const {
    ceph_assert(shard < cumulative_shard_hashes.size());
    return cumulative_shard_hashes[int(shard)];
  }

  uint64_t get_total_chunk_size() const {
    return total_chunk_size;
  }

  void set_total_chunk_size_clear_hash(uint64_t new_chunk_size) {
    cumulative_shard_hashes.clear();
    total_chunk_size = new_chunk_size;
  }

  bool has_chunk_hash() const {
    return !cumulative_shard_hashes.empty();
  }

  void update_to(const HashInfo &rhs) {
    *this = rhs;
  }

  friend std::ostream &operator<<(std::ostream &out, const HashInfo &hi);
};

typedef std::shared_ptr<HashInfo> HashInfoRef;

class shard_extent_map_t {
  static const uint64_t invalid_offset = std::numeric_limits<uint64_t>::max();

public:
  const stripe_info_t *sinfo;
  // The maximal range of all extents maps within rados object space.
  uint64_t ro_start;
  uint64_t ro_end;
  uint64_t start_offset;
  uint64_t end_offset;
  shard_id_map<extent_map> extent_maps;

  slice_iterator<shard_id_t, extent_map> begin_slice_iterator(
      const shard_id_set &out_set);

  /* This caculates the ro offset for an offset into a particular shard */
  uint64_t calc_ro_offset(raw_shard_id_t raw_shard, int shard_offset) const {
    int stripes = shard_offset / sinfo->chunk_size;
    return stripes * sinfo->stripe_width + uint64_t(raw_shard) * sinfo->
        chunk_size +
        shard_offset % sinfo->chunk_size;
  }

  uint64_t calc_ro_end(raw_shard_id_t raw_shard, int shard_offset) const {
    return calc_ro_offset(raw_shard, shard_offset - 1) + 1;
  }

  /* This is a relatively expensive operation to update the ro offset/length.
   * Ideally, we should be able to update offset/length incrementally.
   */
  void compute_ro_range() {
    uint64_t start = invalid_offset;
    uint64_t end = 0;
    uint64_t o_start = invalid_offset;
    uint64_t o_end = 0;

    for (auto &&[shard, emap] : extent_maps) {
      raw_shard_id_t raw_shard = sinfo->get_raw_shard(shard);
      uint64_t start_off = emap.get_start_off();
      uint64_t end_off = emap.get_end_off();
      o_start = std::min(o_start, start_off);
      o_end = std::max(o_end, end_off);

      if (raw_shard < sinfo->get_k()) {
        start = std::min(start, calc_ro_offset(raw_shard, start_off));
        end = std::max(end, calc_ro_end(raw_shard, end_off));
      }
    }
    if (end != 0) {
      ro_start = start;
      ro_end = end;
      start_offset = o_start;
      end_offset = o_end;
    } else {
      ro_start = invalid_offset;
      ro_end = invalid_offset;
      start_offset = invalid_offset;
      end_offset = invalid_offset;
    }
  }

public:
  shard_extent_map_t(const stripe_info_t *sinfo) :
    sinfo(sinfo),
    ro_start(invalid_offset),
    ro_end(invalid_offset),
    start_offset(invalid_offset),
    end_offset(invalid_offset),
    extent_maps(sinfo->get_k_plus_m()) {}

  shard_extent_map_t(const stripe_info_t *sinfo,
                     shard_id_map<extent_map> &&_extent_maps) :
    sinfo(sinfo),
    extent_maps(std::move(_extent_maps)) {
    // Empty shards are not permitted, so clear them out.
    for (auto iter = extent_maps.begin(); iter != extent_maps.end();) {
      if (iter->second.empty()) {
        iter = extent_maps.erase(iter);
      } else {
        ++iter;
      }
    }
    compute_ro_range();
  }

  bool empty() const {
    return ro_end == invalid_offset;
  }

  uint64_t get_ro_start() const {
    return ro_start;
  }

  uint64_t get_ro_end() const {
    return ro_end;
  }

  /* Return the extent maps.  For reading only, set to const as the returned
   * map should not be modified.
   * We want to avoid:
   *  - Empty extent maps on shards
   *  - getting the offset/length out of sync.
   */
  const auto &get_extent_maps() const {
    return extent_maps;
  }

  /* Return a particlar extent map. This must be const because updating it
   * would cause the shard_extent_map to become inconsistent.
   *
   * * This method will raise an exception if the shard has no extents.
   */
  const extent_map &get_extent_map(shard_id_t shard) const {
    return extent_maps.at(shard);
  }

  extent_set get_extent_set(const shard_id_t &shard) const {
    extent_set ret;
    if (extent_maps.contains(shard)) {
      extent_maps.at(shard).to_interval_set(ret);
    }
    return ret;
  }

  void to_shard_extent_set(shard_extent_set_t &set) const {
    for (auto &&[shard, emap] : extent_maps) {
      emap.to_interval_set(set[shard]);
    }
  }

  bool contains_shard(shard_id_t shard) const {
    return extent_maps.contains(shard);
  }

  void erase_after_ro_offset(uint64_t ro_offset);
  shard_extent_map_t intersect_ro_range(uint64_t ro_offset, uint64_t ro_length) const;
  shard_extent_map_t intersect(std::optional<shard_extent_set_t> const &other) const;
  shard_extent_map_t intersect(shard_extent_set_t const &other) const;
  void insert_in_shard(shard_id_t shard, uint64_t off, const buffer::list &bl);
  void insert_in_shard(shard_id_t shard, uint64_t off, const buffer::list &bl,
                       uint64_t new_start, uint64_t new_end);
  void insert_ro_zero_buffer(uint64_t ro_offset, uint64_t ro_length);
  void insert(shard_extent_map_t const &other);
  void append_zeros_to_ro_offset(uint64_t ro_offset);
  void insert_ro_extent_map(const extent_map &host_extent_map);
  extent_set get_extent_superset() const;
  int encode(const ErasureCodeInterfaceRef &ec_impl, const HashInfoRef &hinfo,
             uint64_t before_ro_size);
  int _encode(const ErasureCodeInterfaceRef &ec_impl);
  int encode_parity_delta(const ErasureCodeInterfaceRef &ec_impl,
                          shard_extent_map_t &old_sem);

  void pad_on_shards(const shard_extent_set_t &pad_to,
                     const shard_id_set &shards);
  void pad_on_shards(const extent_set &pad_to,
                     const shard_id_set &shards);
  void pad_on_shard(const extent_set &pad_to,
                    const shard_id_t shard);
  void trim(const shard_extent_set_t &trim_to);
  int decode(const ErasureCodeInterfaceRef &ec_impl,
             const shard_extent_set_t &want,
             uint64_t object_size);
  int _decode(const ErasureCodeInterfaceRef &ec_impl,
              const shard_id_set &want_set,
              const shard_id_set &need_set);
  void get_buffer(shard_id_t shard, uint64_t offset, uint64_t length,
                  buffer::list &append_to) const;
  void get_shard_first_buffer(shard_id_t shard, buffer::list &append_to) const;
  uint64_t get_shard_first_offset(shard_id_t shard) const;
  void zero_pad(shard_extent_set_t const &pad_to);
  void zero_pad(shard_id_t shard, uint64_t offset, uint64_t length);
  void pad_with_other(shard_extent_set_t const &pad_to,
                      shard_extent_map_t const &other);
  void pad_with_other(shard_id_t shard, uint64_t offset, uint64_t length,
                      shard_extent_map_t const &other);
  bufferlist get_ro_buffer(uint64_t ro_offset, uint64_t ro_length) const;
  /* Returns a buffer assuming that there is a single contigious buffer
   * represented by the map. */
  bufferlist get_ro_buffer() const;
  shard_extent_set_t get_extent_set();
  void insert_parity_buffers();
  void erase_shard(shard_id_t shard);
  shard_extent_map_t slice_map(uint64_t offset, uint64_t length) const;
  std::string debug_string(uint64_t inteval, uint64_t offset) const;
  void erase_stripe(uint64_t offset, uint64_t length);
  bool contains(shard_id_t shard) const;
  bool contains(std::optional<shard_extent_set_t> const &other) const;
  bool contains(shard_extent_set_t const &other) const;
  void pad_and_rebuild_to_ec_align();
  uint64_t size();
  void clear();
  uint64_t get_start_offset() const { return start_offset; }
  uint64_t get_end_offset() const { return end_offset; }
  void deep_copy(shard_extent_map_t const &other);
  void swap() {}
  size_t shard_count() { return extent_maps.size(); }


  void assert_buffer_contents_equal(shard_extent_map_t other) const {
    for (auto &&[shard, emap] : extent_maps) {
      for (auto &&i : emap) {
        bufferlist bl = i.get_val();
        bufferlist otherbl;
        other.get_buffer(shard, i.get_off(), i.get_len(), otherbl);
        ceph_assert(bl.contents_equal(otherbl));
      }
    }
  }

  bool add_zero_padding_for_decode(uint64_t object_size, shard_id_set &exclude_set) {
    shard_extent_set_t zeros(sinfo->get_k_plus_m());
    sinfo->ro_size_to_zero_mask(object_size, zeros);
    extent_set superset = get_extent_superset();
    bool changed = false;
    for (auto &&[shard, z] : zeros) {
      if (exclude_set.contains(shard)) {
        continue;
      }
      z.intersection_of(superset);
      for (auto [off, len] : z) {
        changed = true;
        bufferlist bl;
        bl.append_zero(len);
        extent_maps[shard].insert(off, len, bl);
      }
    }

    if (changed) {
      compute_ro_range();
    }

    return changed;
  }

  friend std::ostream &operator<<(std::ostream &lhs,
                                  const shard_extent_map_t &rhs);

  friend bool operator==(const shard_extent_map_t &lhs,
                         const shard_extent_map_t &rhs) {
    return lhs.sinfo == rhs.sinfo
        && lhs.ro_start == rhs.ro_start
        && lhs.ro_end == rhs.ro_end
        && lhs.extent_maps == rhs.extent_maps;
  }
};

typedef enum {
  READ_REQUEST,
  READ_DONE,
  INJECT_EIO,
  CANCELLED,
  ERROR,
  REQUEST_MISSING,
  COMPLETE_ERROR,
  ERROR_CLEAR,
  COMPLETE
} log_event_t;

struct log_entry_t {
  const log_event_t event;
  const pg_shard_t shard;
  const extent_set io;

  log_entry_t(
      const log_event_t event,
      const pg_shard_t &shard,
      const extent_set &io) :
    event(event), shard(shard), io(io) {}

  log_entry_t(
      const log_event_t event,
      const pg_shard_t &shard) :
    event(event), shard(shard) {}

  log_entry_t(
      const log_event_t event,
      const pg_shard_t &pg_shard,
      const shard_extent_map_t &extent_map) :
    event(event), shard(pg_shard),
    io(extent_map.contains(pg_shard.shard)
         ? extent_map.get_extent_set(pg_shard.shard)
         : extent_set()) {}

  friend std::ostream &operator<<(std::ostream &out, const log_entry_t &lhs);
};

bool is_hinfo_key_string(const std::string &key);
const std::string &get_hinfo_key();

WRITE_CLASS_ENCODER(ECUtil::HashInfo)
}

