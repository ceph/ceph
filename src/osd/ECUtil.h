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

#ifndef ECUTIL_H
#define ECUTIL_H

#include <ostream>
#include "erasure-code/ErasureCodeInterface.h"
#include "include/buffer_fwd.h"
#include "include/ceph_assert.h"
#include "include/encoding.h"
#include "common/Formatter.h"

namespace ECUtil {

class stripe_info_t {
  const uint64_t stripe_width;
  const uint64_t chunk_size;
public:
  stripe_info_t(uint64_t stripe_size, uint64_t stripe_width)
    : stripe_width(stripe_width),
      chunk_size(stripe_width / stripe_size) {
    ceph_assert(stripe_width % stripe_size == 0);
  }
  bool logical_offset_is_stripe_aligned(uint64_t logical) const {
    return (logical % stripe_width) == 0;
  }
  uint64_t get_stripe_width() const {
    return stripe_width;
  }
  uint64_t get_chunk_size() const {
    return chunk_size;
  }
  uint64_t get_data_chunk_count() const {
    return get_stripe_width() / get_chunk_size();
  }
  uint64_t logical_to_prev_chunk_offset(uint64_t offset) const {
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t logical_to_next_chunk_offset(uint64_t offset) const {
    return ((offset + stripe_width - 1)/ stripe_width) * chunk_size;
  }
  uint64_t logical_to_prev_stripe_offset(uint64_t offset) const {
    return offset - (offset % stripe_width);
  }
  uint64_t logical_to_next_stripe_offset(uint64_t offset) const {
    return ((offset % stripe_width) ?
      (offset - (offset % stripe_width) + stripe_width) :
      offset);
  }
  uint64_t aligned_logical_offset_to_chunk_offset(uint64_t offset) const {
    ceph_assert(offset % stripe_width == 0);
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t chunk_aligned_logical_offset_to_chunk_offset(uint64_t offset) const {
    [[maybe_unused]] const auto residue_in_stripe = offset % stripe_width;
    ceph_assert(residue_in_stripe % chunk_size == 0);
    ceph_assert(stripe_width % chunk_size == 0);
    // this rounds down
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t chunk_aligned_logical_size_to_chunk_size(uint64_t len) const {
    [[maybe_unused]] const auto residue_in_stripe = len % stripe_width;
    ceph_assert(residue_in_stripe % chunk_size == 0);
    ceph_assert(stripe_width % chunk_size == 0);
    // this rounds up
    return ((len + stripe_width - 1) / stripe_width) * chunk_size;
  }
  uint64_t aligned_chunk_offset_to_logical_offset(uint64_t offset) const {
    ceph_assert(offset % chunk_size == 0);
    return (offset / chunk_size) * stripe_width;
  }
  std::pair<uint64_t, uint64_t> chunk_aligned_offset_len_to_chunk(
    std::pair<uint64_t, uint64_t> in) const;
  std::pair<uint64_t, uint64_t> offset_len_to_stripe_bounds(
    std::pair<uint64_t, uint64_t> in) const {
    uint64_t off = logical_to_prev_stripe_offset(in.first);
    uint64_t len = logical_to_next_stripe_offset(
      (in.first - off) + in.second);
    return std::make_pair(off, len);
  }
  std::pair<uint64_t, uint64_t> offset_len_to_chunk_bounds(
    std::pair<uint64_t, uint64_t> in) const {
    uint64_t off = in.first - (in.first % chunk_size);
    uint64_t tmp_len = (in.first - off) + in.second;
    uint64_t len = ((tmp_len % chunk_size) ?
      (tmp_len - (tmp_len % chunk_size) + chunk_size) :
      tmp_len);
    return std::make_pair(off, len);
  }
  std::pair<uint64_t, uint64_t> offset_length_to_data_chunk_indices(
    uint64_t off, uint64_t len) const {
    assert(chunk_size > 0);
    const auto first_chunk_idx = (off / chunk_size);
    const auto last_chunk_idx = (chunk_size - 1 + off + len) / chunk_size;
    return {first_chunk_idx, last_chunk_idx};
  }
  bool offset_length_is_same_stripe(
    uint64_t off, uint64_t len) const {
    if (len == 0) {
      return true;
    }
    assert(chunk_size > 0);
    const auto first_stripe_idx = off / stripe_width;
    const auto last_inc_stripe_idx = (off + len - 1) / stripe_width;
    return first_stripe_idx == last_inc_stripe_idx;
  }
};

int decode(
  const stripe_info_t &sinfo,
  ceph::ErasureCodeInterfaceRef &ec_impl,
  const std::set<int> want_to_read,
  std::map<int, ceph::buffer::list> &to_decode,
  ceph::buffer::list *out);

int decode(
  const stripe_info_t &sinfo,
  ceph::ErasureCodeInterfaceRef &ec_impl,
  std::map<int, ceph::buffer::list> &to_decode,
  std::map<int, ceph::buffer::list*> &out);

int encode(
  const stripe_info_t &sinfo,
  ceph::ErasureCodeInterfaceRef &ec_impl,
  ceph::buffer::list &in,
  const std::set<int> &want,
  std::map<int, ceph::buffer::list> *out);

class HashInfo {
  uint64_t total_chunk_size = 0;
  std::vector<uint32_t> cumulative_shard_hashes;

  // purely ephemeral, represents the size once all in-flight ops commit
  uint64_t projected_total_chunk_size = 0;
public:
  HashInfo() {}
  explicit HashInfo(unsigned num_chunks) :
    cumulative_shard_hashes(num_chunks, -1) {}
  void append(uint64_t old_size, std::map<int, ceph::buffer::list> &to_append);
  void clear() {
    total_chunk_size = 0;
    cumulative_shard_hashes = std::vector<uint32_t>(
      cumulative_shard_hashes.size(),
      -1);
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<HashInfo*>& o);
  uint32_t get_chunk_hash(int shard) const {
    ceph_assert((unsigned)shard < cumulative_shard_hashes.size());
    return cumulative_shard_hashes[shard];
  }
  uint64_t get_total_chunk_size() const {
    return total_chunk_size;
  }
  uint64_t get_projected_total_chunk_size() const {
    return projected_total_chunk_size;
  }
  uint64_t get_total_logical_size(const stripe_info_t &sinfo) const {
    return get_total_chunk_size() *
      (sinfo.get_stripe_width()/sinfo.get_chunk_size());
  }
  uint64_t get_projected_total_logical_size(const stripe_info_t &sinfo) const {
    return get_projected_total_chunk_size() *
      (sinfo.get_stripe_width()/sinfo.get_chunk_size());
  }
  void set_projected_total_logical_size(
    const stripe_info_t &sinfo,
    uint64_t logical_size) {
    ceph_assert(sinfo.logical_offset_is_stripe_aligned(logical_size));
    projected_total_chunk_size = sinfo.aligned_logical_offset_to_chunk_offset(
      logical_size);
  }
  void set_total_chunk_size_clear_hash(uint64_t new_chunk_size) {
    cumulative_shard_hashes.clear();
    total_chunk_size = new_chunk_size;
  }
  bool has_chunk_hash() const {
    return !cumulative_shard_hashes.empty();
  }
  void update_to(const HashInfo &rhs) {
    auto ptcs = projected_total_chunk_size;
    *this = rhs;
    projected_total_chunk_size = ptcs;
  }
  friend std::ostream& operator<<(std::ostream& out, const HashInfo& hi);
};

typedef std::shared_ptr<HashInfo> HashInfoRef;

bool is_hinfo_key_string(const std::string &key);
const std::string &get_hinfo_key();

WRITE_CLASS_ENCODER(ECUtil::HashInfo)
}
#endif
