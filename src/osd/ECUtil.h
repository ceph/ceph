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

#include <map>
#include <set>

#include "include/memory.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "include/buffer_fwd.h"
#include "include/assert.h"
#include "include/encoding.h"
#include "common/Formatter.h"

namespace ECUtil {

const uint64_t CHUNK_ALIGNMENT = 64;
const uint64_t CHUNK_INFO = 8;
const uint64_t CHUNK_PADDING = 8;
const uint64_t CHUNK_OVERHEAD = 16; // INFO + PADDING

class stripe_info_t {
  const uint64_t stripe_width;
  const uint64_t chunk_size;
public:
  stripe_info_t(uint64_t stripe_size, uint64_t stripe_width)
    : stripe_width(stripe_width),
      chunk_size(stripe_width / stripe_size) {
    assert(stripe_width % stripe_size == 0);
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
    assert(offset % stripe_width == 0);
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t aligned_chunk_offset_to_logical_offset(uint64_t offset) const {
    assert(offset % chunk_size == 0);
    return (offset / chunk_size) * stripe_width;
  }
  pair<uint64_t, uint64_t> aligned_offset_len_to_chunk(
    pair<uint64_t, uint64_t> in) const {
    return make_pair(
      aligned_logical_offset_to_chunk_offset(in.first),
      aligned_logical_offset_to_chunk_offset(in.second));
  }
  pair<uint64_t, uint64_t> offset_len_to_stripe_bounds(
    pair<uint64_t, uint64_t> in) const {
    uint64_t off = logical_to_prev_stripe_offset(in.first);
    uint64_t len = logical_to_next_stripe_offset(
      (in.first - off) + in.second);
    return make_pair(off, len);
  }
};

int decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  bufferlist *out);

int decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  map<int, bufferlist*> &out);

int encode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  bufferlist &in,
  const set<int> &want,
  map<int, bufferlist> *out);

class HashInfo {
  uint64_t total_chunk_size = 0;
  vector<uint32_t> cumulative_shard_hashes;

  // purely ephemeral, represents the size once all in-flight ops commit
  uint64_t projected_total_chunk_size = 0;
public:
  HashInfo() {}
  explicit HashInfo(unsigned num_chunks) :
    cumulative_shard_hashes(num_chunks, -1) {}
  void append(uint64_t old_size, map<int, bufferlist> &to_append);
  void clear() {
    total_chunk_size = 0;
    cumulative_shard_hashes = vector<uint32_t>(
      cumulative_shard_hashes.size(),
      -1);
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<HashInfo*>& o);
  uint32_t get_chunk_hash(int shard) const {
    assert((unsigned)shard < cumulative_shard_hashes.size());
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
    assert(sinfo.logical_offset_is_stripe_aligned(logical_size));
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
};

typedef ceph::shared_ptr<HashInfo> HashInfoRef;

bool is_hinfo_key_string(const string &key);
const string &get_hinfo_key();

}
WRITE_CLASS_ENCODER(ECUtil::HashInfo)
#endif
