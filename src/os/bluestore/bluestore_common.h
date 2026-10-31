// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_COMMON_H
#define CEPH_OSD_BLUESTORE_COMMON_H

#include "include/byteorder.h" // for ceph_le64
#include "include/intarith.h"
#include "include/ceph_assert.h"
#include "kv/KeyValueDB.h"

template <class Bitset, class Func>
void apply_for_bitset_range(uint64_t off,
  uint64_t len,
  uint64_t granularity,
  Bitset &bitset,
  Func f) {
  auto end = round_up_to(off + len, granularity) / granularity;
  ceph_assert(end <= bitset.size());
  uint64_t pos = off / granularity;
  while (pos < end) {
    f(pos, bitset);
    pos++;
  }
}

// merge operators

struct Int64ArrayMergeOperator : public KeyValueDB::MergeOperator {
  void merge_nonexistent(
    const char *rdata, size_t rlen, std::string *new_value) override {
    *new_value = std::string(rdata, rlen);
  }
  void merge(
    const char *ldata, size_t llen,
    const char *rdata, size_t rlen,
    std::string *new_value) override {
    ceph_assert(llen == rlen);
    ceph_assert((rlen % 8) == 0);
    new_value->resize(rlen);
    const ceph_le64* lv = (const ceph_le64*)ldata;
    const ceph_le64* rv = (const ceph_le64*)rdata;
    ceph_le64* nv = &(ceph_le64&)new_value->at(0);
    for (size_t i = 0; i < rlen >> 3; ++i) {
      nv[i] = lv[i] + rv[i];
    }
  }
  // We use each operator name and each prefix to construct the
  // overall RocksDB operator name for consistency check at open time.
  const char *name() const override {
    return "int64_array";
  }
};

// write a label in the first block.  always use this size.  note that
// bluefs makes a matching assumption about the location of its
// superblock (always the second block of the device).
static constexpr uint64_t BDEV_FIRST_LABEL_POSITION = 0;
static constexpr uint64_t BDEV_LABEL_BLOCK_SIZE = 4096;

// reserved for standalone DB volume:
// label (4k) + bluefs super (4k), which means we start at 8k.
static constexpr uint64_t BLUEFS_SUPER_POSITION = 4096;
static constexpr uint64_t BLUEFS_SUPER_BLOCK_SIZE = 4096;
static constexpr uint64_t SUPER_RESERVED = BDEV_LABEL_BLOCK_SIZE + BLUEFS_SUPER_BLOCK_SIZE;


struct FragMetric {
  // Computes fragmentation as the number of disjoint segments
  // produced by a stream of mapped ranges.
  // frag_score == current disjoint segment count.

  std::unordered_set<uint64_t> endpoints;
  uint64_t frag_score = 0;

  FragMetric() {}

  inline void note(uint64_t offset, uint64_t length) {
    bool merge_left = endpoints.count(offset);
    bool merge_right = endpoints.count(offset + length);
    if (merge_left && merge_right) {
      endpoints.erase(offset);
      endpoints.erase(offset + length);
      frag_score--;
    } else if (merge_left) {
      endpoints.erase(offset);
      endpoints.insert(offset + length);
    } else if (merge_right) {
      endpoints.erase(offset + length);
      endpoints.insert(offset);
    } else {
      endpoints.insert(offset);
      endpoints.insert(offset + length);
      frag_score++;
    }
  }
};

// ---------------------------------------------------------------------------
// alloc_tag_t — classifies the data that will occupy an allocated extent.
//
// Bit layout:
//   [63:60]  kind          — top-level category (alloc_tag_kind_t)
//   [59:0]   kind-specific — layout defined per kind; zero if unused
//
// kind = BLUEFS (0x2):
//   [59:56]  subkind       — alloc_tag_bluefs_t
//   [55:0]   reserved/zero
//
// kind = PG (0x3):
//   [59:52]  reserved/zero
//   [51:32]  pool_id       — pg_t::m_pool (low 20 bits)
//   [31:0]   pg_seed       — pg_t::m_seed
//
// A zero-initialised alloc_tag_t is UNKNOWN and is always safe to pass.
// Allocators that do not understand tags ignore it.
// ---------------------------------------------------------------------------

enum class alloc_tag_kind_t : uint8_t {
  UNKNOWN        = 0x0,  // zero-init, safe default; allocator ignores tag
  BLUESTORE_META = 0x1,  // label, reserved regions
  BLUEFS         = 0x2,  // all BlueFS allocations
  PG             = 0x3,  // RADOS object data (PG collections)
  // 0x4..0xF reserved
};

// subkind for kind == BLUEFS, stored in bits [59:56]
enum class alloc_tag_bluefs_t : uint8_t {
  OTHER = 0x0,  // unknown / other BlueFS files
  WAL   = 0x1,  // RocksDB WAL (.log files)
  SST   = 0x2,  // RocksDB SST (.sst files)
  LOG   = 0x3,  // BlueFS journal (ino == 1)
  // 0x4..0xF reserved
};

struct alloc_tag_t {
  uint64_t v = 0; // ZERO UNKNOWN

  static constexpr int      KIND_SHIFT       = 60;
  static constexpr uint64_t KIND_MASK        = 0xFULL << KIND_SHIFT;
  // BLUEFS subkind lives at [59:56]
  static constexpr int      BLUEFS_SK_SHIFT  = 56;
  static constexpr uint64_t BLUEFS_SK_MASK   = 0xFULL << BLUEFS_SK_SHIFT;
  // PG pool_id lives at [51:32] (20 bits), pg_seed at [31:0]
  static constexpr int      PG_POOL_SHIFT    = 32;
  static constexpr uint64_t PG_POOL_MASK     = 0xFFFFFULL << PG_POOL_SHIFT;
  static constexpr uint64_t PG_SEED_MASK     = 0xFFFFFFFFULL;

  static alloc_tag_t unknown() { return {}; }

  static alloc_tag_t bluestore_meta() {
    alloc_tag_t t;
    t.v = (uint64_t)alloc_tag_kind_t::BLUESTORE_META << KIND_SHIFT;
    return t;
  }

  static alloc_tag_t bluefs(alloc_tag_bluefs_t sk) {
    alloc_tag_t t;
    t.v = ((uint64_t)alloc_tag_kind_t::BLUEFS  << KIND_SHIFT) |
          ((uint64_t)sk << BLUEFS_SK_SHIFT);
    return t;
  }

  // pool_id: pg_t::m_pool (only low 20 bits stored)
  // pg_seed: pg_t::m_seed
  static alloc_tag_t pg(uint64_t pool_id, uint32_t pg_seed) {
    alloc_tag_t t;
    t.v = ((uint64_t)alloc_tag_kind_t::PG << KIND_SHIFT) |
          ((pool_id & 0xFFFFFULL) << PG_POOL_SHIFT) |
          (uint64_t)pg_seed;
    return t;
  }

  alloc_tag_kind_t kind() const {
    return (alloc_tag_kind_t)((v & KIND_MASK) >> KIND_SHIFT);
  }

  alloc_tag_bluefs_t bluefs_subkind() const {
    return (alloc_tag_bluefs_t)((v & BLUEFS_SK_MASK) >> BLUEFS_SK_SHIFT);
  }

  uint32_t pg_pool_id() const { return (uint32_t)((v & PG_POOL_MASK) >> PG_POOL_SHIFT); }
  uint32_t pg_seed() const { return (uint32_t)(v & PG_SEED_MASK); }

  bool is_unknown() const { return v == 0; }
  bool is_bluefs() const { return kind() == alloc_tag_kind_t::BLUEFS; }
  bool is_pg() const { return kind() == alloc_tag_kind_t::PG; }

  bool operator==(const alloc_tag_t&) const = default;
};

#endif
