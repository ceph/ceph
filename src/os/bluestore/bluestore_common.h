// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#endif
