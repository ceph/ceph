// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "PriorityCache.h"

namespace PriorityCache {
  int64_t get_chunk(uint64_t usage, uint64_t total_bytes) {
    uint64_t chunk = total_bytes;

    // Find the nearest power of 2
    chunk -= 1;
    chunk |= chunk >> 1;
    chunk |= chunk >> 2;
    chunk |= chunk >> 4;
    chunk |= chunk >> 8;
    chunk |= chunk >> 16;
    chunk |= chunk >> 32;
    chunk += 1;
    // shrink it to 1/256 of the rounded up cache size
    chunk /= 256;

    // bound the chunk size to be between 4MB and 32MB
    chunk = (chunk > 4ul*1024*1024) ? chunk : 4ul*1024*1024;
    chunk = (chunk < 16ul*1024*1024) ? chunk : 16ul*1024*1024;

    /* Add 16 chunks of headroom and round up to the near chunk.  Note that
     * if RocksDB is used, it's a good idea to have N MB of headroom where
     * N is the target_file_size_base value.  RocksDB will read SST files
     * into the block cache during compaction which potentially can force out
     * all existing cached data.  Once compaction is finished, the SST data is
     * released leaving an empty cache.  Having enough headroom to absorb
     * compaction reads allows the kv cache grow even during extremely heavy
     * compaction workloads.
     */
    uint64_t val = usage + (16 * chunk);
    uint64_t r = (val) % chunk;
    if (r > 0)
      val = val + chunk - r;
    return val;
  }

  PriCache::~PriCache() {
  }
}
