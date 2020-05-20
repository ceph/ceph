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

#ifndef CEPH_PRIORITY_CACHE_H
#define CEPH_PRIORITY_CACHE_H

#include <stdint.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include "common/perf_counters.h"
#include "include/ceph_assert.h"

namespace PriorityCache {
  // Reserve 16384 slots for PriorityCache perf counters
  const int PERF_COUNTER_LOWER_BOUND = 1073741824;
  const int PERF_COUNTER_MAX_BOUND = 1073758208;

  enum MallocStats {
    M_FIRST = PERF_COUNTER_LOWER_BOUND,
    M_TARGET_BYTES,
    M_MAPPED_BYTES,
    M_UNMAPPED_BYTES,
    M_HEAP_BYTES,
    M_CACHE_BYTES,
    M_LAST,
  };

  enum Priority {
    PRI0,
    PRI1,
    PRI2,
    PRI3,
    PRI4,
    PRI5,
    PRI6,
    PRI7,
    PRI8,
    PRI9,
    PRI10,
    PRI11,
    LAST = PRI11,
  };

  enum Extra {
    E_RESERVED = Priority::LAST+1,
    E_COMMITTED,
    E_LAST = E_COMMITTED,
  };

  int64_t get_chunk(uint64_t usage, uint64_t total_bytes);

  struct PriCache {
    virtual ~PriCache();

    /* Ask the cache to request memory for the given priority. Note that the
     * cache may ultimately be allocated less memory than it requests here.
     */
    virtual int64_t request_cache_bytes(PriorityCache::Priority pri, uint64_t total_cache) const = 0;

    // Get the number of bytes currently allocated to the given priority.
    virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const = 0;

    // Get the number of bytes currently allocated to all priorities.
    virtual int64_t get_cache_bytes() const = 0;

    // Allocate bytes for a given priority.
    virtual void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) = 0;

    // Allocate additional bytes for a given priority.
    virtual void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) = 0;

    /* Commit the current number of bytes allocated to the cache.  Space is
     * allocated in chunks based on the allocation size and current total size
     * of memory available for caches. */
    virtual int64_t commit_cache_size(uint64_t total_cache) = 0;

    /* Get the current number of bytes allocated to the cache. this may be
     * larger than the value returned by get_cache_bytes as it includes extra
     * space for future growth. */
    virtual int64_t get_committed_size() const = 0;

    // Get the ratio of available memory this cache should target.
    virtual double get_cache_ratio() const = 0;

    // Set the ratio of available memory this cache should target.
    virtual void set_cache_ratio(double ratio) = 0;

    // Get the name of this cache.
    virtual std::string get_cache_name() const = 0;
  };

  class Manager {
    CephContext* cct = nullptr;
    PerfCounters* logger;
    std::unordered_map<std::string, PerfCounters*> loggers;
    std::unordered_map<std::string, std::vector<int>> indexes;
    std::unordered_map<std::string, std::shared_ptr<PriCache>> caches;

    // Start perf counter slots after the malloc stats.
    int cur_index = MallocStats::M_LAST;

    uint64_t min_mem = 0;
    uint64_t max_mem = 0;
    uint64_t target_mem = 0;
    uint64_t tuned_mem = 0;
    bool reserve_extra;
    std::string name;
  public:
    Manager(CephContext *c, uint64_t min, uint64_t max, uint64_t target,
            bool reserve_extra, const std::string& name = std::string());
    ~Manager();
    void set_min_memory(uint64_t min) {
      min_mem = min;
    }
    void set_max_memory(uint64_t max) {
      max_mem = max;
    }
    void set_target_memory(uint64_t target) {
      target_mem = target;
    }
    uint64_t get_tuned_mem() const {
      return tuned_mem;
    }
    void insert(const std::string& name, const std::shared_ptr<PriCache> c,
                bool enable_perf_counters);
    void erase(const std::string& name);
    void clear();
    void tune_memory();
    void balance();

  private:
    void balance_priority(int64_t *mem_avail, Priority pri);
  };
}

#endif
