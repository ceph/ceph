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
#include "common/dout.h"
#include "perfglue/heap_profiler.h"
#define dout_context cct
#define dout_subsys ceph_subsys_prioritycache
#undef dout_prefix
#define dout_prefix *_dout << "prioritycache "

namespace PriorityCache
{
  int64_t get_chunk(uint64_t usage, uint64_t total_bytes)
  {
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

  Manager::Manager(CephContext *c,
                   uint64_t min,
                   uint64_t max,
                   uint64_t target,
                   bool reserve_extra,
		   const std::string& name) :
      cct(c),
      caches{},
      min_mem(min),
      max_mem(max),
      target_mem(target),
      tuned_mem(min),
      reserve_extra(reserve_extra),
      name(name.empty() ? "prioritycache" : name)
  {
    PerfCountersBuilder b(cct, name,
                          MallocStats::M_FIRST, MallocStats::M_LAST);

    b.add_u64(MallocStats::M_TARGET_BYTES, "target_bytes",
              "target process memory usage in bytes", "t",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(MallocStats::M_MAPPED_BYTES, "mapped_bytes",
              "total bytes mapped by the process", "m",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(MallocStats::M_UNMAPPED_BYTES, "unmapped_bytes",
              "unmapped bytes that the kernel has yet to reclaimed", "u",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(MallocStats::M_HEAP_BYTES, "heap_bytes",
              "aggregate bytes in use by the heap", "h",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(MallocStats::M_CACHE_BYTES, "cache_bytes",
              "current memory available for caches.", "c",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    logger = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);

    tune_memory();
  }

  Manager::~Manager()
  {
    clear();
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
  }

  void Manager::tune_memory()
  {
    size_t heap_size = 0;
    size_t unmapped = 0;
    uint64_t mapped = 0;

    ceph_heap_release_free_memory();
    ceph_heap_get_numeric_property("generic.heap_size", &heap_size);
    ceph_heap_get_numeric_property("tcmalloc.pageheap_unmapped_bytes", &unmapped);
    mapped = heap_size - unmapped;

    uint64_t new_size = tuned_mem;
    new_size = (new_size < max_mem) ? new_size : max_mem;
    new_size = (new_size > min_mem) ? new_size : min_mem;

    // Approach the min/max slowly, but bounce away quickly.
    if ((uint64_t) mapped < target_mem) {
      double ratio = 1 - ((double) mapped / target_mem);
      new_size += ratio * (max_mem - new_size);
    } else { 
      double ratio = 1 - ((double) target_mem / mapped);
      new_size -= ratio * (new_size - min_mem);
    }

    ldout(cct, 5) << __func__
                  << " target: " << target_mem
                  << " mapped: " << mapped  
                  << " unmapped: " << unmapped
                  << " heap: " << heap_size
                  << " old mem: " << tuned_mem
                  << " new mem: " << new_size << dendl;

    tuned_mem = new_size;

    logger->set(MallocStats::M_TARGET_BYTES, target_mem);
    logger->set(MallocStats::M_MAPPED_BYTES, mapped);
    logger->set(MallocStats::M_UNMAPPED_BYTES, unmapped);
    logger->set(MallocStats::M_HEAP_BYTES, heap_size);
    logger->set(MallocStats::M_CACHE_BYTES, new_size);
  }

  void Manager::insert(const std::string& name, std::shared_ptr<PriCache> c,
                       bool enable_perf_counters)
  {
    ceph_assert(!caches.count(name));
    ceph_assert(!indexes.count(name));

    caches.emplace(name, c);

    if (!enable_perf_counters) {
      return;
    }

    // TODO: If we ever assign more than
    // PERF_COUNTER_MAX_BOUND - PERF_COUNTER_LOWER_BOUND perf counters for
    // priority caching we could run out of slots.  Recycle them some day?
    // Also note that start and end are *exclusive*.
    int start = cur_index++;
    int end = cur_index + Extra::E_LAST + 1;

    ceph_assert(end < PERF_COUNTER_MAX_BOUND);
    indexes.emplace(name, std::vector<int>(Extra::E_LAST + 1));

    PerfCountersBuilder b(cct, this->name + ":" + name, start, end);

    b.add_u64(cur_index + Priority::PRI0, "pri0_bytes",
              "bytes allocated to pri0", "p0",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI1, "pri1_bytes",
              "bytes allocated to pri1", "p1",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI2, "pri2_bytes",
              "bytes allocated to pri2", "p2",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI3, "pri3_bytes",
              "bytes allocated to pri3", "p3",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI4, "pri4_bytes",
              "bytes allocated to pri4", "p4",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI5, "pri5_bytes",
              "bytes allocated to pri5", "p5",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI6, "pri6_bytes",
              "bytes allocated to pri6", "p6",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI7, "pri7_bytes",
              "bytes allocated to pri7", "p7",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI8, "pri8_bytes",
              "bytes allocated to pri8", "p8",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI9, "pri9_bytes",
              "bytes allocated to pri9", "p9",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI10, "pri10_bytes",
              "bytes allocated to pri10", "p10",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Priority::PRI11, "pri11_bytes",
              "bytes allocated to pri11", "p11",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Extra::E_RESERVED, "reserved_bytes",
              "bytes reserved for future growth.", "r",
              PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));

    b.add_u64(cur_index + Extra::E_COMMITTED, "committed_bytes",
              "total bytes committed,", "c",
              PerfCountersBuilder::PRIO_CRITICAL, unit_t(UNIT_BYTES));

    for (int i = 0; i < Extra::E_LAST+1; i++) {
      indexes[name][i] = cur_index + i;
    }

    auto l = b.create_perf_counters();
    loggers.emplace(name, l);
    cct->get_perfcounters_collection()->add(l);

    cur_index = end;
  }

  void Manager::erase(const std::string& name)
  {
    auto li = loggers.find(name);
    if (li != loggers.end()) {
      cct->get_perfcounters_collection()->remove(li->second);
      delete li->second;
      loggers.erase(li);
    }
    indexes.erase(name);
    caches.erase(name);
  }

  void Manager::clear()
  {
    auto li = loggers.begin();
    while (li != loggers.end()) {
      cct->get_perfcounters_collection()->remove(li->second);
      delete li->second;
      li = loggers.erase(li);
    }
    indexes.clear();
    caches.clear();
  }

  void Manager::balance()
  {
    int64_t mem_avail = tuned_mem;
    // Each cache is going to get a little extra from get_chunk, so shrink the
    // available memory here to compensate.
    if (reserve_extra) {
      mem_avail -= get_chunk(1, tuned_mem) * caches.size();
    }

    if (mem_avail < 0) {
      // There's so little memory available that just assigning a chunk per
      // cache pushes us over the limit. Set mem_avail to 0 and continue to
      // ensure each priority's byte counts are zeroed in balance_priority.
      mem_avail = 0;
    }

    // Assign memory for each priority level
    for (int i = 0; i < Priority::LAST+1; i++) {
      ldout(cct, 10) << __func__ << " assigning cache bytes for PRI: " << i << dendl;

      auto pri = static_cast<Priority>(i);
      balance_priority(&mem_avail, pri);

      // Update the per-priority perf counters
      for (auto &l : loggers) {
        auto it = caches.find(l.first);
        ceph_assert(it != caches.end());

        auto bytes = it->second->get_cache_bytes(pri);
        l.second->set(indexes[it->first][pri], bytes);
      }
    }
    // assert if we assigned more memory than is available.
    ceph_assert(mem_avail >= 0);

    for (auto &l : loggers) {
      auto it = caches.find(l.first);
      ceph_assert(it != caches.end());

      // Commit the new cache size
      int64_t committed = it->second->commit_cache_size(tuned_mem);

      // Update the perf counters
      int64_t alloc = it->second->get_cache_bytes();

      l.second->set(indexes[it->first][Extra::E_RESERVED], committed - alloc);
      l.second->set(indexes[it->first][Extra::E_COMMITTED], committed);
    }
  }

  void Manager::balance_priority(int64_t *mem_avail, Priority pri)
  {
    std::unordered_map<std::string, std::shared_ptr<PriCache>> tmp_caches = caches;
    double cur_ratios = 0;
    double new_ratios = 0;
    uint64_t round = 0;

    // First, zero this priority's bytes, sum the initial ratios.
    for (auto it = caches.begin(); it != caches.end(); it++) {
      it->second->set_cache_bytes(pri, 0);
      cur_ratios += it->second->get_cache_ratio();
    }

    // For other priorities, loop until caches are satisified or we run out of
    // memory (stop if we can't guarantee a full byte allocation).
    while (!tmp_caches.empty() && *mem_avail > static_cast<int64_t>(tmp_caches.size())) {
      uint64_t total_assigned = 0;
      for (auto it = tmp_caches.begin(); it != tmp_caches.end();) {
        int64_t cache_wants = it->second->request_cache_bytes(pri, tuned_mem);
        // Usually the ratio should be set to the fraction of the current caches'
        // assigned ratio compared to the total ratio of all caches that still
        // want memory.  There is a special case where the only caches left are
        // all assigned 0% ratios but still want memory.  In that case, give 
        // them an equal shot at the remaining memory for this priority.
        double ratio = 1.0 / tmp_caches.size();
        if (cur_ratios > 0) {
          ratio = it->second->get_cache_ratio() / cur_ratios;
        }
        int64_t fair_share = static_cast<int64_t>(*mem_avail * ratio);

        ldout(cct, 10) << __func__ << " " << it->first
                       << " pri: " << (int) pri
                       << " round: " << round
                       << " wanted: " << cache_wants
                       << " ratio: " << it->second->get_cache_ratio()
                       << " cur_ratios: " << cur_ratios
                       << " fair_share: " << fair_share
                       << " mem_avail: " << *mem_avail
                       << dendl;

        if (cache_wants > fair_share) {
          // If we want too much, take what we can get but stick around for more
          it->second->add_cache_bytes(pri, fair_share);
          total_assigned += fair_share;
          new_ratios += it->second->get_cache_ratio();
          ++it;
        } else {
          // Otherwise assign only what we want
          if (cache_wants > 0) {
            it->second->add_cache_bytes(pri, cache_wants);
            total_assigned += cache_wants;
          }
          // Either the cache didn't want anything or got what it wanted, so
          // remove it from the tmp list.
          it = tmp_caches.erase(it);
        }
      }
      // Reset the ratios 
      *mem_avail -= total_assigned;
      cur_ratios = new_ratios;
      new_ratios = 0;
      ++round;
    }

    // If this is the last priority, divide up any remaining memory based
    // solely on the ratios.
    if (pri == Priority::LAST) {
      uint64_t total_assigned = 0;
      for (auto it = caches.begin(); it != caches.end(); it++) {
        double ratio = it->second->get_cache_ratio();
        int64_t fair_share = static_cast<int64_t>(*mem_avail * ratio);
        it->second->set_cache_bytes(Priority::LAST, fair_share);
        total_assigned += fair_share;
      }
      *mem_avail -= total_assigned;
      return;
    }
  }

  PriCache::~PriCache()
  {
  }
}
