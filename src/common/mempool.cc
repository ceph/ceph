// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Allen Samuels <allen.samuels@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <thread>
#include "include/mempool.h"
#include "include/demangle.h"

// default to debug_mode off
bool mempool::debug_mode = false;

// --------------------------------------------------------------

namespace mempool {

static size_t num_shard_bits;
static size_t num_shards;

static thread_local size_t thread_shard_index = max_shards;

#if !defined(MEMPOOL_SCHED_GETCPU)
// There is no cheap way of determining which core a thread is assigned to.
// Statically assign threads to shards to minimize how many threads use the
// same shard
static int thread_shard_next;
static std::mutex thread_shard_mtx;

#else
// Use sched_getcpu() to determine which core a thread is running on,
// use this to assign the thread a shard and cache this value to
// eliminate overhead of repeatedly calling sched_getcpu. Periodically
// check the cached value to deal with non-bound threads switching cores.
// Ideally this means that each CPU core accesses its own shard and there
// are no shared CPU cachelines. If a thread switches cores performance
// will be non-optimal (but atomics ensure correctness) until the switch
// is noticed and a new shard is selected

enum {
  thread_shard_recheck_min_interval = 1,
  thread_shard_recheck_max_interval = 10000
};

static thread_local int thread_shard_recheck_count;
static thread_local int thread_shard_recheck_interval;

// Keep statistics on how successfully we manage to track which core a thread
// is running on
//
// correct_core - number of times thread to core binding is correctly tracked
// incorrect_core - number of times thread to core binding is incorrectly tracked
// checked_core - number of times sched_getcpu() has been called
//
// sched_getcpu_cost = 32 cycles
// cacheline_ping_pong_cost = 150 cycles (intra socket), 400 cycles (inter socket)
//
// These costs will vary between CPU architectures, but relative difference between
// costs is likely to be similar
//
// cached_algorithm_cost = checked_core * sched_getcpu_cost + incorrect_core * cacheline_ping_pong_cost
// uncached_algorithm_cost = (correct_core + incorrect_core) * sched_getcpu_cost

struct fault_stats_t {
  ceph::atomic<size_t> correct_core = {0};
  ceph::atomic<size_t> incorrect_core = {0};
  ceph::atomic<size_t> checked_core = {0};

  char __padding[128 - sizeof(ceph::atomic<size_t>)*3];

  void dump(ceph::Formatter *f) const {
    f->dump_int("correct_core", correct_core );
    f->dump_int("incorrect_core", incorrect_core);
    f->dump_int("checked_core", checked_core);
  }

  fault_stats_t& operator+=(const fault_stats_t& o) {
    correct_core += o.correct_core;
    incorrect_core += o.incorrect_core;
    checked_core += o.checked_core;
    return *this;
  }
} __attribute__ ((aligned (128)));
static_assert(sizeof(fault_stats_t) == 128, "fault_stats_t should be cacheline-sized");

static std::unique_ptr<fault_stats_t[]> shard_faults;
#endif

}

size_t mempool::get_num_shards(void) {
  static std::once_flag once;
  std::call_once(once,[&]() {
    unsigned int threads = std::thread::hardware_concurrency();
    if (threads == 0) {
      threads = default_shards;
    }
    threads = std::clamp<unsigned int>( threads, min_shards, max_shards );
    threads--;
    while (threads != 0) {
      num_shard_bits++;
      threads>>=1;
    }
    num_shards = 1 << num_shard_bits;
#if defined(MEMPOOL_SCHED_GETCPU)
    shard_faults = std::make_unique<fault_stats_t[]>(num_shards);
#endif
  });
  return num_shards;
}

int mempool::pick_a_shard_int(void) {
#if !defined(MEMPOOL_SCHED_GETCPU)
  if (thread_shard_index == max_shards) {
    // Thread has not been assigned to a shard yet
    std::lock_guard<std::mutex> lck (thread_shard_mtx);
    thread_shard_index = thread_shard_next++ & ((1 << num_shard_bits) - 1);
  }
#else
  if (thread_shard_index == max_shards) {
    // Thread has not been assigned to a shard yet
    thread_shard_index = sched_getcpu() & ((1 << num_shard_bits) - 1);
    thread_shard_recheck_count = 0;
    thread_shard_recheck_interval = thread_shard_recheck_min_interval;
  }else if (++thread_shard_recheck_count >= thread_shard_recheck_interval) {
    // Periodically recheck that thread has not changed cores
    size_t new_index = sched_getcpu() & ((1 << num_shard_bits) - 1);
    shard_faults[thread_shard_index].checked_core++;
    thread_shard_recheck_count = 0;
    if (thread_shard_index == new_index) {
      // Thread has not moved, recheck less often
      shard_faults[thread_shard_index].correct_core += thread_shard_recheck_interval;
      if (thread_shard_recheck_interval < thread_shard_recheck_max_interval) {
	thread_shard_recheck_interval++;
      }
    }else{
      // Thread has moved, recheck more often
      thread_shard_index = new_index;
      shard_faults[thread_shard_index].incorrect_core += thread_shard_recheck_interval / 2;
      int decrease = thread_shard_recheck_interval / 2;
      if ( decrease == 0) {
	decrease = 1;
      }
      thread_shard_recheck_interval -= decrease;
      if (thread_shard_recheck_interval < thread_shard_recheck_min_interval) {
	thread_shard_recheck_interval = thread_shard_recheck_min_interval;
      }
    }
  }
#endif
  return thread_shard_index;
}

mempool::pool_t& mempool::get_pool(mempool::pool_index_t ix)
{
  // We rely on this array being initialized before any invocation of
  // this function, even if it is called by ctors in other compilation
  // units that are being initialized before this compilation unit.
  static mempool::pool_t table[num_pools];
  return table[ix];
}

const char *mempool::get_pool_name(mempool::pool_index_t ix) {
#define P(x) #x,
  static const char *names[num_pools] = {
    DEFINE_MEMORY_POOLS_HELPER(P)
  };
#undef P
  return names[ix];
}

void mempool::dump(ceph::Formatter *f)
{
  stats_t total;
  f->open_object_section("mempool"); // we need (dummy?) topmost section for 
				     // JSON Formatter to print pool names. It omits them otherwise.
  f->open_object_section("by_pool");
  for (size_t i = 0; i < num_pools; ++i) {
    const pool_t &pool = mempool::get_pool((pool_index_t)i);
    f->open_object_section(get_pool_name((pool_index_t)i));
    pool.dump(f, &total);
    f->close_section();
  }
  f->close_section();
  f->dump_object("total", total);
#if defined(MEMPOOL_SCHED_GETCPU)
  fault_stats_t total_fault_stats;
  for (size_t i = 0; i < get_num_shards(); ++i) {
      total_fault_stats += shard_faults[i];
  }
  f->dump_object("fault_stats", total_fault_stats);
#endif
  f->close_section();
}

void mempool::set_debug_mode(bool d)
{
  debug_mode = d;
}

// --------------------------------------------------------------
// pool_t

size_t mempool::pool_t::allocated_bytes() const
{
  ssize_t result = 0;
  for (size_t i = 0; i < get_num_shards(); ++i) {
    result += shard[i].bytes;
  }
  if (result < 0) {
    // we raced with some unbalanced allocations/deallocations
    result = 0;
  }
  return (size_t) result;
}

size_t mempool::pool_t::allocated_items() const
{
  ssize_t result = 0;
  for (size_t i = 0; i < get_num_shards(); ++i) {
    result += shard[i].items;
  }
  if (result < 0) {
    // we raced with some unbalanced allocations/deallocations
    result = 0;
  }
  return (size_t) result;
}

void mempool::pool_t::adjust_count(ssize_t items, ssize_t bytes)
{
  const auto shid = pick_a_shard_int();
  shard[shid].items += items;
  shard[shid].bytes += bytes;
}

void mempool::pool_t::get_stats(
  stats_t *total,
  std::map<std::string, stats_t> *by_type) const
{
  for (size_t i = 0; i < get_num_shards(); ++i) {
    total->items += shard[i].items;
    total->bytes += shard[i].bytes;
  }
  if (debug_mode) {
    std::lock_guard shard_lock(lock);
    for (auto &p : type_map) {
      std::string n = ceph_demangle(p.second.type_name);
      stats_t &s = (*by_type)[n];
      s.bytes = 0;
      s.items = 0;
      for (size_t i = 0 ; i < get_num_shards(); ++i) {
        s.bytes += p.second.shards[i].items * p.second.item_size;
        s.items += p.second.shards[i].items;
      }
    }
  }
}

void mempool::pool_t::dump(ceph::Formatter *f, stats_t *ptotal) const
{
  stats_t total;
  std::map<std::string, stats_t> by_type;
  get_stats(&total, &by_type);
  if (ptotal) {
    *ptotal += total;
  }
  total.dump(f);
  if (!by_type.empty()) {
    f->open_object_section("by_type");
    for (auto &i : by_type) {
      f->open_object_section(i.first.c_str());
      i.second.dump(f);
      f->close_section();
    }
    f->close_section();
  }
}
