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

std::unique_ptr<shard_t[]> shards = std::make_unique<shard_t[]>(get_num_shards());
}

size_t mempool::get_num_shards(void) {
  static std::once_flag once;
  std::call_once(once,[&]() {
    unsigned int threads = std::thread::hardware_concurrency();
    if (threads == 0) {
      threads = DEFAULT_SHARDS;
    }
    threads = std::clamp<unsigned int>(threads, MIN_SHARDS, MAX_SHARDS);
    threads--;
    while (threads != 0) {
      num_shard_bits++;
      threads>>=1;
    }
    num_shards = 1 << num_shard_bits;
  });
  return num_shards;
}

// There are 2 implementations of pick_a_shard_int, SCHED GETCPU is
// the preferred implementaion, ROUND ROBIN is used if sched_getcpu() is not
// available.
int mempool::pick_a_shard_int(void) {
#if defined(MEMPOOL_SCHED_GETCPU)
  // SCHED_GETCPU: Shards are assigned to CPU cores. Threads use sched_getcpu()
  // to query the core before every access to the shard. Other than the (very
  // rare) situation where a context switch occurs between calling
  // sched_getcpu() and updating the shard there is no cache line
  // contention
  return sched_getcpu() & ((1 << num_shard_bits) - 1);
#else
  // ROUND_ROBIN: Static assignment of threads to shards using a round robin
  // distribution. This minimizes the number of threads sharing the same shard,
  // but threads sharing the same shard will cause cache line ping pong when
  // the threads are running on different cores (likely)
  static int thread_shard_next;
  static std::mutex thread_shard_mtx;
  static thread_local size_t thread_shard_index = MAX_SHARDS;

  if (thread_shard_index == MAX_SHARDS) {
    // Thread has not been assigned to a shard yet
    std::lock_guard<std::mutex> lck (thread_shard_mtx);
    thread_shard_index = thread_shard_next++ & ((1 << num_shard_bits) - 1);
  }
  return thread_shard_index;
#endif
}

mempool::pool_t& mempool::get_pool(mempool::pool_index_t ix)
{
  // We rely on this array being initialized before any invocation of
  // this function, even if it is called by ctors in other compilation
  // units that are being initialized before this compilation unit.
  static mempool::pool_t table[num_pools];
  table[ix].pool_index = ix;
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
    result += shards[i].pool[pool_index].bytes;
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
    result += shards[i].pool[pool_index].items;
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
  auto& shard = shards[shid].pool[pool_index];
  shard.items += items;
  shard.bytes += bytes;
}

void mempool::pool_t::get_stats(
  stats_t *total,
  std::map<std::string, stats_t> *by_type) const
{
  for (size_t i = 0; i < get_num_shards(); ++i) {
    total->items += shards[i].pool[pool_index].items;
    total->bytes += shards[i].pool[pool_index].bytes;
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
