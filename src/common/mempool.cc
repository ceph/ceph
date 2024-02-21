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
// Use sched_getcpu() to determine which core a thread is running on
#endif

}

size_t mempool::get_num_shards(void) {
  if (num_shards==0) {
    size_t threads = std::thread::hardware_concurrency();
    if (threads == 0) {
      threads = default_shards;
    }
    if (threads < min_shards) {
      threads = min_shards;
    }
    if (threads > max_shards) {
      threads = max_shards;
    }
    threads--;
    while (threads!=0) {
      num_shard_bits++;
      threads>>=1;
    }
    num_shards = 1 << num_shard_bits;
  }
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
