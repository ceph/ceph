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

#include "include/mempool.h"
#include "include/demangle.h"

#if defined(_GNU_SOURCE) && defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
#else
// Thread local variables should save index, not &shard[index],
// because shard[] is defined in the class
static thread_local size_t thread_shard_index = mempool::num_shards;
#endif

// default to debug_mode off
bool mempool::debug_mode = false;

// --------------------------------------------------------------

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
  for (size_t i = 0; i < num_shards; ++i) {
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
  for (size_t i = 0; i < num_shards; ++i) {
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
#if defined(_GNU_SOURCE) && defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
  // the expected path: we alway pick the shard for a cpu core
  // a thread is executing on.
  const size_t shard_index = pick_a_shard_int();
#else
  // fallback for lack of sched_getcpu()
  const size_t shard_index = []() {
    if (thread_shard_index == num_shards) {
      thread_shard_index = pick_a_shard_int();
    }
    return thread_shard_index;
  }();
#endif
  shard[shard_index].items += items;
  shard[shard_index].bytes += bytes;
}

void mempool::pool_t::get_stats(
  stats_t *total,
  std::map<std::string, stats_t> *by_type) const
{
  for (size_t i = 0; i < num_shards; ++i) {
    total->items += shard[i].items;
    total->bytes += shard[i].bytes;
  }
  if (debug_mode) {
    std::lock_guard shard_lock(lock);
    for (auto &p : type_map) {
      std::string n = ceph_demangle(p.second.type_name);
      stats_t &s = (*by_type)[n];
#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
      s.bytes = 0;
      s.items = 0;
      for (size_t i = 0 ; i < num_shards; ++i) {
        s.bytes += p.second.shards[i].items * p.second.item_size;
        s.items += p.second.shards[i].items;
      }
#else
      s.bytes = p.second.items * p.second.item_size;
      s.items = p.second.items;
#endif
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
