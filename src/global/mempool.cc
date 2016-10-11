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


// default to debug_mode off
static bool debug_mode = false;

// --------------------------------------------------------------

static mempool::pool_t *pools[mempool::num_pools] = {
#define P(x) nullptr,
   DEFINE_MEMORY_POOLS_HELPER(P)
#undef P
};

mempool::pool_t& mempool::get_pool(mempool::pool_index_t ix)
{
  if (pools[ix]) {
    return *pools[ix];
  }

  switch (ix) {
#define P(x)								\
  case x: pools[ix] = new mempool::pool_t(#x,debug_mode); break;
    DEFINE_MEMORY_POOLS_HELPER(P);
#undef P
  default: assert(0);
  }
  return *pools[ix];
}

void mempool::dump(ceph::Formatter *f, size_t skip)
{
  for (size_t i = skip; i < num_pools; ++i) {
    const pool_t &pool = mempool::get_pool((pool_index_t)i);
    f->open_object_section(pool.get_name().c_str());
    pool.dump(f);
    f->close_section();
  }
}

void mempool::set_debug_mode(bool d)
{
  debug_mode = d;
  for (size_t i = 0; i < mempool::num_pools; ++i) {
    if (pools[i]) {
      pools[i]->debug = d;
    }
  }
}

// --------------------------------------------------------------
// pool_t

size_t mempool::pool_t::allocated_bytes() const
{
  ssize_t result = 0;
  for (size_t i = 0; i < num_shards; ++i) {
    result += shard[i].bytes;
  }
  return (size_t) result;
}

size_t mempool::pool_t::allocated_items() const
{
  ssize_t result = 0;
  for (size_t i = 0; i < num_shards; ++i) {
    result += shard[i].items;
  }
  return (size_t) result;
}

void mempool::pool_t::get_stats(
  stats_t *total,
  std::map<std::string, stats_t> *by_type) const
{
  for (size_t i = 0; i < num_shards; ++i) {
    total->items += shard[i].items;
    total->bytes += shard[i].bytes;
    if (debug) {
      std::unique_lock<std::mutex> shard_lock(shard[i].lock);
      for (const list_member_t *p = shard[i].containers.next;
	   p != &shard[i].containers;
	   p = p->next) {
	const pool_allocator_base_t *c =
	  reinterpret_cast<const pool_allocator_base_t *>(p);
	std::string n = ceph_demangle(c->type_id);
	stats_t &s = (*by_type)[n];
	s.bytes = c->items * c->item_size;
	s.items = c->items;
      }
    }
  }
}

void mempool::pool_t::dump(ceph::Formatter *f) const
{
  stats_t total;
  std::map<std::string, stats_t> by_type;
  get_stats(&total, &by_type);
  f->dump_object("total", total);
  if (!by_type.empty()) {
    for (auto &i : by_type) {
      f->open_object_section(i.first.c_str());
      i.second.dump(f);
      f->close_section();
    }
  }
}
