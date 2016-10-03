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

std::map<std::string,mempool::pool_t *> *mempool::pool_t::pool_head = nullptr;
std::mutex mempool::pool_t::pool_head_lock;


static mempool::pool_t *pools[mempool::num_pools] = {
#define P(x) nullptr,
   DEFINE_MEMORY_POOLS_HELPER(P)
#undef P
};

mempool::pool_t& mempool::GetPool(mempool::pool_index_t ix) {
   if (pools[ix]) return *pools[ix];
#define P(x) \
   case x: pools[ix] = new mempool::pool_t(#x,true); break;

   switch (ix) {
      DEFINE_MEMORY_POOLS_HELPER(P);
      default: assert(0);
   }
   return *pools[ix];
#undef P
}

size_t mempool::pool_t::allocated_bytes() const {
   ssize_t result = 0;
   for (size_t i = 0; i < shard_size; ++i) {
       result += shard[i].allocated;
   }
   return (size_t) result;
}

//
// Accumulate stats sorted by ...
//
void mempool::pool_t::StatsBySlots(
  const std::string& prefix,
  std::multimap<size_t,StatsBySlots_t>& bySlots,
  size_t trim)  
{
   VisitAllPools(prefix,bySlots,trim);
}

void mempool::pool_t::StatsByBytes(
   const std::string& prefix,
   std::multimap<size_t,StatsByBytes_t>& byBytes,
   size_t trim) {
   VisitAllPools(prefix,byBytes,trim);
}

void mempool::pool_t::StatsBySlabs(
   const std::string& prefix,
   std::multimap<size_t,StatsBySlabs_t>& bySlabs,
   size_t trim) {
   VisitAllPools(prefix,bySlabs,trim);
}

void mempool::pool_t::StatsByTypeID(
   const std::string& prefix,
   std::map<const char *,StatsByTypeID_t>& byTypeID,
   size_t trim) {
   VisitAllPools(prefix,byTypeID,trim);
}

//
// Here's where the work is done
//
void mempool::slab_allocator_base_t::UpdateStats(std::multimap<size_t,StatsByBytes_t>&m) const {
   StatsByBytes_t s;
   s.typeID = typeID;
   s.slots  = slots;
   s.slabs  = slabs;
   m.insert(std::make_pair(bytes,s));  
}

void mempool::slab_allocator_base_t::UpdateStats(std::multimap<size_t,StatsBySlots_t>&m) const {
   StatsBySlots_t s;
   s.typeID = typeID;
   s.bytes  = bytes;
   s.slabs  = slabs;
   m.insert(std::make_pair(slots,s));  
}

void mempool::slab_allocator_base_t::UpdateStats(std::multimap<size_t,StatsBySlabs_t>&m) const {
   StatsBySlabs_t s;
   s.typeID = typeID;
   s.slots  = slots;
   s.bytes  = bytes;
   m.insert(std::make_pair(slabs,s));  
}

void mempool::slab_allocator_base_t::UpdateStats(std::map<const char *,StatsByTypeID_t>&m) const {
   StatsByTypeID_t &s = m[typeID];
   s.slots  += slots;
   s.slabs  += slabs;
   s.bytes  += bytes;
}




