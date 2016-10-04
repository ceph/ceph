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

static std::string demangle(const char* name);

void mempool::FormatStatsByBytes(const std::multimap<size_t,StatsByBytes_t>&m, ceph::Formatter *f) {
   f->open_array_section("ByBytes");
   for (auto& p : m) {
      f->dump_unsigned("Bytes",p.first);
      p.second.dump(f);
   }
   f->close_section();
}

void mempool::FormatStatsBySlots(const std::multimap<size_t,StatsBySlots_t>&m, ceph::Formatter *f) {
   f->open_array_section("BySlots");
   for (auto& p : m) {
      f->dump_unsigned("Slots",p.first);
      p.second.dump(f);
   }
   f->close_section();
}

void mempool::FormatStatsBySlabs(const std::multimap<size_t,StatsBySlabs_t>&m, ceph::Formatter *f) {
   f->open_array_section("BySlabs");
   for (auto& p : m) {
      f->dump_unsigned("Slabs",p.first);
      p.second.dump(f);
   }
   f->close_section();
}

void mempool::FormatStatsByTypeID(const std::map<const char *,StatsByTypeID_t>&m, ceph::Formatter *f) {
   f->open_array_section("ByTypeID");
   for (auto& p : m) {
      f->dump_string("TypeID",demangle(p.first));
      p.second.dump(f);
   }
   f->close_section();
}

void mempool::DumpStatsByBytes(const std::string& prefix,ceph::Formatter *f,size_t trim) {
   std::multimap<size_t,mempool::StatsByBytes_t> m;
   pool_t::StatsByBytes(prefix,m,trim);
   FormatStatsByBytes(m,f);
}

void mempool::DumpStatsBySlots(const std::string& prefix,ceph::Formatter *f,size_t trim) {
   std::multimap<size_t,StatsBySlots_t> m;
   pool_t::StatsBySlots(prefix,m,trim);
   FormatStatsBySlots(m,f);
}

void mempool::DumpStatsBySlabs(const std::string& prefix,ceph::Formatter *f,size_t trim) {
   std::multimap<size_t,StatsBySlabs_t> m;
   pool_t::StatsBySlabs(prefix,m,trim);
   FormatStatsBySlabs(m,f);
}

void mempool::DumpStatsByTypeID(const std::string& prefix,ceph::Formatter *f,size_t trim) {
   std::map<const char *,StatsByTypeID_t> m;
   pool_t::StatsByTypeID(prefix,m,trim);
   FormatStatsByTypeID(m,f);
}

//// Stole this code from http://stackoverflow.com/questions/281818/unmangling-the-result-of-stdtype-infoname
#ifdef __GNUG__
#include <cstdlib>
#include <memory>
#include <cxxabi.h>

static std::string demangle(const char* name) {

    int status = -4; // some arbitrary value to eliminate the compiler warning

    // enable c++11 by passing the flag -std=c++11 to g++
    std::unique_ptr<char, void(*)(void*)> res {
        abi::__cxa_demangle(name, NULL, NULL, &status),
        std::free
    };

    return (status==0) ? res.get() : name ;
}

#else

// does nothing if not g++
static std::string demangle(const char* name) {
    return name;
}

#endif

void mempool::StatsByBytes_t::dump(ceph::Formatter *f) const {
   f->dump_unsigned("Slots",slots);
   f->dump_unsigned("Slabs",slabs);
   f->dump_string("TypeID",demangle(typeID));
}

void mempool::StatsBySlots_t::dump(ceph::Formatter *f) const {
   f->dump_unsigned("Bytes",bytes);
   f->dump_unsigned("Slabs",slabs);
   f->dump_string("TypeID",demangle(typeID));
}

void mempool::StatsBySlabs_t::dump(ceph::Formatter *f) const {
   f->dump_unsigned("Bytes",bytes);
   f->dump_unsigned("Slots",slots);
   f->dump_string("TypeID",demangle(typeID));
}

void mempool::StatsByTypeID_t::dump(ceph::Formatter *f) const {
   f->dump_unsigned("Bytes",bytes);
   f->dump_unsigned("Slots",slots);
   f->dump_unsigned("Slabs",slabs);
}


