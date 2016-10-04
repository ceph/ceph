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

#ifndef _MEMPOOL_H
#define _MEMPOOL_H
#include <iostream>
#include <fstream>

#include <cstddef>
#include <map>
#include <set>
#include <vector>
#include <assert.h>
#include <list>
#include <mutex>
#include <atomic>
#include <climits>
#include <typeinfo>

#include <common/Formatter.h>

/**********************

Memory Pools

A memory pool isn't a physical entity, no is made to the underlying malloc/free strategy or implementation. 
Rather a memory pool is a method for accounting the consumption of memory of a set of containers.

Memory pools are statically declared (see pool_index_t).

Containers (i.e., stl containers) and objects are declared to be part of a particular pool (again, statically).
As those containers grow and shrink as well as are created and destroyed the memory consumption is tracked.

The goal is to be able to inexpensively answer the question: How much memory is pool 'x' using?

However, there is also a "debug" mode which enables substantial additional statistics to be tracked.
It is hoped that the 'debug' mode is inexpensive enough to allow it to be enabled on production systems.


**********************/

namespace mempool {


#define DEFINE_MEMORY_POOLS_HELPER(f) \
   f(unittest_1) \
   f(uniitest_2)   


#define P(x) x,
enum pool_index_t {
   DEFINE_MEMORY_POOLS_HELPER(P)
   num_pools        // Must be last.
};
#undef P

struct slab_allocator_base_t;
class pool_t;

//
// Doubly linked list membership.
//
struct list_member_t {
   list_member_t *next;
   list_member_t *prev;
   list_member_t() : next(this), prev(this) {}
   ~list_member_t() { assert(next == this && prev == this); }
   void insert(list_member_t *i) {
      i->next = next;
      i->prev = this;
      next = i;
   }
   void remove() {
      prev->next = next;
      next->prev = prev;
      next = this;
      prev = this;
   }
};

struct shard_t {
   std::atomic<size_t> allocated;
   mutable std::mutex lock;  // Only used for containers list
   list_member_t containers;
   shard_t() : allocated(0) {}
};

//
// Stats structures
//
struct StatsByBytes_t {
   const char* typeID;
   size_t slots;
   size_t slabs;
   StatsByBytes_t() : typeID(nullptr), slots(0), slabs(0) {}
   void dump(ceph::Formatter *f) const;
};
struct StatsBySlots_t {
   const char *typeID;
   size_t slabs;
   size_t bytes;
   StatsBySlots_t() : typeID(nullptr), slabs(0), bytes(0) {} 
   void dump(ceph::Formatter *f) const;
};
struct StatsBySlabs_t {
   const char *typeID;
   size_t slots;
   size_t bytes;
   StatsBySlabs_t() : typeID(nullptr), slots(0), bytes(0) {}
   void dump(ceph::Formatter *f) const;
};

struct StatsByTypeID_t {
   size_t slots;
   size_t slabs;
   size_t bytes;
   StatsByTypeID_t() : slots(0), slabs(0), bytes(0) {}
   void dump(ceph::Formatter *f) const;
};

void FormatStatsByBytes(const std::multimap<size_t,StatsByBytes_t>&m, ceph::Formatter *f);
void FormatStatsBySlots(const std::multimap<size_t,StatsBySlots_t>&m, ceph::Formatter *f);
void FormatStatsBySlabs(const std::multimap<size_t,StatsBySlabs_t>&m, ceph::Formatter *f);
void FormatStatsByTypeID(const std::map<const char *,StatsByTypeID_t>&m, ceph::Formatter *f);

void DumpStatsByBytes(const std::string& prefix,ceph::Formatter *f,size_t trim = 50);
void DumpStatsBySlots(const std::string& prefix,ceph::Formatter *f,size_t trim = 50);
void DumpStatsBySlabs(const std::string& prefix,ceph::Formatter *f,size_t trim = 50);
void DumpStatsByTypeID(const std::string& prefix,ceph::Formatter *f,size_t trim = 50);

//
// Root of all allocators, this enables the container information to operation easily
//
// These fields are "always" accurate ;-)
//
struct slab_allocator_base_t {
   list_member_t list_member;
   pool_t *pool;
   shard_t *shard;
   const char *typeID;
   size_t slots;
   size_t slabs;
   size_t bytes;
   slab_allocator_base_t() : pool(nullptr), shard(nullptr), typeID(nullptr), slots(0), slabs(0), bytes(0) {}
   //
   // Helper functions for Stats
   //
   void UpdateStats(std::multimap<size_t,StatsByBytes_t>& byBytes) const;
   void UpdateStats(std::multimap<size_t,StatsBySlots_t>& bySlots) const;
   void UpdateStats(std::multimap<size_t,StatsBySlabs_t>& bySlabs) const;
   void UpdateStats(std::map<const char *,StatsByTypeID_t>& byTypeID) const;
   //
   // Effective constructor
   //
   void AttachPool(pool_index_t index,const char *typeID);
   ~slab_allocator_base_t();
};

enum { shard_size = 64 }; // Sharding of headers

pool_t& GetPool(pool_index_t ix);

class pool_t {
   static std::map<std::string,pool_t *> *pool_head;
   static std::mutex pool_head_lock;
   std::string name;
   shard_t shard[shard_size];
   bool debug;
   friend class slab_allocator_base_t;
public:
   //
   // How much this pool consumes. O(<shard-size>)
   //
   size_t allocated_bytes() const;
   //
   // Aggregate stats by consumed.
   //
   static void StatsByBytes(const std::string& prefix,std::multimap<size_t,StatsByBytes_t>& bybytes,size_t trim);
   static void StatsBySlots(const std::string& prefix,std::multimap<size_t,StatsBySlots_t>& bySlots,size_t trim);
   static void StatsBySlabs(const std::string& prefix,std::multimap<size_t,StatsBySlabs_t>& bySlabs,size_t trim);
   static void StatsByTypeID(const std::string& prefix,std::map<const char *,StatsByTypeID_t>& byTypeID,size_t trim);
   shard_t* pick_a_shard() {
      size_t me = (size_t)pthread_self(); // Dirt cheap, see: http://fossies.org/dox/glibc-2.24/pthread__self_8c_source.html
      size_t i = (me >> 3) % shard_size;
      return &shard[i];
   }
public:
   pool_t(const std::string& n, bool _debug) : name(n), debug(_debug) {
      std::unique_lock<std::mutex> lock(pool_head_lock);
      if (pool_head == nullptr) {
         pool_head = new std::map<std::string,pool_t *>;
      }
      assert(pool_head->find(name) == pool_head->end());
      (*pool_head)[name] = this;
   }
   virtual ~pool_t() {
      std::unique_lock<std::mutex> lock(pool_head_lock);
      assert(pool_head->find(name) != pool_head->end());
      pool_head->erase(pool_head->find(name));
      if (pool_head->size() == 0) {
         delete pool_head;
         pool_head = nullptr;
      }      
   }
   //
   // Tracking of container ctor/dtor
   //
   void AttachAllocator(slab_allocator_base_t *base);
   void DetachAllocator(slab_allocator_base_t *base);
private:
   //
   // Helpers for per-pool stats
   //
   template<typename maptype> void VisitPool(maptype& map,size_t trim) const {
      for (size_t i = 0; i < shard_size; ++i) {
         std::unique_lock<std::mutex> shard_lock(shard[i].lock);
         for (const list_member_t *p = shard[i].containers.next;
              p != &shard[i].containers;
              p = p->next) {
            const slab_allocator_base_t *c = reinterpret_cast<const slab_allocator_base_t *>(p);
            c->UpdateStats(map);
            while (map.size() > trim) {
               map.erase(map.begin());
            }
         }
      }
   }
   template<typename maptype> static void VisitAllPools(
      const std::string& prefix,
      maptype& map,
      size_t trim) {
      //
      // Scan all of the pools for prefix match
      //
      for (size_t i = 0; i < num_pools; ++i) {
        const pool_t &pool = mempool::GetPool((pool_index_t)i);
        if (prefix == pool.name.substr(0,std::min(prefix.size(),pool.name.size()))) {
           pool.VisitPool(map,trim);
        }
      }
   }
};

inline void slab_allocator_base_t::AttachPool(pool_index_t index,const char *_typeID) {
   assert(pool == nullptr);
   pool = &GetPool(index);
   shard = pool->pick_a_shard();
   typeID = _typeID;
   if (pool->debug) {
      std::unique_lock<std::mutex> lock(shard->lock);
      shard->containers.insert(&list_member);
   }
}

inline slab_allocator_base_t::~slab_allocator_base_t() {
   if (pool && pool->debug) {
      std::unique_lock<std::mutex> lock(shard->lock);
      list_member.remove();
   }
}

//
// The ceph::slab_xxxx containers are made from standard STL containers with a custom allocator.
//
// When you declare a slab container you provide 1 or 2 additional integer template parameters that
// modify the memory allocation pattern. The point is to amortize the memory allocations for Slots
// within the container so that the memory allocation time and space overheads are reduced.
//
//  ceph::slab_map     <key,value,stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::slab_multimap<key,value,stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::slab_set     <value,    stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::slab_multiset<value,    stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::list         <value,    stackSize,heapSize = stackSize>
//   
//  stackSize indicates the number of Slots that will be allocated within the container itself.
//      in other words, if the container never has more than stackSize Slots, there will be no additional
//      memory allocation calls.
//
//  heapSize indicates the number of Slots that will be requested when a memory allocation is required.
//      In other words, Slots are allocated in batches of heapSize.
//
//  All of this wizardry comes with a price. There are two basic restrictions:
//  (1) Slots allocated in a batch can only be freed in the same batch amount
//  (2) Slots cannot escape a container, i.e., be transferred to another container
//
//  The first restriction suggests that long-lived containers might not want to use this code. As some allocation/free
//      patterns can result in large amounts of unused, but un-freed memory (worst case 'excess' memory occurs when
//      each batch contains only a single in-use node). Worst-case unused memory consumption is thus equal to:
//         container.size() * (heapSize -1) * sizeof(Node)
//      This computation assumes that the slab_xxxx::reserve function is NOT used. If that function is used then
//      the maximum unused memory consumption is related to its parameters.
//  The second restriction means that some functions like list::splice are now O(N), not O(1)
//      list::swap is supported but is O(2N) not O(1) as before.
//      vector::swap is also supported. It converts any stack elements into heap elements and then does an O(1) swap. So
//          it's worst-case runtime is O(2*stackSize), which is likely to be pretty good :)
//      set::swap, multiset::swap, map::swap and multimap::swap are unavailable, but could be implemented if needed (though EXPENSIVELY).
//

//
// fast slab allocator
//
// This is an STL allocator intended for use with short-lived node-heavy containers, i.e., map, set, etc.
//
// Memory is allocated in slabs. Each slab contains a fixed number of slots for objects.
//
// The first slab is part of the object itself, meaning that no memory allocation is required
// if the container doesn't exceed "stackSize" Slots.
//
// Subsequent slabs are allocated using the normal heap. A slab on the heap, by default, contains 'heapSize' Slots.
// However, a "reserve" function (same functionality as vector::reserve) is provided that ensure a minimum number
// of free Slots is available without further memory allocation. If the slab_xxxx::reserve function needs to allocate
// additional Slots, only a single memory allocation will be done. Meaning that it's possible to have slabs that
// are larger (or smaller) than 'heapSize' Slots.
//

template<typename T,size_t stackSize, size_t heapSize>
class slab_allocator : public slab_allocator_base_t {
   struct slab_t;
   struct slabHead_t {
      slabHead_t *prev;
      slabHead_t *next;
   };
   //
   // Each slot has a pointer to it's containing slab PLUS either one Object OR if it's free exactly one pointer 
   // in a per-slab freelist.
   // Since this is raw memory, we don't want to declare something of type "T" to avoid
   // accidental constructor/destructor calls.
   //
   struct slot_t {
      slab_t *slab; // Pointer to my slab, NULL for slots on the stack (Technically, this could be an index but I'm lazy :))
      enum { OBJECT_IN_POINTERS = (sizeof(T) + sizeof(void *) - 1) / sizeof(void *) };
      slot_t *storage[OBJECT_IN_POINTERS]; // Either one object OR index[0] points to next element in free list for this slab
   };
   //
   // Each slab has a freelist of objects within the slab and the size of that list
   // The size is needed to cheaply determine when all of the slots within a slab are unused
   // so that we can free the slab itself    
   //
   struct slab_t {
      slabHead_t slabHead;  // membership of slab in list of slabs with free elements
      uint32_t slabSize;    // # of allocated slots in this slab
      uint32_t freeSlots;   // # of free slots, i.e., size of freelist of slots within this slab
      slot_t *freeHead;     // Head of list of freeslots OR NULL if none
      slot_t slot[0];       // slots
   };
   slab_t *slabHeadToSlab(slabHead_t *head) { return reinterpret_cast<slab_t *>(head); }
   //
   // Exactly one of these as part of this container (the slab on the stack :))
   //
   struct stackSlab_t : public slab_t {
      slot_t stackSlot[stackSize]; // Allows the compiler to get the right size :)
   };
   //
   // Initialize a new slab, remember, "T" might not be correct use the stored sizes.
   //
   void initSlab(slab_t *slab,size_t sz) {
      slab->slabSize = sz;
      slab->freeSlots = 0; // Pretend that it was completely allocated before :)
      slab->freeHead = NULL;
      slab->slabHead.next = NULL;
      slab->slabHead.prev = NULL;
      char *raw = reinterpret_cast<char *>(slab->slot);
      for (size_t i = 0; i < sz; ++i) {
         slot_t *slot = reinterpret_cast<slot_t *>(raw);
         slot->slab = slab;
         ++slots; // decremented by freeslot
         freeslot(slot,false);
         raw += trueSlotSize;
      }
   }
   //
   // Free a slot, the "freeEmpty" parameter indicates if this slab should be freed if it's emptied
   // 
   void freeslot(slot_t *s, bool freeEmpty) {
      slab_t *slab = s->slab;
      //
      // Put this slot onto the per-slab freelist
      //
      s->storage[0] = slab->freeHead;
      slab->freeHead = s;
      slab->freeSlots++;
      ++freeSlotCount;
      assert(slots > 0);
      --slots;
      if (slab->freeSlots == 1) {
         //
         // put slab onto the container's slab freelist
         //
         slab->slabHead.next = freeSlabHeads.next;
         freeSlabHeads.next->prev = &slab->slabHead;
         freeSlabHeads.next = &slab->slabHead;
         slab->slabHead.prev = &freeSlabHeads;
      }         
      if (freeEmpty && slab->freeSlots == slab->slabSize && slab != &stackSlab) {
         //
         // Slab is entirely free
         //
         slab->slabHead.next->prev = slab->slabHead.prev;
         slab->slabHead.prev->next = slab->slabHead.next;
         assert(freeSlotCount >= slab->slabSize);
         freeSlotCount -= slab->slabSize;
         assert(slabs > 0);
         slabs--;
         size_t sz = sizeof(slab_t) + (trueSlotSize * slab->slabSize);
         assert(bytes >= sz);
         bytes -= sz;
         shard->allocated -= sz;
         ::free(slab);
      }
   }
   //
   // Danger, because of the my_actual_allocator hack. You can't rely on T to be correct, nor any value or offset that's
   // derived directly or indirectly from T. We use the values saved during initialization, when T was correct.
   //
   void addSlab(size_t slabSize) {
       //
       // I need to compute the size of this structure
       //
       //   struct .... {
       //        heapSlab_t *next;
       //        T          slots[slabSize];
       //   };
       //
       // However, here sizeof(T) isn't correct [see warning above, 'T' might not be correct]
       // so I use the sizeof(T) that's actually correct: 'trueSlotSize'
       //
       size_t sz = sizeof(slab_t) + (slabSize * trueSlotSize);
       //
       // Allocate the slab and free the slots within.
       //
       slab_t *slab = reinterpret_cast<slab_t *>(::malloc(sz));
       shard->allocated += sz;
       bytes += sz;
       slabs ++;
       initSlab(slab,slabSize);
   }
   
   slot_t *allocslot() {
      if (freeSlabHeads.next == &freeSlabHeads) {
         addSlab(heapSize);
      }
      slab_t *freeSlab = slabHeadToSlab(freeSlabHeads.next);
      slot_t *freeSlot = freeSlab->freeHead;
      freeSlab->freeHead = freeSlot->storage[0];
      assert(freeSlab->freeSlots > 0);
      freeSlab->freeSlots--;
      if (freeSlab->freeSlots == 0) {
         //
         // remove slab from list
         //
         assert(freeSlab->freeHead == nullptr);
         freeSlabHeads.next = freeSlab->slabHead.next;
         freeSlab->slabHead.next->prev = &freeSlabHeads;
         freeSlab->slabHead.next = nullptr;
         freeSlab->slabHead.prev = nullptr;
      }
      --freeSlotCount;
      ++slots;
      return freeSlot;
   }

   void _reserve(size_t freeCount) {
      if (freeSlotCount < freeCount) {
         addSlab(freeCount - freeSlotCount);
      }      
   }

   slab_allocator *selfPointer;                 // for selfCheck
   slabHead_t freeSlabHeads;	                // List of slabs that have free slots
   size_t freeSlotCount;                        // # of slots currently in the freelist * Only used for debug integrity check *
   size_t trueSlotSize;	                        // Actual Slot Size


   // Must always be last item declared, because of get_my_allocator hack which won't have the right types, hence it'll get the stack wrong....
   stackSlab_t stackSlab; 			// stackSlab is always allocated with the object :)
  
public:
   typedef slab_allocator<T,stackSize,heapSize> allocator_type;
   typedef T value_type;
   typedef value_type *pointer;
   typedef const value_type * const_pointer;
   typedef value_type& reference;
   typedef const value_type& const_reference;
   typedef std::size_t size_type;
   typedef std::ptrdiff_t difference_type;

   template<typename U> struct rebind { typedef slab_allocator<U,stackSize,heapSize> other; };

   slab_allocator() : freeSlotCount(0), trueSlotSize(sizeof(slot_t)) {
      //
      // For the "in the stack" slots, put them on the free list
      //
      freeSlabHeads.next = &freeSlabHeads;
      freeSlabHeads.prev = &freeSlabHeads;
      initSlab(&stackSlab,stackSize);
      selfPointer = this;
      typeID = typeid(*this).name();
   }
   ~slab_allocator() {
      //
      // If you fail here, it's because you've allowed a node to escape the enclosing object. Something like a swap
      // or a splice operation. Probably the slab_xxx container is missing a "using" that serves to hide some operation.
      //
      assert(freeSlotCount == stackSize);
      assert(freeSlabHeads.next == &stackSlab.slabHead); // Empty list should have stack slab on it
   }

   pointer allocate(size_t cnt,void *p = nullptr) {
      assert(cnt == 1); // if you fail this you've used this class with the wrong STL container.
      assert(sizeof(slot_t) == trueSlotSize);
      return reinterpret_cast<pointer>(sizeof(void *) + (char *)allocslot());
   }

   void deallocate(pointer p, size_type s) {
      freeslot(reinterpret_cast<slot_t *>((char *)p - sizeof(void *)),true);
   }

   void destroy(pointer p) {
      p->~T();
   }

   template<class U> void destroy(U *p) {
      p->~U();
   }

   void construct(pointer p,const_reference val) {
      ::new ((void *)p) T(val);
   }

   template<class U, class... Args> void construct(U* p,Args&&... args) {
      ::new((void *)p) U(std::forward<Args>(args)...);
   }

   bool operator==(const slab_allocator&) { return true; }
   bool operator!=(const slab_allocator&) { return false; }

   //
   // Extra function for our use
   //
   void reserve(size_t freeCount) {
      _reserve(freeCount);
   }

   void selfCheck() const {
      assert(this == selfPointer); // If you fail here, the horrible get_my_allocator hack is failing. 
   }

private:

   // Can't copy or assign this guy
   slab_allocator(slab_allocator&) = delete;
   slab_allocator(slab_allocator&&) = delete;
   void operator=(const slab_allocator&) = delete;
   void operator=(const slab_allocator&&) = delete;
};

//
// Extended containers
//
template<
   pool_index_t pool_ix,
   typename key,
   typename value,
   size_t   stackCount, 
   size_t   heapCount, 
   typename compare = std::less<key> >
   struct map : 
      public std::map<key,value,compare,slab_allocator<std::pair<key,value>,stackCount,heapCount> > {
   map() {
      get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name());
   }
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::map<key,value,compare,slab_allocator<std::pair<key,value>,stackCount,heapCount>> map_type;
   //
   // Disallowed operations
   //
   using map_type::swap;

   //
   // Unfortunately, the get_allocator operation returns a COPY of the allocator, not a reference :( :( :( :(
   // We need the actual underlying object. This terrible hack accomplishes that because the STL library on
   // all of the platforms we care about actually instantiate the allocator right at the start of the object :)
   // we do have a check for this :)
   //
   // It's also the case that the instantiation type of the underlying allocator won't match the type of the allocator
   // That's here (that's because the container instantiates the node type itself, i.e., with container-specific
   // additional members.
   // But that doesn't matter for this hack...
   //
   typedef slab_allocator<std::pair<key,value>,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

template<
   pool_index_t pool_ix,
   typename key,
   typename value,
   size_t   stackCount, 
   size_t   heapCount, 
   typename compare = std::less<key> >
   struct multimap : public std::multimap<key,value,compare,slab_allocator<std::pair<key,value>,stackCount,heapCount> > {
   multimap() {
      get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name());
   }
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::multimap<key,value,compare,slab_allocator<std::pair<key,value>,stackCount,heapCount>> map_type;
   //
   // Disallowed operations
   //
   using map_type::swap;
   //
   // Unfortunately, the get_allocator operation returns a COPY of the allocator, not a reference :( :( :( :(
   // We need the actual underlying object. This terrible hack accomplishes that because the STL library on
   // all of the platforms we care about actually instantiate the allocator right at the start of the object :)
   // we do have a check for this :)
   //
   // It's also the case that the instantiation type of the underlying allocator won't match the type of the allocator
   // That's here (that's because the container instantiates the node type itself, i.e., with container-specific
   // additional members.
   // But that doesn't matter for this hack...
   //
   typedef slab_allocator<std::pair<key,value>,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

template<
   pool_index_t pool_ix,
   typename key,
   size_t   stackCount, 
   size_t   heapCount, 
   typename compare = std::less<key> >
   struct set : public std::set<key,compare,slab_allocator<key,stackCount,heapCount> > {
   set() {
      get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name());
   }
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::set<key,compare,slab_allocator<key,stackCount,heapCount>> set_type;
   //
   // Disallowed operations
   //
   using set_type::swap;
   //
   // Unfortunately, the get_allocator operation returns a COPY of the allocator, not a reference :( :( :( :(
   // We need the actual underlying object. This terrible hack accomplishes that because the STL library on
   // all of the platforms we care about actually instantiate the allocator right at the start of the object :)
   // we do have a check for this :)
   //
   // It's also the case that the instantiation type of the underlying allocator won't match the type of the allocator
   // That's here (that's because the container instantiates the node type itself, i.e., with container-specific
   // additional members.
   // But that doesn't matter for this hack...
   //
   typedef slab_allocator<key,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

template<
   pool_index_t pool_ix,
   typename key,
   size_t   stackCount, 
   size_t   heapCount, 
   typename compare = std::less<key> >
   struct multiset : public std::multiset<key,compare,slab_allocator<key,stackCount,heapCount> > {
   multiset() {
      get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name());
   }
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::multiset<key,compare,slab_allocator<key,stackCount,heapCount>> set_type;
   //
   // Disallowed operations
   //
   using set_type::swap;
   //
   // Unfortunately, the get_allocator operation returns a COPY of the allocator, not a reference :( :( :( :(
   // We need the actual underlying object. This terrible hack accomplishes that because the STL library on
   // all of the platforms we care about actually instantiate the allocator right at the start of the object :)
   // we do have a check for this :)
   //
   // It's also the case that the instantiation type of the underlying allocator won't match the type of the allocator
   // That's here (that's because the container instantiates the node type itself, i.e., with container-specific
   // additional members.
   // But that doesn't matter for this hack...
   //
   typedef slab_allocator<key,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

template<
   pool_index_t pool_ix,
   typename node,
   size_t   stackCount, 
   size_t   heapCount >
   struct list : public std::list<node,slab_allocator<node,stackCount,heapCount> > {

   //
   // copy and assignment
   //
   list() { get_my_actual_allocator()->AttachPool(pool_ix, typeid(*this).name()); }
   list(const list& o) {  get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name()); copy(o); }; // copy
   list& operator=(const list& o) { copy(o); return *this; }

   typedef typename std::list<node,slab_allocator<node,stackCount,heapCount>>::iterator it;
   //
   // We support splice, but it requires actually copying each node, so it's O(N) not O(1)
   //
   void splice(it pos, list& other)        { this->splice(pos, other, other.begin(), other.end()); }
   void splice(it pos, list& other, it it) { this->splice(pos, other, it, it == other.end() ? it : std::next(it)); }
   void splice(it pos, list& other, it first, it last) {
      while (first != last) {
         pos = std::next(this->insert(pos,*first)); // points after insertion of this element
         first = other.erase(first);
      }
   }
   //
   // Swap is supported, but it's O(2N)
   //
   void swap(list& o) {
      it ofirst = o.begin();
      it olast  = o.end();
      it mfirst = this->begin();
      it mlast  = this->end();
      //
      // copy and erase Slots from other to end of my list
      //
      while (ofirst != olast) {
         this->push_back(std::move(*ofirst));
         ofirst = o.erase(ofirst);
      }
      //
      // Copy original Slots of my list to other container
      //
      while (mfirst != mlast) {
         o.push_back(std::move(*mfirst));
         mfirst = this->erase(mfirst);
      }
   }
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::list<node,slab_allocator<node,stackCount,heapCount>> list_type;

   void copy(const list& o) {
      this->clear();
      for (auto& e : o) {
         this->push_back(e);
      }
   }
   //
   // Disallowed operations
   //
   // Unfortunately, the get_allocator operation returns a COPY of the allocator, not a reference :( :( :( :(
   // We need the actual underlying object. This terrible hack accomplishes that because the STL library on
   // all of the platforms we care about actually instantiate the allocator right at the start of the object :)
   // we do have a cheap run-time check for this, in case you're platform doesn't match the same layout :)
   //
   // It's also the case that the instantiation type of the underlying allocator won't match the type of the allocator
   // That's here (that's because the container instantiates the node type itself, i.e., with container-specific
   // additional members.
   // But that doesn't matter for this hack...
   //
   typedef slab_allocator<node,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

//
// Special allocator for vector
//
//  Unlike the more sophisticated allocator above, we always have the right type, so we can save a lot of machinery
//
template<typename T,size_t stackSize>
class slab_vector_allocator : public slab_allocator_base_t {
   const slab_vector_allocator *check;
   T stackSlot[stackSize]; 			// stackSlab is always allocated with the object :)
  
public:
   typedef slab_vector_allocator<T,stackSize> allocator_type;
   typedef T value_type;
   typedef value_type *pointer;
   typedef const value_type * const_pointer;
   typedef value_type& reference;
   typedef const value_type& const_reference;
   typedef std::size_t size_type;
   typedef std::ptrdiff_t difference_type;

   template<typename U> struct rebind { typedef slab_vector_allocator<U,stackSize> other; };

   slab_vector_allocator() : check(this) {
   }
   ~slab_vector_allocator() {
   }
   pointer allocate(size_t cnt,void *p = nullptr) {
      if (cnt <= stackSize) return stackSlot;
      //
      // Need a new slab, be sure to track the size of this slab which we put into the allocated chunk.
      slots += cnt;
      slabs++;
      size_t to_allocate = (cnt * sizeof(T)) + sizeof(size_t);
      bytes += to_allocate;
      shard->allocated += to_allocate;
      size_t *chunk = static_cast<size_t *>(::malloc(to_allocate));
      *chunk = cnt;
      return reinterpret_cast<pointer>(chunk + 1);
   }

   void deallocate(pointer p, size_type s) {
      if (p != stackSlot) {
         size_t *to_free = reinterpret_cast<size_t *>(p);
         size_t cnt = to_free[-1];
         ::free(to_free-1);
         size_t to_deallocate = (cnt * sizeof(T)) + sizeof(size_t);
         assert(bytes >= to_deallocate);
         shard->allocated -= to_deallocate;
         bytes -= to_deallocate;
         slabs--;
         slots -= cnt;
      }
   }

   void destroy(pointer p) {
      p->~T();
   }

   template<class U> void destroy(U *p) {
      p->~U();
   }

   void construct(pointer p,const_reference val) {
      ::new ((void *)p) T(val);
   }

   template<class U, class... Args> void construct(U* p,Args&&... args) {
      ::new((void *)p) U(std::forward<Args>(args)...);
   }

   bool operator==(const slab_vector_allocator&) { return true; }
   bool operator!=(const slab_vector_allocator&) { return false; }

   void selfCheck() {
      assert(this == check);
   }

   void swap(slab_vector_allocator& rhs) {
      //
      // Helper for vector::swap, gets the accounting right :)
      // We cheat because we know that their is only a single outstanding slab for each of the participants
      //
      assert(slabs == 1);
      assert(rhs.slabs == 1);
      std::swap(slots,rhs.slots);
      ssize_t delta = bytes - rhs.bytes; // difference between bytes, need to adjust shard allocations
      shard->allocated -= delta;
      rhs.shard->allocated += delta;
      std::swap(bytes,rhs.bytes);
   }

private:

   // Can't copy or assign this guy
   slab_vector_allocator(slab_vector_allocator&) = delete;
   slab_vector_allocator(slab_vector_allocator&&) = delete;
   void operator=(const slab_vector_allocator&) = delete;
   void operator=(const slab_vector_allocator&&) = delete;
};

//
// Vector. We rely on having an initial "reserve" call that ensures we wire-in the in-stack memory allocations
//
template<pool_index_t pool_index, typename value,size_t   stackCount >
   class vector : public std::vector<value,slab_vector_allocator<value,stackCount> > {
   typedef std::vector<value,slab_vector_allocator<value,stackCount>> vector_type;
public:

   vector() {
      get_my_actual_allocator()->AttachPool(pool_index,typeid(*this).name());
      this->reserve(stackCount);
   }

   vector(size_t initSize,const value& val = value()) {
      this->reserve(std::max(initSize,stackCount));
      for (size_t i = 0; i < initSize; ++i) this->push_back(val);
   }

   vector(const std::vector<value>& rhs) {
      get_my_actual_allocator()->AttachPool(pool_index,typeid(*this).name());
      this->reserve(stackCount);
      *this = rhs;
   }

   vector& operator=(const std::vector<value>& rhs) {
      this->reserve(rhs.size());
      this->clear();
      for (auto& i : rhs) {
         this->push_back(i);
      }
      return *this;
   }
   //
   // sadly, this previously O(1) operation now becomes O(N) for N less than stackSize. Otherwise it's still O(1) :)
   //
   void swap(vector& rhs) {
      //
      // Lots of ways to optimize this, but we'll just do something simple....
      //
      // Use reserve to force the underlying code to malloc.
      //
      this->reserve(stackCount + 1);
      rhs.reserve(stackCount + 1);
      this->vector_type::swap(rhs);
      get_my_actual_allocator()->swap(*rhs.get_my_actual_allocator());
   }
   

private:
   //
   // Unfortunately, the get_allocator operation returns a COPY of the allocator, not a reference :( :( :( :(
   // We need the actual underlying object. This terrible hack accomplishes that because the STL library on
   // all of the platforms we care about actually instantiate the allocator right at the start of the object :)
   // we do have a check for this :)
   //
   // It's also the case that the instantiation type of the underlying allocator won't match the type of the allocator
   // That's here (that's because the container instantiates the node type itself, i.e., with container-specific
   // additional members.
   // But that doesn't matter for this hack...
   //
   typedef slab_vector_allocator<value,stackCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }

};

}; // Namespace mempool

//
// Simple function to compute the size of a heapSlab, in the absence of a default.
//

enum { _desired_slab_size = 256 }; // approximate preferred allocation size

inline constexpr size_t defaultSlabHeapCount(size_t Slotsize,size_t overheads) {
   return (_desired_slab_size / (Slotsize + (overheads * sizeof(void *)))) ?
          (_desired_slab_size / (Slotsize + (overheads * sizeof(void *)))) : 
          size_t(1); // can't uses std::max, it's not constexpr
}


#define P(x) \
namespace x { \
  template<typename k,typename v, int stackSize, int heapSize = defaultSlabHeapCount(sizeof(k) + sizeof(v),3), typename cmp = std::less<k> > \
      using map = mempool::map<mempool::x,k,v,stackSize,heapSize,cmp>; \
  template<typename k,typename v, int stackSize, int heapSize = defaultSlabHeapCount(sizeof(k) + sizeof(v),3), typename cmp = std::less<k> > \
      using multimap = mempool::multimap<mempool::x,k,v,stackSize,heapSize,cmp>; \
  template<typename k, int stackSize, int heapSize = defaultSlabHeapCount(sizeof(k),2), typename cmp = std::less<k> > \
      using set = mempool::set<mempool::x,k,stackSize,heapSize,cmp>; \
  template<typename v, int stackSize, int heapSize  = defaultSlabHeapCount(sizeof(v),2) > \
      using list = mempool::list<mempool::x,v,stackSize,heapSize>; \
  template<typename v, int stackSize = defaultSlabHeapCount(sizeof(v),0) > \
      using vector = mempool::vector<mempool::x,v,stackSize>; \
  inline size_t allocated_bytes() { \
      return mempool::GetPool(mempool::x).allocated_bytes(); \
  } \
};

DEFINE_MEMORY_POOLS_HELPER(P)

#undef P

#endif // _slab_CONTAINERS_H

