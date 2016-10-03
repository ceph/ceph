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
/**********************
*/

namespace mempool {

struct slab_allocator_base;
class pool_t;
class container_t;

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
   std::atomic<ssize_t> allocated;
   mutable std::mutex lock;  // Only used for containers list
   list_member_t containers;
};

//
// Stats structures
//
struct StatsByBytes_t {
   const char* typeID;
   size_t slots;
   size_t slabs;
   StatsByBytes_t() : typeID(nullptr), slots(0), slabs(0) {}
};
struct StatsBySlots_t {
   const char *typeID;
   size_t slabs;
   size_t bytes;
   StatsBySlots_t() : typeID(nullptr), slabs(0), bytes(0) {} 
};
struct StatsBySlabs_t {
   const char *typeID;
   size_t slots;
   size_t bytes;
   StatsBySlabs_t() : typeID(nullptr), slots(0), bytes(0) {}
};

struct StatsByTypeID_t {
   size_t slots;
   size_t slabs;
   size_t bytes;
   StatsByTypeID_t() : slots(0), slabs(0), bytes(0) {}
};

void UpdateStats(std::multimap<size_t,StatsByBytes_t>& map,const slab_allocator_base& b);
void UpdateStats(std::multimap<size_t,StatsBySlots_t>& map,const slab_allocator_base& b);
void UpdateStats(std::multimap<size_t,StatsBySlabs_t>& map,const slab_allocator_base& b);
void UpdateStats(std::map<const char *,StatsByTypeID_t>& map,const slab_allocator_base& b);


class container_t 
{
   list_member_t list_member;
   const slab_allocator_base &base;
   pool_t &pool;
   shard_t *shard;
public:
   container_t(pool_t &p,const slab_allocator_base &b);
   ~container_t();

protected:
   //
   // Helper functions for Stats
   //
   void VisitByBytes(std::multimap<size_t,StatsByBytes_t>& byBytes) const;
   void VisitBySlots(std::multimap<size_t,StatsBySlots_t>& bySlots) const;
   void VisitBySlabs(std::multimap<size_t,StatsBySlabs_t>& bySlabs) const;
   void VisitByTypeID(std::map<const char *,StatsByTypeID_t>& byTypeID ) const;
   //
   // Helper functions for allocators
   //
   void *do_malloc(size_t bytes) {
      shard->allocated += bytes;
      return ::malloc(bytes);
   }
   void do_free(void *slab,size_t bytes) {
     shard->allocated -= bytes;
     ::free(slab);
   }

   friend class pool_t;
};

enum { shard_size = 64 }; // Sharding of headers

class pool_t {
   static std::map<std::string,pool_t *> *pool_head;
   static std::mutex pool_head_lock;
public:
private:
   std::string name;
   shard_t shard[shard_size];
   bool debug;
public:
   //
   // How much this pool consumes. O(<shard-size>)
   //
   size_t allocated_bytes() const;

   //
   // Aggregate stats by consumed.
   //
   static void StatsByBytes(const std::string& prefix,std::multimap<size_t,StatsByBytes_t>& bybytes,size_t trim = INT_MAX);
   static void StatsBySlots(const std::string& prefix,std::multimap<size_t,StatsBySlots_t>& bySlots,size_t trim = INT_MAX);
   static void StatsBySlabs(const std::string& prefix,std::multimap<size_t,StatsBySlabs_t>& bySlabs,size_t trim = INT_MAX);
   static void StatsByTypeID(const std::string& prefix,std::map<const char *,StatsByTypeID_t>& byTypeID,size_t trim = INT_MAX);
protected:
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
   void ctorContainer(container_t *container) {
      container->shard = pick_a_shard();
      if (debug) {
         std::unique_lock<std::mutex> lock(container->shard->lock);
         container->shard->containers.insert(&container->list_member);
      }
   }
   void dtorContainer(container_t *container) {
      if (debug) {
         std::unique_lock<std::mutex> lock(container->shard->lock);
         container->list_member.remove();
      }
   }
private:
   //
   // Helpers for per-pool stats
   //
   template<typename maptype> void VisitPool(void (container_t::*f)(maptype& map) const,maptype& map,size_t trim) const {
      for (size_t i = 0; i < shard_size; ++i) {
         std::unique_lock<std::mutex> shard_lock(shard[i].lock);
         for (const list_member_t *p = shard[i].containers.next;
              p != &shard[i].containers;
              p = p->next) {
            const container_t *c = reinterpret_cast<const container_t *>(p);
            UpdateStats(map,c->base);
            while (map.size() > trim) {
               map.erase(map.begin());
            }
         }
      }
   }
   template<typename maptype> static void VisitAllPools(
      const std::string& prefix,
      maptype& map,
      void (container_t::*f)(maptype&) const,
      size_t trim) {
      //
      // Scan all of the pools for prefix match
      //
      std::unique_lock<std::mutex> global_lock(pool_head_lock);
      for (auto& p : *pool_head) {
        const pool_t *pool = p.second;
        if (pool && prefix == pool->name.substr(0,std::min(prefix.size(),pool->name.size()))) {
           pool->VisitPool(f,map,trim);
        }
      }
   }
};

//
// Finish definition of container_t
//
inline container_t::container_t(pool_t &p,const slab_allocator_base &b) : base(b), pool(p) {
   pool.ctorContainer(this);
}

inline container_t::~container_t() {
   pool.dtorContainer(this);
}

#define DEFINE_MEMORY_POOLS_HELPER(f) \
   f(unittest_1) \
   f(uniitest_2)   


#define P(x) x,
enum pool_index_t {
   DEFINE_MEMORY_POOLS_HELPER(P)
   num_pools        // Must be last.
};
#undef P

pool_t& GetPool(pool_index_t ix);

//
// Root of all allocators, this enables the container information to operation easily
//
// These fields are "always" accurate ;-)
//
struct slab_allocator_base {
   const char *typeID;
   size_t slots;
   size_t slabs;
   size_t bytes;
   slab_allocator_base() : typeID(nullptr), slots(0), slabs(0), bytes(0) {}
};

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
class slab_allocator : public slab_allocator_base {
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
// Simple function to compute the size of a heapSlab
//
//   we assume that we want heapSlabs to be about 1KBytes.
//

enum { _desired_slab_size = 128 }; // approximate preferred allocation size

inline constexpr size_t defaultSlabHeapCount(size_t Slotsize) {
   return (_desired_slab_size / Slotsize) ? (_desired_slab_size / Slotsize) : size_t(1); // can't uses std::max, it's not constexpr
}

//
// Extended containers
//
template<
   pool_index_t pool_ix,
   typename key,
   typename value,
   size_t   stackCount, 
   size_t   heapCount = defaultSlabHeapCount(sizeof(key) + sizeof(value)), 
   typename compare = std::less<key> >
   struct map : 
      public std::map<key,value,compare,slab_allocator<std::pair<key,value>,stackCount,heapCount> > {
   map() : container(GetPool(pool_ix),*get_my_actual_allocator()) {
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
   const my_alloc_type * get_my_actual_allocator() const {
      const my_alloc_type *alloc = reinterpret_cast<const my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
   container_t container;
};

template<
   typename key,
   typename value,
   size_t   stackCount, 
   size_t   heapCount = defaultSlabHeapCount(sizeof(key) + sizeof(value)), 
   typename compare = std::less<key> >
   struct slab_multimap : public std::multimap<key,value,compare,slab_allocator<std::pair<key,value>,stackCount,heapCount> > {
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
   typename key,
   size_t   stackCount, 
   size_t   heapCount = defaultSlabHeapCount(sizeof(key)), 
   typename compare = std::less<key> >
   struct slab_set : public std::set<key,compare,slab_allocator<key,stackCount,heapCount> > {
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
   typename key,
   size_t   stackCount, 
   size_t   heapCount = defaultSlabHeapCount(sizeof(key)), 
   typename compare = std::less<key> >
   struct slab_multiset : public std::multiset<key,compare,slab_allocator<key,stackCount,heapCount> > {
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
   typename node,
   size_t   stackCount, 
   size_t   heapCount = defaultSlabHeapCount(sizeof(node)) >
struct slab_list : public std::list<node,slab_allocator<node,stackCount,heapCount> > {

   //
   // copy and assignment
   //
   slab_list() {}
   slab_list(const slab_list& o) { copy(o); }; // copy
   slab_list& operator=(const slab_list& o) { copy(o); return *this; }

   typedef typename std::list<node,slab_allocator<node,stackCount,heapCount>>::iterator it;
   //
   // We support splice, but it requires actually copying each node, so it's O(N) not O(1)
   //
   void splice(it pos, slab_list& other)        { this->splice(pos, other, other.begin(), other.end()); }
   void splice(it pos, slab_list& other, it it) { this->splice(pos, other, it, it == other.end() ? it : std::next(it)); }
   void splice(it pos, slab_list& other, it first, it last) {
      while (first != last) {
         pos = std::next(this->insert(pos,*first)); // points after insertion of this element
         first = other.erase(first);
      }
   }
   //
   // Swap is supported, but it's O(2N)
   //
   void swap(slab_list& o) {
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

   void copy(const slab_list& o) {
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
class slab_vector_allocator {
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

   slab_vector_allocator() {
   }
   ~slab_vector_allocator() {
   }

   pointer allocate(size_t cnt,void *p = nullptr) {
      if (cnt <= stackSize) return stackSlot;
      return static_cast<pointer>(::malloc(cnt * sizeof(T)));
   }

   void deallocate(pointer p, size_type s) {
      if (p != stackSlot) ::free(p);
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
template<typename value,size_t   stackCount >
   class slab_vector : public std::vector<value,slab_vector_allocator<value,stackCount> > {
   typedef std::vector<value,slab_vector_allocator<value,stackCount>> vector_type;
public:

   slab_vector() {
      this->reserve(stackCount);
   }

   slab_vector(size_t initSize,const value& val = value()) {
      this->reserve(std::max(initSize,stackCount));
      for (size_t i = 0; i < initSize; ++i) this->push_back(val);
   }

   slab_vector(const slab_vector& rhs) {
      this->reserve(stackCount);
      *this = rhs;
   }

   slab_vector& operator=(const slab_vector& rhs) {
      this->reserve(rhs.size());
      this->clear();
      for (auto& i : rhs) {
         this->push_back(i);
      }
      return *this;
   }
   //
   // sadly, this previously O(1) operation now becomes O(N) for small N :)
   //
   void swap(slab_vector& rhs) {
      //
      // Lots of ways to optimize this, but we'll just do something simple....
      //
      // Use reserve to force the underlying code to malloc.
      //
      this->reserve(stackCount + 1);
      rhs.reserve(stackCount + 1);
      this->vector_type::swap(rhs);
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
#endif // _slab_CONTAINERS_H

