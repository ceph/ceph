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

#ifndef _SLAB_CONTAINERS_H
#define _SLAB_CONTAINERS_H

#include <include/mempool.h>

namespace mempool {

/*

The slab_xxxx containers are only available with a mempool. If you don't know what a mempool
is see mempool.h

The slab_xxxx containers are made from standard STL containers with a custom allocator.
The declaration for each slab container mimics the corresponding stl container
but has two additional parameters: stackSize and slabSize (which has a default).

The basic idea of slab_containers is that rather than malloc/free for each container node,
you malloc a 'slab' of nodes and hand those out one by one (per-container). In this
documentation we'll use the term 'slabSize' as a short hand for "number of nodes in a slab".

The ADVANTAGE of slab containers is that the run-time cost of the per-node malloc/free is
reduced by 'slabSize' (well technically amortized across slabSize number of STL alloc/frees).
Also, heap fragmentation is typically reduced. Especially for containers with lots of small 
modes.

The DISADVANTAGE of slab containers is that you malloc/free entire slabs. Thus slab
containers always consume more memory than their STL equivalents. The amount of extra
memory consumed is dependent on the access pattern (and container type). The worst-case
memory expansion is 'slabSize-1' times an STL container (approximately). However it's
likely to be very hard to create a worst-case memory pattern (unless you're trying ;-).
This also leads toward keeping the slabSizes modest -- unless you have special knowledge
of the access pattern OR the container itself is expected to be ephemeral.

Another disdvantage of slabs is that you can't move them from one container to another.
This causes certain operations that used to have O(1) cost to have O(N) cost.

   list::splice  O(N)
   list::swap    O(2N)
   vector::swap is O(2*stackSize)

Since I'm lazy, not all of the stl::xxxx.swap operations have been implemented. They
are pretty much discouraged for slab_containers anyways.

In order to reduce malloc/free traffic further, each container has within it one slab
that is part of the container itself (logically pre-allocated, but it's not a separate
malloc call -- its part of the container itself). The size of this slab is set through
the template parameter 'stackSize' which must be specified on every declaration (no default).
Thus the first 'stackSize' items in this slab container result in no malloc/free invokation
at the expense of burning that memory whether it's used or not. This is ideal for
continers that are normally one or two nodes but have the ability to grow on that rare
occasion when it's needed.

There are two ways to create more slabs: Normal usage or "reserve".
In the normal usage case, when no more free nodes are available, a slab is allocated
using the slabSize template parameter, which has a default value intended to try to
round up all allocations to something like 256 Bytes.

slab containers generally have a "reserve" function that emulates the vector::reserve
function in that it guarantees space is available for that many nodes. Reserve always
results in either 0 or 1 calls to malloc (0 for when the currently available space
is sufficient). If more nodes are required, they are allocated into a single slab
regardless of the value of the slabSize template parameter.

STATISTICS
----------

Normal mempools assume that all of the memory associated with a mempool is "allocated",
but slab_containers introduce another interesting state known as "free". Bytes and items
are in the "free" state when they are part of slab but not currently being used by
the client container. Thus 100% of a slab is considered "allocated" regardless of
whether any of the contained items are being used by the client container. Hence
there are several new per-pool statistics:

slabs          number of slabs in system (regardless of size)
free_items     number of items in slabs not being used by client container
free_bytes     number of bytes in slabs not begin used by client container
inuse_items    number of items in use (generally same as slab_xxxx.size())
inuse_bytes    number of bytes in use

Note, since each slab container has within itself one slab, that slab is added to the
statistics as soon as the container is ctor'ed.

See the unit test "documentation_test" in src/test/test_slab_containers.cc for
a step by step examination of how these statistics work.


*/

//
// fast slab allocator
//
// This is an STL allocator intended for use with node-heavy containers, i.e., map, set, etc.
//
// Memory is allocated in slabs. Each slab contains a fixed number of slots for objects.
//
// The first slab is part of the object itself, meaning that no memory allocation is required
// if the container doesn't exceed "stackSize" nodes.
//
// Subsequent slabs are allocated using the supplied allocator. A slab on the heap, by default, contains 'slabSize' nodes.
// However, a "reserve" function (same functionality as vector::reserve) is provided that ensure a minimum number
// of free nodes is available without further memory allocation. If the slab_xxxx::reserve function needs to allocate
// additional nodes, only a single memory allocation will be done. Meaning that it's possible to have slabs that
// are larger (or smaller) than 'slabSize' nodes.
//
template<pool_index_t pool_ix,typename T,size_t stackSize, size_t slabSize>
class slab_allocator : public pool_slab_allocator<pool_ix,T> {
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
      uint32_t size;    // # of allocated slots in this slab
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
      slab->size = sz;
      slab->freeSlots = 0; // Pretend that it was completely allocated before :)
      slab->freeHead = NULL;
      slab->slabHead.next = NULL;
      slab->slabHead.prev = NULL;
      this->slab_allocate(sz,trueSlotSize,sizeof(slab_t));
      char *raw = reinterpret_cast<char *>(slab->slot);
      for (size_t i = 0; i < sz; ++i) {
         slot_t *slot = reinterpret_cast<slot_t *>(raw);
         slot->slab = slab;
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
      this->slab_item_free(trueSlotSize);
      if (slab->freeSlots == 1) {
         //
         // put slab onto the contianer's slab freelist
         //
         slab->slabHead.next = freeSlabHeads.next;
         freeSlabHeads.next->prev = &slab->slabHead;
         freeSlabHeads.next = &slab->slabHead;
         slab->slabHead.prev = &freeSlabHeads;
      }         
      if (freeEmpty && slab->freeSlots == slab->size && slab != &stackSlab) {
         //
         // Slab is entirely free
         //
         slab->slabHead.next->prev = slab->slabHead.prev;
         slab->slabHead.prev->next = slab->slabHead.next;
         assert(freeSlotCount >= slab->size);
         freeSlotCount -= slab->size;
         this->slab_deallocate(slab->freeSlots,trueSlotSize,sizeof(slab_t),true);
         delete[] slab;
      }
   }
   //
   // Danger, because of the my_actual_allocator hack. You can't rely on T to be correct, nor any value or offset that's
   // derived directly or indirectly from T. We use the values saved during initialization, when T was correct.
   //
   void addSlab(size_t size) {
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
       //
       // Allocate the slab and free the slots within.
       //
       size_t total = (size * trueSlotSize) + sizeof(slab_t);
       slab_t *slab = reinterpret_cast<slab_t *>(new char[total]);
       initSlab(slab,size);
   }
   
   slot_t *allocslot() {
      if (freeSlabHeads.next == &freeSlabHeads) {
         addSlab(slabSize);
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
      this->slab_item_allocate(trueSlotSize);
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
   size_t allocSlotCount;                       // # of slabs allocated                 * Only used for debug integrity check *
   size_t trueSlotSize;	                        // Actual Slot Size

   // Must always be last item declared, because of get_my_allocator hack which won't have the right types, hence it'll get the stack wrong....
   stackSlab_t stackSlab; 			// stackSlab is always allocated with the object :)
  
public:
  template<typename U> struct rebind {
    typedef slab_allocator<pool_ix,U,stackSize,slabSize> other;
  };

  typedef slab_allocator<pool_ix,T,stackSize,slabSize> allocator_type;

   slab_allocator() : freeSlotCount(0), allocSlotCount(stackSize), trueSlotSize(sizeof(slot_t)) {
      //
      // For the "in the stack" slots, put them on the free list
      //
      freeSlabHeads.next = &freeSlabHeads;
      freeSlabHeads.prev = &freeSlabHeads;
      initSlab(&stackSlab,stackSize);
      selfPointer = this;
   }
   ~slab_allocator() {
      //
      // If you fail here, it's because you've allowed a node to escape the enclosing object. Something like a swap
      // or a splice operation. Probably the slab_xxx container is missing a "using" that serves to hide some operation.
      //
      assert(freeSlotCount == allocSlotCount);
      assert(freeSlotCount == stackSize);
      assert(freeSlabHeads.next == &stackSlab.slabHead); // Empty list should have stack slab on it
      this->slab_deallocate(stackSize,trueSlotSize,sizeof(slab_t),true);
   }

   T* allocate(size_t cnt,void *p = nullptr) {
      assert(cnt == 1); // if you fail this you've used this class with the wrong STL container.
      assert(sizeof(slot_t) == trueSlotSize);
      return reinterpret_cast<T *>(sizeof(void *) + (char *)allocslot());
   }

   void deallocate(T* p, size_t s) {
      freeslot(reinterpret_cast<slot_t *>((char *)p - sizeof(void *)),true);
   }

   //
   // Extra function for our use
   //
   void reserve(size_t freeCount) {
      _reserve(freeCount);
   }

   void selfCheck() {
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
   struct slab_map : public std::map<key,value,compare,slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount> > {
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::map<key,value,compare,slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount>> map_type;
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
   typedef slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount> my_alloc_type;
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
   struct slab_multimap : public std::multimap<key,value,compare,slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount> > {
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::multimap<key,value,compare,slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount>> map_type;
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
   typedef slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount> my_alloc_type;
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
   struct slab_set : public std::set<key,compare,slab_allocator<pool_ix,key,stackCount,heapCount> > {
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::set<key,compare,slab_allocator<pool_ix,key,stackCount,heapCount>> set_type;
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
   typedef slab_allocator<pool_ix,key,stackCount,heapCount> my_alloc_type;
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
   struct slab_multiset : public std::multiset<key,compare,slab_allocator<pool_ix,key,stackCount,heapCount> > {
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::multiset<key,compare,slab_allocator<pool_ix,key,stackCount,heapCount>> set_type;
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
   typedef slab_allocator<pool_ix,key,stackCount,heapCount> my_alloc_type;
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
   size_t   heapCount>
struct slab_list : public std::list<node,slab_allocator<pool_ix,node,stackCount,heapCount> > {

   //
   // copy and assignment
   //
   slab_list() {}
   slab_list(const slab_list& o) { copy(o); }; // copy
   slab_list& operator=(const slab_list& o) { copy(o); return *this; }

   typedef typename std::list<node,slab_allocator<pool_ix,node,stackCount,heapCount>>::iterator it;
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
      // copy and erase nodes from other to end of my list
      //
      while (ofirst != olast) {
         this->push_back(std::move(*ofirst));
         ofirst = o.erase(ofirst);
      }
      //
      // Copy original nodes of my list to other container
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
   typedef std::list<node,slab_allocator<pool_ix,node,stackCount,heapCount>> list_type;

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
   typedef slab_allocator<pool_ix,node,stackCount,heapCount> my_alloc_type;
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
template<pool_index_t pool_ix,typename T,size_t stackSize>
class slab_vector_allocator : public pool_slab_allocator<pool_ix,T> {
   T stackSlot[stackSize]; 			// stackSlab is always allocated with the object :)
  
public:
   typedef slab_vector_allocator<pool_ix,T,stackSize> allocator_type;

   template<typename U> struct rebind { typedef slab_vector_allocator<pool_ix,U,stackSize> other; };

   slab_vector_allocator() {
      this->slab_allocate(stackSize,sizeof(T),0);
   }
   ~slab_vector_allocator() {
      this->slab_deallocate(stackSize,sizeof(T),0,false);
   }

   T * allocate(size_t cnt,void *p = nullptr) {
      if (cnt <= stackSize) return stackSlot;
      this->slab_allocate(cnt,sizeof(T),0);
      return reinterpret_cast<T *>(new char[cnt * sizeof(T)]);
   }

   void deallocate(T *p, size_t s) {
      if (p != stackSlot) {
         this->slab_deallocate(s,sizeof(T),0,false);
         delete[] reinterpret_cast<char *>(p);
      }
   }

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
template<pool_index_t pool_ix,typename value,size_t   stackCount >
   class slab_vector : public std::vector<value,slab_vector_allocator<pool_ix,value,stackCount> > {
   typedef std::vector<value,slab_vector_allocator<pool_ix,value,stackCount>> vector_type;
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
   typedef slab_vector_allocator<pool_ix,value,stackCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }

};

}; // namespace

//
// Simple function to compute the size of a heapSlab
//
//   we assume that we want heapSlabs to be about 1KBytes.
//

enum { _desired_slab_size = 256 }; // approximate preferred allocation size

#define P(x)								\
  namespace x {								\
    inline constexpr size_t defaultSlabSize(size_t nodeSize) {          \
      return (_desired_slab_size / nodeSize) ?                          \
             (_desired_slab_size / nodeSize) : size_t(1);               \
    }                                                                   \
                                                                        \
    template<typename k,typename v,size_t stackSize,                    \
             size_t slabSize = defaultSlabSize(sizeof(k)+sizeof(v)),    \
             typename cmp = std::less<k> >	                        \
    using slab_map = mempool::slab_map<id, k, v,stackSize,slabSize,cmp>;\
                                                                        \
    template<typename k,typename v,size_t stackSize,                    \
             size_t slabSize = defaultSlabSize(sizeof(k)+sizeof(v)),    \
             typename cmp = std::less<k> >	                        \
    using slab_multimap                                                 \
       = mempool::slab_multimap<id,k,v,stackSize,slabSize,cmp>;         \
                                                                        \
    template<typename k,size_t stackSize,                               \
             size_t slabSize = defaultSlabSize(sizeof(k)),              \
             typename cmp = std::less<k> >	                        \
    using slab_set = mempool::slab_set<id,k, stackSize, slabSize,cmp>;  \
                                                                        \
    template<typename k,size_t stackSize,                               \
             size_t slabSize = defaultSlabSize(sizeof(k)),              \
             typename cmp = std::less<k> >	                        \
    using slab_multiset =                                               \
       mempool::slab_multiset<id,k,stackSize,slabSize,cmp>;             \
                                                                        \
    template<typename v,size_t stackSize,                               \
             size_t slabSize = defaultSlabSize(sizeof(v))>              \
    using slab_list = mempool::slab_list<id,v,stackSize,slabSize>;	\
                                                                        \
    template<typename v,size_t stackSize>                               \
    using slab_vector = mempool::slab_vector<id,v,stackSize>;	        \
  };


namespace mempool {
DEFINE_MEMORY_POOLS_HELPER(P)
};

#undef P
#endif // _slab_CONTAINERS_H

