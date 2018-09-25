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

#include <boost/intrusive/list.hpp>
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

There is only one way to create more slabs: When no more free nodes are
available, a slab is allocated using the slabSize template parameter,
which has a default value intended to try to round up all allocations
to something like 256 Bytes.


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

template<pool_index_t pool_ix,
	 typename T,
	 std::size_t StackSlabSizeV,
	 std::size_t HeapSlabSizeV>
class slab_allocator2 : public pool_slab_allocator<pool_ix,T> {
  struct heap_slab_t;
  struct heap_slot_t {
    heap_slab_t* owner;
    std::aligned_storage_t<sizeof(T), alignof(T)> storage;
  };

  struct heap_slab_t : public boost::intrusive::list_base_hook<> {
    // XXX: control data could be put at the end of struct if we want
    // to take care about types of extended alignment
    ceph::math::bitset<HeapSlabSizeV> free_map;
    std::array<heap_slot_t, HeapSlabSizeV> slots;
  };

  using stack_slot_t = std::aligned_storage_t<sizeof(T), alignof(T)>;

  struct stack_slab_t {
    ceph::math::bitset<StackSlabSizeV> free_map;
    std::array<stack_slot_t, StackSlabSizeV> storage;
  };

  // The pointer-to-tail of doubly linked list is useless for us
  // but we really need the ability to do O(1) list management
  // being given only the pointer slab.
  boost::intrusive::list<heap_slab_t,
    boost::intrusive::constant_time_size<false>> heap_free_list;
  stack_slab_t stack_slab;

  T* allocate_dynamic() {
    if (heap_free_list.empty()) {
      auto& slab = *new heap_slab_t;
      slab.free_map.set_first(HeapSlabSizeV);
      heap_free_list.push_front(slab);
    }

    auto& slab = heap_free_list.front();
    const std::size_t idx = slab.free_map.find_first_set();
    slab.free_map.reset(idx);
    slab.slots[idx].owner = &slab;

    if (slab.free_map.none()) {
      heap_free_list.erase(heap_free_list.iterator_to(slab));
    }

    return reinterpret_cast<T*>(&slab.slots[idx].storage);
  }

  void deallocate_dynamic(T* const p) {
    auto* slot = reinterpret_cast<heap_slot_t*>((char *)p - sizeof(heap_slab_t*));
    heap_slab_t& slab = *(slot->owner);

    const bool was_empty = slab.free_map.none();
    const std::size_t idx = slot - &slab.slots[0];
    slab.free_map.set(idx);

    if (was_empty) {
      heap_free_list.push_front(slab);
    }
    if (slab.free_map.all_first_set(HeapSlabSizeV)) {
      heap_free_list.erase(heap_free_list.iterator_to(slab));
      delete &slab;
    }
  }

public:
  typedef slab_allocator2<pool_ix, T, StackSlabSizeV, HeapSlabSizeV> allocator_type;

  template<typename U> struct rebind {
    typedef slab_allocator2<pool_ix, U, StackSlabSizeV, HeapSlabSizeV> other;
  };

  slab_allocator2() {
    stack_slab.free_map.set_first(StackSlabSizeV);
  }

  ~slab_allocator2() {
    // If you fail here, it's because you've allowed a node to escape
    // the enclosing object. Something like a swap or a splice operation.
    // Probably the slab_xxx container is missing a "using" that serves
    // to hide some operation.
    ceph_assert(heap_free_list.empty());
  }

  T* allocate(const size_t cnt, void* const p = nullptr) {
    if (stack_slab.free_map.any()) {
      const std::size_t idx = stack_slab.free_map.find_first_set();
      stack_slab.free_map.reset(idx);
      return reinterpret_cast<T*>(&stack_slab.storage[idx]);
    } else {
      return allocate_dynamic();
    }
  }

  void deallocate(T* p, size_t s) {
    const std::uintptr_t maybe_stackstlot_idx = \
      reinterpret_cast<stack_slot_t*>(p) - &stack_slab.storage[0];
    if (maybe_stackstlot_idx < StackSlabSizeV) {
      stack_slab.free_map.set(maybe_stackstlot_idx);
    } else {
      deallocate_dynamic(p);
    }
  }

private:
   // Can't copy or assign this guy
   slab_allocator2(slab_allocator2&) = delete;
   slab_allocator2(slab_allocator2&&) = delete;
   void operator=(const slab_allocator2&) = delete;
   void operator=(const slab_allocator2&&) = delete;
};

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
// Subsequent slabs are allocated using the supplied allocator. A slab on
// the heap contains 'slabSize' nodes. It's impossible to have slabs that
// are larger (or smaller) than 'slabSize' nodes EXCEPT the very specific
// case of stack slabs. They are controlled by the `stacksize` parameter
// and are internal part of the allocator's object.
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
      ceph::math::bitset<std::max(stackSize,slabSize)> freeMap;
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
   template <std::size_t sz>
   void initSlab(slab_t *slab) {
      slab->freeMap.set_first(sz);
      slab->slabHead.next = NULL;
      slab->slabHead.prev = NULL;

      this->slab_allocate(sz, sizeof(slot_t), sizeof(slab_t));
      this->slab_item_free(sizeof(slot_t), sz);

      //
      // put slab onto the contianer's slab freelist
      //
      slab->slabHead.next = freeSlabHeads.next;
      freeSlabHeads.next->prev = &slab->slabHead;
      freeSlabHeads.next = &slab->slabHead;
      slab->slabHead.prev = &freeSlabHeads;

      // Setting slot_t::slab is performed during the actual object
      // allocation. We really want to be lazy here to not collect
      // TLB misses/page faults twice.
   }

   void freeStackSlot(slab_t* const slab, const size_t idx) {
      //
      // Put this slot onto the per-slab freelist
      //
      const bool was_empty = slab->freeMap.none();
      slab->freeMap.set(idx);
      if (was_empty) {
         //
         // put slab onto the contianer's slab freelist
         //
         slab->slabHead.next = freeSlabHeads.next;
         freeSlabHeads.next->prev = &slab->slabHead;
         freeSlabHeads.next = &slab->slabHead;
         slab->slabHead.prev = &freeSlabHeads;
      }
   }

   void freeHeapSlot(slab_t* const slab, const size_t idx) {
      //
      // Put this slot onto the per-slab freelist
      //
      const bool was_empty = slab->freeMap.none();
      slab->freeMap.set(idx);
      if (was_empty) {
         //
         // put slab onto the contianer's slab freelist
         //
         slab->slabHead.next = freeSlabHeads.next;
         freeSlabHeads.next->prev = &slab->slabHead;
         freeSlabHeads.next = &slab->slabHead;
         slab->slabHead.prev = &freeSlabHeads;
      }

      if (slab->freeMap.all_first_set(slabSize)) {
         //
         // Slab is entirely free
         //
         slab->slabHead.next->prev = slab->slabHead.prev;
         slab->slabHead.prev->next = slab->slabHead.next;
         this->slab_deallocate(slabSize,sizeof(slot_t),sizeof(slab_t),true);
         delete[] slab;
      }
   }

   void addHeapSlab() {
       //
       // I need to compute the size of this structure
       //
       //   struct .... {
       //        heapSlab_t *next;
       //        T          slots[slabSize];
       //   };
       //
       // Allocate the slab and free the slots within.
       //
       size_t total = (slabSize * sizeof(slot_t)) + sizeof(slab_t);
       slab_t *slab = reinterpret_cast<slab_t *>(new char[total]);
       initSlab<slabSize>(slab);
   }
   
   slot_t *allocslot() {
      if (freeSlabHeads.next == &freeSlabHeads) {
         addHeapSlab();
      }
      slab_t *freeSlab = slabHeadToSlab(freeSlabHeads.next);
      const std::size_t idx = freeSlab->freeMap.find_first_set();
      freeSlab->freeMap.reset(idx);
      if (freeSlab->freeMap.none()) {
         //
         // remove slab from list
         //
         freeSlabHeads.next = freeSlab->slabHead.next;
         freeSlab->slabHead.next->prev = &freeSlabHeads;
         freeSlab->slabHead.next = nullptr;
         freeSlab->slabHead.prev = nullptr;
      }

      this->slab_item_allocate(sizeof(slot_t));

      slot_t& slot = freeSlab->slot[idx];
      slot.slab = freeSlab;
      return &slot;
   }

   slabHead_t freeSlabHeads;	                // List of slabs that have free slots
   stackSlab_t stackSlab; 			// stackSlab is always allocated with the object :)
  
public:
  template<typename U> struct rebind {
    typedef slab_allocator<pool_ix,U,stackSize,slabSize> other;
  };

  typedef slab_allocator<pool_ix,T,stackSize,slabSize> allocator_type;

   slab_allocator() {
      //
      // For the "in the stack" slots, put them on the free list
      //
      freeSlabHeads.next = &freeSlabHeads;
      freeSlabHeads.prev = &freeSlabHeads;
      initSlab<stackSize>(&stackSlab);
   }
   ~slab_allocator() {
      //
      // If you fail here, it's because you've allowed a node to escape the enclosing object. Something like a swap
      // or a splice operation. Probably the slab_xxx container is missing a "using" that serves to hide some operation.
      //
      assert(freeSlabHeads.next == &stackSlab.slabHead); // Empty list should have stack slab on it
      this->slab_deallocate(stackSize,sizeof(slot_t),sizeof(slab_t),true);
   }

   T* allocate(size_t cnt,void *p = nullptr) {
      assert(cnt == 1); // if you fail this you've used this class with the wrong STL container.
      return reinterpret_cast<T *>(sizeof(void *) + (char *)allocslot());
   }

   void deallocate(T* p, size_t s) {
      auto* slot = reinterpret_cast<slot_t*>((char *)p - sizeof(void *));
      auto* slab = slot->slab;

      const std::size_t idx = (slot - &(slab->slot[0]));
      if (slab == &stackSlab) {
	freeStackSlot(slab, idx);
      } else {
	freeHeapSlot(slab, idx);
      }

      this->slab_item_free(sizeof(slot_t));
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
private:
   typedef std::map<key,value,compare,slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount>> map_type;
   //
   // Disallowed operations
   //
   using map_type::swap;
};

template<
   pool_index_t pool_ix,
   typename key,
   typename value,
   size_t   stackCount, 
   size_t   heapCount, 
   typename compare = std::less<key> >
   struct slab_multimap : public std::multimap<key,value,compare,slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount> > {
private:
   typedef std::multimap<key,value,compare,slab_allocator<pool_ix,std::pair<key,value>,stackCount,heapCount>> map_type;
   //
   // Disallowed operations
   //
   using map_type::swap;
};

template<
   pool_index_t pool_ix,
   typename key,
   size_t   stackCount, 
   size_t   heapCount, 
   typename compare = std::less<key> >
   struct slab_set : public std::set<key,compare,slab_allocator<pool_ix,key,stackCount,heapCount> > {
private:
   typedef std::set<key,compare,slab_allocator<pool_ix,key,stackCount,heapCount>> set_type;
   //
   // Disallowed operations
   //
   using set_type::swap;
};

template<
   pool_index_t pool_ix,
   typename key,
   size_t   stackCount, 
   size_t   heapCount, 
   typename compare = std::less<key> >
   struct slab_multiset : public std::multiset<key,compare,slab_allocator<pool_ix,key,stackCount,heapCount> > {
private:
   typedef std::multiset<key,compare,slab_allocator<pool_ix,key,stackCount,heapCount>> set_type;
   //
   // Disallowed operations
   //
   using set_type::swap;
};

template<
   pool_index_t pool_ix,
   typename node,
   size_t   stackCount, 
   size_t   heapCount>
struct slab_list : public std::list<node,slab_allocator2<pool_ix,node,stackCount,heapCount> > {

   //
   // copy and assignment
   //
   slab_list() {}
   slab_list(const slab_list& o) { copy(o); }; // copy
   slab_list& operator=(const slab_list& o) { copy(o); return *this; }

   typedef typename std::list<node,slab_allocator2<pool_ix,node,stackCount,heapCount>>::iterator it;
   typedef typename std::list<node,slab_allocator2<pool_ix,node,stackCount,heapCount> > base;
   //
   // We support splice, but it requires actually copying each node, so it's O(N) not O(1)
   //
   // TODO: static_asserts for original splice() + rename the stuff below to splice_same
   void splice(it pos, slab_list& other) {
     if (std::addressof(other) == this) {
       base::splice(pos, other);
       return;
     } else {
       ceph_assert("not supported" == nullptr);
     }
   }
   void splice(it pos, slab_list& other, it it) {
     if (std::addressof(other) == this) {
       base::splice(pos, other, it);
       return;
     } else {
       ceph_assert("not supported" == nullptr);
     }
   }
   void splice(it pos, slab_list& other, it first, it last) {
     if (std::addressof(other) == this) {
       base::splice(pos, other, first, last);
       return;
     } else {
       ceph_assert("not supported" == nullptr);
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
private:
   typedef std::list<node,slab_allocator<pool_ix,node,stackCount,heapCount>> list_type;

   void copy(const slab_list& o) {
      this->clear();
      for (auto& e : o) {
         this->push_back(e);
      }
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

