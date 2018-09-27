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

#include <list>
#include <map>
#include <unordered_map>
#include <set>

#include <boost/intrusive/list.hpp>

#include "include/ceph_assert.h"
#include "include/intarith.h"

namespace ceph::slab {

// ceph::slab - STL-like containers with slabbed memory management.
//
// The slabbed containers are composed from standard STL containers and
// custom allocator that aggregates multiple memory (de)allocations into
// bigger units called 'slabs' to amortize the cost and improve data
// locality. That is, instead of calling malloc/free for each its node
// node separately, slabbed container mallocs a slab, divides it across
// many nodes and -- when it's no longer necessary -- frees as a whole.
// In this documentation we'll use the term 'slabSize' as a short hand
// for "number of nodes in a slab".
//
// The way of declaring a slab container mimics its base STL container
// but has two additional parameters: StackSlabSizeV and  HeapSlabSizeV.
//
// The ADVANTAGE of slab containers is that the run-time cost of the per-
// node malloc/free is reduced by 'slabSize' (well technically amortized
// across slabSize number of STL alloc/frees). Also, heap fragmentation
// is typically reduced. Especially for containers with lots of small
// modes.
//
// The DISADVANTAGE of slab containers is that you malloc/free entire
// slabs. Thus slab containers always consume more memory than their
// STL  equivalents. The amount of extra memory consumed is dependent
// on the access pattern (and container type). The worst-case  memory
// expansion is 'slabSize-1' times an STL container (approximately).
// However it's likely to be very hard to create a worst-case memory
// pattern (unless you're trying ;-).
// This also leads toward keeping the slabSizes modest -- unless you
// have special knowledge  of the access pattern OR the container itself
// is expected to be ephemeral.
//
// Another disdvantage of slabs is that you can't move them from one
// container to another. This causes certain operations that used to
// have O(1) cost to have O(N) cost.
//
//    list::splice  O(N)
//    list::swap    O(2N)
//    vector::swap is O(2*stackSize)
//
// Not all of the stl::xxxx.swap operations are currently implemented.
// They are pretty much discouraged for slab_containers anyways.
//
// In order to reduce malloc/free traffic further, each container has
// within it one slab that is part of the container itself (logically
// pre-allocated, but it's not a separate malloc call -- its part of
// the container itself). The size of this slab is set by the template
// parameter 'StackSlabSizeV' which must be specified on every
// declaration (no default).  Thus the first 'StackSlabSizeV' items in
// this slab container result in no malloc/free invocation at the expense
// of burning that memory whether it's used or not. This is ideal for
// containers that are normally one or two nodes but have the ability
// to grow on that rare occasion when it's needed.
//
// There is only one way to create more slabs: When no more free nodes
// are available, a slab is allocated using the HeapSlabSizeV template
// parameter.

template<typename T, std::size_t StackSlabSizeV, std::size_t HeapSlabSizeV>
class slab_allocator {
  struct heap_slab_t;

  // In contrast to an embedded slot, each heap slot has overhead
  // of one pointer.
  struct heap_slot_t {
    heap_slab_t* owner;
    std::aligned_storage_t<sizeof(T), alignof(T)> storage;
  };

  // In contrast to an embedded slab, each heap slab can be tracked
  // using a freelist. Together with the need for removal in O(1),
  // this results in using a doubly linked list.
  struct heap_slab_t : public boost::intrusive::list_base_hook<> {
    // Control data should be put at the end of struct if we would
    // like to take care about types with extended alignment.
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

    // Setting slot_t::slab is performed during the actual object
    // allocation. We really want to be lazy here to not collect
    // TLB misses/page faults twice.
    auto& slab = heap_free_list.front();
    const std::size_t idx = slab.free_map.find_first_set();
    slab.free_map.reset(idx);
    slab.slots[idx].owner = &slab;

    // Depleted slab must be removed from the slab freelist.
    if (slab.free_map.none()) {
      heap_free_list.erase(heap_free_list.iterator_to(slab));
    }

    return reinterpret_cast<T*>(&slab.slots[idx].storage);
  }

  void deallocate_dynamic(T* const p) {
    auto* slot = \
      reinterpret_cast<heap_slot_t*>((char *)p - sizeof(heap_slab_t*));
    heap_slab_t& slab = *(slot->owner);

    const bool was_empty = slab.free_map.none();
    if (was_empty) {
      heap_free_list.push_front(slab);
    }

    const std::size_t idx = slot - &slab.slots[0];
    slab.free_map.set(idx);
    if (slab.free_map.all_first_set(HeapSlabSizeV)) {
      heap_free_list.erase(heap_free_list.iterator_to(slab));
      delete &slab;
    }
  }

public:
  typedef T value_type;

  template<typename U> struct rebind {
    typedef slab_allocator<U, StackSlabSizeV, HeapSlabSizeV> other;
  };

  slab_allocator() {
    stack_slab.free_map.set_first(StackSlabSizeV);
  }

  ~slab_allocator() {
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

  bool operator==(const slab_allocator& rhs) const {
    return std::addressof(rhs) == this;
  }
  bool operator!=(const slab_allocator& rhs) const {
    return std::addressof(rhs) != this;
  }

private:
  // Can't copy or assign this guy
  slab_allocator(slab_allocator&) = delete;
  slab_allocator(slab_allocator&&) = delete;
  void operator=(const slab_allocator&) = delete;
  void operator=(const slab_allocator&&) = delete;
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

//
// Extended containers
//

template<
  typename Key,
  typename T,
  std::size_t StackSlabSizeV,
  std::size_t HeapSlabSizeV,
  typename Compare = std::less<Key>,
  typename Allocator = slab_allocator<std::pair<const Key, T>,
				      StackSlabSizeV,HeapSlabSizeV>>
struct slab_map : public std::map<Key, T, Compare, Allocator> {
private:
  typedef std::map<Key, T, Compare, Allocator> map_type;
  // Disallowed operations
  using map_type::swap;
};

template<
  typename Key,
  typename T,
  std::size_t StackSlabSizeV,
  std::size_t HeapSlabSizeV,
  typename Compare = std::less<Key>,
  typename Allocator = slab_allocator<std::pair<const Key, T>,
				      StackSlabSizeV, HeapSlabSizeV>>
struct slab_multimap : public std::multimap<Key, T, Compare, Allocator> {
private:
  typedef std::multimap<Key, T, Compare, Allocator> map_type;
  //
  // Disallowed operations
  //
  using map_type::swap;
};

template<
  typename Key,
  std::size_t StackSlabSizeV,
  std::size_t HeapSlabSizeV,
  typename Compare = std::less<Key>,
  typename Allocator = slab_allocator<Key, StackSlabSizeV, HeapSlabSizeV>>
struct slab_set : public std::set<Key, Compare, Allocator> {
private:
   typedef std::set<Key, Compare, Allocator> set_type;
   // Disallowed operations
   using set_type::swap;
};

template<
  typename Key,
  std::size_t StackSlabSizeV,
  std::size_t HeapSlabSizeV,
  typename Compare = std::less<Key>,
  typename Allocator = slab_allocator<Key, StackSlabSizeV, HeapSlabSizeV>>
struct slab_multiset : public std::multiset<Key, Compare, Allocator> {
private:
  typedef std::multiset<Key, Compare, Allocator> set_type;
  // Disallowed operations
  using set_type::swap;
};

// https://stackoverflow.com/a/34863387
template<
  typename T,
  std::size_t StackSlabSizeV,
  std::size_t HeapSlabSizeV,
  typename Allocator = slab_allocator<T, StackSlabSizeV, HeapSlabSizeV>>
struct slab_list : public std::list<T, Allocator> {

  // copy and assignment
  //
  slab_list() = default;
  slab_list(const slab_list& o) { copy(o); }; // copy
  slab_list& operator=(const slab_list& o) { copy(o); return *this; }

  typedef typename std::list<T, Allocator>::iterator it;
  typedef typename std::list<T, Allocator> base;
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
  typedef std::list<T, Allocator> list_type;

  void copy(const slab_list& o) {
     this->clear();
     for (auto& e : o) {
        this->push_back(e);
     }
  }
};

}; // namespace ceph::slab

#endif // _slab_CONTAINERS_H

