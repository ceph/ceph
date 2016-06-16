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

#ifndef _FAST_CONTAINERS_H
#define _FAST_CONTAINERS_H

#include <map>
#include <set>
#include <assert.h>
#include <list>

//
// The ceph::fast_xxxx containers are standard STL containers with a custom allocator.
//
// When you declare a fast container you provide 1 or 2 additional integer template parameters that
// modify the memory allocation pattern. The point is to amortize the memory allocations for nodes
// within the container.
//
//  ceph::fast_map     <key,value,stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::fast_multimap<key,value,stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::fast_set     <value,    stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::fast_multiset<value,    stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::list         <value,    stackSize,heapSize = stackSize>
//   
//  stackSize indicates the number of nodes that will be allocated within the container itself.
//      in other words, if the container never has more than stackSize nodes, there will be no additional
//      memory allocation calls.
//
//  heapSize indicates the number of nodes that will be requested when a memory allocation is required.
//      In other words, nodes are allocated in batches of heapSize.
//
//  All of this wizardry comes with a price. There are two basic restrictions:
//  (1) nodes are never deallocated until the container is destroyed.
//  (2) nodes cannot escape a container, i.e., be transferred to another container
//
//  The first restriction suggests that long-lived containers might want to use this code.
//  The second restriction is enforced by hiding the functions that cause problems like xxx::swap and list::splice.
//

namespace ceph {
//
// fast slab allocator
//
// This is an STL allocator intended for use with short-lived node-heavy containers, i.e., map, set, etc.
//
// Memory is allocated in slabs. Each slab contains a fixed number of slots for objects.
//
// The first slab is part of the object itself, meaning that no memory allocation is required
// if the container doesn't exceed "stackSize" nodes.
//
// Subsequent slabs are allocated using the normal heap. A slab on the heap contains 'heapSize' nodes.
//
// This allocator is optimized for ephemeral objects. It's wastes space to gain time. One
// optimization is that once a slab is allocated, it's never freed until the allocator is destructed.
//
// Technically, this allocator is not entirely safe. That's because it doesn't allow the lifetime of an
// allocated node to exceed the lifetime of the declaring object. For many containers, this isn't a problem
// but for some containers, you have the ability to move nodes directly between containers. For example list::splice
// If you use this allocator with those containers, you'll corrupt memory. Fortunately, this situation is detected
// automatically, but only after the fact. Violations will result in an assert failure at run-time see ~fs_allocator
//
template<typename T,size_t stackSize, size_t heapSize = stackSize>
class fs_allocator {
   //
   // Stores one Object OR if it's free, exactly one pointer to a in a free slot list.
   // Since this is raw memory, we don't want to declare something of type "T" to avoid
   // accidental constructor/destructor calls.
   //
   struct slot_t {
      enum { SLOT_SIZE_IN_POINTERS = (sizeof(T) + sizeof(slot_t *) - 1) / sizeof(slot_t *) };
      slot_t *storage[SLOT_SIZE_IN_POINTERS];
   };

   //
   // Exactly one of these as part of this object (the slab on the stack :))
   //
   struct stackSlab_t {
      slot_t slots[stackSize];
   };
   //
   // These are malloc/freeded.
   //
   struct heapSlab_t {
      heapSlab_t *next;
      slot_t slots[heapSize];
   };      
   
   void freeslot(slot_t *s) {
      s->storage[0] = freelist;
      freelist = s;
      ++freeSlotCount;
   }
   
   slot_t *allocslot() {
      if (freelist == nullptr) {
         // empty, alloc another slab
  	 heapSlab_t *s = new heapSlab_t();
         s->next = heapslabs;
         heapslabs = s;
         heapSlabCount++;
         for (size_t i = 0; i < heapSize; ++i) {
            freeslot(&s->slots[i]);
         }
      }
      slot_t *result = freelist;
      freelist = freelist->storage[0];
      --freeSlotCount;
      return result;
   }

   slot_t *freelist;                            // list of slots that are free
   heapSlab_t *heapslabs;		        // List of slabs to free on dtor
   stackSlab_t stackslab; 			// stackSlab is always allocated with the object :)
   size_t freeSlotCount;                        // # of slots currently in the freelist * Only used for debug integrity check *
   size_t heapSlabCount;                        // # of slabs allocated on the heap     * Only used for debug integrity check *

public:
   typedef T value_type;
   typedef value_type *pointer;
   typedef const value_type * const_pointer;
   typedef value_type& reference;
   typedef const value_type& const_reference;
   typedef std::size_t size_type;
   typedef std::ptrdiff_t difference_type;

   template<typename U> struct rebind { typedef fs_allocator<U,stackSize,heapSize> other; };

   fs_allocator() : freelist(nullptr), heapslabs(nullptr), freeSlotCount(0), heapSlabCount(0) {
      for (size_t i = 0; i < stackSize; ++i) {
         freeslot(&stackslab.slots[i]);
      }
   }
   ~fs_allocator() {
      //
      // If you fail here, it's because you've allowed a node to escape the enclosing object. Something like a swap
      // or a splice operation. Probably the fast_xxx container is missing a "using" that serves to hide some operation.
      //
      assert(freeSlotCount == ((heapSlabCount * heapSize) + stackSize));
      while (heapslabs != nullptr) {
         heapSlab_t *aslab = heapslabs;
         heapslabs = heapslabs->next;
         delete aslab;
      }     
   }

   pointer allocate(size_t cnt,void *p = nullptr) {
      assert(cnt == 1); // if you fail this you've used this class with the wrong STL container.
      return reinterpret_cast<pointer>(allocslot());
   }

   void deallocate(pointer p, size_type s) {
      freeslot(reinterpret_cast<slot_t *>(p));
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

   bool operator==(const fs_allocator&) { return true; }
   bool operator!=(const fs_allocator&) { return false; }

private:

   // Can't copy or assign this guy
   fs_allocator(fs_allocator&) = delete;
   fs_allocator(fs_allocator&&) = delete;
   void operator=(const fs_allocator&) = delete;
   void operator=(const fs_allocator&&) = delete;
};

//
// Extended containers
//

template<
   typename key,
   typename value,
   size_t   stackCount, 
   size_t   heapCount = stackCount, 
   typename compare = std::less<key> >
   struct fast_map : public std::map<key,value,compare,fs_allocator<std::pair<key,value>,stackCount,heapCount> > {
private:
   typedef std::map<key,value,compare,fs_allocator<std::pair<key,value>,stackCount,heapCount>> map_type;
   //
   // Disallowed operations
   //
   using map_type::swap;
};

template<
   typename key,
   typename value,
   size_t   stackCount, 
   size_t   heapCount = stackCount, 
   typename compare = std::less<key> >
   struct fast_multimap : public std::multimap<key,value,compare,fs_allocator<std::pair<key,value>,stackCount,heapCount> > {
private:
   typedef std::multimap<key,value,compare,fs_allocator<std::pair<key,value>,stackCount,heapCount>> map_type;
   //
   // Disallowed operations
   //
   using map_type::swap;
};

template<
   typename key,
   size_t   stackCount, 
   size_t   heapCount = stackCount, 
   typename compare = std::less<key> >
   struct fast_set : public std::set<key,compare,fs_allocator<key,stackCount,heapCount> > {
private:
   typedef std::set<key,compare,fs_allocator<key,stackCount,heapCount>> set_type;
   //
   // Disallowed operations
   //
   using set_type::swap;
};

template<
   typename key,
   size_t   stackCount, 
   size_t   heapCount = stackCount, 
   typename compare = std::less<key> >
   struct fast_multiset : public std::multiset<key,compare,fs_allocator<key,stackCount,heapCount> > {
private:
   typedef std::multiset<key,compare,fs_allocator<key,stackCount,heapCount>> set_type;
   //
   // Disallowed operations
   //
   using set_type::swap;
};

template<
   typename node,
   size_t   stackCount, 
   size_t   heapCount = stackCount >
   struct fast_list : public std::list<node,fs_allocator<node,stackCount,heapCount> > {
private:
   typedef std::list<node,fs_allocator<node,stackCount,heapCount>> list_type;
   //
   // Disallowed operations
   //
   using list_type::swap;
   using list_type::splice;
};

};

#endif // _FAST_CONTAINERS_H

