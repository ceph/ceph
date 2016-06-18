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
   };
 
   void freeslot(slot_t *s) {
      s->storage[0] = freelist;
      freelist = s;
      ++freeSlotCount;
   }

   //
   // Danger, because of the my_actual_allocator hack. You can't rely on T to be correct, nor any value or offset that's
   // derived directly or indirectly from T.
   //
   void addSlab(size_t slabSize) {
       size_t sz = sizeof(heapSlab_t) + slabSize * trueSlotSize;
       heapSlab_t *s = reinterpret_cast<heapSlab_t *>(::malloc(sz));
std::cout << "addSlab " << slabSize << " sizeof:" << trueSlotSize << ',' << sizeof(T) << " sz = " << sz << " @ " << (void *)s << "\n";
       s->next = heapSlabs;
       heapSlabs = s;
       //
       // Now, free the slots. This is a bit ugly because slotSize isn't known at compile time (because T is incorrect)
       //
       char *raw = reinterpret_cast<char *>(s+1);
       for (size_t i = 0; i < slabSize; ++i) {
std::cout << "Freeing Slot " << (void *)raw << "\n";
          freeslot(reinterpret_cast<slot_t *>(raw));
          raw += trueSlotSize;
          allocSlotCount++;
       }
   }
   
   slot_t *allocslot() {
      if (freelist == nullptr) {
         // empty, alloc another slab
         addSlab(heapSize);
      }
      slot_t *result = freelist;
      freelist = freelist->storage[0];
      --freeSlotCount;
      return result;
   }

   void _reserve(size_t freeCount) {
      if (freeSlotCount < freeCount) {
         addSlab(freeCount - freeSlotCount);
      }      
   }

   fs_allocator *selfPointer;                   // for selfCheck
   slot_t *freelist;                            // list of slots that are free
   heapSlab_t *heapSlabs;		        // List of slabs to free on dtor
   size_t freeSlotCount;                        // # of slots currently in the freelist * Only used for debug integrity check *
   size_t allocSlotCount;                       // # of slabs allocated                 * Only used for debug integrity check *
   size_t trueSlotSize;	                        // Actual Slot Size

   // Must always be last item declared, because of get_my_allocator hack which won't have the right types, hence it'll get the stack wrong....
   stackSlab_t stackSlab; 			// stackSlab is always allocated with the object :)
  
public:
   typedef fs_allocator<T,stackSize,heapSize> allocator_type;
   typedef T value_type;
   typedef value_type *pointer;
   typedef const value_type * const_pointer;
   typedef value_type& reference;
   typedef const value_type& const_reference;
   typedef std::size_t size_type;
   typedef std::ptrdiff_t difference_type;

   template<typename U> struct rebind { typedef fs_allocator<U,stackSize,heapSize> other; };

   fs_allocator() : freelist(nullptr), heapSlabs(nullptr), freeSlotCount(0), allocSlotCount(stackSize), trueSlotSize(sizeof(slot_t)) {
      for (size_t i = 0; i < stackSize; ++i) {
         freeslot(&stackSlab.slots[i]);
      }
      selfPointer = this;
   }
   ~fs_allocator() {
      //
      // If you fail here, it's because you've allowed a node to escape the enclosing object. Something like a swap
      // or a splice operation. Probably the fast_xxx container is missing a "using" that serves to hide some operation.
      //
      assert(freeSlotCount == allocSlotCount);
      while (heapSlabs != nullptr) {
         heapSlab_t *aslab = heapSlabs;
         heapSlabs = heapSlabs->next;
         ::free(aslab);
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
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::map<key,value,compare,fs_allocator<std::pair<key,value>,stackCount,heapCount>> map_type;
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
   typedef fs_allocator<std::pair<key,value>,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

template<
   typename key,
   typename value,
   size_t   stackCount, 
   size_t   heapCount = stackCount, 
   typename compare = std::less<key> >
   struct fast_multimap : public std::multimap<key,value,compare,fs_allocator<std::pair<key,value>,stackCount,heapCount> > {
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::multimap<key,value,compare,fs_allocator<std::pair<key,value>,stackCount,heapCount>> map_type;
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
   typedef fs_allocator<std::pair<key,value>,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

template<
   typename key,
   size_t   stackCount, 
   size_t   heapCount = stackCount, 
   typename compare = std::less<key> >
   struct fast_set : public std::set<key,compare,fs_allocator<key,stackCount,heapCount> > {
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::set<key,compare,fs_allocator<key,stackCount,heapCount>> set_type;
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
   typedef fs_allocator<key,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

template<
   typename key,
   size_t   stackCount, 
   size_t   heapCount = stackCount, 
   typename compare = std::less<key> >
   struct fast_multiset : public std::multiset<key,compare,fs_allocator<key,stackCount,heapCount> > {
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::multiset<key,compare,fs_allocator<key,stackCount,heapCount>> set_type;
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
   typedef fs_allocator<key,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

template<
   typename node,
   size_t   stackCount, 
   size_t   heapCount = stackCount >
   struct fast_list : public std::list<node,fs_allocator<node,stackCount,heapCount> > {

   typedef typename std::list<node,fs_allocator<node,stackCount,heapCount>>::const_iterator ci;
   //
   // We support splice, but it requires actually copying each node, so it's O(N) not O(1)
   //
   void splice(ci pos, fast_list& other)        { this->splice(pos, other, other.begin(), other.end()); }
   void splice(ci pos, fast_list& other, ci it) { this->splice(pos, other, it, std::next(it)); }
   void splice(ci pos, fast_list& other, ci first, ci last) {
      while (first != last) {
         other.insert(pos,first);
         pos++;
         first++;
      }
   }
   //
   // Extended operator. reserve is now meaningful.
   //
   void reserve(size_t freeCount) { this->get_my_actual_allocator()->reserve(freeCount); }
private:
   typedef std::list<node,fs_allocator<node,stackCount,heapCount>> list_type;
   //
   // Disallowed operations
   //
   using list_type::swap;
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
   typedef fs_allocator<node,stackCount,heapCount> my_alloc_type;
   my_alloc_type * get_my_actual_allocator() {
      my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
      alloc->selfCheck();
      return alloc;
   }
};

};

#endif // _FAST_CONTAINERS_H

