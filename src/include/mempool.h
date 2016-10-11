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

#ifndef _CEPH_INCLUDE_MEMPOOL_H
#define _CEPH_INCLUDE_MEMPOOL_H
#include <iostream>
#include <fstream>

#include <cstddef>
#include <map>
#include <unordered_map>
#include <set>
#include <vector>
#include <assert.h>
#include <list>
#include <mutex>
#include <atomic>
#include <climits>
#include <typeinfo>

#include <common/Formatter.h>

/*

Memory Pools
============

A memory pool is a method for accounting the consumption of memory of
a set of containers.

Memory pools are statically declared (see pool_index_t).

Each memory pool tracks the number of bytes and items it contains.

Allocators can be declared and associated with a type so that they are
tracked independently of the pool total.  This additional accounting
is optional and only incurs an overhead if the debugging is enabled at
runtime.  This allows developers to see what types are consuming the
pool resources.


Declaring
---------

Using memory pools is very easy.

To create a new memory pool, simply add a new name into the list of
memory pools that's defined in "DEFINE_MEMORY_POOLS_HELPER".  That's
it.  :)

For each memory pool that's created a C++ namespace is also
automatically created (name is same as in DEFINE_MEMORY_POOLS_HELPER).
That namespace contains a set of common STL containers that are predefined
with the appropriate allocators.

Thus for mempool "unittest_1" we have automatically available to us:

   unittest_1::map
   unittest_1::multimap
   unittest_1::set
   unittest_1::multiset
   unittest_1::list
   unittest_1::vector
   unittest_1::unordered_map


Putting objects in a mempool
----------------------------

In order to use a memory pool with a particular type, a few additional
declarations are needed.

For a class:

  struct Foo {
    MEMBER_OF_MEMPOOL()
    ...
  };

Then, in an appropriate .cc file,

  MEMPOOL_DEFINE_OBJECT_FACTORY(Foo, foo, unittest_1);

The second argument can generally be identical to the first, except
when the type contains a nested scope.  For example, for
BlueStore::Onode, we need to do

  MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::Onode, bluestore_onode,
                                bluestore_meta);

(This is just because we need to name some static varables and we
can't use :: in a variable name.)

In order to use the STL containers, a few additional declarations
are needed.  For example,

  unittest_1::map<int> myvec;

requires

  MEMPOOL_DEFINE_FACTORY(int, int, unittest_1);
  MEMPOOL_DEFINE_MAP_FACTORY(int, int, unittest_1);

There are similar macros for SET, LIST, and UNORDERED_MAP.  The MAP
macro serves both std::map and std::multimap, and std::vector doesn't
need a second container-specific declaration (because it only
allocates T and T* arrays; there is no internal container-specific
wrapper type).

unordered_map is trickier.  First, it has to allocate the hash
buckets, which requires an extra mempool-wide definition that is
shared by all different types.  Second, sometimes the hash value is
cached in the hash node and sometimes it is not.  The glibc STL makes
its own decision if you don't explicitly define traits, so you either
need to match your definition with its inference, or explicitly define
traits, or simply define the allocator for with the cached and
uncached case and leave one of them unused (but polluting your
debug dumps).  For example,

  unittest_2::unordered_map<int, std::string> one;       // not cached
  unittest_2::unordered_map<std::string, int> one;       // cached

needs

 MEMPOOL_DEFINE_UNORDERED_MAP_BASE_FACTORY(unittest_2);  // used by both
 MEMPOOL_DEFINE_UNORDERED_MAP_FACTORY(uint64_t, std::string, false, int_str,
                                      unittest_2);
 MEMPOOL_DEFINE_UNORDERED_MAP_FACTORY(std::string, uint64_t, true, str_int,
                                      unittest_2);


Introspection
-------------

The simplest way to interrogate the process is with

  Formater *f = ...
  mempool::dump(f);

This will dump information about *all* memory pools.  When debug mode
is enabled, the runtime complexity of dump is O(num_shards *
num_types).  When debug name is disabled it is O(num_shards).

You can also interogate a specific pool programatically with

  size_t bytes = unittest_2::allocated_bytes();
  size_t items = unittest_2::allocated_items();

The runtime complexity is O(num_shards).

Note that you cannot easily query per-type, primarily because debug
mode is optional and you should not rely on that information being
available.

*/

namespace mempool {

// --------------------------------------------------------------
// define memory pools

#define DEFINE_MEMORY_POOLS_HELPER(f) \
  f(unittest_1)			      \
  f(unittest_2)			      \
  f(bluestore_meta_onode)	      \
  f(bluestore_meta_other)

// give them integer ids
#define P(x) x,
enum pool_index_t {
  DEFINE_MEMORY_POOLS_HELPER(P)
  num_pools        // Must be last.
};
#undef P

extern void set_debug_mode(bool d);

// --------------------------------------------------------------
struct pool_allocator_base_t;
class pool_t;

// doubly linked list
struct list_member_t {
  list_member_t *next;
  list_member_t *prev;
  list_member_t() : next(this), prev(this) {}
  ~list_member_t() {
    assert(next == this && prev == this);
  }
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

// we shard pool stats across many shard_t's to reduce the amount
// of cacheline ping pong.
enum { num_shards = 64 };

struct shard_t {
  std::atomic<size_t> bytes = {0};
  std::atomic<size_t> items = {0};
  mutable std::mutex lock;  // only used for types list
  list_member_t types;      // protected by lock
};

struct stats_t {
  ssize_t items = 0;
  ssize_t bytes = 0;
  void dump(ceph::Formatter *f) const {
    f->dump_int("items", items);
    f->dump_int("bytes", bytes);
  }
};

// Root of all allocators, this enables the container information to
// operation easily. These fields are "always" accurate.
struct pool_allocator_base_t {
  list_member_t list_member;   // this must come first; see get_stats() hackery

  pool_t *pool = nullptr;
  shard_t *shard = nullptr;
  const char *type_id = nullptr;
  size_t item_size = 0;

  // for debug mode
  std::atomic<ssize_t> items = {0};  // signed

  // effective constructor
  void attach_pool(pool_index_t index, const char *type_id);

  ~pool_allocator_base_t();
};

pool_t& get_pool(pool_index_t ix);

class pool_t {
  std::string name;
  shard_t shard[num_shards];
  friend class pool_allocator_base_t;
public:
  bool debug;

  pool_t(const std::string& n, bool _debug)
    : name(n), debug(_debug) {
  }

  //
  // How much this pool consumes. O(<num_shards>)
  //
  size_t allocated_bytes() const;
  size_t allocated_items() const;

  const std::string& get_name() const {
    return name;
  }

  shard_t* pick_a_shard() {
    // Dirt cheap, see:
    //   http://fossies.org/dox/glibc-2.24/pthread__self_8c_source.html
    size_t me = (size_t)pthread_self();
    size_t i = (me >> 3) % num_shards;
    return &shard[i];
  }

  // get pool stats.  by_type is not populated if !debug
  void get_stats(stats_t *total,
		 std::map<std::string, stats_t> *by_type) const;

  void dump(ceph::Formatter *f) const;
};

// skip unittest_[12] by default
void dump(ceph::Formatter *f, size_t skip=2);

inline void pool_allocator_base_t::attach_pool(
  pool_index_t index,
  const char *_type_id)
{
  assert(pool == nullptr);
  pool = &get_pool(index);
  shard = pool->pick_a_shard();
  type_id = _type_id;

  // unconditionally register type, even if debug is currently off
  std::unique_lock<std::mutex> lock(shard->lock);
  shard->types.insert(&list_member);
}

inline pool_allocator_base_t::~pool_allocator_base_t()
{
  if (pool) {
    std::unique_lock<std::mutex> lock(shard->lock);
    list_member.remove();
  }
}


// Stateless STL allocator for use with containers.  All actual state
// is stored in the static pool_allocator_base_t, which saves us from
// passing the allocator to container constructors.

template<pool_index_t pool_ix, typename T>
class pool_allocator {
  static pool_allocator_base_t base;

public:
  typedef pool_allocator<pool_ix, T> allocator_type;
  typedef T value_type;
  typedef value_type *pointer;
  typedef const value_type * const_pointer;
  typedef value_type& reference;
  typedef const value_type& const_reference;
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;

  template<typename U> struct rebind {
    typedef pool_allocator<pool_ix,U> other;
  };

  pool_allocator() {
    // initialize fields in the static member.  this should only happen
    // once, but it's also harmless if we do it multiple times.
    base.type_id = typeid(T).name();
    base.item_size = sizeof(T);
  }
  template<typename U>
  pool_allocator(const pool_allocator<pool_ix,U>&) {}
  void operator=(const allocator_type&) {}

  void attach_pool(pool_index_t index, const char *type_id) {
    base.attach_pool(index, type_id);
  }

  pointer allocate(size_t n, void *p = nullptr) {
    size_t total = sizeof(T) * n;
    base.shard->bytes += total;
    base.shard->items += n;
    if (base.pool->debug) {
      base.items += n;
    }
    pointer r = reinterpret_cast<pointer>(new char[total]);
    return r;
  }

  void deallocate(pointer p, size_type n) {
    size_t total = sizeof(T) * n;
    base.shard->bytes -= total;
    base.shard->items -= n;
    if (base.pool->debug) {
      base.items -= n;
    }
    delete[] reinterpret_cast<char*>(p);
  }

  void destroy(pointer p) {
    p->~T();
  }

  template<class U>
  void destroy(U *p) {
    p->~U();
  }

  void construct(pointer p, const_reference val) {
    ::new ((void *)p) T(val);
  }

  template<class U, class... Args> void construct(U* p,Args&&... args) {
    ::new((void *)p) U(std::forward<Args>(args)...);
  }

  bool operator==(const pool_allocator&) { return true; }
  bool operator!=(const pool_allocator&) { return false; }
};


// There is one factory associated with every type that lives in a
// mempool.

template<pool_index_t pool_ix,typename o>
class factory {
public:
  typedef pool_allocator<pool_ix,o> allocator_type;
  static allocator_type alloc;

  factory() {
    alloc.attach_pool(pool_ix, typeid(o).name());
  }
  static void *allocate() {
    return (void *)alloc.allocate(1);
  }
  static void free(void *p) {
    alloc.deallocate((o *)p, 1);
  }
};

};


// Namespace mempool

#define P(x)								\
  namespace x {								\
    static const mempool::pool_index_t pool_ix = mempool::x;			\
    template<typename v>						\
    using pool_allocator = mempool::pool_allocator<mempool::x,v>;	\
    template<typename k,typename v, typename cmp = std::less<k> >	\
    using map = std::map<k, v, cmp,					\
			 pool_allocator<std::pair<k,v>>>;		\
    template<typename k,typename v, typename cmp = std::less<k> >	\
    using multimap = std::multimap<k,v,cmp,				\
				   pool_allocator<std::pair<k,v>>>;	\
    template<typename k, typename cmp = std::less<k> >			\
    using set = std::set<k,cmp,pool_allocator<k>>; \
    template<typename v>						\
    using list = std::list<v,pool_allocator<v>>;			\
    template<typename v>						\
    using vector = std::vector<v,pool_allocator<v>>;			\
    template<typename k, typename v,					\
	     typename h=std::hash<k>,					\
	     typename eq = std::equal_to<k>>				\
    using unordered_map =						\
      std::unordered_map<k,v,h,eq,pool_allocator<std::pair<k,v>>>;	\
    template<typename v>						\
    using factory = mempool::factory<mempool::x,v>;			\
    inline size_t allocated_bytes() {					\
      return mempool::get_pool(mempool::x).allocated_bytes();		\
    }									\
    inline size_t allocated_items() {					\
      return mempool::get_pool(mempool::x).allocated_items();		\
    }									\
  };

DEFINE_MEMORY_POOLS_HELPER(P)

#undef P

// Use this for any type that is contained by a container (unless it
// is a class you defined; see below).
#define MEMPOOL_DEFINE_FACTORY(obj, factoryname, pool)			\
  template<>								\
  mempool::pool_allocator_base_t					\
    mempool::pool_allocator<pool::pool_ix,obj>::base = {};		\
  template<>								\
  typename pool::factory<obj>::allocator_type				\
    pool::factory<obj>::alloc = {};					\
  static pool::factory<obj> _factory_##factoryname;

// Use this for each class that belongs to a mempool.  For example,
//
//   class T {
//     MEMPOOL_CLASS_HELPERS();
//     ...
//   };
//
#define MEMPOOL_CLASS_HELPERS()						\
  void *operator new(size_t size);					\
  void *operator new[](size_t size) { assert(0 == "no array new"); }	\
  void  operator delete(void *);					\
  void  operator delete[](void *) { assert(0 == "no array delete"); }

// Use this in some particular .cc file to match each class with a
// MEMPOOL_CLASS_HELPERS().
#define MEMPOOL_DEFINE_OBJECT_FACTORY(obj,factoryname,pool)		\
  MEMPOOL_DEFINE_FACTORY(obj, factoryname, pool)			\
  void * obj::operator new(size_t size) {				\
    assert(size == sizeof(obj));					\
    return pool::factory<obj>::allocate();				\
  }									\
  void obj::operator delete(void *p)  {					\
    pool::factory<obj>::free(p);					\
  }

// for std::set
#define MEMPOOL_DEFINE_SET_FACTORY(t, factoryname, pool)		\
  MEMPOOL_DEFINE_FACTORY(std::_Rb_tree_node<t>,			\
			  factoryname##_rbtree_node, pool);

// for std::list
#define MEMPOOL_DEFINE_LIST_FACTORY(t, factoryname, pool)		\
  MEMPOOL_DEFINE_FACTORY(std::_List_node<t>,				\
			  factoryname##_list_node, pool);

// for std::map
#define MEMPOOL_DEFINE_MAP_FACTORY(k, v, factoryname, pool)		\
  typedef std::pair<k const,v> _factory_type_##factoryname##pair_t;	\
  MEMPOOL_DEFINE_FACTORY(						\
    _factory_type_##factoryname##pair_t,				\
    factoryname##_pair, pool);						\
  typedef std::pair<k const,v> _factory_type_##factoryname##pair_t;	\
  MEMPOOL_DEFINE_FACTORY(						\
    std::_Rb_tree_node<_factory_type_##factoryname##pair_t>,		\
    factoryname##_rbtree_node, pool);

// for std::unordered_map
#define MEMPOOL_DEFINE_UNORDERED_MAP_FACTORY(k, v, cached, factoryname, pool) \
  typedef std::pair<k const,v> _factory_type_##factoryname##pair_t;	\
  typedef std::__detail::_Hash_node<_factory_type_##factoryname##pair_t, \
				    cached>				\
  _factory_type_##factoryname##type;					\
  MEMPOOL_DEFINE_FACTORY(						\
    _factory_type_##factoryname##type,					\
    factoryname##_unordered_hash_node, pool);

#define MEMPOOL_DEFINE_UNORDERED_MAP_BASE_FACTORY(pool)		\
  MEMPOOL_DEFINE_FACTORY(std::__detail::_Hash_node_base*,	\
			 pool##_unordered_hash_node_ptr, pool);


#endif
