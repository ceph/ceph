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

#include <cstddef>
#include <map>
#include <unordered_map>
#include <set>
#include <vector>
#include <list>
#include <mutex>
#include <atomic>
#include <typeinfo>
#include <boost/container/flat_set.hpp>
#include <boost/container/flat_map.hpp>

#include <common/Formatter.h>
#include "include/ceph_assert.h"
#include "include/compact_map.h"
#include "include/compact_set.h"


/*

Memory Pools
============

A memory pool is a method for accounting the consumption of memory of
a set of containers and objects.

Memory pools are statically declared (see pool_index_t).

Each memory pool tracks a several statistics about things within the pool.

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
it.  :) There's really very little cost to a memory pool but the 
interface is built assuming maybe 10's of pools.

For each memory pool that's created a C++ namespace is also
automatically created (name is same as in DEFINE_MEMORY_POOLS_HELPER).
That namespace contains a set of common STL containers that are predefined
with the appropriate allocators.

Thus for mempool "osd" we have automatically available to us:

   mempool::osd::map
   mempool::osd::multimap
   mempool::osd::set
   mempool::osd::multiset
   mempool::osd::list
   mempool::osd::vector
   mempool::osd::unordered_map

These containers operate identically to the std::xxxxx named version of same
(same template parameters, etc.). However, while these are derived classes
from the std::xxxxx namespace version they all have allocators provided which
aren't compatible with the std::xxxxx version of the containers so that 
pointers, references, iterators, etc. aren't compatible.

Best practice is to use a typedef to define the mempool/container and then use
the typedef'ed name everywhere. Better documentation and easier to move
containers from std::xxxx or to a different mempool if the occasion warrants.
(This is particularly helpful with slab_containers, see slab_containers.h)

Putting objects in a mempool
----------------------------

The stuff above applies to containers. In order to put a particular object
into a namespace you must use the following declarations and mechanism. 
Note that this mechanism is per-object-type, in other words you can't put 
some instances of a class in one mempool and other instances of the class
in another mempool (or outside of any mempool) -- It's all or nothing at all :)

For a class declaration (the .h file):

  struct Foo {
    MEMPOOL_CLASS_HELPERS();
    ...
  };

Then, in an appropriate .cc file,

  MEMPOOL_DEFINE_OBJECT_FACTORY(Foo, foo, osd);

The second argument can generally be identical to the first, except
when the type contains a nested scope. For example, for
BlueStore::Onode, we need to do

  MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::Onode, bluestore_onode,
                                bluestore_meta);

(This is just because we need to name some static variables and we
can't use :: in a variable name.)

XXX Note: the new operator hard-codes the allocation size to the size of the
object given in MEMPOOL_DEFINE_OBJECT_FACTORY. For this reason, you cannot
incorporate mempools into a base class without also defining a helper/factory
for the child class as well (as the base class is usually smaller than the
child class).

In order to use the STL containers, simply use the namespaced variant
of the container type.  For example,

  mempool::osd::map<int> myvec;

Once this machinery is in place, you can do new/delete of these objects
without any additional decoration/machinery. It's all done through
a per-class operator new and operator delete.

NOTE: Right now it's strictly scalar new/data. Array new/delete will
assert out. There is no fundamental problem here. Just didn't get around
to writing the necessary code.

When you look at the statistics, what you'll find is that there is a
single container declared for ALL of your objects of this class.
(Effectively it's like an unordered_multiset<Foo>, except there's no
hashing or key involved.) In other words, if you use the new/delete 
factory (above), your statistics will show one container and 'N' items
where N is the number of currently existing instances of class 'Foo'.

Statistics Tracked
------------------

There are several statistics that are tracked for each mempool.

In addition, when 'debug' mode is enabled, the same statistics
are tracked on a per-typename basis. In other words, let's assume
you have a type: mempool::unittest_1::set<Foo> and there are 25
instances of that type, the debug-mode statistics will aggregate
the stats for all of those 25 instances. One note: 'debug' mode
CAN be dynamically enabled/disabled. The implementation of 'debug'
mode is 'sampled' by the constructor of each container. In other words
the debug information is only collected for containers that are ctor'ed
when 'debug' is ON. Once the construction is finished, 'debug' mode
is not sampled for that container -- forever. Thus if you have a
container that is declared at file scope, the only way to enable
'debug' for that container is to change the default in the mempool.cc 
file and recompile.

The stats are:

bytes       - number of bytes malloced
items       - number of elements in each container.
containers  - number of containers

There are 5 additional stats also part of this machinery, but
they only make sense if you understand slab_containers. So
read the description in slab_containers.h :)

slabs       - number of outstanding slabs
free_bytes  - number of malloc'ed bytes that are not in use.
free_items  - number of malloc'ed items that are not in use.
inuse_Items - bytes - free_bytes (but computed more cheaply)
inuse_bytes - items - free_items (but computed more cheaply)

By using the introspection feature (see below), 

Introspection
-------------

The simplest way to interrogate the process is with

  Formater *f = ...
  mempool::dump(f);

This will dump information about *all* memory pools.  When debug mode
is enabled, the runtime complexity of dump is O(num_shards *
num_types).  When debug name is disabled it is O(num_shards).

You can also interrogate a specific pool programmatically with

  size_t bytes       = mempool::unittest_2::allocated_bytes();
  size_t items       = mempool::unittest_2::allocated_items();
  size_t slabs       = mempool::unittest_2::slabs();
  size_t container   = mempool::unittest_2::containers();
  size_t free_bytes  = mempool::unittest_2::free_bytes();
  size_t free_items  = mempool::unittest_2::free_items();
  size_t inuse_bytes = mempool::unittest_2::inuse_bytes();
  size_T inuse_items = mempool::unittest_2::inuse_items();

The runtime complexity of these is O(num_shards)., However you're
likely to sustain O(num_shards/2) cache misses, so it's not
an operation that you want to throw around too much.

Note that you cannot easily query per-type, primarily because debug
mode is optional and you should not rely on that information being
available. Also, the type definitions are a bit ugly and filled
with STL-isms -- caveat developer.

*/

namespace mempool {

// --------------------------------------------------------------
// define memory pools

#define DEFINE_MEMORY_POOLS_HELPER(f) \
  f(bloom_filter)		      \
  f(bluestore_alloc)		      \
  f(bluestore_cache_data)	      \
  f(bluestore_cache_onode)	      \
  f(bluestore_cache_other)	      \
  f(bluestore_fsck)		      \
  f(bluestore_txc)		      \
  f(bluestore_writing_deferred)	      \
  f(bluestore_writing)		      \
  f(bluefs)			      \
  f(buffer_anon)		      \
  f(buffer_meta)		      \
  f(osd)			      \
  f(osd_mapbl)			      \
  f(osd_pglog)			      \
  f(osdmap)			      \
  f(osdmap_mapping)		      \
  f(pgmap)			      \
  f(mds_co)			      \
  f(unittest_1)			      \
  f(unittest_2)


// give them integer ids
#define P(x) mempool_##x,
enum pool_index_t {
  DEFINE_MEMORY_POOLS_HELPER(P)
  num_pools        // Must be last.
};
#undef P

extern bool debug_mode;
extern void set_debug_mode(bool d);

// --------------------------------------------------------------
class pool_t;

// we shard pool stats across many shard_t's to reduce the amount
// of cacheline ping pong.
enum {
  num_shard_bits = 5
};
enum {
  num_shards = 1 << num_shard_bits
};

//
// For each mempool, there is an array [num_shards] of these objects. The idea is that each shard
// will be in it's own cacheline, so as to avoid cache ping pongs. We assume that a cacheline is
// about 8 size_t objects (this is a pretty good assumption on all known processors)
//
struct shard_t {
  std::atomic<ssize_t> bytes = {0};
  std::atomic<ssize_t> items = {0};
  std::atomic<ssize_t> free_items = {0};  // Number of bytes malloc'ed but not in use by client (i.e., free in a slab)
  std::atomic<ssize_t> free_bytes = {0};  // Number of client objects      not in use by client (i.e., free in a slab)
  std::atomic<ssize_t> containers = {0};  // Number of containers (Object Factory = 1 container)
  std::atomic<ssize_t> slabs      = {0};  // Number of slabs
  char __padding[128 - sizeof(std::atomic<size_t>)*6];
} __attribute__ ((aligned (128)));

static_assert(sizeof(shard_t) == 128, "shard_t should be cacheline-sized");

struct stats_t {
  ssize_t items = 0;
  ssize_t bytes = 0;
  ssize_t free_bytes = 0;
  ssize_t free_items = 0;
  ssize_t containers = 0;
  ssize_t slabs = 0;
  void dump(ceph::Formatter *f) const {
    f->dump_int("items", items);
    f->dump_int("bytes", bytes);
    f->dump_int("free_bytes", free_bytes);
    f->dump_int("free_items", free_items);
    f->dump_int("containers", containers);
    f->dump_int("slabs", slabs);
  }

  stats_t& operator+=(const stats_t& o) {
    items += o.items;
    bytes += o.bytes;
    free_bytes = o.free_bytes;
    free_items = o.free_items;
    containers = o.containers;
    slabs = o.slabs;
    return *this;
  }
};

pool_t& get_pool(pool_index_t ix);
const char *get_pool_name(pool_index_t ix);

//
// Only used when debug mode is on, these accumulate per-object-type-signature statistics
// we didn't bother to shard them or cache-line align them
//
struct type_t {
  const char *type_name;
  size_t item_size;
  std::atomic<ssize_t> items = {0};  // signed
  std::atomic<ssize_t> free_items = {0};
  // XXX: should we have free_bytes as well?
  std::atomic<ssize_t> containers = {0};
  std::atomic<ssize_t> slabs = {0};
};

struct type_info_hash {
  std::size_t operator()(const std::type_info& k) const {
    return k.hash_code();
  }
};

class pool_t {
  alignas(shard_t) shard_t shard[num_shards];

  mutable std::mutex lock;  // only used for types list
  std::unordered_map<const char *, type_t> type_map;

public:
  //
  // How much this pool consumes. O(<num_shards>)
  //
  size_t allocated_bytes() const;
  size_t allocated_items() const;
  void adjust_count(ssize_t items, ssize_t bytes);
  size_t free_items() const;
  size_t free_bytes() const;
  size_t inuse_bytes() const;
  size_t inuse_items() const;
  size_t containers()  const;
  size_t slabs() const;
  size_t sumup(std::atomic<ssize_t> shard_t::*pm) const;
  size_t sumupdiff(std::atomic<ssize_t> shard_t::*pl,std::atomic<ssize_t> shard_t::*pr) const;

  shard_t* pick_a_shard() {
    // Dirt cheap, see:
    //   http://fossies.org/dox/glibc-2.24/pthread__self_8c_source.html
    size_t me = (size_t)pthread_self();
    size_t i = (me >> 3) & ((1 << num_shard_bits) - 1);
    return &shard[i];
  }

  type_t *get_type(const std::type_info& ti, size_t size) {
    std::lock_guard<std::mutex> l(lock);
    auto p = type_map.find(ti.name());
    if (p != type_map.end()) {
      return &p->second;
    }
    type_t &t = type_map[ti.name()];
    t.type_name = ti.name();
    t.item_size = size;
    t.containers ++;
    return &t;
  }

  // get pool stats.  by_type is not populated if !debug
  void get_stats(stats_t *total,
		 std::map<std::string, stats_t> *by_type) const;

  void dump(ceph::Formatter *f, stats_t *ptotal=0) const;
};

void dump(ceph::Formatter *f, size_t skip=0);


//
// There are actually a couple of types of allocators
// so we make a base class that can be customized.

template<pool_index_t pool_ix, typename T>
class pool_allocator_base_t {
protected:
  pool_t *pool;
  type_t *type = nullptr;

  // a sorta-constructor
  void ctor(bool force_register,size_t sizeofT) {
    pool = &get_pool(pool_ix);
    if (debug_mode || force_register) {
      type = pool->get_type(typeid(T), sizeofT);
    }
    shard_t *shard = pool->pick_a_shard();
    shard->containers++;
  }

  void dtor() {
    if (type) type->containers--;
    shard_t *shard = pool->pick_a_shard();
    shard->containers--;
  }

public:
  typedef T value_type;
  typedef value_type *pointer;
  typedef const value_type * const_pointer;
  typedef value_type& reference;
  typedef const value_type& const_reference;
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;

  void destroy(T* p) {
    p->~T();
  }

  template<class U>
  void destroy(U *p) {
    p->~U();
  }

  void construct(T* p, const T& val) {
    ::new ((void *)p) T(val);
  }

  template<class U, class... Args> void construct(U* p,Args&&... args) {
    ::new((void *)p) U(std::forward<Args>(args)...);
  }

  bool operator==(const pool_allocator_base_t&) const { return true; }
  bool operator!=(const pool_allocator_base_t&) const { return false; }
};

// STL allocator for use with regular containers.  All actual state
// is stored in the static pool_allocator_base_t, which saves us from
// passing the allocator to container constructors.
template<pool_index_t pool_ix, typename T>
class pool_allocator : public pool_allocator_base_t<pool_ix,T> {
public:
  typedef pool_allocator<pool_ix, T> allocator_type;

  template<typename U> struct rebind {
    typedef pool_allocator<pool_ix,U> other;
  };

  // a sorta-copy constructor, but a different type :)
  template<typename U>
  pool_allocator(const pool_allocator<pool_ix,U>&) {
    this->ctor(false,sizeof(T));
  }

  pool_allocator(bool force_register=false) {
    this->ctor(force_register,sizeof(T));
  }

  ~pool_allocator() {
    this->dtor();
  }

  T* allocate(size_t n, void *p = nullptr) {
    size_t total = sizeof(T) * n;
    shard_t *shard = this->pool->pick_a_shard();
    shard->bytes += total;
    shard->items += n;
    if (this->type) {
      this->type->items += n;
    }
    T* r = reinterpret_cast<T*>(new char[total]);
    return r;
  }

  void deallocate(T* p, size_t n) {
    size_t total = sizeof(T) * n;
    shard_t *shard = this->pool->pick_a_shard();
    shard->bytes -= total;
    shard->items -= n;
    if (this->type) {
      this->type->items -= n;
    }
    delete[] reinterpret_cast<char*>(p);
  }

  T* allocate_aligned(size_t n, size_t align, void *p = nullptr) {
    size_t total = sizeof(T) * n;
    shard_t *shard = this->pool->pick_a_shard();
    shard->bytes += total;
    shard->items += n;
    if (this->type) {
      this->type->items += n;
    }
    char *ptr;
    int rc = ::posix_memalign((void**)(void*)&ptr, align, total);
    if (rc)
      throw std::bad_alloc();
    T* r = reinterpret_cast<T*>(ptr);
    return r;
  }

  void deallocate_aligned(T* p, size_t n) {
    size_t total = sizeof(T) * n;
    shard_t *shard = this->pool->pick_a_shard();
    shard->bytes -= total;
    shard->items -= n;
    if (this->type) {
      this->type->items -= n;
    }
    ::free(p);
  }
};

//
// An specialized sorta-allocator for slabs, sorta-belongs in slab_containers.h,
// I put it here because it's all about the mempool stats
//
template<pool_index_t pool_ix, typename T>
class pool_slab_allocator : public pool_allocator_base_t<pool_ix,T> {

public:

  pool_slab_allocator(bool force_register=false, size_t sizeof_T = sizeof(T)) {
    this->ctor(force_register,sizeof_T);
  }

  ~pool_slab_allocator() {
    this->dtor();
  }

  //
  // Slab allocate and free. They aren't always symmettric because slots in slab
  // get iterated over to put them on the freelist -> which causes some accounting there :)
  // But when you delete a slab you don't iterate over the slots, so we have to do that here
  //
  void slab_allocate(size_t n, size_t sizeof_T, size_t extra) {
    size_t total = sizeof_T * n;
    shard_t *shard = this->pool->pick_a_shard();
    shard->bytes += total + extra;
    shard->items += n;
    shard->slabs++;
    if (this->type) {
      this->type->items += n;
      this->type->slabs++;
    }
  }

  void slab_deallocate(size_t n, size_t sizeof_T, size_t extra,bool free_free_items) {
    size_t total = sizeof_T * n;
    shard_t *shard = this->pool->pick_a_shard();
    shard->bytes -= total + extra;
    shard->items -= n;
    shard->slabs --;
    if (free_free_items) {
      shard->free_items -= n;  
      shard->free_bytes -= total;
    }
    if (this->type) {
      this->type->items -= n;
      this->type->slabs--;
      if (free_free_items) this->type->free_items -= n;
    }
  }

  void slab_item_allocate(size_t sizeof_T) {
    shard_t *shard = this->pool->pick_a_shard();
    shard->free_bytes -= sizeof_T;
    shard->free_items -= 1;
    if (this->type) {
      this->type->free_items -= 1;
    }
  }

  void slab_item_free(size_t sizeof_T) {
    shard_t *shard = this->pool->pick_a_shard();
    shard->free_bytes += sizeof_T;
    shard->free_items += 1;
    if (this->type) {
      this->type->free_items += 1;
    }
  }

};

// There is one factory associated with every type that lives in a
// mempool.

template<pool_index_t pool_ix,typename o>
class factory {
public:
  typedef pool_allocator<pool_ix,o> allocator_type;
  static allocator_type alloc;

  static void *allocate() {
    return (void *)alloc.allocate(1);
  }
  static void free(void *p) {
    alloc.deallocate((o *)p, 1);
  }
};


// Namespace mempool

#define P(x)								\
  namespace x {								\
    static const mempool::pool_index_t id = mempool::mempool_##x;	\
    template<typename v>						\
    using pool_allocator = mempool::pool_allocator<id,v>;		\
                                                                        \
    using string = std::basic_string<char,std::char_traits<char>,       \
                                     pool_allocator<char>>;             \
                                                                        \
    template<typename k,typename v, typename cmp = std::less<k> >	\
    using map = std::map<k, v, cmp,					\
			 pool_allocator<std::pair<const k,v>>>;		\
                                                                        \
    template<typename k,typename v, typename cmp = std::less<k> >       \
    using compact_map = compact_map<k, v, cmp,                          \
			 pool_allocator<std::pair<const k,v>>>;         \
                                                                        \
    template<typename k,typename v, typename cmp = std::less<k> >       \
    using compact_multimap = compact_multimap<k, v, cmp,                \
			 pool_allocator<std::pair<const k,v>>>;         \
                                                                        \
    template<typename k, typename cmp = std::less<k> >                  \
    using compact_set = compact_set<k, cmp, pool_allocator<k>>;         \
                                                                        \
    template<typename k,typename v, typename cmp = std::less<k> >	\
    using multimap = std::multimap<k,v,cmp,				\
				   pool_allocator<std::pair<const k,	\
							    v>>>;	\
                                                                        \
    template<typename k, typename cmp = std::less<k> >			\
    using set = std::set<k,cmp,pool_allocator<k>>;			\
                                                                        \
    template<typename k, typename cmp = std::less<k> >			\
    using flat_set = boost::container::flat_set<k,cmp,pool_allocator<k>>; \
									\
    template<typename k, typename v, typename cmp = std::less<k> >	\
    using flat_map = boost::container::flat_map<k,v,cmp,		\
						pool_allocator<std::pair<k,v>>>; \
                                                                        \
    template<typename v>						\
    using list = std::list<v,pool_allocator<v>>;			\
                                                                        \
    template<typename v>						\
    using vector = std::vector<v,pool_allocator<v>>;			\
                                                                        \
    template<typename k, typename v,					\
	     typename h=std::hash<k>,					\
	     typename eq = std::equal_to<k>>				\
    using unordered_map =						\
      std::unordered_map<k,v,h,eq,pool_allocator<std::pair<const k,v>>>;\
                                                                        \
    template<typename v>						\
    using factory = mempool::factory<id,v>;				\
                                                                        \
    inline size_t allocated_bytes() {					\
      return mempool::get_pool(id).allocated_bytes();			\
    }									\
    inline size_t allocated_items() {					\
      return mempool::get_pool(id).allocated_items();			\
    }									\
    inline size_t containers() {                                        \
      return mempool::get_pool(id).containers();                        \
    }                                                                   \
    inline size_t free_bytes() {					\
      return mempool::get_pool(id).free_bytes();			\
    }									\
    inline size_t free_items() {					\
      return mempool::get_pool(id).free_items();			\
    }									\
    inline size_t inuse_bytes() {					\
      return mempool::get_pool(id).inuse_bytes();			\
    }									\
    inline size_t inuse_items() {					\
      return mempool::get_pool(id).inuse_items();			\
    }									\
    inline size_t slabs() {		          			\
      return mempool::get_pool(id).slabs();	         		\
    }									\
  };

DEFINE_MEMORY_POOLS_HELPER(P)

#undef P

};

// the elements allocated by mempool is in the same memory space as the ones
// allocated by the default allocator. so compare them in an efficient way:
// libstdc++'s std::equal is specialized to use memcmp if T is integer or
// pointer. this is good enough for our usecase. use
// std::is_trivially_copyable<T> to expand the support to more types if
// nececssary.
template<typename T, mempool::pool_index_t pool_index>
bool operator==(const std::vector<T, std::allocator<T>>& lhs,
		const std::vector<T, mempool::pool_allocator<pool_index, T>>& rhs)
{
  return (lhs.size() == rhs.size() &&
	  std::equal(lhs.begin(), lhs.end(), rhs.begin()));
}

template<typename T, mempool::pool_index_t pool_index>
bool operator!=(const std::vector<T, std::allocator<T>>& lhs,
		const std::vector<T, mempool::pool_allocator<pool_index, T>>& rhs)
{
  return !(lhs == rhs);
}

template<typename T, mempool::pool_index_t pool_index>
bool operator==(const std::vector<T, mempool::pool_allocator<pool_index, T>>& lhs,
		const std::vector<T, std::allocator<T>>& rhs)
{
  return rhs == lhs;
}

template<typename T, mempool::pool_index_t pool_index>
bool operator!=(const std::vector<T, mempool::pool_allocator<pool_index, T>>& lhs,
		const std::vector<T, std::allocator<T>>& rhs)
{
  return !(lhs == rhs);
}

// Use this for any type that is contained by a container (unless it
// is a class you defined; see below).
#define MEMPOOL_DECLARE_FACTORY(obj, factoryname, pool)			\
  namespace mempool {							\
    namespace pool {							\
      extern pool_allocator<obj> alloc_##factoryname;			\
    }									\
  }

#define MEMPOOL_DEFINE_FACTORY(obj, factoryname, pool)			\
  namespace mempool {							\
    namespace pool {							\
      pool_allocator<obj> alloc_##factoryname = {true};			\
    }									\
  }

// Use this for each class that belongs to a mempool.  For example,
//
//   class T {
//     MEMPOOL_CLASS_HELPERS();
//     ...
//   };
//
#define MEMPOOL_CLASS_HELPERS()						\
  void *operator new(size_t size);					\
  void *operator new[](size_t size) noexcept {				\
    ceph_abort_msg("no array new");					\
    return nullptr; }							\
  void  operator delete(void *);					\
  void  operator delete[](void *) { ceph_abort_msg("no array delete"); }


// Use this in some particular .cc file to match each class with a
// MEMPOOL_CLASS_HELPERS().
#define MEMPOOL_DEFINE_OBJECT_FACTORY(obj,factoryname,pool)		\
  MEMPOOL_DEFINE_FACTORY(obj, factoryname, pool)			\
  void *obj::operator new(size_t size) {				\
    return mempool::pool::alloc_##factoryname.allocate(1); \
  }									\
  void obj::operator delete(void *p)  {					\
    return mempool::pool::alloc_##factoryname.deallocate((obj*)p, 1);	\
  }

#endif
