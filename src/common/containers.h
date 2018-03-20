// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
//
// Ceph - scalable distributed file system
//
// Copyright (C) 2018 Red Hat, Inc.
//
// This is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License version 2.1, as published by the Free Software
// Foundation.  See file COPYING.
//

#ifndef CEPH_COMMON_CONTAINERS_H
#define CEPH_COMMON_CONTAINERS_H

#include <type_traits>

#include "include/intarith.h"

namespace ceph::containers {

// tiny_vector - a CPU-friendly container like small_vector but for
// mutexes, atomics and other non-movable things.
//
// The purpose of the container is to store arbitrary number of objects
// with absolutely minimal requirements regarding constructibility
// and assignability while minimizing memory indirection.
// There is no obligation for MoveConstructibility, CopyConstructibility,
// MoveAssignability, CopyAssignability nor even DefaultConstructibility
// which allows to handle std::mutexes, std::atomics or any type embedding
// them.
//
// Few requirements translate into tiny interface. The container isn't
// Copy- nor MoveConstructible. Although it does offer random access
// iterator, insertion in the middle is not allowed. The maximum number
// of elements must be known at run-time. This shouldn't be an issue in
// the intended use case: sharding.
//
// Alternatives:
//  1. std::vector<boost::optional<ValueT>> initialized with the known
//     size and emplace_backed(). boost::optional inside provides
//     the DefaultConstructibility. Imposes extra memory indirection.
//  2. boost::container::small_vector + boost::optional always
//     requires MoveConstructibility.
//  3. boost::container::static_vector feed via emplace_back().
//     Good for performance but enforces upper limit on elements count.
//     For sharding this means we can't handle arbitrary number of
//     shards (weird configs).
//  4. std::unique_ptr<ValueT>: extra indirection together with memory
//     fragmentation.

template<typename Value, std::size_t Capacity>
class tiny_vector {
  using storage_unit_t = \
    std::aligned_storage_t<sizeof(Value), alignof(Value)>;

  std::size_t _size = 0;
  storage_unit_t* const data = nullptr;
  storage_unit_t internal[Capacity];

public:
  typedef std::size_t size_type;
  typedef std::add_lvalue_reference_t<Value> reference;
  typedef std::add_const_t<reference> const_reference;
  typedef std::add_pointer_t<Value> pointer;

  // emplacer is the piece of weirdness that comes from handling
  // unmovable-and-uncopyable things. The only way to instantiate
  // such types I know is to create instances in-place perfectly
  // forwarding necessary data to constructor.
  // Abstracting that is the exact purpose of emplacer.
  //
  // The usage scenario is:
  //   1. The tiny_vector's ctor is provided with a) maximum number
  //      of instances and b) a callable taking emplacer.
  //   2. The callable can (but isn't obliged to!) use emplacer to
  //      construct an instance without knowing at which address
  //      in memory it will be put. Callable is also supplied with
  //      an unique integer from the range <0, maximum number of
  //      instances).
  //   3. If callable decides to instantiate, it calls ::emplace
  //      of emplacer passing all arguments required by the type
  //      hold in tiny_vector.
  //
  // Example:
  // ```
  //   static constexpr const num_internally_allocated_slots = 32;
  //   tiny_vector<T, num_internally_allocated_slots> mytinyvec {
  //     num_of_instances,
  //     [](const size_t i, auto emplacer) {
  //       emplacer.emplace(argument_for_T_ctor);
  //     }
  //   }
  // ```

  class emplacer {
    friend class tiny_vector;

    tiny_vector* parent;
    emplacer(tiny_vector* const parent)
      : parent(parent) {
    }

  public:
    template<class... Args>
    void emplace(Args&&... args) {
      if (!parent) {
        return;
      }
      new (&parent->data[parent->_size++]) Value(std::forward<Args>(args)...);
      parent = nullptr;
    }
  };

  template<typename F>
  tiny_vector(const std::size_t count, F&& f)
    : data(count <= Capacity ? internal
                             : new storage_unit_t[count]) {
    for (std::size_t i = 0; i < count; ++i) {
      // caller MAY emplace up to `count` elements but it IS NOT
      // obliged to do so. The emplacer guarantees that the limit
      // will never be exceeded.
      f(i, emplacer(this));
    }
  }

  ~tiny_vector() {
    for (auto& elem : *this) {
      elem.~Value();
    }

    const auto data_addr = reinterpret_cast<uintptr_t>(data);
    const auto this_addr = reinterpret_cast<uintptr_t>(this);
    if (data_addr < this_addr ||
        data_addr >= this_addr + sizeof(*this)) {
      delete[] data;
    }
  }

  reference       operator[](size_type pos) {
    return reinterpret_cast<reference>(data[pos]);
  }
  const_reference operator[](size_type pos) const {
    return reinterpret_cast<const_reference>(data[pos]);
  }

  size_type size() const {
    return _size;
  }

  pointer begin() {
    return reinterpret_cast<pointer>(&data[0]);
  }
  pointer end() {
    return reinterpret_cast<pointer>(&data[_size]);
  }

  const pointer begin() const {
    return reinterpret_cast<pointer>(&data[0]);
  }
  const pointer end() const {
    return reinterpret_cast<pointer>(&data[_size]);
  }

  const pointer cbegin() const {
    return reinterpret_cast<pointer>(&data[0]);
  }
  const pointer cend() const {
    return reinterpret_cast<pointer>(&data[_size]);
  }
};


static constexpr const std::size_t CACHE_LINE_SIZE = 64;

// align_wrapper - helper class for providing cache line alignment to
// any Value type.
//
// We're going with extended alignment here. AFAIK the standard makes
// it non-obligatory for compiler vendors, albeit this feature should
// be hopefully available on most platforms.

template <typename Value>
struct alignas(CACHE_LINE_SIZE) align_wrapper : public Value {
  template<class... Args>
  align_wrapper(Args&&... args)
    : Value(std::forward<Args>(args)...) {
  }
};


// shard_vector - a tiny_vector specially designated for shard handling.
//
// The container puts the same requirements on Value as tiny_vector but
// provides interface enriched with facilities for fast shard accesses.
// It is possible because assuming (end enforcing in run-time) Capacity
// is power-of-2. Thanks to that, the modulo operation is implementable
// with bit twiddling in place of costly divisions.
//
// Additionally, the shard_vector takes care of aligning shards to cache
// line boundary. This is useful as, typically, handled type has a mutex
// or plain atomic inside. The optimization allows to avoid costly ping-
// pong between CPUs as well as memory fencing.

template<typename Value, std::size_t Capacity>
class shard_vector : public tiny_vector<align_wrapper<Value>, Capacity> {
  typedef tiny_vector<align_wrapper<Value>, Capacity> base_t;
  typedef typename base_t::size_type size_type;
  typedef typename base_t::reference reference;
  typedef typename base_t::const_reference const_reference;

public:
  template<typename F>
  shard_vector(const ceph::math::p2_t<size_type> count, F&& f)
    : base_t(count, std::forward<F>(f)) {
    assert(count == base_t::size());
  }

  // Yes, we're overriding (not hiding!) the size() from base_t.
  ceph::math::p2_t<size_type> size() const {
    return ceph::math::p2_t<size_type>::from_p2(base_t::size());
  }

  reference get_shard(const size_type selector) {
    return (*this)[selector % size()];
  }

  const_reference get_shard(const size_type selector) const {
    return (*this)[selector % size()];
  }
};

} // namespace ceph::containers

#endif // CEPH_COMMON_CONTAINERS_H
