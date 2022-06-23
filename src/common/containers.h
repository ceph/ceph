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

#include <cstdint>
#include <type_traits>

namespace ceph::containers {

// tiny_vector – CPU friendly, small_vector-like container for mutexes,
// atomics and other non-movable things.
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
// For the special case of no internal slots (InternalCapacity eq 0),
// tiny_vector doesn't require moving any elements (changing pointers
// is enough), and thus should be MoveConstructible.
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

template<typename Value, std::size_t InternalCapacity = 0>
class tiny_vector {
  // NOTE: to avoid false sharing consider aligning to cache line
  using storage_unit_t = \
    std::aligned_storage_t<sizeof(Value), alignof(Value)>;

  std::size_t _size = 0;
  storage_unit_t* const data = nullptr;
  storage_unit_t internal[InternalCapacity];

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
  //
  // For the sake of supporting the ceph::make_mutex() family of
  // factories, which relies on C++17's guaranteed copy elision,
  // the emplacer provides `data()` to retrieve the location for
  // constructing the instance with placement-new. This is handy
  // as the `emplace()` depends on perfect forwarding, and thus
  // interfere with the elision for cases like:
  // ```
  //   emplacer.emplace(ceph::make_mutex("mtx-name"));
  // ```
  // See: https://stackoverflow.com/a/52498826

  class emplacer {
    friend class tiny_vector;

    tiny_vector* parent;
    emplacer(tiny_vector* const parent)
      : parent(parent) {
    }

  public:
    void* data() {
      void* const ret = &parent->data[parent->_size++];
      parent = nullptr;
      return ret;
    }

    template<class... Args>
    void emplace(Args&&... args) {
      if (parent) {
        new (data()) Value(std::forward<Args>(args)...);
      }
    }
  };

  template<typename F>
  tiny_vector(const std::size_t count, F&& f)
    : data(count <= InternalCapacity ? internal
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

    const auto data_addr = reinterpret_cast<std::uintptr_t>(data);
    const auto this_addr = reinterpret_cast<std::uintptr_t>(this);
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

} // namespace ceph::containers

#endif // CEPH_COMMON_CONTAINERS_H
