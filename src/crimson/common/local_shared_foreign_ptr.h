// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/smp.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace crimson {

/**
 * local_shared_foreign_ptr
 *
 * See seastar/include/seastar/core/sharded.hh:foreign_ptr
 *
 * seastar::foreign_ptr wraps a smart ptr by proxying the copy() and destructor
 * operations back to the original core.  This works well except that copy()
 * requires a cross-core call.  We need a smart_ptr which allows cross-core
 * caching of (for example) OSDMaps, but we want to avoid the overhead inherent
 * in incrementing the source smart_ptr on every copy.  Thus,
 * local_shared_foreign_ptr maintains a core-local foreign_ptr back to the
 * original core instance with core-local ref counting.
 */
template <typename PtrType>
class local_shared_foreign_ptr {
  using element_type = typename std::pointer_traits<PtrType>::element_type;
  using pointer = element_type*;

  seastar::lw_shared_ptr<seastar::foreign_ptr<PtrType>> ptr;

  /// Wraps a pointer object and remembers the current core.
  local_shared_foreign_ptr(seastar::foreign_ptr<PtrType> &&fptr)
    : ptr(seastar::make_lw_shared(std::move(fptr))) {}

  template <typename T>
  friend local_shared_foreign_ptr<T> make_local_shared_foreign(
    seastar::foreign_ptr<T> &&);

public:
  /// Constructs a null local_shared_foreign_ptr<>.
  local_shared_foreign_ptr() = default;

  /// Constructs a null local_shared_foreign_ptr<>.
  local_shared_foreign_ptr(std::nullptr_t) : local_shared_foreign_ptr() {}

  /// Moves a local_shared_foreign_ptr<> to another object.
  local_shared_foreign_ptr(local_shared_foreign_ptr&& other) = default;

  /// Copies a local_shared_foreign_ptr<>
  local_shared_foreign_ptr(const local_shared_foreign_ptr &other) = default;

  /// Releases reference to ptr eventually releasing the contained foreign_ptr
  ~local_shared_foreign_ptr() = default;

  /// Creates a copy of this foreign ptr. Only works if the stored ptr is copyable.
  seastar::future<seastar::foreign_ptr<PtrType>> get_foreign() {
    return ptr->copy();
  }

  /// Accesses the wrapped object.
  element_type& operator*() const noexcept {
    assert(ptr && *ptr);
    return **ptr;
  }
  /// Accesses the wrapped object.
  element_type* operator->() const noexcept {
    assert(ptr && *ptr);
    return &**ptr;
  }

  /// Access the raw pointer to the wrapped object.
  pointer get() const noexcept {
    assert(!ptr || (ptr && *ptr));
    return ptr ? ptr->get() : nullptr;
  }

  /// Return the owner-shard of the contained foreign_ptr.
  unsigned get_owner_shard() const noexcept {
    assert(ptr && *ptr);
    return ptr->get_owner_shard();
  }

  /// Checks whether the wrapped pointer is non-null.
  operator bool() const noexcept {
    assert(!ptr || (ptr && *ptr));
    return static_cast<bool>(ptr);
  }

  /// Move-assigns a \c local_shared_foreign_ptr<>.
  local_shared_foreign_ptr& operator=(local_shared_foreign_ptr&& other) noexcept {
    ptr = std::move(other.ptr);
    return *this;
  }
};

/// Wraps a smart_ptr T in a local_shared_foreign_ptr<>.
template <typename T>
local_shared_foreign_ptr<T> make_local_shared_foreign(
  seastar::foreign_ptr<T> &&ptr) {
  return local_shared_foreign_ptr<T>(std::move(ptr));
}

/// Wraps ptr in a local_shared_foreign_ptr<>.
template <typename T>
local_shared_foreign_ptr<T> make_local_shared_foreign(T &&ptr) {
  return make_local_shared_foreign<T>(
    seastar::make_foreign(std::forward<T>(ptr)));
}

}

namespace seastar {

template<typename T>
struct is_smart_ptr<crimson::local_shared_foreign_ptr<T>> : std::true_type {};

}
