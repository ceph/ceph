// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_COMMON_ALLOCATOR_H
#define CEPH_COMMON_ALLOCATOR_H

#include "acconfig.h"

#ifdef LIBTCMALLOC_MISSING_ALIGNED_ALLOC
#include <malloc.h>
#include <new>
#endif
#include <memory>

namespace ceph {

#ifdef LIBTCMALLOC_MISSING_ALIGNED_ALLOC

// If libtcmalloc is missing 'aligned_alloc', provide a new allocator class that
// uses memalign which is what newer versions of tcmalloc do internally. C++17
// will automatically use 'operator new(size_t, align_val_t)' for aligned
// structures, which will invoke the missing 'aligned_alloc' tcmalloc function.
// This method was added to tcmalloc (gperftools) in commit d406f228 after
// the 2.6.1 release was tagged.
template <typename T>
struct allocator : public std::allocator<T> {
  using pointer = typename std::allocator<T>::pointer;
  using size_type = typename std::allocator<T>::size_type;

  template<class U>
  struct rebind {
    typedef allocator<U> other;
  };

  allocator() noexcept {
  }

  allocator(const allocator& other) noexcept : std::allocator<T>(other) {
  }

  template <class U>
  allocator(const allocator<U>& other) noexcept : std::allocator<T>(other) {
  }

  pointer allocate(size_type n, const void* hint = nullptr) {
    if (n > this->max_size()) {
      throw std::bad_alloc();
    }

    if (alignof(T) > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
      return static_cast<T*>(memalign(alignof(T), n * sizeof(T)));
    }
    return std::allocator<T>::allocate(n, hint);
  }
};

#else  // LIBTCMALLOC_MISSING_ALIGNED_ALLOC

// re-use the full std::allocator implementation if tcmalloc is functional

template <typename T>
using allocator = std::allocator<T>;

#endif // LIBTCMALLOC_MISSING_ALIGNED_ALLOC

} // namespace ceph

#endif // CEPH_COMMON_ALLOCATOR_H

