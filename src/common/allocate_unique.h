// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <memory>

namespace ceph {

/// An allocator-aware 'Deleter' for std::unique_ptr<T, Deleter>. The
/// allocator's traits must have a value_type of T.
template <typename Alloc>
class deallocator {
  using allocator_type = Alloc;
  using allocator_traits = std::allocator_traits<allocator_type>;
  using pointer = typename allocator_traits::pointer;
  allocator_type alloc;
 public:
  explicit deallocator(const allocator_type& alloc) noexcept : alloc(alloc) {}
  void operator()(pointer p) {
    allocator_traits::destroy(alloc, p);
    allocator_traits::deallocate(alloc, p, 1);
  }
};

/// deallocator alias that rebinds Alloc's value_type to T
template <typename T, typename Alloc>
using deallocator_t = deallocator<typename std::allocator_traits<Alloc>
      ::template rebind_alloc<T>>;

/// std::unique_ptr alias that rebinds Alloc if necessary, and avoids repetition
/// of the template parameter T.
template <typename T, typename Alloc>
using allocated_unique_ptr = std::unique_ptr<T, deallocator_t<T, Alloc>>;


/// Returns a std::unique_ptr whose memory is managed by the given allocator.
template <typename T, typename Alloc, typename... Args>
static auto allocate_unique(Alloc& alloc, Args&&... args)
  -> allocated_unique_ptr<T, Alloc>
{
  static_assert(!std::is_array_v<T>, "allocate_unique() does not support T[]");

  using allocator_type = typename std::allocator_traits<Alloc>
      ::template rebind_alloc<T>;
  using allocator_traits = std::allocator_traits<allocator_type>;
  auto a = allocator_type{alloc};
  auto p = allocator_traits::allocate(a, 1);
  try {
    allocator_traits::construct(a, p, std::forward<Args>(args)...);
    return {p, deallocator<allocator_type>{a}};
  } catch (...) {
    allocator_traits::deallocate(a, p, 1);
    throw;
  }
}

} // namespace ceph
