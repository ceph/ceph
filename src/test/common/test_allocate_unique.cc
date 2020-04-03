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

#include "common/allocate_unique.h"
#include <string>
#include <vector>
#include <gtest/gtest.h>

namespace {

// allocation events recorded by logging_allocator
struct event {
  size_t size;
  bool allocated; // true for allocate(), false for deallocate()
};
using event_log = std::vector<event>;

template <typename T>
struct logging_allocator {
  event_log *const log;

  using value_type = T;

  explicit logging_allocator(event_log *log) : log(log) {}
  logging_allocator(const logging_allocator& other) : log(other.log) {}

  template <typename U>
  logging_allocator(const logging_allocator<U>& other) : log(other.log) {}

  T* allocate(size_t n, const void* hint=0)
  {
    auto p = std::allocator<T>{}.allocate(n, hint);
    log->emplace_back(event{n * sizeof(T), true});
    return p;
  }
  void deallocate(T* p, size_t n)
  {
    std::allocator<T>{}.deallocate(p, n);
    if (p) {
      log->emplace_back(event{n * sizeof(T), false});
    }
  }
};

} // anonymous namespace

namespace ceph {

TEST(AllocateUnique, Allocate)
{
  event_log log;
  auto alloc = logging_allocator<char>{&log};
  {
    auto p = allocate_unique<char>(alloc, 'a');
    static_assert(std::is_same_v<decltype(p),
                  std::unique_ptr<char, deallocator<logging_allocator<char>>>>);
    ASSERT_TRUE(p);
    ASSERT_EQ(1, log.size());
    EXPECT_EQ(1, log.front().size);
    EXPECT_EQ(true, log.front().allocated);
  }
  ASSERT_EQ(2, log.size());
  EXPECT_EQ(1, log.back().size);
  EXPECT_EQ(false, log.back().allocated);
}

TEST(AllocateUnique, RebindAllocate)
{
  event_log log;
  auto alloc = logging_allocator<char>{&log};
  {
    auto p = allocate_unique<std::string>(alloc, "a");
    static_assert(std::is_same_v<decltype(p),
                  std::unique_ptr<std::string,
                                  deallocator<logging_allocator<std::string>>>>);
    ASSERT_TRUE(p);
    ASSERT_EQ(1, log.size());
    EXPECT_EQ(sizeof(std::string), log.front().size);
    EXPECT_EQ(true, log.front().allocated);
  }
  ASSERT_EQ(2, log.size());
  EXPECT_EQ(sizeof(std::string), log.back().size);
  EXPECT_EQ(false, log.back().allocated);
}

} // namespace ceph
