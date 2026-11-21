// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#include "common/StackStringStream.h"

#include <semaphore>
#include <thread>
#include <boost/context/fiber.hpp>
#include <gtest/gtest.h>

TEST(CachedStackStringStream, Reuse)
{
  std::thread([] {
    CachedStackStringStream::sss* expected;
    {
      CachedStackStringStream stream;
      expected = stream.get();
      *stream << std::string(8192, 'x') << std::hex << 255;
      EXPECT_EQ(stream->str(), std::string(8192, 'x') + "ff");
      stream->setstate(std::ios::badbit);
    }
    CachedStackStringStream stream;
    EXPECT_EQ(stream.get(), expected);
    EXPECT_TRUE(stream->strv().empty());
    EXPECT_TRUE(stream->good());
    *stream << 255;
    EXPECT_EQ(stream->str(), "255");
  }).join();
}

TEST(CachedStackStringStream, AfterThreadCacheDestruction)
{
  struct LateUser {
    std::unique_ptr<CachedStackStringStream> held;
    bool* checked = nullptr;
    ~LateUser() {
      held.reset();
      CachedStackStringStream stream;
      *stream << "after cache destruction";
      EXPECT_EQ(stream->str(), "after cache destruction");
      *checked = true;
    }
  };
  bool checked = false;
  std::thread([&] {
    // Register this destructor first, so it runs after the cache destructor.
    thread_local LateUser late;
    late.checked = &checked;
    late.held = std::make_unique<CachedStackStringStream>();
    CachedStackStringStream stream;
    *stream << "populate cache";
  }).join();
  EXPECT_TRUE(checked);
}

TEST(CachedStackStringStream, CoroutineMigration)
{
  constexpr int rounds = 1000;
  std::binary_semaphore turns[2]{std::binary_semaphore{1},
                                 std::binary_semaphore{0}};
  boost::context::fiber fiber([](boost::context::fiber&& caller) {
    for (int i = 0; i < rounds; ++i) {
      {
        CachedStackStringStream stream;
        *stream << "message " << i;
      }
      caller = std::move(caller).resume();
    }
    return std::move(caller);
  });
  auto worker = [&](int id) {
    CachedStackStringStream::sss* expected;
    {
      CachedStackStringStream prime;
      expected = prime.get();
    }
    // Alternate the coroutine between two live threads. After each resume,
    // its stream must have been returned to the executing thread's cache.
    // Optimized builds must not hoist a TLS address out of the coroutine loop.
    for (int i = id; i <= rounds; i += 2) {
      turns[id].acquire();
      fiber = std::move(fiber).resume();
      {
        CachedStackStringStream probe;
        EXPECT_EQ(probe.get(), expected) << "worker " << id << ", round " << i;
      }
      turns[1 - id].release();
    }
  };
  std::thread first(worker, 0);
  std::thread second(worker, 1);
  first.join();
  second.join();
  EXPECT_FALSE(fiber);
}
