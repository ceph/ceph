// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "rgw/rgw_bucket_sync_cache.h"
#include <gtest/gtest.h>

using namespace rgw::bucket_sync;

// helper function to construct rgw_bucket_shard
static rgw_bucket_shard make_key(const std::string& tenant,
                                 const std::string& bucket, int shard)
{
  auto key = rgw_bucket_key{tenant, bucket};
  return rgw_bucket_shard{key, shard};
}

TEST(BucketSyncCache, ReturnCachedPinned)
{
  auto cache = Cache::create(0);
  const auto key = make_key("", "1", 0);
  auto h1 = cache->get(key); // pin
  h1->counter = 1;
  auto h2 = cache->get(key);
  EXPECT_EQ(1, h2->counter);
}

TEST(BucketSyncCache, ReturnNewUnpinned)
{
  auto cache = Cache::create(0);
  const auto key = make_key("", "1", 0);
  cache->get(key)->counter = 1; // pin+unpin
  EXPECT_EQ(0, cache->get(key)->counter);
}

TEST(BucketSyncCache, DistinctTenant)
{
  auto cache = Cache::create(2);
  const auto key1 = make_key("a", "bucket", 0);
  const auto key2 = make_key("b", "bucket", 0);
  cache->get(key1)->counter = 1;
  EXPECT_EQ(0, cache->get(key2)->counter);
}

TEST(BucketSyncCache, DistinctShards)
{
  auto cache = Cache::create(2);
  const auto key1 = make_key("", "bucket", 0);
  const auto key2 = make_key("", "bucket", 1);
  cache->get(key1)->counter = 1;
  EXPECT_EQ(0, cache->get(key2)->counter);
}

TEST(BucketSyncCache, DontEvictPinned)
{
  auto cache = Cache::create(0);

  const auto key1 = make_key("", "1", 0);
  const auto key2 = make_key("", "2", 0);

  auto h1 = cache->get(key1);
  EXPECT_EQ(key1, h1->key);
  auto h2 = cache->get(key2);
  EXPECT_EQ(key2, h2->key);
  EXPECT_EQ(key1, h1->key); // h1 unchanged
}

TEST(BucketSyncCache, HandleLifetime)
{
  const auto key = make_key("", "1", 0);

  Handle h; // test that handles keep the cache referenced
  {
    auto cache = Cache::create(0);
    h = cache->get(key);
  }
  EXPECT_EQ(key, h->key);
}

TEST(BucketSyncCache, TargetSize)
{
  auto cache = Cache::create(2);

  const auto key1 = make_key("", "1", 0);
  const auto key2 = make_key("", "2", 0);
  const auto key3 = make_key("", "3", 0);

  // fill cache up to target_size=2
  cache->get(key1)->counter = 1;
  cache->get(key2)->counter = 2;
  // test that each unpinned entry is still cached
  EXPECT_EQ(1, cache->get(key1)->counter);
  EXPECT_EQ(2, cache->get(key2)->counter);
  // overflow the cache and recycle key1
  cache->get(key3)->counter = 3;
  // test that the oldest entry was recycled
  EXPECT_EQ(0, cache->get(key1)->counter);
}

TEST(BucketSyncCache, HandleMoveAssignEmpty)
{
  auto cache = Cache::create(0);

  const auto key1 = make_key("", "1", 0);
  const auto key2 = make_key("", "2", 0);

  Handle j1;
  {
    auto h1 = cache->get(key1);
    j1 = std::move(h1); // assign over empty handle
    EXPECT_EQ(key1, j1->key);
  }
  auto h2 = cache->get(key2);
  EXPECT_EQ(key1, j1->key); // j1 stays pinned
}

TEST(BucketSyncCache, HandleMoveAssignExisting)
{
  const auto key1 = make_key("", "1", 0);
  const auto key2 = make_key("", "2", 0);

  Handle h1;
  {
    auto cache1 = Cache::create(0);
    h1 = cache1->get(key1);
  } // j1 has the last ref to cache1
  {
    auto cache2 = Cache::create(0);
    auto h2 = cache2->get(key2);
    h1 = std::move(h2); // assign over existing handle
  }
  EXPECT_EQ(key2, h1->key);
}

TEST(BucketSyncCache, HandleCopyAssignEmpty)
{
  auto cache = Cache::create(0);

  const auto key1 = make_key("", "1", 0);
  const auto key2 = make_key("", "2", 0);

  Handle j1;
  {
    auto h1 = cache->get(key1);
    j1 = h1; // assign over empty handle
    EXPECT_EQ(&*h1, &*j1);
  }
  auto h2 = cache->get(key2);
  EXPECT_EQ(key1, j1->key); // j1 stays pinned
}

TEST(BucketSyncCache, HandleCopyAssignExisting)
{
  const auto key1 = make_key("", "1", 0);
  const auto key2 = make_key("", "2", 0);

  Handle h1;
  {
    auto cache1 = Cache::create(0);
    h1 = cache1->get(key1);
  } // j1 has the last ref to cache1
  {
    auto cache2 = Cache::create(0);
    auto h2 = cache2->get(key2);
    h1 = h2; // assign over existing handle
    EXPECT_EQ(&*h1, &*h2);
  }
  EXPECT_EQ(key2, h1->key);
}
