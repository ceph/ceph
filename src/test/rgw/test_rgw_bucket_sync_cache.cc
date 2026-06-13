// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include <common/ceph_time.h>
#include <gtest/gtest.h>

#include "rgw_bucket_sync_cache.h"

using namespace rgw::bucket_sync;

using Shard = ShardState;
using Gen = GenState;

namespace {
/// Create a key suitable for a given cache
///
/// \tparam State One of `Shard` or `Gen`.
///
/// \param[in] tenant Owning tenant
/// \param[in] bucket Bucket name
/// \param[in] shard Bucket shard, ignored if `State` is `Gen`.
///
/// \return A key for the given bucket
template <typename State>
auto make_key(const std::string& tenant, const std::string& bucket, int shard) =
    delete;

template <>
auto
make_key<Shard>(const std::string& tenant, const std::string& bucket, int shard)
{
  auto key = rgw_bucket_key{tenant, bucket};
  return rgw_bucket_shard{std::move(key), shard};
}

template <>
auto
make_key<Gen>(
    const std::string& tenant,
    const std::string& bucket,
    // Dummy parameter for overload
    int)
{
  auto key = rgw_bucket_key{tenant, bucket};
  rgw_bucket b{std::move(key)};
  return b.get_key();
}

/// Stick an integer into a state
///
/// \tparam State One of `Shard` or `Gen`.
///
/// \param[inout] state State to modify
/// \param[in] value Value to store
template <typename State>
void mutate(State&, unsigned int value) = delete;

template <>
void
mutate<Shard>(ShardState& state, unsigned int value)
{
  state.counter = value;
}

template <>
void
mutate<Gen>(GenState& state, unsigned int value)
{
  state.last_future_generation_recovery = ceph::coarse_mono_time{
      ceph::timespan{value}};
}

/// Retrieve an integer from a state
///
/// \note Intended only for values stored with `mutate`, anything else
/// will be truncated to the capacity of an `unsigned int`.
///
/// \tparam State One of `Shard` or `Gen`.
///
/// \param[in] state State to modify
///
/// \return The integer previously stored in the state
template <typename State>
unsigned int extract(const State&) = delete;

template <>
unsigned int
extract<Shard>(const Shard& state)
{
  return static_cast<unsigned int>(state.counter);
}

template <>
unsigned int
extract<Gen>(const Gen& state)
{
  return static_cast<unsigned int>(
      state.last_future_generation_recovery.time_since_epoch().count());
}
} // namespace

template <typename State>
void
ReturnCachedPinned()
{
  auto cache = Cache<State>::create(0);
  const auto key = make_key<State>("", "1", 0);
  auto h1 = cache->get(key, std::nullopt); // pin
  mutate(*h1, 1);
  auto h2 = cache->get(key, std::nullopt);
  EXPECT_EQ(1, extract(*h2));
}

TEST(BucketShardSyncCache, ReturnCachedPinned) { ReturnCachedPinned<Shard>(); }

TEST(BucketGenSyncCache, ReturnCachedPinned) { ReturnCachedPinned<Gen>(); }

template <typename State>
void
ReturnNewUnpinned()
{
  auto cache = Cache<State>::create(0);
  const auto key = make_key<State>("", "1", 0);
  mutate(*cache->get(key, std::nullopt), 1); // pin+unpin
  EXPECT_EQ(0, extract(*cache->get(key, std::nullopt)));
}

TEST(BucketShardSyncCache, ReturnNewUnpinned) { ReturnNewUnpinned<Shard>(); }

TEST(BucketGenSyncCache, ReturnNewUnpinned) { ReturnNewUnpinned<Gen>(); }

template <typename State>
void
DistinctTenant()
{
  auto cache = Cache<State>::create(2);
  const auto key1 = make_key<State>("a", "bucket", 0);
  const auto key2 = make_key<State>("b", "bucket", 0);
  mutate(*cache->get(key1, std::nullopt), 1);
  EXPECT_EQ(0, extract(*cache->get(key2, std::nullopt)));
}

TEST(BucketShardSyncCache, DistinctTenant) { DistinctTenant<Shard>(); }

TEST(BucketGenSyncCache, DistinctTenant) { DistinctTenant<Gen>(); }

TEST(BucketShardSyncCache, DistinctShards)
{
  auto cache = ShardCache::create(2);
  const auto key1 = make_key<Shard>("", "bucket", 0);
  const auto key2 = make_key<Shard>("", "bucket", 1);
  cache->get(key1, std::nullopt)->counter = 1;
  EXPECT_EQ(0, cache->get(key2, std::nullopt)->counter);
}

template <typename State>
void
DistinctGen()
{
  auto cache = Cache<State>::create(2);
  const auto key = make_key<State>("", "bucket", 0);
  std::optional<uint64_t> gen1; // empty
  std::optional<uint64_t> gen2 = 5;
  mutate(*cache->get(key, gen1), 1);
  EXPECT_EQ(0, extract(*cache->get(key, gen2)));
}

TEST(BucketShardSyncCache, DistinctGen) { DistinctGen<Shard>(); }

TEST(BucketGenSyncCache, DistinctGen) { DistinctGen<Gen>(); }

template <typename State>
void
DontEvictPinned()
{
  auto cache = Cache<State>::create(0);

  const auto key1 = make_key<State>("", "1", 0);
  const auto key2 = make_key<State>("", "2", 0);

  auto h1 = cache->get(key1, std::nullopt);
  EXPECT_EQ(key1, h1->key.first);
  auto h2 = cache->get(key2, std::nullopt);
  EXPECT_EQ(key2, h2->key.first);
  EXPECT_EQ(key1, h1->key.first); // h1 unchanged
}

TEST(BucketShardSyncCache, DontEvictPinned) { DontEvictPinned<Shard>(); }

TEST(BucketGenSyncCache, DontEvictPinned) { DontEvictPinned<Gen>(); }

template <typename State>
void
HandleLifetime()
{
  const auto key = make_key<State>("", "1", 0);

  Handle<State> h; // test that handles keep the cache referenced
  {
    auto cache = Cache<State>::create(0);
    h = cache->get(key, std::nullopt);
  }
  EXPECT_EQ(key, h->key.first);
}

TEST(BucketShardSyncCache, HandleLifetime) { HandleLifetime<Shard>(); }

TEST(BucketGenSyncCache, HandleLifetime) { HandleLifetime<Gen>(); }

template <typename State>
void
TargetSize()
{
  auto cache = Cache<State>::create(2);

  const auto key1 = make_key<State>("", "1", 0);
  const auto key2 = make_key<State>("", "2", 0);
  const auto key3 = make_key<State>("", "3", 0);

  // fill cache up to target_size=2
  mutate(*cache->get(key1, std::nullopt), 1);
  mutate(*cache->get(key2, std::nullopt), 2);
  // test that each unpinned entry is still cached
  EXPECT_EQ(1, extract(*cache->get(key1, std::nullopt)));
  EXPECT_EQ(2, extract(*cache->get(key2, std::nullopt)));
  // overflow the cache and recycle key1
  mutate(*cache->get(key3, std::nullopt), 3);
  // test that the oldest entry was recycled
  EXPECT_EQ(0, extract(*cache->get(key1, std::nullopt)));
}

TEST(BucketShardSyncCache, TargetSize) { TargetSize<Shard>(); }

TEST(BucketGenSyncCache, TargetSize) { TargetSize<Gen>(); }

template <typename State>
void
HandleMoveAssignEmpty()
{
  auto cache = Cache<State>::create(0);

  const auto key1 = make_key<State>("", "1", 0);
  const auto key2 = make_key<State>("", "2", 0);

  Handle<State> j1;
  {
    auto h1 = cache->get(key1, std::nullopt);
    j1 = std::move(h1); // assign over empty handle
    EXPECT_EQ(key1, j1->key.first);
  }
  auto h2 = cache->get(key2, std::nullopt);
  EXPECT_EQ(key1, j1->key.first); // j1 stays pinned
}

TEST(BucketShardSyncCache, HandleMoveAssignEmpty)
{
  HandleMoveAssignEmpty<Shard>();
}

TEST(BucketGenSyncCache, HandleMoveAssignEmpty)
{
  HandleMoveAssignEmpty<Gen>();
}

template <typename State>
void
HandleMoveAssignExisting()
{
  const auto key1 = make_key<State>("", "1", 0);
  const auto key2 = make_key<State>("", "2", 0);

  Handle<State> h1;
  {
    auto cache1 = Cache<State>::create(0);
    h1 = cache1->get(key1, std::nullopt);
  } // h1 has the last ref to cache1
  {
    auto cache2 = Cache<State>::create(0);
    auto h2 = cache2->get(key2, std::nullopt);
    h1 = std::move(h2); // assign over existing handle
  }
  EXPECT_EQ(key2, h1->key.first);
}

TEST(BucketShardSyncCache, HandleMoveAssignExisting)
{
  HandleMoveAssignExisting<Shard>();
}

TEST(BucketGenSyncCache, HandleMoveAssignExisting)
{
  HandleMoveAssignExisting<Gen>();
}

template <typename State>
void
HandleCopyAssignEmpty()
{
  auto cache = Cache<State>::create(0);

  const auto key1 = make_key<State>("", "1", 0);
  const auto key2 = make_key<State>("", "2", 0);

  Handle<State> j1;
  {
    auto h1 = cache->get(key1, std::nullopt);
    j1 = h1; // assign over empty handle
    EXPECT_EQ(&*h1, &*j1);
  }
  auto h2 = cache->get(key2, std::nullopt);
  EXPECT_EQ(key1, j1->key.first); // j1 stays pinned
}

TEST(BucketShardSyncCache, HandleCopyAssignEmpty)
{
  HandleCopyAssignEmpty<Shard>();
}

TEST(BucketGenSyncCache, HandleCopyAssignEmpty)
{
  HandleCopyAssignEmpty<Gen>();
}

template <typename State>
void
HandleCopyAssignExisting()
{
  const auto key1 = make_key<State>("", "1", 0);
  const auto key2 = make_key<State>("", "2", 0);

  Handle<State> h1;
  {
    auto cache1 = Cache<State>::create(0);
    h1 = cache1->get(key1, std::nullopt);
  } // h1 has the last ref to cache1
  {
    auto cache2 = Cache<State>::create(0);
    auto h2 = cache2->get(key2, std::nullopt);
    h1 = h2; // assign over existing handle
    EXPECT_EQ(&*h1, &*h2);
  }
  EXPECT_EQ(key2, h1->key.first);
}

TEST(BucketShardSyncCache, HandleCopyAssignExisting)
{
  HandleCopyAssignExisting<Shard>();
}

TEST(BucketGenSyncCache, HandleCopyAssignExisting)
{
  HandleCopyAssignExisting<Gen>();
}
