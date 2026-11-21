// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Hetzner Cloud GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <gtest/gtest.h>
#include "common/dout.h"
#include "global/global_context.h"
#include "rgw_cache.h"

#define dout_subsys ceph_subsys_rgw

static ObjectCacheInfo make_versioned(uint64_t ver, const std::string& tag,
                                      const std::string& payload = "") {
  ObjectCacheInfo info;
  info.status = 0;
  info.flags = CACHE_FLAG_OBJV | CACHE_FLAG_DATA;
  info.version.ver = ver;
  info.version.tag = tag;
  bufferlist bl;
  bl.append(payload);
  info.data = bl;
  return info;
}

class ObjectCacheTest : public ::testing::Test {
protected:
  ObjectCache cache;
  NoDoutPrefix dpp;

  ObjectCacheTest() : dpp(g_ceph_context, dout_subsys) {
    cache.set_ctx(g_ceph_context);
    cache.set_enabled(true);
  }
};

// A stale put (same tag, lower ver) must not overwrite a newer cached entry.
TEST_F(ObjectCacheTest, StaleVersionRejected)
{
  auto info3 = make_versioned(3, "tagA", "v3-data");
  cache.put(&dpp, "key1", info3, nullptr);

  auto info2 = make_versioned(2, "tagA", "v2-data");
  cache.put(&dpp, "key1", info2, nullptr);

  ObjectCacheInfo got;
  ASSERT_EQ(0, cache.get(&dpp, "key1", got, CACHE_FLAG_OBJV | CACHE_FLAG_DATA, nullptr));
  EXPECT_EQ(3u, got.version.ver);
  EXPECT_EQ("tagA", got.version.tag);
}

// A newer put (same tag, higher ver) must overwrite the cached entry.
TEST_F(ObjectCacheTest, NewerVersionAccepted)
{
  auto info2 = make_versioned(2, "tagA", "v2-data");
  cache.put(&dpp, "key2", info2, nullptr);

  auto info4 = make_versioned(4, "tagA", "v4-data");
  cache.put(&dpp, "key2", info4, nullptr);

  ObjectCacheInfo got;
  ASSERT_EQ(0, cache.get(&dpp, "key2", got, CACHE_FLAG_OBJV | CACHE_FLAG_DATA, nullptr));
  EXPECT_EQ(4u, got.version.ver);
}

// A put with a different tag always wins (object was recreated under the same key).
TEST_F(ObjectCacheTest, DifferentTagAlwaysAccepted)
{
  auto infoA = make_versioned(5, "tagA", "v5-data");
  cache.put(&dpp, "key3", infoA, nullptr);

  auto infoB = make_versioned(1, "tagB", "v1-data");
  cache.put(&dpp, "key3", infoB, nullptr);

  ObjectCacheInfo got;
  ASSERT_EQ(0, cache.get(&dpp, "key3", got, CACHE_FLAG_OBJV | CACHE_FLAG_DATA, nullptr));
  EXPECT_EQ(1u, got.version.ver);
  EXPECT_EQ("tagB", got.version.tag);
}

// The first put for a key always succeeds (no prior entry to compare against).
TEST_F(ObjectCacheTest, NoExistingEntryAlwaysAccepted)
{
  auto info = make_versioned(2, "tagA", "data");
  cache.put(&dpp, "key4", info, nullptr);

  ObjectCacheInfo got;
  ASSERT_EQ(0, cache.get(&dpp, "key4", got, CACHE_FLAG_OBJV | CACHE_FLAG_DATA, nullptr));
  EXPECT_EQ(2u, got.version.ver);
}

// A put without CACHE_FLAG_OBJV bypasses the version guard and always overwrites.
TEST_F(ObjectCacheTest, NoObjvFlagSkipsGuard)
{
  auto info3 = make_versioned(3, "tagA", "v3-data");
  cache.put(&dpp, "key5", info3, nullptr);

  ObjectCacheInfo info_no_v;
  info_no_v.status = 0;
  info_no_v.flags = CACHE_FLAG_DATA;
  bufferlist bl;
  bl.append("no-objv-data");
  info_no_v.data = bl;
  cache.put(&dpp, "key5", info_no_v, nullptr);

  // The entry was overwritten: CACHE_FLAG_OBJV is now absent (type miss).
  ObjectCacheInfo got;
  EXPECT_EQ(-ENOENT, cache.get(&dpp, "key5", got, CACHE_FLAG_OBJV, nullptr));
  // But CACHE_FLAG_DATA alone still hits.
  EXPECT_EQ(0, cache.get(&dpp, "key5", got, CACHE_FLAG_DATA, nullptr));
}

// Equal versions are not considered stale; the put goes through.
TEST_F(ObjectCacheTest, EqualVersionAccepted)
{
  auto info3a = make_versioned(3, "tagA", "first");
  cache.put(&dpp, "key6", info3a, nullptr);

  auto info3b = make_versioned(3, "tagA", "second");
  cache.put(&dpp, "key6", info3b, nullptr);

  ObjectCacheInfo got;
  ASSERT_EQ(0, cache.get(&dpp, "key6", got, CACHE_FLAG_OBJV | CACHE_FLAG_DATA, nullptr));
  EXPECT_EQ(3u, got.version.ver);
}
