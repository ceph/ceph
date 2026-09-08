// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <atomic>
#include <thread>
#include <vector>

#include "common/perf_counters.h"

#include "mgr/mgr_perf_counters.h"
#include "mgr/MgrMapCache.h"

#include "gtest/gtest.h"

PerfCounters *perfcounter = nullptr;

using namespace std;

TEST(LFUCache, Get) {
	LFUCache<int> c{{"foo"}, 100};
	c.insert("foo", 1);
	int foo = c.get("foo");
	ASSERT_EQ(foo, 1);
}

TEST(LFUCache, Erase) {
	LFUCache<int> c{{"foo"}, 100};
	c.insert("foo", 1);
	int foo;
	if (!c.try_get("foo", &foo)) {
		FAIL();
	}
	ASSERT_EQ(foo, 1);
	c.erase("foo");
  try{
    foo = c.get("foo");
    FAIL();
  } catch (std::out_of_range& e) {
    SUCCEED();
  }
}

TEST(LFUCache, Clear) {
	LFUCache<int> c{{"osd_map", "pg_dump", "pg_stats"}, 100};
	c.insert("osd_map", 1);
	c.insert("pg_dump", 2);
	c.insert("pg_stats", 3);
	ASSERT_EQ(c.size(), 3);
	c.clear();
	ASSERT_EQ(c.size(), 0);
	try{
		c.get("osd_map");
		FAIL();
	} catch (std::out_of_range& e) {
		SUCCEED();
	}
}

TEST(LFUCache, NotEnabled) {
	LFUCache<int> c{{"foo"}, 100};
	c.insert("foo", 1);
	int foo = c.get("foo");
	ASSERT_EQ(foo, 1);
	c.set_enabled(false);
  try{
	foo = c.get("foo");
	FAIL();
  } catch (std::out_of_range& e) {
	SUCCEED();
  }
}

TEST(LFUCache, SizeLimit) {
	LFUCache<int> c{{"foo", "osd_map", "pg_dump", "pg_stats", "mon_status"}, 4, true};
	c.insert("foo", 1);
	c.insert("osd_map", 2);
	c.insert("pg_dump", 3);
	c.insert("pg_stats", 4);
	c.get("foo"); // foo hits 1
	c.get("pg_dump"); // pg_dump hits 1
	for (int i = 0; i < 100; ++i) {
		c.get("pg_stats"); // pg_stats hits 100
	}
	c.insert("mon_status", 5); // This should evict "osd_map" since it has the least hits
	ASSERT_EQ(c.size(), 4);
	int foo = c.get("foo");
	int pg_dump = c.get("pg_dump");
	int pg_stats = c.get("pg_stats");
	int mon_status = c.get("mon_status");
	try {
		c.get("osd_map"); // This should throw an exception since it was evicted
		FAIL(); // If nothing throws, this will fail
	} catch (std::out_of_range& e) {
		ASSERT_EQ(foo, 1);
		ASSERT_EQ(pg_dump, 3);
		ASSERT_EQ(pg_stats, 4);
		ASSERT_EQ(mon_status, 5);
		SUCCEED();
	}
}

TEST(LFUCache, HitRatio) {
	LFUCache<int> c{{"osd_map", "pg_dump", "pg_stats"}, 100, true};
	c.insert("osd_map", 1);
	c.insert("pg_dump", 2);
	c.insert("pg_stats", 3);
	c.get("osd_map"); // hits 1
	c.get("osd_map"); // hits 2
	c.get("osd_map"); // hits 3
	c.get("pg_dump"); // hits 4
	std::pair<uint64_t, uint64_t> hit_miss_ratio = {c.get_hits(), c.get_misses()};
	ASSERT_EQ(std::get<1>(hit_miss_ratio), 3);
	ASSERT_EQ(std::get<0>(hit_miss_ratio), 4);
}

TEST(LFUCache, InsertResNewKey) {
  LFUCache<int> c{{"foo"}, 100};
  auto res = c.insert("foo", 42);
  ASSERT_TRUE(res.inserted);
  ASSERT_FALSE(res.replaced.has_value());
  ASSERT_FALSE(res.evicted.has_value());
}

TEST(LFUCache, InsertResReplaced) {
  LFUCache<int> c{{"foo"}, 100};
  c.insert("foo", 1);
  auto res = c.insert("foo", 2);
  ASSERT_TRUE(res.inserted);
  ASSERT_TRUE(res.replaced.has_value());
  ASSERT_EQ(res.replaced.value(), 1);
  ASSERT_FALSE(res.evicted.has_value());
  // new value is live in cache
  int v;
  ASSERT_TRUE(c.try_get("foo", &v));
  ASSERT_EQ(v, 2);
}

TEST(LFUCache, InsertResEvicted) {
  LFUCache<int> c{{"foo", "osd_map", "pg_dump", "pg_stats"}, 3};
  c.insert("osd_map", 10);
  c.insert("pg_dump", 20);
  c.insert("pg_stats", 30);
  // Give pg_dump and pg_stats hits so osd_map has the clear minimum (0 hits)
  c.get("pg_dump");
  c.get("pg_stats");
  auto res = c.insert("foo", 99);
  ASSERT_TRUE(res.inserted);
  ASSERT_FALSE(res.replaced.has_value());
  ASSERT_TRUE(res.evicted.has_value());
  ASSERT_EQ(res.evicted.value(), 10);
}

TEST(LFUCache, InsertResRejectedWhenDisabled) {
  LFUCache<int> c{{"foo"}, 100};
  c.set_enabled(false);
  auto res = c.insert("foo", 1);
  ASSERT_FALSE(res.inserted);
  ASSERT_FALSE(res.replaced.has_value());
  ASSERT_FALSE(res.evicted.has_value());
  ASSERT_EQ(c.size(), 0);
}

TEST(LFUCache, InsertResRejectedForUnknownKey) {
  LFUCache<int> c{{"foo"}, 100};
  auto res = c.insert("not_an_allowed_key", 1);
  ASSERT_FALSE(res.inserted);
  ASSERT_EQ(c.size(), 0);
}

TEST(LFUCache, InsertResRejectedAfterDisableDuringWait) {
  // Simulate the post-lock enabled re-check: disable the cache, then call
  // insert directly on LFUCache (bypassing the outer can_write_cache guard)
  // by re-enabling just long enough to pass can_write_cache, then disabling.
  // We approximate this by disabling between two sequential inserts and
  // verifying the second is rejected.
  LFUCache<int> c{{"foo"}, 100};
  auto res1 = c.insert("foo", 1);
  ASSERT_TRUE(res1.inserted);
  c.set_enabled(false);
  auto res2 = c.insert("foo", 2);
  ASSERT_FALSE(res2.inserted);
  // cache was cleared by set_enabled(false)
  ASSERT_EQ(c.size(), 0);
}

TEST(LFUCache, ConcurrentReads) {
  LFUCache<int> c{{"foo"}, 100, true};
  c.insert("foo", 42);

  std::atomic<int> success_count{0};
  std::vector<std::thread> threads;

  // Launch 10 threads doing 1000 reads each
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&c, &success_count]() {
      for (int j = 0; j < 1000; ++j) {
        int val;
        if (c.try_get("foo", &val)) {
          success_count++;
        }
      }
    });
  }

  for (auto& t : threads) t.join();

  ASSERT_EQ(success_count, 10000);
  ASSERT_EQ(c.get_hits(), 10000);
}
