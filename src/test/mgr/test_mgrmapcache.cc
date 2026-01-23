
#include <iostream>
#include "common/perf_counters.h"
PerfCounters *perfcounter = nullptr;

#include "mgr/mgr_perf_counters.h"
#include "mgr/MgrMapCache.h"

#include "gtest/gtest.h"

using namespace std;

TEST(LFUCache, Get) {
	LFUCache<string, int> c{100};
	c.insert("foo", 1);
	int foo = c.get("foo");
	ASSERT_EQ(foo, 1);
}

TEST(LFUCache, Erase) {
	LFUCache<string, int> c{100};
	c.insert("foo", 1);
	int foo = c.get("foo");
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
	LFUCache<string, int> c{100};
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
	LFUCache<string, int> c{100};
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
	LFUCache<string, int> c{4, true};
	c.insert("foo", 1);
	c.insert("osd_map", 2);
	c.insert("pg_dump", 3);
	c.insert("pg_stats", 4);
	c.get("foo"); // foo hits 1
	c.get("pg_dump"); // pg_dump hits 1
	for (int i = 0; i < 100; ++i) {
		c.get("pg_stats"); // pg_stats hits 100
	}
	c.insert("mon_map", 5); // This should evict "osd_map" since it has the least hits
	ASSERT_EQ(c.size(), 4);
	int foo = c.get("foo");
	int pg_dump = c.get("pg_dump");
	int pg_stats = c.get("pg_stats");
	try {
		int mon_map = c.get("mon_map");
		ASSERT_EQ(mon_map, 5);
		c.get("osd_map"); // This should throw an exception since it was evicted
		FAIL(); // If nothing throws, this will fail
	} catch (std::out_of_range& e) {
		ASSERT_EQ(foo, 1); // Should be 1, as it was evicted
		ASSERT_EQ(pg_dump, 3);
		ASSERT_EQ(pg_stats, 4);
		SUCCEED();
	}
}

TEST(LFUCache, HitRatio) {
	LFUCache<string, int> c{100, true};
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
