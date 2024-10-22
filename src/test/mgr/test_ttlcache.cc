#include <iostream>

#include "mgr/TTLCache.h"
#include "gtest/gtest.h"

using namespace std;

TEST(TTLCache, Get) {
	TTLCache<string, int> c{100};
	c.insert("foo", 1);
	int foo = c.get("foo");
	ASSERT_EQ(foo, 1);
}

TEST(TTLCache, Erase) {
	TTLCache<string, int> c{100};
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

TEST(TTLCache, Clear) {
	TTLCache<string, int> c{100};
	c.insert("foo", 1);
	c.insert("foo2", 2);
	c.clear();
	ASSERT_FALSE(c.size());
}

TEST(TTLCache, NoTTL) {
	TTLCache<string, int> c{100};
	c.insert("foo", 1);
	int foo = c.get("foo");
	ASSERT_EQ(foo, 1);
	c.set_ttl(0);
	c.insert("foo2", 2);
  try{
    foo = c.get("foo2");
    FAIL();
  } catch (std::out_of_range& e) {
    SUCCEED();
  }
}

TEST(TTLCache, SizeLimit) {
	TTLCache<string, int> c{100, 2};
	c.insert("foo", 1);
	c.insert("foo2", 2);
	c.insert("foo3", 3);
	ASSERT_EQ(c.size(), 2);
}

TEST(TTLCache, HitRatio) {
	TTLCache<string, int> c{100};
	c.insert("foo", 1);
	c.insert("foo2", 2);
	c.insert("foo3", 3);
	c.get("foo2");
	c.get("foo3");
	std::pair<uint64_t, uint64_t> hit_miss_ratio = c.get_hit_miss_ratio();
	ASSERT_EQ(std::get<1>(hit_miss_ratio), 3);
	ASSERT_EQ(std::get<0>(hit_miss_ratio), 2);
}
