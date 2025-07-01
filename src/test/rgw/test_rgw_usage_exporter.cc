#include "rgw_usage_cache.h"
#include <gtest/gtest.h>
#include <filesystem>

TEST(RGWUsageCache, PutGet) {
  std::string dir = std::filesystem::temp_directory_path() / "rgw_usage_test";
  std::filesystem::remove_all(dir);
  RGWUsageCache cache(dir);
  ASSERT_EQ(0, cache.put_user("u", 10, 2));
  ASSERT_EQ(0, cache.put_bucket("b", 5, 1));
  uint64_t bytes=0, objs=0;
  ASSERT_TRUE(cache.get_user("u", bytes, objs));
  ASSERT_EQ(10u, bytes);
  ASSERT_EQ(2u, objs);
  ASSERT_TRUE(cache.get_bucket("b", bytes, objs));
  ASSERT_EQ(5u, bytes);
  ASSERT_EQ(1u, objs);
}

