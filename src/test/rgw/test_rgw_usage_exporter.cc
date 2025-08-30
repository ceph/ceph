#include "gtest/gtest.h"
#include "rgw_usage_cache.h"

#include <unistd.h>

TEST(RGWUsageCache, PutGet) {
  std::string path = "usage_cache_test.mdb";
  ::unlink(path.c_str());
  RGWUsageCache cache(path);
  RGWUsageRecord urec{123, 2};
  ASSERT_EQ(0, cache.put_user("user1", urec));
  RGWUsageRecord out{};
  ASSERT_EQ(0, cache.get_user("user1", &out));
  EXPECT_EQ(urec.used_bytes, out.used_bytes);
  EXPECT_EQ(urec.num_objects, out.num_objects);

  RGWUsageRecord brec{456, 3};
  ASSERT_EQ(0, cache.put_bucket("bucket1", brec));
  ASSERT_EQ(0, cache.get_bucket("bucket1", &out));
  EXPECT_EQ(brec.used_bytes, out.used_bytes);
  EXPECT_EQ(brec.num_objects, out.num_objects);

  ::unlink(path.c_str());
}

