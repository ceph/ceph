#include "gtest/gtest.h"
#include "rgw_usage_cache.h"
#include <cstdio>

class RGWUsageCacheTest : public ::testing::Test {
protected:
  std::string db_path = "/tmp/rgw_test_lmdb";
  CephContext *cct = nullptr;
  RGWUsageCache *cache = nullptr;

  void SetUp() override {
    cct = (new CephContext(CephContext::get_module_type("test")));
    cache = new RGWUsageCache(cct, db_path);
    ASSERT_EQ(cache->init(), 0);
  }

  void TearDown() override {
    if (cache) {
      cache->close();
      delete cache;
    }
    if (cct) {
      delete cct;
    }
    std::string cmd = "rm -rf " + db_path;
    system(cmd.c_str());
  }
};

TEST_F(RGWUsageCacheTest, PutAndGetBucketUsage) {
  RGWUsageStats stats_in{1234, 56}, stats_out;
  ASSERT_EQ(cache->put_bucket_usage("bucket:foo/bar", stats_in), 0);
  ASSERT_TRUE(cache->get_bucket_usage("bucket:foo/bar", &stats_out));
  EXPECT_EQ(stats_out.used_bytes, 1234);
  EXPECT_EQ(stats_out.num_objects, 56);
}

TEST_F(RGWUsageCacheTest, PutAndGetUserUsage) {
  RGWUsageStats stats_in{7890, 100}, stats_out;
  ASSERT_EQ(cache->put_user_usage("user:alice", stats_in), 0);
  ASSERT_TRUE(cache->get_user_usage("user:alice", &stats_out));
  EXPECT_EQ(stats_out.used_bytes, 7890);
  EXPECT_EQ(stats_out.num_objects, 100);
}
