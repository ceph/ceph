// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include <iostream>
#include <sstream>
#include <filesystem>
#include <random>
#include <boost/intrusive_ptr.hpp>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"
#include "rgw/rgw_usage_cache.h"

namespace fs = std::filesystem;

// Global CephContext for all tests
static boost::intrusive_ptr<CephContext> g_cct_holder;
static CephContext* g_cct = nullptr;

class TestRGWUsagePerfCounters : public ::testing::Test {
protected:
  std::unique_ptr<rgw::UsageCache> cache;
  std::string test_db_path;
  
  void SetUp() override {
    ASSERT_NE(g_cct, nullptr) << "CephContext not initialized";
    
    // Generate unique test database path
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1000000, 9999999);
    test_db_path = "/tmp/test_rgw_perf_" + std::to_string(dis(gen)) + ".mdb";
    
    // Initialize cache with CephContext to enable internal perf counters
    rgw::UsageCache::Config config;
    config.db_path = test_db_path;
    config.max_db_size = 1 << 20;  // 1MB
    config.ttl = std::chrono::seconds(2);
    
    // Use the constructor that accepts CephContext to enable perf counters
    cache = std::make_unique<rgw::UsageCache>(g_cct, config);
    
    ASSERT_EQ(0, cache->init());
  }
  
  void TearDown() override {
    cache->shutdown();
    cache.reset();
    
    // Clean up test database
    try {
      fs::remove(test_db_path);
      fs::remove(test_db_path + "-lock");
    } catch (const std::exception& e) {
      // Ignore cleanup errors
    }
  }
};

TEST_F(TestRGWUsagePerfCounters, BasicMetrics) {
  // First, just test that the cache works
  std::cout << "Testing basic cache operations..." << std::endl;
  
  // Test basic cache functionality first
  auto stats = cache->get_user_stats("nonexistent");
  EXPECT_FALSE(stats.has_value());
  
  // Add a user
  ASSERT_EQ(0, cache->update_user_stats("user1", 1024, 10));
  
  // Access existing user
  stats = cache->get_user_stats("user1");
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(1024u, stats->bytes_used);
  EXPECT_EQ(10u, stats->num_objects);
  
  std::cout << "Basic cache operations successful" << std::endl;
  
  // Test performance counters - they should be available since we used CephContext
  uint64_t hits = cache->get_cache_hits();
  uint64_t misses = cache->get_cache_misses();
  
  std::cout << "Performance counter values:" << std::endl;
  std::cout << "  Hits: " << hits << std::endl;
  std::cout << "  Misses: " << misses << std::endl;
  
  // We expect at least one miss (from the first get_user_stats)
  // and one hit (from the second get_user_stats)
  EXPECT_GE(misses, 1u);
  EXPECT_GE(hits, 1u);
}

TEST_F(TestRGWUsagePerfCounters, HitRateCalculation) {
  // Perform a mix of hits and misses
  cache->get_user_stats("miss1");  // Miss
  cache->get_user_stats("miss2");  // Miss
  
  cache->update_user_stats("hit1", 1024, 1);
  cache->update_user_stats("hit2", 2048, 2);
  
  cache->get_user_stats("hit1");  // Hit
  cache->get_user_stats("hit2");  // Hit
  cache->get_user_stats("miss3"); // Miss
  
  uint64_t hits = cache->get_cache_hits();
  uint64_t misses = cache->get_cache_misses();
  
  EXPECT_EQ(2u, hits);
  EXPECT_EQ(3u, misses);
  
  double hit_rate = cache->get_hit_rate();
  double expected_rate = (2.0 / 5.0) * 100.0;  // 40%
  
  EXPECT_DOUBLE_EQ(expected_rate, hit_rate);
  
  std::cout << "Cache statistics:" << std::endl;
  std::cout << "  Hits: " << hits << std::endl;
  std::cout << "  Misses: " << misses << std::endl;
  std::cout << "  Hit rate: " << hit_rate << "%" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, UserAndBucketSeparateTracking) {
  // Test that user and bucket stats are tracked separately
  
  // User operations
  cache->get_user_stats("user_miss");     // User miss
  cache->update_user_stats("user1", 1024, 1);
  cache->get_user_stats("user1");         // User hit
  
  // Bucket operations
  cache->get_bucket_stats("bucket_miss"); // Bucket miss
  cache->update_bucket_stats("bucket1", 2048, 2);
  cache->get_bucket_stats("bucket1");     // Bucket hit
  
  // Check overall counters (should include both)
  EXPECT_EQ(2u, cache->get_cache_hits());    // 1 user hit + 1 bucket hit
  EXPECT_EQ(2u, cache->get_cache_misses());  // 1 user miss + 1 bucket miss
}

TEST_F(TestRGWUsagePerfCounters, ExpiredEntryTracking) {
  // Add a user
  ASSERT_EQ(0, cache->update_user_stats("expiry_test", 1024, 1));
  
  // Access it - should be a hit
  auto stats = cache->get_user_stats("expiry_test");
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(1u, cache->get_cache_hits());
  
  // Wait for TTL to expire (2 seconds + buffer)
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  
  // Access expired entry - should be a miss
  stats = cache->get_user_stats("expiry_test");
  EXPECT_FALSE(stats.has_value());
  
  // After expiry, we should have an additional miss
  EXPECT_EQ(1u, cache->get_cache_hits());
  EXPECT_EQ(1u, cache->get_cache_misses());  // The expired entry counts as a miss
}

TEST_F(TestRGWUsagePerfCounters, RemoveOperationTracking) {
  // Add users
  cache->update_user_stats("user1", 1024, 1);
  cache->update_user_stats("user2", 2048, 2);
  
  // Remove one user
  ASSERT_EQ(0, cache->remove_user_stats("user1"));
  
  // Try to access removed user - should be a miss
  auto stats = cache->get_user_stats("user1");
  EXPECT_FALSE(stats.has_value());
  EXPECT_EQ(1u, cache->get_cache_misses());
  
  // Access existing user - should be a hit
  stats = cache->get_user_stats("user2");
  EXPECT_TRUE(stats.has_value());
  EXPECT_EQ(1u, cache->get_cache_hits());
}

TEST_F(TestRGWUsagePerfCounters, ZeroDivisionInHitRate) {
  // Test hit rate calculation when there are no operations
  double hit_rate = cache->get_hit_rate();
  EXPECT_EQ(0.0, hit_rate);  // Should handle division by zero gracefully
  
  // After one miss
  cache->get_user_stats("nonexistent");
  hit_rate = cache->get_hit_rate();
  EXPECT_EQ(0.0, hit_rate);  // 0 hits / 1 total = 0%
  
  // After one hit
  cache->update_user_stats("user1", 1024, 1);
  cache->get_user_stats("user1");
  hit_rate = cache->get_hit_rate();
  EXPECT_EQ(50.0, hit_rate);  // 1 hit / 2 total = 50%
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  
  // Initialize CephContext once for all tests
  std::vector<const char*> args;
  // Add --no-mon-config to skip fetching config from monitors
  args.push_back("--no-mon-config");
  
  g_cct_holder = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY, 
                             CINIT_FLAG_NO_MON_CONFIG);  // Add this flag
  g_cct = g_cct_holder.get();
  common_init_finish(g_cct);
  
  int result = RUN_ALL_TESTS();
  
  // Clean up
  g_cct = nullptr;
  g_cct_holder.reset();
  
  return result;
}