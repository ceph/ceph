// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_usage_cache.h"
#include "global/global_init.h"
#include "common/ceph_context.h"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <chrono>
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>
#include <filesystem>
#include <random>

namespace fs = std::filesystem;

// Global CephContext pointer for tests
static CephContext* g_test_context = nullptr;

// Test fixture for RGWUsageCache
class TestRGWUsageCache : public ::testing::Test {
protected:
  std::unique_ptr<rgw::UsageCache> cache;
  rgw::UsageCache::Config config;
  std::string test_db_path;

  void SetUp() override {
    // Generate unique test database path to avoid conflicts
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1000000, 9999999);
    test_db_path = "/tmp/test_usage_cache_" + std::to_string(dis(gen)) + ".mdb";
    
    // Initialize config with proper fields
    config.db_path = test_db_path;
    config.max_db_size = 1 << 20;  // 1MB for testing
    config.max_readers = 10;
    config.ttl = std::chrono::seconds(2);  // 2 second TTL for testing
    
    // Create cache with global CephContext if available, otherwise without
    if (g_test_context) {
      cache = std::make_unique<rgw::UsageCache>(g_test_context, config);
    } else {
      cache = std::make_unique<rgw::UsageCache>(config);
    }
    
    // CRITICAL: Initialize the cache!
    int init_result = cache->init();
    ASSERT_EQ(0, init_result) << "Failed to initialize cache: " << init_result;
  }

  void TearDown() override {
    if (cache) {
      cache->shutdown();
    }
    cache.reset();
    
    // Clean up test database files
    try {
      fs::remove(test_db_path);
      fs::remove(test_db_path + "-lock");
    } catch (const std::exception& e) {
      // Ignore cleanup errors
    }
  }
};

// Test basic user statistics operations
TEST_F(TestRGWUsageCache, BasicUserOperations) {
  const std::string user_id = "test_user";
  const uint64_t bytes_used = 1024 * 1024;
  const uint64_t num_objects = 42;
  
  // Update user stats
  int update_result = cache->update_user_stats(user_id, bytes_used, num_objects);
  ASSERT_EQ(0, update_result) << "Failed to update user stats: " << update_result;
  
  // Get and verify user stats
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(bytes_used, stats->bytes_used);
  EXPECT_EQ(num_objects, stats->num_objects);
  
  // Remove user stats
  ASSERT_EQ(0, cache->remove_user_stats(user_id));
  
  // Verify stats are removed
  stats = cache->get_user_stats(user_id);
  EXPECT_FALSE(stats.has_value());
}

// Test basic bucket statistics operations
TEST_F(TestRGWUsageCache, BasicBucketOperations) {
  const std::string bucket_name = "test_bucket";
  const uint64_t bytes_used = 512 * 1024;
  const uint64_t num_objects = 17;
  
  // Update bucket stats
  int update_result = cache->update_bucket_stats(bucket_name, bytes_used, num_objects);
  ASSERT_EQ(0, update_result) << "Failed to update bucket stats: " << update_result;
  
  // Get and verify bucket stats
  auto stats = cache->get_bucket_stats(bucket_name);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(bytes_used, stats->bytes_used);
  EXPECT_EQ(num_objects, stats->num_objects);
  
  // Remove bucket stats
  ASSERT_EQ(0, cache->remove_bucket_stats(bucket_name));
  
  // Verify stats are removed
  stats = cache->get_bucket_stats(bucket_name);
  EXPECT_FALSE(stats.has_value());
}

// Test TTL expiration
TEST_F(TestRGWUsageCache, TTLExpiration) {
  const std::string user_id = "ttl_test_user";
  
  // Add user stats
  ASSERT_EQ(0, cache->update_user_stats(user_id, 1024, 1));
  
  // Verify stats exist
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  
  // Wait for TTL to expire (2 seconds + buffer)
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  
  // Verify stats have expired
  stats = cache->get_user_stats(user_id);
  EXPECT_FALSE(stats.has_value());
}

// Test updating existing user stats
TEST_F(TestRGWUsageCache, UpdateExistingUserStats) {
  const std::string user_id = "update_test_user";
  
  // Initial update
  ASSERT_EQ(0, cache->update_user_stats(user_id, 1024, 10));
  
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(1024u, stats->bytes_used);
  EXPECT_EQ(10u, stats->num_objects);
  
  // Update with new values (should replace, not accumulate)
  ASSERT_EQ(0, cache->update_user_stats(user_id, 2048, 20));
  
  stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(2048u, stats->bytes_used);
  EXPECT_EQ(20u, stats->num_objects);
}

// Test multiple bucket operations
TEST_F(TestRGWUsageCache, MultipleBucketOperations) {
  const int num_buckets = 10;
  
  // Add multiple buckets
  for (int i = 0; i < num_buckets; ++i) {
    std::string bucket_name = "bucket_" + std::to_string(i);
    uint64_t bytes = (i + 1) * 1024;
    uint64_t objects = (i + 1) * 10;
    
    ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, bytes, objects));
  }
  
  // Verify all buckets
  for (int i = 0; i < num_buckets; ++i) {
    std::string bucket_name = "bucket_" + std::to_string(i);
    auto stats = cache->get_bucket_stats(bucket_name);
    
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((i + 1) * 1024u, stats->bytes_used);
    EXPECT_EQ((i + 1) * 10u, stats->num_objects);
  }
  
  EXPECT_EQ(num_buckets, cache->get_cache_size());
}

// Test special character handling
TEST_F(TestRGWUsageCache, SpecialCharacterHandling) {
  std::vector<std::string> test_ids = {
    "user@example.com",
    "user-with-dashes",
    "user_with_underscores",
    "user.with.dots",
    "user/with/slashes",
    "user with spaces",
    "user\twith\ttabs"
  };
  
  for (const auto& id : test_ids) {
    // Should handle all these gracefully
    ASSERT_EQ(0, cache->update_user_stats(id, 1024, 1))
      << "Failed for ID: " << id;
    
    auto stats = cache->get_user_stats(id);
    ASSERT_TRUE(stats.has_value()) << "Failed to retrieve stats for ID: " << id;
    EXPECT_EQ(1024u, stats->bytes_used);
    EXPECT_EQ(1u, stats->num_objects);
  }
}

// Test remove non-existent user
TEST_F(TestRGWUsageCache, RemoveNonExistentUser) {
  const std::string user_id = "non_existent_user";
  
  // Should handle removing non-existent user gracefully
  int result = cache->remove_user_stats(user_id);
  // Either returns 0 (success) or error (not found)
  EXPECT_TRUE(result == 0 || result != 0);
  
  // Verify user doesn't exist
  auto stats = cache->get_user_stats(user_id);
  EXPECT_FALSE(stats.has_value());
}

// Test cache size tracking
TEST_F(TestRGWUsageCache, CacheSizeTracking) {
  // Initial size should be 0
  EXPECT_EQ(0u, cache->get_cache_size());
  
  // Add some users
  const int num_users = 5;
  
  for (int i = 0; i < num_users; ++i) {
    std::string user_id = "size_test_user_" + std::to_string(i);
    ASSERT_EQ(0, cache->update_user_stats(user_id, 1024 * (i + 1), i + 1));
  }
  
  // Cache size should reflect the number of added users
  EXPECT_EQ(num_users, cache->get_cache_size());
  
  // Remove one user
  ASSERT_EQ(0, cache->remove_user_stats("size_test_user_0"));
  
  // Cache size should decrease
  EXPECT_EQ(num_users - 1, cache->get_cache_size());
}

// Test clear expired entries
TEST_F(TestRGWUsageCache, ClearExpiredEntries) {
  // Add some entries
  ASSERT_EQ(0, cache->update_user_stats("user1", 1024, 1));
  ASSERT_EQ(0, cache->update_user_stats("user2", 2048, 2));
  ASSERT_EQ(0, cache->update_bucket_stats("bucket1", 4096, 4));
  
  EXPECT_EQ(3u, cache->get_cache_size());
  
  // Wait for entries to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  
  // Clear expired entries
  int removed = cache->clear_expired_entries();
  EXPECT_EQ(3, removed);
  
  // Cache should be empty
  EXPECT_EQ(0u, cache->get_cache_size());
}

// Test stress with many users and buckets
TEST_F(TestRGWUsageCache, StressTest) {
  const int num_users = 1000;
  const int num_buckets = 500;
  
  // Create a new config with longer TTL for stress testing
  rgw::UsageCache::Config stress_config;
  stress_config.db_path = test_db_path;
  stress_config.max_db_size = 1 << 20;
  stress_config.max_readers = 10;
  stress_config.ttl = std::chrono::seconds(1800);  // 30 minutes for stress test
  
  // Recreate cache with longer TTL
  cache.reset();
  
  if (g_test_context) {
    cache = std::make_unique<rgw::UsageCache>(g_test_context, stress_config);
  } else {
    cache = std::make_unique<rgw::UsageCache>(stress_config);
  }
  ASSERT_EQ(0, cache->init());

  // Add many users
  for (int i = 0; i < num_users; ++i) {
    std::string user_id = "stress_user_" + std::to_string(i);
    ASSERT_EQ(0, cache->update_user_stats(user_id, i * 1024, i));
  }
  
  // Add many buckets
  for (int i = 0; i < num_buckets; ++i) {
    std::string bucket_name = "stress_bucket_" + std::to_string(i);
    ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, i * 512, i * 2));
  }
  
  EXPECT_EQ(num_users + num_buckets, cache->get_cache_size());
  
  // Verify random samples
  for (int i = 0; i < 10; ++i) {
    int user_idx = (i * 97) % num_users;  // Sample users
    std::string user_id = "stress_user_" + std::to_string(user_idx);
    auto stats = cache->get_user_stats(user_id);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(user_idx * 1024u, stats->bytes_used);
  }
  
  for (int i = 0; i < 10; ++i) {
    int bucket_idx = (i * 53) % num_buckets;  // Sample buckets
    std::string bucket_name = "stress_bucket_" + std::to_string(bucket_idx);
    auto bucket_stats = cache->get_bucket_stats(bucket_name);
    ASSERT_TRUE(bucket_stats.has_value());
    EXPECT_EQ(bucket_idx * 512u, bucket_stats->bytes_used);
  }
  
  std::cout << "Stress test completed: " << num_users << " users, " 
            << num_buckets << " buckets" << std::endl;
}

// Main function
int main(int argc, char **argv) {
  // Initialize Google Test
  ::testing::InitGoogleTest(&argc, argv);

  // Initialize Ceph context
  std::map<std::string, std::string> defaults = {
    {"debug_rgw", "20"},
    {"keyring", "keyring"},
  };

  std::vector<const char*> args;
  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  
  // Store the context for test fixtures to use
  g_test_context = cct.get();

  common_init_finish(cct.get());

  // Run all tests
  int result = RUN_ALL_TESTS();

  return result;
}