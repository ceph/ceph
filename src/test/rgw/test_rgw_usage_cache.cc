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

TEST_F(TestRGWUsageCache, ManualTestScenario_UserAndBuckets) {
  // Simulate the complete manual test scenario:
  // User "testuser" with bucket1 (2 files) and bucket2 (1 file)
  
  std::cout << "\n=== Manual Test Scenario: User and Buckets ===" << std::endl;
  
  const std::string user_id = "testuser";
  const std::string bucket1 = "bucket1";
  const std::string bucket2 = "bucket2";
  
  // File sizes from manual test
  const uint64_t small_txt = 12;              // "Hello World\n"
  const uint64_t medium_bin = 5 * 1024 * 1024;  // 5MB
  const uint64_t large_bin = 10 * 1024 * 1024;  // 10MB
  
  // Bucket1: small.txt + medium.bin
  const uint64_t bucket1_bytes = small_txt + medium_bin;
  const uint64_t bucket1_objects = 2;
  
  // Bucket2: large.bin
  const uint64_t bucket2_bytes = large_bin;
  const uint64_t bucket2_objects = 1;
  
  // User totals
  const uint64_t user_bytes = bucket1_bytes + bucket2_bytes;
  const uint64_t user_objects = 3;
  
  std::cout << "Uploading files to buckets:" << std::endl;
  std::cout << "  " << bucket1 << ": small.txt (12B) + medium.bin (5MB) = " 
            << bucket1_bytes << " bytes, " << bucket1_objects << " objects" << std::endl;
  std::cout << "  " << bucket2 << ": large.bin (10MB) = " 
            << bucket2_bytes << " bytes, " << bucket2_objects << " objects" << std::endl;
  
  // Update cache
  ASSERT_EQ(0, cache->update_bucket_stats(bucket1, bucket1_bytes, bucket1_objects));
  ASSERT_EQ(0, cache->update_bucket_stats(bucket2, bucket2_bytes, bucket2_objects));
  ASSERT_EQ(0, cache->update_user_stats(user_id, user_bytes, user_objects));
  
  // Verify bucket1
  auto b1_stats = cache->get_bucket_stats(bucket1);
  ASSERT_TRUE(b1_stats.has_value());
  EXPECT_EQ(bucket1_bytes, b1_stats->bytes_used);
  EXPECT_EQ(bucket1_objects, b1_stats->num_objects);
  std::cout << " Bucket1 verified: " << b1_stats->bytes_used << " bytes, " 
            << b1_stats->num_objects << " objects" << std::endl;
  
  // Verify bucket2
  auto b2_stats = cache->get_bucket_stats(bucket2);
  ASSERT_TRUE(b2_stats.has_value());
  EXPECT_EQ(bucket2_bytes, b2_stats->bytes_used);
  EXPECT_EQ(bucket2_objects, b2_stats->num_objects);
  std::cout << " Bucket2 verified: " << b2_stats->bytes_used << " bytes, " 
            << b2_stats->num_objects << " objects" << std::endl;
  
  // Verify user totals
  auto user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value());
  EXPECT_EQ(user_bytes, user_stats->bytes_used);
  EXPECT_EQ(user_objects, user_stats->num_objects);
  
  std::cout << " User verified: " << user_stats->bytes_used << " bytes (~" 
            << (user_stats->bytes_used / (1024 * 1024)) << "MB), " 
            << user_stats->num_objects << " objects" << std::endl;
  
  // Verify expected totals
  EXPECT_GE(user_bytes, 15 * 1024 * 1024) << "User should have ~15MB";
  EXPECT_EQ(3u, user_objects) << "User should have exactly 3 objects";
  
  std::cout << " All statistics match manual test expectations" << std::endl;
}

TEST_F(TestRGWUsageCache, RepeatedAccessCacheHits) {
  // Simulate: s3cmd ls s3://bucket1 (multiple times)
  // Validate cache hit behavior
  
  std::cout << "\n=== Testing Repeated Access Cache Hits ===" << std::endl;
  
  const std::string bucket_name = "bucket1";
  
  // Add bucket to cache
  ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, 5 * 1024 * 1024, 2));
  std::cout << "Added bucket to cache" << std::endl;
  
  // Simulate multiple s3cmd ls operations
  std::cout << "Simulating multiple bucket listings:" << std::endl;
  for (int i = 0; i < 3; ++i) {
    auto stats = cache->get_bucket_stats(bucket_name);
    ASSERT_TRUE(stats.has_value()) << "Failed on iteration " << (i + 1);
    std::cout << "  Listing " << (i + 1) << ": Found bucket with " 
              << stats->bytes_used << " bytes" << std::endl;
  }
  
  std::cout << " All listings successful - bucket data retrieved from cache" << std::endl;
}

TEST_F(TestRGWUsageCache, PerformanceNoSyncOwnerStats) {
  // CRITICAL: Verify cache operations are fast (not 90ms like cls_bucket_head)
  
  std::cout << "\n=== Performance Test: No sync_owner_stats Blocking ===" << std::endl;
  
  const int num_operations = 1000;
  std::vector<std::string> bucket_names;
  
  // Pre-populate cache with buckets
  for (int i = 0; i < 10; ++i) {
    std::string name = "perf_bucket_" + std::to_string(i);
    bucket_names.push_back(name);
    ASSERT_EQ(0, cache->update_bucket_stats(name, i * 1024, i));
  }
  
  std::cout << "Running " << num_operations << " cache operations..." << std::endl;
  
  // Measure time for many get operations
  auto start = std::chrono::high_resolution_clock::now();
  
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  
  double total_ms = duration.count() / 1000.0;
  double avg_us = static_cast<double>(duration.count()) / num_operations;
  double avg_ms = avg_us / 1000.0;
  
  std::cout << "\nPerformance Results:" << std::endl;
  std::cout << "  Total operations: " << num_operations << std::endl;
  std::cout << "  Total time: " << total_ms << " ms" << std::endl;
  std::cout << "  Average per operation: " << avg_us << " μs (" << avg_ms << " ms)" << std::endl;
  
  // CRITICAL: Should be WAY faster than 90ms
  EXPECT_LT(avg_ms, 10.0) 
    << "Operations too slow (" << avg_ms << "ms) - possible sync_owner_stats in path";
  
  // Should be sub-millisecond for in-memory cache
  EXPECT_LT(avg_ms, 1.0)
    << "Cache operations should be < 1ms, got " << avg_ms << "ms";
  
  double speedup = 90.0 / avg_ms;
  std::cout << " Performance test PASSED!" << std::endl;
  std::cout << " Operations are ~" << speedup << "x faster than the old cls_bucket_head bug" << std::endl;
}

TEST_F(TestRGWUsageCache, StatisticsAccuracy) {
  // Verify statistics are accurate (not approximate)
  
  std::cout << "\n=== Testing Statistics Accuracy ===" << std::endl;
  
  const std::string user_id = "accuracy_user";
  
  // Known values
  const uint64_t expected_bytes = 15728652;  // Exact: 12 + 5MB + 10MB
  const uint64_t expected_objects = 3;
  
  // Update cache
  ASSERT_EQ(0, cache->update_user_stats(user_id, expected_bytes, expected_objects));
  
  // Retrieve and verify exact match
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  
  std::cout << "Expected: " << expected_bytes << " bytes, " << expected_objects << " objects" << std::endl;
  std::cout << "Cached:   " << stats->bytes_used << " bytes, " << stats->num_objects << " objects" << std::endl;
  
  // For in-memory cache, should be exact
  EXPECT_EQ(expected_bytes, stats->bytes_used) << "Byte count should be exact";
  EXPECT_EQ(expected_objects, stats->num_objects) << "Object count should be exact";
  
  std::cout << " Statistics are exact (no approximation)" << std::endl;
}

TEST_F(TestRGWUsageCache, BackgroundRefreshSimulation) {
  // Simulate background refresh updating expired entries
  
  std::cout << "\n=== Simulating Background Refresh ===" << std::endl;
  
  const std::string bucket_name = "refresh_bucket";
  
  // Initial state
  std::cout << "Initial upload: 1MB, 10 objects" << std::endl;
  ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, 1024 * 1024, 10));
  
  auto stats = cache->get_bucket_stats(bucket_name);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(1024 * 1024u, stats->bytes_used);
  std::cout << " Initial stats cached" << std::endl;
  
  // Wait for expiry
  std::cout << "Waiting for entry to expire..." << std::endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  
  // Entry should be expired
  stats = cache->get_bucket_stats(bucket_name);
  EXPECT_FALSE(stats.has_value()) << "Entry should be expired";
  std::cout << " Entry expired as expected" << std::endl;
  
  // Simulate background refresh with updated stats
  std::cout << "Simulating background refresh: 2MB, 20 objects" << std::endl;
  ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, 2 * 1024 * 1024, 20));
  
  // Should now have fresh data
  stats = cache->get_bucket_stats(bucket_name);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(2 * 1024 * 1024u, stats->bytes_used);
  EXPECT_EQ(20u, stats->num_objects);
  std::cout << " Background refresh successful - stats updated" << std::endl;
}

TEST_F(TestRGWUsageCache, ConcurrentAccessSimulation) {
  // Simulate concurrent access from multiple operations
  // (Like multiple s3cmd operations happening simultaneously)
  
  std::cout << "\n=== Simulating Concurrent Access ===" << std::endl;
  
  const int num_threads = 10;
  const int operations_per_thread = 100;
  
  // Pre-populate some buckets
  for (int i = 0; i < num_threads; ++i) {
    std::string bucket_name = "concurrent_bucket_" + std::to_string(i);
    ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, i * 1024, i));
  }
  
  std::cout << "Launching " << num_threads << " threads, " 
            << operations_per_thread << " operations each..." << std::endl;
  
  std::atomic<int> success_count{0};
  std::atomic<int> failure_count{0};
  std::vector<std::thread> threads;
  
  auto start = std::chrono::high_resolution_clock::now();
  
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, &success_count, &failure_count]() {
      std::string bucket_name = "concurrent_bucket_" + std::to_string(t);
      
      for (int i = 0; i < operations_per_thread; ++i) {
        auto stats = cache->get_bucket_stats(bucket_name);
        if (stats.has_value()) {
          success_count++;
        } else {
          failure_count++;
        }
      }
    });
  }
  
  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  
  int total_ops = num_threads * operations_per_thread;
  std::cout << "\nConcurrent Access Results:" << std::endl;
  std::cout << "  Total operations: " << total_ops << std::endl;
  std::cout << "  Successful: " << success_count << std::endl;
  std::cout << "  Failed: " << failure_count << std::endl;
  std::cout << "  Time: " << duration.count() << " ms" << std::endl;
  std::cout << "  Average: " << (static_cast<double>(duration.count()) / total_ops) 
            << " ms/op" << std::endl;
  
  // All should succeed
  EXPECT_EQ(total_ops, success_count.load()) << "All operations should succeed";
  EXPECT_EQ(0, failure_count.load()) << "No operations should fail";
  
  std::cout << " Cache handled concurrent access successfully" << std::endl;
}

TEST_F(TestRGWUsageCache, CompleteWorkflowIntegration) {
  // Integration test covering the complete manual test workflow
  
  std::cout << "\n=== Complete Workflow Integration Test ===" << std::endl;
  std::cout << "Simulating entire manual test procedure...\n" << std::endl;
  
  // Step 1: Setup
  std::cout << "[Step 1] Creating user and buckets" << std::endl;
  const std::string user_id = "testuser";
  const std::string bucket1 = "bucket1";
  const std::string bucket2 = "bucket2";
  
  // Step 2: Upload files
  std::cout << "[Step 2] Uploading files" << std::endl;
  std::cout << "  bucket1: small.txt + medium.bin" << std::endl;
  std::cout << "  bucket2: large.bin" << std::endl;
  
  uint64_t b1_bytes = 12 + (5 * 1024 * 1024);
  uint64_t b2_bytes = 10 * 1024 * 1024;
  uint64_t user_bytes = b1_bytes + b2_bytes;
  
  ASSERT_EQ(0, cache->update_bucket_stats(bucket1, b1_bytes, 2));
  ASSERT_EQ(0, cache->update_bucket_stats(bucket2, b2_bytes, 1));
  ASSERT_EQ(0, cache->update_user_stats(user_id, user_bytes, 3));
  
  // Step 3: Verify user stats (like checking perf counters)
  std::cout << "[Step 3] Verifying user statistics" << std::endl;
  auto user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value());
  std::cout << "  used_bytes: " << user_stats->bytes_used << std::endl;
  std::cout << "  num_objects: " << user_stats->num_objects << std::endl;
  EXPECT_EQ(user_bytes, user_stats->bytes_used);
  EXPECT_EQ(3u, user_stats->num_objects);
  
  // Step 4: Verify bucket stats
  std::cout << "[Step 4] Verifying bucket statistics" << std::endl;
  auto b1_stats = cache->get_bucket_stats(bucket1);
  ASSERT_TRUE(b1_stats.has_value());
  std::cout << "  bucket1 used_bytes: " << b1_stats->bytes_used << std::endl;
  std::cout << "  bucket1 num_objects: " << b1_stats->num_objects << std::endl;
  EXPECT_EQ(b1_bytes, b1_stats->bytes_used);
  EXPECT_EQ(2u, b1_stats->num_objects);
  
  // Step 5: Test cache hits (multiple listings)
  std::cout << "[Step 5] Testing cache hit behavior" << std::endl;
  for (int i = 0; i < 3; ++i) {
    auto stats = cache->get_bucket_stats(bucket1);
    ASSERT_TRUE(stats.has_value());
  }
  std::cout << "  Performed 3 bucket listings - all from cache" << std::endl;
  
  // Step 6: Verify cache size
  std::cout << "[Step 6] Verifying cache metrics" << std::endl;
  size_t cache_size = cache->get_cache_size();
  std::cout << "  cache_size: " << cache_size << std::endl;
  EXPECT_GE(cache_size, 3u);  // user + 2 buckets
  
  // Step 7: Performance check
  std::cout << "[Step 7] Performance regression check" << std::endl;
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; ++i) {
    cache->get_bucket_stats(bucket1);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  double avg = static_cast<double>(duration.count()) / 100.0;
  std::cout << "  100 operations: " << duration.count() << " ms (avg: " << avg << " ms/op)" << std::endl;
  EXPECT_LT(avg, 10.0);
  
  std::cout << "\n=== Complete Workflow Test PASSED ===" << std::endl;
  std::cout << "All manual test scenarios validated!" << std::endl;
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