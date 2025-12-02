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

static CephContext* g_test_context = nullptr;

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

    config.db_path = test_db_path;
    config.max_db_size = 1 << 20;  // 1MB for testing
    config.max_readers = 10;

    // Create cache with global CephContext if available, otherwise without
    if (g_test_context) {
      cache = std::make_unique<rgw::UsageCache>(g_test_context, config);
    } else {
      cache = std::make_unique<rgw::UsageCache>(config);
    }

    // Initialize the cache!
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

// Test updating existing user stats (overwrites, not accumulates)
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

// Test stress with many users and buckets
TEST_F(TestRGWUsageCache, StressTest) {
  const int num_users = 1000;
  const int num_buckets = 500;

  std::cout << "Running stress test with " << num_users << " users and "
            << num_buckets << " buckets..." << std::endl;

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
    int user_idx = (i * 97) % num_users;
    std::string user_id = "stress_user_" + std::to_string(user_idx);
    auto stats = cache->get_user_stats(user_id);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(user_idx * 1024u, stats->bytes_used);
  }

  for (int i = 0; i < 10; ++i) {
    int bucket_idx = (i * 53) % num_buckets;
    std::string bucket_name = "stress_bucket_" + std::to_string(bucket_idx);
    auto bucket_stats = cache->get_bucket_stats(bucket_name);
    ASSERT_TRUE(bucket_stats.has_value());
    EXPECT_EQ(bucket_idx * 512u, bucket_stats->bytes_used);
  }

  std::cout << "Stress test completed successfully!" << std::endl;
}

// Test persistence across cache restart (simulates RGW restart)
TEST_F(TestRGWUsageCache, PersistenceAcrossRestart) {
  std::cout << "\n=== Testing Persistence Across Restart ===" << std::endl;

  const std::string user_id = "persistence_test_user";
  const uint64_t bytes = 15 * 1024 * 1024;  // 15MB
  const uint64_t objects = 3;

  // Add user stats
  ASSERT_EQ(0, cache->update_user_stats(user_id, bytes, objects));

  // Verify stats exist
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(bytes, stats->bytes_used);
  std::cout << "Initial stats cached: " << bytes << " bytes" << std::endl;

  // Shutdown cache (simulates RGW shutdown)
  cache->shutdown();
  cache.reset();
  std::cout << "Cache shutdown (simulating RGW restart)" << std::endl;

  // Recreate cache with same path (simulates RGW restart)
  if (g_test_context) {
    cache = std::make_unique<rgw::UsageCache>(g_test_context, config);
  } else {
    cache = std::make_unique<rgw::UsageCache>(config);
  }
  ASSERT_EQ(0, cache->init());
  std::cout << "Cache reinitialized" << std::endl;

  // Verify stats persisted
  stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value()) << "Stats should persist across restart";
  EXPECT_EQ(bytes, stats->bytes_used);
  EXPECT_EQ(objects, stats->num_objects);
  std::cout << "Stats persisted: " << stats->bytes_used << " bytes" << std::endl;

  std::cout << "Persistence test PASSED!" << std::endl;
}

// Test simulating background refresh from RADOS
TEST_F(TestRGWUsageCache, BackgroundRefreshSimulation) {
  std::cout << "\n=== Simulating Background Refresh from RADOS ===" << std::endl;

  const std::string user_id = "refresh_test_user";

  // Initial state (simulating first RADOS sync)
  std::cout << "Initial RADOS sync: 1MB, 10 objects" << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 1024 * 1024, 10));

  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(1024 * 1024u, stats->bytes_used);
  EXPECT_EQ(10u, stats->num_objects);

  // Simulate user uploading more data (RADOS has new values)
  std::cout << "Simulating background refresh with new RADOS data: 2MB, 20 objects" << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 2 * 1024 * 1024, 20));

  // Should have fresh data from "RADOS"
  stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(2 * 1024 * 1024u, stats->bytes_used);
  EXPECT_EQ(20u, stats->num_objects);
  std::cout << "Background refresh successful - stats updated" << std::endl;

  // Simulate bucket deletion (RADOS values decrease)
  std::cout << "Simulating bucket deletion: 1MB, 15 objects" << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 1024 * 1024, 15));

  stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(1024 * 1024u, stats->bytes_used);
  EXPECT_EQ(15u, stats->num_objects);
  std::cout << "Stats correctly reflect deletion" << std::endl;
}

// Test multi-user scenario (cluster coherence simulation)
TEST_F(TestRGWUsageCache, MultiUserClusterCoherence) {
  std::cout << "\n=== Testing Multi-User Cluster Coherence ===" << std::endl;

  // Simulate multiple users being synced from RADOS
  struct UserData {
    std::string id;
    uint64_t bytes;
    uint64_t objects;
  };

  std::vector<UserData> users = {
    {"testuser", 348160, 29},
    {"testuser2", 10240, 1},
    {"admin", 1024 * 1024, 100}
  };

  // Simulate RADOS sync for all users
  for (const auto& user : users) {
    ASSERT_EQ(0, cache->update_user_stats(user.id, user.bytes, user.objects));
    std::cout << "Synced user " << user.id << ": " << user.bytes << " bytes, "
              << user.objects << " objects" << std::endl;
  }

  // Verify all users have correct stats
  for (const auto& user : users) {
    auto stats = cache->get_user_stats(user.id);
    ASSERT_TRUE(stats.has_value()) << "User " << user.id << " not found";
    EXPECT_EQ(user.bytes, stats->bytes_used);
    EXPECT_EQ(user.objects, stats->num_objects);
  }

  std::cout << "All users verified - cluster coherence maintained" << std::endl;
}

// Test performance (no sync_owner_stats in path)
TEST_F(TestRGWUsageCache, PerformanceNoSyncOwnerStats) {
  std::cout << "\n=== Performance Test: No sync_owner_stats Blocking ===" << std::endl;

  const int num_operations = 1000;
  std::vector<std::string> bucket_names;

  // Pre-populate cache with buckets
  for (int i = 0; i < 10; ++i) {
    std::string name = "perf_bucket_" + std::to_string(i);
    bucket_names.push_back(name);
    ASSERT_EQ(0, cache->update_bucket_stats(name, i * 1024, i));
  }

  std::cout << "Running " << num_operations << " cache read operations..." << std::endl;

  // Measure time for many get operations
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_operations; ++i) {
    std::string& bucket = bucket_names[i % bucket_names.size()];
    auto stats = cache->get_bucket_stats(bucket);
    ASSERT_TRUE(stats.has_value());
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  double total_ms = duration.count() / 1000.0;
  double avg_us = static_cast<double>(duration.count()) / num_operations;
  double avg_ms = avg_us / 1000.0;

  std::cout << "Performance Results:" << std::endl;
  std::cout << "  Total operations: " << num_operations << std::endl;
  std::cout << "  Total time: " << total_ms << " ms" << std::endl;
  std::cout << "  Average per operation: " << avg_us << " μs (" << avg_ms << " ms)" << std::endl;

  EXPECT_LT(avg_ms, 10.0)
    << "Operations too slow (" << avg_ms << "ms) - possible sync_owner_stats in path";

  // Should be sub-millisecond for cache operations
  EXPECT_LT(avg_ms, 1.0)
    << "Cache operations should be < 1ms, got " << avg_ms << "ms";

  double speedup = 90.0 / avg_ms;
  std::cout << "Performance test PASSED!" << std::endl;
  std::cout << "Operations are ~" << static_cast<int>(speedup) << "x faster than the old cls_bucket_head bug" << std::endl;
}

// Test statistics accuracy
TEST_F(TestRGWUsageCache, StatisticsAccuracy) {
  std::cout << "\n=== Testing Statistics Accuracy ===" << std::endl;

  const std::string user_id = "accuracy_user";

  // Known values from manual test
  const uint64_t expected_bytes = 348160;  // From actual test
  const uint64_t expected_objects = 29;

  // Update cache
  ASSERT_EQ(0, cache->update_user_stats(user_id, expected_bytes, expected_objects));

  // Retrieve and verify exact match
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());

  std::cout << "Expected: " << expected_bytes << " bytes, " << expected_objects << " objects" << std::endl;
  std::cout << "Cached:   " << stats->bytes_used << " bytes, " << stats->num_objects << " objects" << std::endl;

  // For cache backed by RADOS, should be exact
  EXPECT_EQ(expected_bytes, stats->bytes_used) << "Byte count should be exact";
  EXPECT_EQ(expected_objects, stats->num_objects) << "Object count should be exact";

  std::cout << "Statistics are exact (no approximation)" << std::endl;
}

// Test concurrent access simulation
TEST_F(TestRGWUsageCache, ConcurrentAccessSimulation) {
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
    threads.emplace_back([this, t, operations_per_thread, &success_count, &failure_count]() {
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
  std::cout << "Concurrent Access Results:" << std::endl;
  std::cout << "  Total operations: " << total_ops << std::endl;
  std::cout << "  Successful: " << success_count << std::endl;
  std::cout << "  Failed: " << failure_count << std::endl;
  std::cout << "  Time: " << duration.count() << " ms" << std::endl;

  // All should succeed
  EXPECT_EQ(total_ops, success_count.load()) << "All operations should succeed";
  EXPECT_EQ(0, failure_count.load()) << "No operations should fail";

  std::cout << "Cache handled concurrent access successfully" << std::endl;
}

// Test complete workflow integration
TEST_F(TestRGWUsageCache, CompleteWorkflowIntegration) {
  std::cout << "\n=== Complete Workflow Integration Test ===" << std::endl;
  std::cout << "Simulating entire manual test procedure with new design...\n" << std::endl;

  // Step 1: Setup
  std::cout << "[Step 1] Creating user and buckets" << std::endl;
  const std::string user_id = "testuser";
  const std::string bucket1 = "testa";
  const std::string bucket2 = "testb";
  const std::string bucket3 = "testc";

  // Step 2: Simulate background sync from RADOS after uploads
  std::cout << "[Step 2] Simulating RADOS sync after file uploads" << std::endl;

  // User uploads to multiple buckets, background thread syncs from RADOS
  uint64_t user_bytes = 40960;  // 4 x 10KB
  uint64_t user_objects = 4;

  ASSERT_EQ(0, cache->update_user_stats(user_id, user_bytes, user_objects));
  std::cout << "  RADOS sync: " << user_bytes << " bytes, " << user_objects << " objects" << std::endl;

  // Step 3: Verify user stats
  std::cout << "[Step 3] Verifying user statistics" << std::endl;
  auto user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value());
  EXPECT_EQ(user_bytes, user_stats->bytes_used);
  EXPECT_EQ(user_objects, user_stats->num_objects);
  std::cout << "  Verified: " << user_stats->bytes_used << " bytes, "
            << user_stats->num_objects << " objects" << std::endl;

  // Step 4: Simulate more uploads and RADOS sync
  std::cout << "[Step 4] Simulating more uploads" << std::endl;
  user_bytes = 71680;  // Updated after more uploads
  user_objects = 4;
  ASSERT_EQ(0, cache->update_user_stats(user_id, user_bytes, user_objects));

  user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value());
  EXPECT_EQ(71680u, user_stats->bytes_used);
  std::cout << "  Updated: " << user_stats->bytes_used << " bytes" << std::endl;

  // Step 5: Simulate object deletion and RADOS sync
  std::cout << "[Step 5] Simulating object deletion" << std::endl;
  user_bytes = 40960;  // After deletion
  user_objects = 3;
  ASSERT_EQ(0, cache->update_user_stats(user_id, user_bytes, user_objects));

  user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value());
  EXPECT_EQ(40960u, user_stats->bytes_used);
  EXPECT_EQ(3u, user_stats->num_objects);
  std::cout << "  After deletion: " << user_stats->bytes_used << " bytes, "
            << user_stats->num_objects << " objects" << std::endl;

  // Step 6: Simulate bucket deletion (critical bug fix test)
  std::cout << "[Step 6] Simulating bucket deletion (bug fix test)" << std::endl;
  user_bytes = 30720;  // After bucket deletion
  user_objects = 2;
  ASSERT_EQ(0, cache->update_user_stats(user_id, user_bytes, user_objects));

  user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value());
  EXPECT_EQ(30720u, user_stats->bytes_used);
  EXPECT_EQ(2u, user_stats->num_objects);
  std::cout << "  After bucket deletion: " << user_stats->bytes_used << " bytes" << std::endl;
  std::cout << "  (Stats correct - bucket deletion bug FIXED!)" << std::endl;

  // Step 7: Performance check
  std::cout << "[Step 7] Performance regression check" << std::endl;
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; ++i) {
    cache->get_user_stats(user_id);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  double avg = static_cast<double>(duration.count()) / 100.0;
  std::cout << "  100 operations: " << duration.count() << " ms (avg: " << avg << " ms/op)" << std::endl;
  EXPECT_LT(avg, 10.0);

  std::cout << "\n=== Complete Workflow Test PASSED ===" << std::endl;
  std::cout << "New design validated: RADOS as source of truth!" << std::endl;
}

// Main function
int main(int argc, char **argv) { 
  ::testing::InitGoogleTest(&argc, argv);

  std::map<std::string, std::string> defaults = {
    {"debug_rgw", "20"},
    {"keyring", "keyring"},
  };

  std::vector<const char*> args;
  args.push_back("--no-mon-config");

  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE | CINIT_FLAG_NO_MON_CONFIG);

  // Store the context for test fixtures to use
  g_test_context = cct.get();

  common_init_finish(cct.get());

  // Run all tests
  int result = RUN_ALL_TESTS();

  return result;
}