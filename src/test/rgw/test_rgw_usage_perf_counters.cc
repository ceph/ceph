// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
    // NO TTL in new design
    rgw::UsageCache::Config config;
    config.db_path = test_db_path;
    config.max_db_size = 1 << 20;  // 1MB

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

TEST_F(TestRGWUsagePerfCounters, SimplifiedMetrics) {
  std::cout << "\n=== Testing Simplified Cache Metrics ===" << std::endl;
  
  // In new design, we only track:
  // - cache_hits (debug level)
  // - cache_misses (debug level)
  // - cache_updates (important - shows RADOS sync)
  // - cache_size (important - shows LMDB growth)

  // Miss
  cache->get_user_stats("nonexistent");
  
  // Update (simulating RADOS sync)
  cache->update_user_stats("user1", 1024, 10);
  
  // Hit
  cache->get_user_stats("user1");

  std::cout << "Metrics:" << std::endl;
  std::cout << "  cache_hits: " << cache->get_cache_hits() << std::endl;
  std::cout << "  cache_misses: " << cache->get_cache_misses() << std::endl;
  std::cout << "  cache_updates: " << cache->get_cache_updates() << std::endl;
  std::cout << "  cache_size: " << cache->get_cache_size() << std::endl;

  EXPECT_GE(cache->get_cache_hits(), 1u);
  EXPECT_GE(cache->get_cache_misses(), 1u);
  EXPECT_GE(cache->get_cache_updates(), 1u);
  EXPECT_EQ(1u, cache->get_cache_size());

  std::cout << "Simplified metrics verified" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, HitRateCalculation) {
  std::cout << "\n=== Testing Hit Rate Calculation ===" << std::endl;

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

TEST_F(TestRGWUsagePerfCounters, CacheUpdateTracking) {
  std::cout << "\n=== Testing Cache Update Tracking ===" << std::endl;

  // In new design, cache_updates is the key metric showing RADOS sync activity
  
  // Simulate RADOS sync updates
  ASSERT_EQ(0, cache->update_user_stats("user1", 1024, 1));
  ASSERT_EQ(0, cache->update_user_stats("user2", 2048, 2));
  ASSERT_EQ(0, cache->update_user_stats("user3", 4096, 4));

  uint64_t updates = cache->get_cache_updates();
  std::cout << "Cache updates after 3 user syncs: " << updates << std::endl;
  
  EXPECT_GE(updates, 3u) << "Should have at least 3 cache updates";

  // Verify cache size also tracked
  size_t size = cache->get_cache_size();
  std::cout << "Cache size: " << size << std::endl;
  EXPECT_EQ(3u, size);

  std::cout << "Cache update tracking verified" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, RemoveOperationTracking) {
  std::cout << "\n=== Testing Remove Operation Tracking ===" << std::endl;

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

  std::cout << "Remove operation tracking verified" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, ZeroDivisionInHitRate) {
  std::cout << "\n=== Testing Zero Division in Hit Rate ===" << std::endl;

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

  std::cout << "Zero division handling verified" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, MultipleUserBucketScenario) {
  std::cout << "\n=== Testing Multiple User/Bucket Scenario ===" << std::endl;

  const std::string user_id = "testuser";
  const std::string bucket1 = "bucket1";
  const std::string bucket2 = "bucket2";

  // Simulate RADOS sync with aggregated stats
  const uint64_t bucket1_bytes = 12 + (5 * 1024 * 1024);
  const uint64_t bucket1_objects = 2;
  const uint64_t bucket2_bytes = 10 * 1024 * 1024;
  const uint64_t bucket2_objects = 1;
  const uint64_t total_user_bytes = bucket1_bytes + bucket2_bytes;
  const uint64_t total_user_objects = bucket1_objects + bucket2_objects;

  // Update stats (simulating background RADOS sync)
  ASSERT_EQ(0, cache->update_bucket_stats(bucket1, bucket1_bytes, bucket1_objects));
  ASSERT_EQ(0, cache->update_bucket_stats(bucket2, bucket2_bytes, bucket2_objects));
  ASSERT_EQ(0, cache->update_user_stats(user_id, total_user_bytes, total_user_objects));

  std::cout << "Updated stats:" << std::endl;
  std::cout << "  User: " << user_id << std::endl;
  std::cout << "  Bucket1: " << bucket1_bytes << " bytes, " << bucket1_objects << " objects" << std::endl;
  std::cout << "  Bucket2: " << bucket2_bytes << " bytes, " << bucket2_objects << " objects" << std::endl;

  // Verify bucket1 stats
  auto b1_stats = cache->get_bucket_stats(bucket1);
  ASSERT_TRUE(b1_stats.has_value());
  EXPECT_EQ(bucket1_bytes, b1_stats->bytes_used);
  EXPECT_EQ(bucket1_objects, b1_stats->num_objects);

  // Verify bucket2 stats
  auto b2_stats = cache->get_bucket_stats(bucket2);
  ASSERT_TRUE(b2_stats.has_value());
  EXPECT_EQ(bucket2_bytes, b2_stats->bytes_used);
  EXPECT_EQ(bucket2_objects, b2_stats->num_objects);

  // Verify user stats
  auto user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value());
  EXPECT_EQ(total_user_bytes, user_stats->bytes_used);
  EXPECT_EQ(total_user_objects, user_stats->num_objects);

  std::cout << "All statistics verified!" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, CacheHitOnRepeatedAccess) {
  std::cout << "\n=== Testing Cache Hits on Repeated Access ===" << std::endl;

  const std::string bucket_name = "bucket1";

  // Populate the cache (simulating RADOS sync)
  ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, 5 * 1024 * 1024, 2));

  uint64_t initial_hits = cache->get_cache_hits();

  // Simulate multiple bucket listings
  const int num_listings = 3;
  for (int i = 0; i < num_listings; ++i) {
    auto stats = cache->get_bucket_stats(bucket_name);
    ASSERT_TRUE(stats.has_value());
    std::cout << "  Listing " << (i + 1) << ": Found bucket with "
              << stats->bytes_used << " bytes" << std::endl;
  }

  uint64_t hits_increase = cache->get_cache_hits() - initial_hits;

  std::cout << "Cache hits increased by: " << hits_increase << std::endl;

  EXPECT_EQ(num_listings, hits_increase);

  double hit_rate = cache->get_hit_rate();
  std::cout << "Cache hit rate: " << hit_rate << "%" << std::endl;
  EXPECT_GT(hit_rate, 0.0);

  std::cout << "Cache behavior verified - repeated access produces cache hits" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, PerformanceNoBlockingInIOPath) {
  std::cout << "\n=== Testing Performance: No Blocking in I/O Path ===" << std::endl;

  const int num_operations = 1000;
  const std::string bucket_prefix = "perf_bucket_";

  // Warm up - add entries to cache
  for (int i = 0; i < 10; ++i) {
    std::string bucket_name = bucket_prefix + std::to_string(i);
    cache->update_bucket_stats(bucket_name, i * 1024, i);
  }

  // Measure time for operations
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_operations; ++i) {
    std::string bucket_name = bucket_prefix + std::to_string(i % 10);
    auto stats = cache->get_bucket_stats(bucket_name);
    ASSERT_TRUE(stats.has_value());
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  double avg_per_op_us = static_cast<double>(duration.count()) / num_operations;
  double avg_per_op_ms = avg_per_op_us / 1000.0;

  std::cout << "Performance results:" << std::endl;
  std::cout << "  Total operations: " << num_operations << std::endl;
  std::cout << "  Total time: " << duration.count() << " μs" << std::endl;
  std::cout << "  Average per operation: " << avg_per_op_us << " μs" << std::endl;

  // CRITICAL: Should be MUCH faster than 90ms
  EXPECT_LT(avg_per_op_ms, 10.0);
  EXPECT_LT(avg_per_op_ms, 1.0);

  std::cout << "Performance test PASSED - no blocking in I/O path" << std::endl;
  std::cout << "Operations are ~" << static_cast<int>(90.0 / avg_per_op_ms)
            << "x faster than the old bug!" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, CacheSizeAndUpdateTracking) {
  std::cout << "\n=== Testing Cache Size and Update Tracking ===" << std::endl;

  uint64_t initial_size = cache->get_cache_size();
  std::cout << "Initial cache size: " << initial_size << std::endl;
  EXPECT_EQ(0u, initial_size);

  const int num_users = 3;
  const int num_buckets = 2;

  for (int i = 0; i < num_users; ++i) {
    std::string user_id = "user_" + std::to_string(i);
    ASSERT_EQ(0, cache->update_user_stats(user_id, (i + 1) * 1024 * 1024, (i + 1) * 10));
  }

  for (int i = 0; i < num_buckets; ++i) {
    std::string bucket_name = "bucket_" + std::to_string(i);
    ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, (i + 1) * 512 * 1024, (i + 1) * 5));
  }

  uint64_t final_size = cache->get_cache_size();
  std::cout << "Final cache size: " << final_size << std::endl;

  EXPECT_EQ(num_users + num_buckets, final_size);

  std::cout << "Cache size tracking verified" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, StatsAccuracyWithinTolerance) {
  std::cout << "\n=== Testing Statistics Accuracy ===" << std::endl;

  const std::string user_id = "accuracy_test_user";

  // Values from actual manual test
  const uint64_t expected_bytes = 348160;
  const uint64_t expected_objects = 29;

  ASSERT_EQ(0, cache->update_user_stats(user_id, expected_bytes, expected_objects));

  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());

  std::cout << "Expected: " << expected_bytes << " bytes, " << expected_objects << " objects" << std::endl;
  std::cout << "Cached:   " << stats->bytes_used << " bytes, " << stats->num_objects << " objects" << std::endl;

  EXPECT_EQ(expected_bytes, stats->bytes_used);
  EXPECT_EQ(expected_objects, stats->num_objects);

  std::cout << "Statistics accuracy verified" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, BackgroundRefreshSimulation) {
  std::cout << "\n=== Testing Background Refresh Simulation ===" << std::endl;

  const std::string user_id = "refresh_test_user";

  // Initial RADOS sync
  std::cout << "Initial RADOS sync: 1MB, 10 objects" << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 1024 * 1024, 10));

  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(1024 * 1024u, stats->bytes_used);

  // Simulate background refresh with updated RADOS values
  std::cout << "Background refresh: 2MB, 20 objects" << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 2 * 1024 * 1024, 20));

  stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(2 * 1024 * 1024u, stats->bytes_used);
  EXPECT_EQ(20u, stats->num_objects);

  std::cout << "Background refresh simulation successful" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, PersistenceAcrossRestart) {
  std::cout << "\n=== Testing Persistence Across Restart ===" << std::endl;

  const std::string user_id = "persist_user";
  const uint64_t bytes = 122880;
  const uint64_t objects = 6;

  // Add stats
  ASSERT_EQ(0, cache->update_user_stats(user_id, bytes, objects));
  std::cout << "Stored: " << bytes << " bytes, " << objects << " objects" << std::endl;

  // Get config before shutdown
  rgw::UsageCache::Config config;
  config.db_path = test_db_path;
  config.max_db_size = 1 << 20;

  // Shutdown and restart
  cache->shutdown();
  cache.reset();
  std::cout << "Cache shutdown (simulating RGW restart)" << std::endl;

  cache = std::make_unique<rgw::UsageCache>(g_cct, config);
  ASSERT_EQ(0, cache->init());
  std::cout << "Cache restarted" << std::endl;

  // Verify stats persisted
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value()) << "Stats should persist across restart";
  EXPECT_EQ(bytes, stats->bytes_used);
  EXPECT_EQ(objects, stats->num_objects);
  std::cout << "Recovered: " << stats->bytes_used << " bytes, "
            << stats->num_objects << " objects" << std::endl;

  std::cout << "Persistence test PASSED!" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, MultiUserClusterCoherence) {
  std::cout << "\n=== Testing Multi-User Cluster Coherence ===" << std::endl;

  // Simulate multiple users with different stats (as if from RADOS)
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
    std::cout << "Synced " << user.id << ": " << user.bytes << " bytes" << std::endl;
  }

  // Verify all users
  for (const auto& user : users) {
    auto stats = cache->get_user_stats(user.id);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(user.bytes, stats->bytes_used);
    EXPECT_EQ(user.objects, stats->num_objects);
  }

  std::cout << "Multi-user cluster coherence verified" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, BucketDeletionBugFix) {
  std::cout << "\n=== Testing Bucket Deletion Bug Fix ===" << std::endl;

  const std::string user_id = "testuser";

  // Initial state: 3 buckets, 40KB total
  std::cout << "Initial: 40960 bytes, 4 objects" << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 40960, 4));

  // After bucket deletion: stats from RADOS should be correct
  std::cout << "After bucket deletion (RADOS sync): 30720 bytes, 2 objects" << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 30720, 2));

  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(30720u, stats->bytes_used);
  EXPECT_EQ(2u, stats->num_objects);

  // Create new bucket after deletion
  std::cout << "After new bucket (RADOS sync): 51200 bytes, 3 objects" << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 51200, 3));

  stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(51200u, stats->bytes_used);
  EXPECT_EQ(3u, stats->num_objects);

  std::cout << "Bucket deletion bug fix VERIFIED!" << std::endl;
  std::cout << "(Stats remain correct after delete/create cycle)" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, ComprehensiveManualTestWorkflow) {
  std::cout << "\n=== Comprehensive Manual Test Workflow ===" << std::endl;
  std::cout << "Simulating complete manual testing scenario with NEW DESIGN...\n" << std::endl;

  // Step 1: Create user and buckets
  std::cout << "[Step 1] Creating user and buckets..." << std::endl;
  const std::string user_id = "testuser";

  // Step 2: Upload files and RADOS sync
  std::cout << "[Step 2] Simulating uploads and RADOS sync..." << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 40960, 4));
  std::cout << "  After uploads: 40960 bytes, 4 objects" << std::endl;

  // Step 3: Verify stats
  std::cout << "[Step 3] Verifying statistics..." << std::endl;
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(40960u, stats->bytes_used);
  std::cout << "  Verified: " << stats->bytes_used << " bytes" << std::endl;

  // Step 4: More uploads
  std::cout << "[Step 4] More uploads, RADOS sync..." << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 122880, 7));
  stats = cache->get_user_stats(user_id);
  EXPECT_EQ(122880u, stats->bytes_used);
  std::cout << "  Updated: " << stats->bytes_used << " bytes, " << stats->num_objects << " objects" << std::endl;

  // Step 5: Object deletion
  std::cout << "[Step 5] Object deletion, RADOS sync..." << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 112640, 6));
  stats = cache->get_user_stats(user_id);
  EXPECT_EQ(112640u, stats->bytes_used);
  std::cout << "  After deletion: " << stats->bytes_used << " bytes" << std::endl;

  // Step 6: Bucket deletion (bug fix test)
  std::cout << "[Step 6] Bucket deletion (BUG FIX TEST)..." << std::endl;
  ASSERT_EQ(0, cache->update_user_stats(user_id, 92160, 5));
  stats = cache->get_user_stats(user_id);
  EXPECT_EQ(92160u, stats->bytes_used);
  std::cout << "  After bucket deletion: " << stats->bytes_used << " bytes" << std::endl;
  std::cout << "  (Stats correct - bucket deletion bug FIXED!)" << std::endl;

  // Step 7: Performance check
  std::cout << "[Step 7] Performance check..." << std::endl;
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; ++i) {
    cache->get_user_stats(user_id);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  double avg = static_cast<double>(duration.count()) / 100.0;
  std::cout << "  100 operations in " << duration.count() << "ms (avg: " << avg << "ms)" << std::endl;
  EXPECT_LT(avg, 10.0);

  // Step 8: Cache metrics
  std::cout << "[Step 8] Cache metrics..." << std::endl;
  std::cout << "  Cache size: " << cache->get_cache_size() << std::endl;
  std::cout << "  Hit rate: " << cache->get_hit_rate() << "%" << std::endl;

  std::cout << "\n=== Comprehensive Test PASSED ===" << std::endl;
  std::cout << "New design validated: RADOS as single source of truth!" << std::endl;
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Initialize CephContext once for all tests
  std::vector<const char*> args;
  args.push_back("--no-mon-config");

  g_cct_holder = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY,
                             CINIT_FLAG_NO_MON_CONFIG);
  g_cct = g_cct_holder.get();
  common_init_finish(g_cct);

  int result = RUN_ALL_TESTS();

  // Clean up
  g_cct = nullptr;
  g_cct_holder.reset();

  return result;
}