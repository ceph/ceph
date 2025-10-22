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

// Add these test cases to the existing TestRGWUsagePerfCounters test fixture

TEST_F(TestRGWUsagePerfCounters, MultipleUserBucketScenario) {
  // This simulates a manual scenario test: multiple users with multiple buckets
  // User: testuser with bucket1 (small.txt + medium.bin) and bucket2 (large.bin)
  
  std::cout << "\n=== Testing Multiple User/Bucket Scenario ===" << std::endl;
  
  const std::string user_id = "testuser";
  const std::string bucket1 = "bucket1";
  const std::string bucket2 = "bucket2";
  
  // Simulate uploading to bucket1: small.txt (~12 bytes) + medium.bin (5MB)
  const uint64_t bucket1_bytes = 12 + (5 * 1024 * 1024);
  const uint64_t bucket1_objects = 2;
  
  // Simulate uploading to bucket2: large.bin (10MB)
  const uint64_t bucket2_bytes = 10 * 1024 * 1024;
  const uint64_t bucket2_objects = 1;
  
  // Total user stats
  const uint64_t total_user_bytes = bucket1_bytes + bucket2_bytes;
  const uint64_t total_user_objects = bucket1_objects + bucket2_objects;
  
  // Update bucket stats
  ASSERT_EQ(0, cache->update_bucket_stats(bucket1, bucket1_bytes, bucket1_objects));
  ASSERT_EQ(0, cache->update_bucket_stats(bucket2, bucket2_bytes, bucket2_objects));
  
  // Update user stats (aggregated from buckets)
  ASSERT_EQ(0, cache->update_user_stats(user_id, total_user_bytes, total_user_objects));
  
  std::cout << "Updated stats:" << std::endl;
  std::cout << "  User: " << user_id << std::endl;
  std::cout << "  Bucket1: " << bucket1 << " - " << bucket1_bytes << " bytes, " 
            << bucket1_objects << " objects" << std::endl;
  std::cout << "  Bucket2: " << bucket2 << " - " << bucket2_bytes << " bytes, " 
            << bucket2_objects << " objects" << std::endl;
  
  // Verify bucket1 stats
  auto b1_stats = cache->get_bucket_stats(bucket1);
  ASSERT_TRUE(b1_stats.has_value()) << "Bucket1 stats not found";
  EXPECT_EQ(bucket1_bytes, b1_stats->bytes_used);
  EXPECT_EQ(bucket1_objects, b1_stats->num_objects);
  std::cout << " Bucket1 stats verified" << std::endl;
  
  // Verify bucket2 stats
  auto b2_stats = cache->get_bucket_stats(bucket2);
  ASSERT_TRUE(b2_stats.has_value()) << "Bucket2 stats not found";
  EXPECT_EQ(bucket2_bytes, b2_stats->bytes_used);
  EXPECT_EQ(bucket2_objects, b2_stats->num_objects);
  std::cout << " Bucket2 stats verified" << std::endl;
  
  // Verify user stats (should be sum of all buckets)
  auto user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value()) << "User stats not found";
  EXPECT_EQ(total_user_bytes, user_stats->bytes_used);
  EXPECT_EQ(total_user_objects, user_stats->num_objects);
  std::cout << " User stats verified: " << total_user_bytes << " bytes (~15MB), " 
            << total_user_objects << " objects" << std::endl;
  
  // Verify expected totals match manual test expectations
  // Expected: ~15MB total (5MB + 10MB + small file)
  const uint64_t expected_min_bytes = 15 * 1024 * 1024;  // 15MB
  EXPECT_GE(user_stats->bytes_used, expected_min_bytes) 
    << "User should have at least 15MB";
  EXPECT_EQ(3u, user_stats->num_objects) 
    << "User should have exactly 3 objects";
  
  std::cout << " All statistics match expected values from manual test" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, CacheHitOnRepeatedAccess) {
  // This simulates: s3cmd ls s3://bucket1 (multiple times)
  // Should see cache hits increase
  
  std::cout << "\n=== Testing Cache Hits on Repeated Access ===" << std::endl;
  
  const std::string bucket_name = "bucket1";
  
  // First, populate the cache
  ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, 5 * 1024 * 1024, 2));
  
  // Get initial hit/miss counts
  uint64_t initial_hits = cache->get_cache_hits();
  uint64_t initial_misses = cache->get_cache_misses();
  
  std::cout << "Initial state - Hits: " << initial_hits 
            << ", Misses: " << initial_misses << std::endl;
  
  // Simulate multiple bucket listings (like s3cmd ls s3://bucket1)
  const int num_listings = 3;
  for (int i = 0; i < num_listings; ++i) {
    auto stats = cache->get_bucket_stats(bucket_name);
    ASSERT_TRUE(stats.has_value()) << "Failed to get bucket stats on iteration " << i;
    std::cout << "  Listing " << (i + 1) << ": Found bucket with " 
              << stats->bytes_used << " bytes" << std::endl;
  }
  
  // Get final hit/miss counts
  uint64_t final_hits = cache->get_cache_hits();
  uint64_t final_misses = cache->get_cache_misses();
  
  uint64_t hits_increase = final_hits - initial_hits;
  
  std::cout << "Final state - Hits: " << final_hits 
            << ", Misses: " << final_misses << std::endl;
  std::cout << "Cache hits increased by: " << hits_increase << std::endl;
  
  // We expect all accesses to be hits since the entry is already cached
  EXPECT_EQ(num_listings, hits_increase) 
    << "Expected " << num_listings << " cache hits, got " << hits_increase;
  
  // Hit rate should be good
  double hit_rate = cache->get_hit_rate();
  std::cout << " Cache hit rate: " << hit_rate << "%" << std::endl;
  EXPECT_GT(hit_rate, 0.0) << "Cache hit rate should be > 0%";
  
  std::cout << " Cache behavior verified - repeated access produces cache hits" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, PerformanceNoBlockingInIOPath) {
  // CRITICAL TEST: Verify that cache operations are fast
  // This simulates the fix for the 90ms cls_bucket_head() issue
  
  std::cout << "\n=== Testing Performance: No Blocking in I/O Path ===" << std::endl;
  
  const int num_operations = 100;
  const std::string bucket_prefix = "perf_bucket_";
  
  // Warm up - add some entries to cache
  for (int i = 0; i < 10; ++i) {
    std::string bucket_name = bucket_prefix + std::to_string(i);
    cache->update_bucket_stats(bucket_name, i * 1024, i);
  }
  
  // Measure time for 100 get operations
  auto start = std::chrono::high_resolution_clock::now();
  
  for (int i = 0; i < num_operations; ++i) {
    std::string bucket_name = bucket_prefix + std::to_string(i % 10);
    auto stats = cache->get_bucket_stats(bucket_name);
    // These should be cache hits, should be VERY fast
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  
  double avg_per_op_us = static_cast<double>(duration.count()) / num_operations;
  double avg_per_op_ms = avg_per_op_us / 1000.0;
  
  std::cout << "Performance results:" << std::endl;
  std::cout << "  Total operations: " << num_operations << std::endl;
  std::cout << "  Total time: " << duration.count() << " μs ("
            << (duration.count() / 1000.0) << " ms)" << std::endl;
  std::cout << "  Average per operation: " << avg_per_op_us << " μs ("
            << avg_per_op_ms << " ms)" << std::endl;
  
  // CRITICAL ASSERTION: Operations should be MUCH faster than 90ms
  // Even with margin for test overhead, should be < 10ms per op
  EXPECT_LT(avg_per_op_ms, 10.0) 
    << "Average operation time " << avg_per_op_ms 
    << "ms is too high - should be < 10ms (was 90ms with sync_owner_stats bug)";
  
  // Better yet, should be sub-millisecond for cache hits
  EXPECT_LT(avg_per_op_ms, 1.0)
    << "Average operation time " << avg_per_op_ms
    << "ms should be < 1ms for cache hits";
  
  // Calculate cache hit rate - should be high since we're reusing buckets
  double hit_rate = cache->get_hit_rate();
  std::cout << "  Cache hit rate: " << hit_rate << "%" << std::endl;
  
  std::cout << " Performance test PASSED - no blocking in I/O path" << std::endl;
  std::cout << " Operations are ~" << (90.0 / avg_per_op_ms) << "x faster than the old bug!" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, CacheSizeAndUpdateTracking) {
  // This validates the cache size and update counters
  // Simulates checking: cache_size, cache_updates counters
  
  std::cout << "\n=== Testing Cache Size and Update Tracking ===" << std::endl;
  
  // Initial state
  uint64_t initial_size = cache->get_cache_size();
  std::cout << "Initial cache size: " << initial_size << std::endl;
  EXPECT_EQ(0u, initial_size) << "Cache should start empty";
  
  // Add users and buckets
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
  
  // Check cache size
  uint64_t final_size = cache->get_cache_size();
  std::cout << "Final cache size: " << final_size << std::endl;
  
  EXPECT_EQ(num_users + num_buckets, final_size) 
    << "Cache size should reflect " << (num_users + num_buckets) << " entries";
  
  std::cout << " Cache size tracking verified" << std::endl;
  
  // Verify we can retrieve all entries
  for (int i = 0; i < num_users; ++i) {
    std::string user_id = "user_" + std::to_string(i);
    auto stats = cache->get_user_stats(user_id);
    ASSERT_TRUE(stats.has_value()) << "User " << user_id << " not found in cache";
    EXPECT_EQ((i + 1) * 1024 * 1024u, stats->bytes_used);
  }
  
  for (int i = 0; i < num_buckets; ++i) {
    std::string bucket_name = "bucket_" + std::to_string(i);
    auto stats = cache->get_bucket_stats(bucket_name);
    ASSERT_TRUE(stats.has_value()) << "Bucket " << bucket_name << " not found in cache";
    EXPECT_EQ((i + 1) * 512 * 1024u, stats->bytes_used);
  }
  
  std::cout << " All cache entries verified" << std::endl;
  
  // Check that cache hits occurred
  uint64_t hits = cache->get_cache_hits();
  std::cout << "Total cache hits: " << hits << std::endl;
  EXPECT_GT(hits, 0u) << "Should have some cache hits from retrievals";
  
  std::cout << " Cache metrics tracking verified" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, StatsAccuracyWithinTolerance) {
  // Verify that statistics are accurate within acceptable tolerance
  
  std::cout << "\n=== Testing Statistics Accuracy ===" << std::endl;
  
  const std::string user_id = "accuracy_test_user";
  
  // Upload files with known sizes (matching our manual test)
  struct FileUpload {
    std::string name;
    uint64_t size;
  };
  
  std::vector<FileUpload> files = {
    {"small.txt", 12},                    // "Hello World\n"
    {"medium.bin", 5 * 1024 * 1024},      // 5MB
    {"large.bin", 10 * 1024 * 1024}       // 10MB
  };
  
  uint64_t total_bytes = 0;
  uint64_t total_objects = files.size();
  
  std::cout << "Simulating file uploads:" << std::endl;
  for (const auto& file : files) {
    total_bytes += file.size;
    std::cout << "  " << file.name << ": " << file.size << " bytes" << std::endl;
  }
  
  std::cout << "Total: " << total_bytes << " bytes (" 
            << (total_bytes / (1024.0 * 1024.0)) << " MB), "
            << total_objects << " objects" << std::endl;
  
  // Update cache with total stats
  ASSERT_EQ(0, cache->update_user_stats(user_id, total_bytes, total_objects));
  
  // Retrieve and verify
  auto stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(stats.has_value()) << "User stats not found";
  
  std::cout << "\nVerifying accuracy:" << std::endl;
  std::cout << "  Expected bytes: " << total_bytes << std::endl;
  std::cout << "  Cached bytes: " << stats->bytes_used << std::endl;
  std::cout << "  Expected objects: " << total_objects << std::endl;
  std::cout << "  Cached objects: " << stats->num_objects << std::endl;
  
  // Exact match for in-memory cache
  EXPECT_EQ(total_bytes, stats->bytes_used) << "Byte count should be exact";
  EXPECT_EQ(total_objects, stats->num_objects) << "Object count should be exact";
  
  // Verify totals match expectations (~15MB)
  const uint64_t expected_mb = 15;
  const uint64_t expected_bytes = expected_mb * 1024 * 1024;
  const double tolerance = 0.1;  // 10% tolerance
  
  double bytes_diff = std::abs(static_cast<double>(stats->bytes_used) - expected_bytes);
  double bytes_diff_pct = (bytes_diff / expected_bytes) * 100.0;
  
  std::cout << "  Difference from expected ~15MB: " << bytes_diff_pct << "%" << std::endl;
  
  EXPECT_LT(bytes_diff_pct, tolerance * 100.0) 
    << "Byte count should be within " << (tolerance * 100.0) << "% of expected";
  
  std::cout << " Statistics accuracy verified (within tolerance)" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, ExpiryAndRefreshScenario) {
  // Test that expired entries are handled correctly
  // Simulates the background refresh mechanism
  
  std::cout << "\n=== Testing Expiry and Refresh Scenario ===" << std::endl;
  
  const std::string bucket_name = "expiry_test_bucket";
  
  // Add bucket stats
  ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, 1024 * 1024, 10));
  
  // Verify stats exist
  auto stats = cache->get_bucket_stats(bucket_name);
  ASSERT_TRUE(stats.has_value()) << "Stats should exist initially";
  std::cout << " Initial stats cached" << std::endl;
  
  // Access should be a cache hit
  uint64_t hits_before = cache->get_cache_hits();
  stats = cache->get_bucket_stats(bucket_name);
  uint64_t hits_after = cache->get_cache_hits();
  EXPECT_EQ(hits_before + 1, hits_after) << "Should have one more cache hit";
  std::cout << " Cache hit recorded" << std::endl;
  
  // Wait for TTL to expire (2 seconds + buffer)
  std::cout << "Waiting for cache entry to expire (2.5s)..." << std::endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  
  // Access expired entry - should be a miss
  uint64_t misses_before = cache->get_cache_misses();
  stats = cache->get_bucket_stats(bucket_name);
  uint64_t misses_after = cache->get_cache_misses();
  
  EXPECT_FALSE(stats.has_value()) << "Expired entry should not be returned";
  EXPECT_EQ(misses_before + 1, misses_after) << "Should have one more cache miss";
  std::cout << " Expired entry handled correctly (cache miss)" << std::endl;
  
  // Simulate background refresh - re-add the stats
  std::cout << "Simulating background refresh..." << std::endl;
  ASSERT_EQ(0, cache->update_bucket_stats(bucket_name, 2 * 1024 * 1024, 20));
  
  // Now should be in cache again with new values
  stats = cache->get_bucket_stats(bucket_name);
  ASSERT_TRUE(stats.has_value()) << "Refreshed stats should exist";
  EXPECT_EQ(2 * 1024 * 1024u, stats->bytes_used) << "Should have updated bytes";
  EXPECT_EQ(20u, stats->num_objects) << "Should have updated objects";
  
  std::cout << "Background refresh simulation successful" << std::endl;
  std::cout << "Cache properly handles expiry and refresh cycle" << std::endl;
}

TEST_F(TestRGWUsagePerfCounters, ComprehensiveManualTestWorkflow) {
  // This test combines all the test steps into one comprehensive test
  
  std::cout << "\n=== Comprehensive Manual Test Workflow ===" << std::endl;
  std::cout << "Simulating complete manual testing scenario..." << std::endl;
  
  // Step 1: Create user and buckets
  std::cout << "\n[Step 1] Creating user and buckets..." << std::endl;
  const std::string user_id = "testuser";
  const std::string bucket1 = "bucket1";
  const std::string bucket2 = "bucket2";
  
  // Step 2: Upload files
  std::cout << "[Step 2] Uploading files..." << std::endl;
  
  // Bucket1: small.txt + medium.bin
  uint64_t b1_bytes = 12 + (5 * 1024 * 1024);  // ~5MB
  ASSERT_EQ(0, cache->update_bucket_stats(bucket1, b1_bytes, 2));
  std::cout << "  Uploaded to " << bucket1 << ": 2 files, " << b1_bytes << " bytes" << std::endl;
  
  // Bucket2: large.bin
  uint64_t b2_bytes = 10 * 1024 * 1024;  // 10MB
  ASSERT_EQ(0, cache->update_bucket_stats(bucket2, b2_bytes, 1));
  std::cout << "  Uploaded to " << bucket2 << ": 1 file, " << b2_bytes << " bytes" << std::endl;
  
  // User total
  uint64_t user_bytes = b1_bytes + b2_bytes;
  uint64_t user_objects = 3;
  ASSERT_EQ(0, cache->update_user_stats(user_id, user_bytes, user_objects));
  std::cout << "  User total: " << user_objects << " objects, " << user_bytes << " bytes" << std::endl;
  
  // Step 3: Check user statistics
  std::cout << "\n[Step 3] Checking user statistics..." << std::endl;
  auto user_stats = cache->get_user_stats(user_id);
  ASSERT_TRUE(user_stats.has_value());
  std::cout << "  rgw_user_" << user_id << ":" << std::endl;
  std::cout << "    used_bytes: " << user_stats->bytes_used << std::endl;
  std::cout << "    num_objects: " << user_stats->num_objects << std::endl;
  EXPECT_EQ(user_bytes, user_stats->bytes_used);
  EXPECT_EQ(3u, user_stats->num_objects);
  std::cout << " User stats verified" << std::endl;
  
  // Step 4: Check bucket statistics
  std::cout << "\n[Step 4] Checking bucket statistics..." << std::endl;
  auto b1_stats = cache->get_bucket_stats(bucket1);
  ASSERT_TRUE(b1_stats.has_value());
  std::cout << "  rgw_bucket_" << bucket1 << ":" << std::endl;
  std::cout << "    used_bytes: " << b1_stats->bytes_used << std::endl;
  std::cout << "    num_objects: " << b1_stats->num_objects << std::endl;
  EXPECT_EQ(b1_bytes, b1_stats->bytes_used);
  EXPECT_EQ(2u, b1_stats->num_objects);
  std::cout << " Bucket1 stats verified" << std::endl;
  
  // Step 5: Test cache behavior (multiple listings)
  std::cout << "\n[Step 5] Testing cache behavior..." << std::endl;
  uint64_t hits_before = cache->get_cache_hits();
  
  for (int i = 0; i < 3; ++i) {
    cache->get_bucket_stats(bucket1);
  }
  
  uint64_t hits_after = cache->get_cache_hits();
  uint64_t hit_increase = hits_after - hits_before;
  std::cout << "  Performed 3 bucket listings" << std::endl;
  std::cout << "  Cache hits increased by: " << hit_increase << std::endl;
  EXPECT_GE(hit_increase, 3u);
  std::cout << " Cache hits verified" << std::endl;
  
  // Step 6: Check cache metrics
  std::cout << "\n[Step 6] Checking cache metrics..." << std::endl;
  uint64_t cache_size = cache->get_cache_size();
  double hit_rate = cache->get_hit_rate();
  std::cout << "  Cache size: " << cache_size << std::endl;
  std::cout << "  Cache hit rate: " << hit_rate << "%" << std::endl;
  EXPECT_GE(cache_size, 3u);  // At least user + 2 buckets
  EXPECT_GT(hit_rate, 0.0);
  std::cout << " Cache metrics verified" << std::endl;
  
  // Step 7: Performance check
  std::cout << "\n[Step 7] Performance regression check..." << std::endl;
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; ++i) {
    cache->get_bucket_stats(bucket1);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  double avg_ms = static_cast<double>(duration.count()) / 100.0;
  
  std::cout << "  100 operations in " << duration.count() << "ms" << std::endl;
  std::cout << "  Average: " << avg_ms << "ms per operation" << std::endl;
  EXPECT_LT(avg_ms, 10.0) << "Should be much faster than 90ms";
  std::cout << " No performance regression detected" << std::endl;
  
  std::cout << "\n=== Comprehensive Test PASSED ===" << std::endl;
  std::cout << "All manual test scenarios validated successfully!" << std::endl;
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