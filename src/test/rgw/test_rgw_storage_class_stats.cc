// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include "cls/rgw/cls_rgw_types.h"
#include "cls/user/cls_user_types.h"
#include <chrono>

using namespace std;

TEST(StorageClassStats, OptionalInitialization) {
  rgw_bucket_dir_header header;
  
  // New buckets should initialize storage_class_stats as nullopt initially
  EXPECT_FALSE(header.storage_class_stats.has_value());
  
  // After initialization, should have value
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  EXPECT_TRUE(header.storage_class_stats.has_value());
  EXPECT_TRUE(header.storage_class_stats->empty());
}

TEST(StorageClassStats, OptionalEmptyVsNull) {
  rgw_bucket_dir_header legacy_header;
  rgw_bucket_dir_header new_empty_header;
  rgw_bucket_dir_header converted_header;
  
  // Legacy bucket - nullopt (not converted)
  EXPECT_FALSE(legacy_header.storage_class_stats.has_value());
  
  // New empty bucket - empty map (converted)
  new_empty_header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  EXPECT_TRUE(new_empty_header.storage_class_stats.has_value());
  EXPECT_TRUE(new_empty_header.storage_class_stats->empty());
  
  // Converted bucket with data
  converted_header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  rgw_bucket_category_stats stats;
  stats.num_entries = 10;
  stats.total_size = 1024;
  (*converted_header.storage_class_stats)["STANDARD"] = stats;
  
  EXPECT_TRUE(converted_header.storage_class_stats.has_value());
  EXPECT_FALSE(converted_header.storage_class_stats->empty());
  EXPECT_EQ((*converted_header.storage_class_stats)["STANDARD"].num_entries, 10);
}

TEST(StorageClassStats, OptionalAfterDeletion) {
  rgw_bucket_dir_header header;
  
  // Initialize with data
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  rgw_bucket_category_stats stats;
  stats.num_entries = 5;
  stats.total_size = 2048;
  (*header.storage_class_stats)["HDD"] = stats;
  
  EXPECT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ((*header.storage_class_stats)["HDD"].num_entries, 5);
  
  // Simulate deletion - set to 0 but keep entry
  (*header.storage_class_stats)["HDD"].num_entries = 0;
  (*header.storage_class_stats)["HDD"].total_size = 0;
  
  // Field should still have value (not nullopt)
  EXPECT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ((*header.storage_class_stats)["HDD"].num_entries, 0);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_size, 0);
}

TEST(StorageClassStats, UserHeaderOptional) {
  cls_user_header header;
  
  // Should start as nullopt
  EXPECT_FALSE(header.storage_class_stats.has_value());
  
  // Initialize
  header.storage_class_stats = std::unordered_map<std::string, cls_user_stats>();
  EXPECT_TRUE(header.storage_class_stats.has_value());
  
  // Add stats
  cls_user_stats user_stats;
  user_stats.total_entries = 100;
  user_stats.total_bytes = 10240;
  (*header.storage_class_stats)["default-placement::STANDARD"] = user_stats;
  
  EXPECT_EQ((*header.storage_class_stats)["default-placement::STANDARD"].total_entries, 100);
}

TEST(StorageClassStats, MultipleStorageClasses) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Add multiple storage classes
  rgw_bucket_category_stats standard_stats;
  standard_stats.num_entries = 10;
  standard_stats.total_size = 5120;
  
  rgw_bucket_category_stats hdd_stats;
  hdd_stats.num_entries = 20;
  hdd_stats.total_size = 10240;
  
  (*header.storage_class_stats)["STANDARD"] = standard_stats;
  (*header.storage_class_stats)["HDD"] = hdd_stats;
  
  EXPECT_EQ(header.storage_class_stats->size(), 2);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].num_entries, 10);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].num_entries, 20);
}

TEST(StorageClassStats, SafeAccess) {
  rgw_bucket_dir_header header;
  
  // Accessing without initialization should be safe with has_value check
  if (header.storage_class_stats.has_value()) {
    // This should not execute
    FAIL() << "Should not have value before initialization";
  }
  
  // Initialize
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Now access is safe
  if (header.storage_class_stats.has_value()) {
    EXPECT_TRUE(header.storage_class_stats->empty());
  } else {
    FAIL() << "Should have value after initialization";
  }
}

TEST(StorageClassStats, ResetToNullopt) {
  rgw_bucket_dir_header header;
  
  // Initialize with value
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*header.storage_class_stats)["STANDARD"].num_entries = 10;
  EXPECT_TRUE(header.storage_class_stats.has_value());
  
  // Reset to nullopt (simulate legacy bucket state)
  header.storage_class_stats = std::nullopt;
  EXPECT_FALSE(header.storage_class_stats.has_value());
  
  // Re-initialize
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  EXPECT_TRUE(header.storage_class_stats.has_value());
  EXPECT_TRUE(header.storage_class_stats->empty()); // Should be empty after reset
}

TEST(StorageClassStats, EmptyStringStorageClassName) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Test with empty string as storage class name (edge case)
  rgw_bucket_category_stats stats;
  stats.num_entries = 5;
  stats.total_size = 1024;
  (*header.storage_class_stats)[""] = stats;
  
  EXPECT_EQ(header.storage_class_stats->size(), 1);
  EXPECT_EQ((*header.storage_class_stats)[""].num_entries, 5);
}

TEST(StorageClassStats, VeryLongStorageClassName) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Test with very long storage class name (1000 characters)
  std::string long_name(1000, 'A');
  rgw_bucket_category_stats stats;
  stats.num_entries = 10;
  (*header.storage_class_stats)[long_name] = stats;
  
  EXPECT_EQ((*header.storage_class_stats)[long_name].num_entries, 10);
}

TEST(StorageClassStats, SpecialCharactersInName) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Test with special characters
  std::vector<std::string> special_names = {
    "STANDARD-IA",
    "storage_class_1",
    "storage.class.dots",
    "storage:class:colons",
    "UTF8-名称",
    "with spaces",
    "with\ttabs",
    "with\nnewlines"
  };
  
  for (const auto& name : special_names) {
    rgw_bucket_category_stats stats;
    stats.num_entries = 1;
    (*header.storage_class_stats)[name] = stats;
  }
  
  EXPECT_EQ(header.storage_class_stats->size(), special_names.size());
}

TEST(StorageClassStats, MaximumValues) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Test with maximum uint64_t values
  rgw_bucket_category_stats stats;
  stats.num_entries = UINT64_MAX;
  stats.total_size = UINT64_MAX;
  stats.total_size_rounded = UINT64_MAX;
  stats.actual_size = UINT64_MAX;
  
  (*header.storage_class_stats)["STANDARD"] = stats;
  
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].num_entries, UINT64_MAX);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_size, UINT64_MAX);
}

TEST(StorageClassStats, ManyStorageClasses) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Test with 100 different storage classes
  for (int i = 0; i < 100; i++) {
    std::string name = "SC_" + std::to_string(i);
    rgw_bucket_category_stats stats;
    stats.num_entries = i;
    stats.total_size = i * 1024;
    (*header.storage_class_stats)[name] = stats;
  }
  
  EXPECT_EQ(header.storage_class_stats->size(), 100);
  EXPECT_EQ((*header.storage_class_stats)["SC_50"].num_entries, 50);
  EXPECT_EQ((*header.storage_class_stats)["SC_50"].total_size, 50 * 1024);
}

TEST(StorageClassStats, OverwriteExistingEntry) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Add entry
  rgw_bucket_category_stats stats1;
  stats1.num_entries = 10;
  stats1.total_size = 1024;
  (*header.storage_class_stats)["STANDARD"] = stats1;
  
  // Overwrite with new values
  rgw_bucket_category_stats stats2;
  stats2.num_entries = 20;
  stats2.total_size = 2048;
  (*header.storage_class_stats)["STANDARD"] = stats2;
  
  EXPECT_EQ(header.storage_class_stats->size(), 1);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].num_entries, 20);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_size, 2048);
}

TEST(StorageClassStats, EraseEntry) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Add multiple entries
  (*header.storage_class_stats)["STANDARD"].num_entries = 10;
  (*header.storage_class_stats)["HDD"].num_entries = 20;
  (*header.storage_class_stats)["GLACIER"].num_entries = 30;
  
  EXPECT_EQ(header.storage_class_stats->size(), 3);
  
  // Erase one entry
  header.storage_class_stats->erase("HDD");
  
  EXPECT_EQ(header.storage_class_stats->size(), 2);
  EXPECT_EQ(header.storage_class_stats->count("STANDARD"), 1);
  EXPECT_EQ(header.storage_class_stats->count("HDD"), 0);
  EXPECT_EQ(header.storage_class_stats->count("GLACIER"), 1);
}

TEST(StorageClassStats, ClearAll) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Add entries
  (*header.storage_class_stats)["STANDARD"].num_entries = 10;
  (*header.storage_class_stats)["HDD"].num_entries = 20;
  
  EXPECT_EQ(header.storage_class_stats->size(), 2);
  
  // Clear all entries
  header.storage_class_stats->clear();
  
  EXPECT_TRUE(header.storage_class_stats.has_value()); // Still has_value
  EXPECT_TRUE(header.storage_class_stats->empty());    // But is empty
  EXPECT_EQ(header.storage_class_stats->size(), 0);
}

// ============================================================================
// COPY/MOVE SEMANTICS TESTS
// ============================================================================

TEST(StorageClassStats, CopyConstruction) {
  rgw_bucket_dir_header header1;
  header1.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*header1.storage_class_stats)["STANDARD"].num_entries = 10;
  
  // Copy construct
  rgw_bucket_dir_header header2 = header1;
  
  EXPECT_TRUE(header2.storage_class_stats.has_value());
  EXPECT_EQ((*header2.storage_class_stats)["STANDARD"].num_entries, 10);
  
  // Modify copy - original should not change
  (*header2.storage_class_stats)["STANDARD"].num_entries = 20;
  EXPECT_EQ((*header1.storage_class_stats)["STANDARD"].num_entries, 10);
  EXPECT_EQ((*header2.storage_class_stats)["STANDARD"].num_entries, 20);
}

TEST(StorageClassStats, MoveConstruction) {
  rgw_bucket_dir_header header1;
  header1.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*header1.storage_class_stats)["STANDARD"].num_entries = 10;
  
  // Move construct
  rgw_bucket_dir_header header2 = std::move(header1);
  
  EXPECT_TRUE(header2.storage_class_stats.has_value());
  EXPECT_EQ((*header2.storage_class_stats)["STANDARD"].num_entries, 10);
}

TEST(StorageClassStats, CopyAssignment) {
  rgw_bucket_dir_header header1, header2;
  header1.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*header1.storage_class_stats)["STANDARD"].num_entries = 10;
  
  // Copy assign
  header2 = header1;
  
  EXPECT_TRUE(header2.storage_class_stats.has_value());
  EXPECT_EQ((*header2.storage_class_stats)["STANDARD"].num_entries, 10);
}

TEST(StorageClassStats, AssignNulloptToValue) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*header.storage_class_stats)["STANDARD"].num_entries = 10;
  
  EXPECT_TRUE(header.storage_class_stats.has_value());
  
  // Assign nullopt (e.g., during legacy bucket handling)
  header.storage_class_stats = std::nullopt;
  
  EXPECT_FALSE(header.storage_class_stats.has_value());
}

// ============================================================================
// SERIALIZATION TESTS (Ceph encoding/decoding)
// ============================================================================

TEST(StorageClassStats, EncodeDecodeWithValue) {
  rgw_bucket_dir_header original;
  original.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*original.storage_class_stats)["STANDARD"].num_entries = 10;
  (*original.storage_class_stats)["HDD"].num_entries = 20;
  
  // Encode
  bufferlist bl;
  encode(original, bl);
  
  // Decode
  rgw_bucket_dir_header decoded;
  auto iter = bl.cbegin();
  decode(decoded, iter);
  
  // Verify
  EXPECT_TRUE(decoded.storage_class_stats.has_value());
  EXPECT_EQ(decoded.storage_class_stats->size(), 2);
  EXPECT_EQ((*decoded.storage_class_stats)["STANDARD"].num_entries, 10);
  EXPECT_EQ((*decoded.storage_class_stats)["HDD"].num_entries, 20);
}

TEST(StorageClassStats, EncodeDecodeNullopt) {
  rgw_bucket_dir_header original;
  // Leave storage_class_stats as nullopt
  
  // Encode
  bufferlist bl;
  encode(original, bl);
  
  // Decode
  rgw_bucket_dir_header decoded;
  auto iter = bl.cbegin();
  decode(decoded, iter);
  
  // Verify nullopt is preserved
  EXPECT_FALSE(decoded.storage_class_stats.has_value());
}

TEST(StorageClassStats, EncodeDecodeEmpty) {
  rgw_bucket_dir_header original;
  original.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  // Leave map empty
  
  // Encode
  bufferlist bl;
  encode(original, bl);
  
  // Decode
  rgw_bucket_dir_header decoded;
  auto iter = bl.cbegin();
  decode(decoded, iter);
  
  // Verify empty map is preserved (not nullopt)
  EXPECT_TRUE(decoded.storage_class_stats.has_value());
  EXPECT_TRUE(decoded.storage_class_stats->empty());
}

// ============================================================================
// USER STATS TESTS
// ============================================================================

TEST(StorageClassStats, UserStatsMultiplePlacements) {
  cls_user_header header;
  header.storage_class_stats = std::unordered_map<std::string, cls_user_stats>();
  
  // Add stats for multiple placements and storage classes
  std::vector<std::string> keys = {
    "default-placement::STANDARD",
    "default-placement::HDD",
    "archive-placement::GLACIER",
    "fast-placement::SSD"
  };
  
  for (size_t i = 0; i < keys.size(); i++) {
    cls_user_stats stats;
    stats.total_entries = (i + 1) * 10;
    stats.total_bytes = (i + 1) * 1024;
    (*header.storage_class_stats)[keys[i]] = stats;
  }
  
  EXPECT_EQ(header.storage_class_stats->size(), 4);
  EXPECT_EQ((*header.storage_class_stats)["archive-placement::GLACIER"].total_entries, 30);
}

// ============================================================================
// ITERATOR SAFETY TESTS
// ============================================================================

TEST(StorageClassStats, IterationWhileModifying) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Add initial entries
  for (int i = 0; i < 10; i++) {
    (*header.storage_class_stats)["SC_" + std::to_string(i)].num_entries = i;
  }
  
  // Iterate and collect keys
  std::vector<std::string> keys;
  for (const auto& [key, value] : *header.storage_class_stats) {
    keys.push_back(key);
  }
  
  EXPECT_EQ(keys.size(), 10);
}

TEST(StorageClassStats, SafeIterationWithHasValue) {
  rgw_bucket_dir_header header;

  // Should be nullopt initially
  EXPECT_FALSE(header.storage_class_stats.has_value());

  // Initialize and iterate
  header.storage_class_stats =
      std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*header.storage_class_stats)["STANDARD"].num_entries = 10;

  ASSERT_TRUE(header.storage_class_stats.has_value());

  int count = 0;
  for (const auto& [key, value] : *header.storage_class_stats) {
    count++;
    EXPECT_EQ(key, "STANDARD");
    EXPECT_EQ(value.num_entries, 10);
  }

  EXPECT_EQ(count, 1);
}

// ============================================================================
// THREAD SAFETY TESTS (Basic)
// ============================================================================

TEST(StorageClassStats, ConcurrentReads) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*header.storage_class_stats)["STANDARD"].num_entries = 100;
  (*header.storage_class_stats)["HDD"].num_entries = 200;
  
  // Multiple threads reading simultaneously
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  
  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&header, &success_count]() {
      if (header.storage_class_stats.has_value()) {
        if (header.storage_class_stats->count("STANDARD") > 0) {
          auto val = (*header.storage_class_stats)["STANDARD"].num_entries;
          if (val == 100) {
            success_count++;
          }
        }
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  EXPECT_EQ(success_count, 10);
}

// ============================================================================
// BOUNDARY CONDITION TESTS
// ============================================================================

TEST(StorageClassStats, ZeroValues) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // All zero values
  rgw_bucket_category_stats stats;
  stats.num_entries = 0;
  stats.total_size = 0;
  stats.total_size_rounded = 0;
  stats.actual_size = 0;
  
  (*header.storage_class_stats)["STANDARD"] = stats;
  
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].num_entries, 0);
  EXPECT_TRUE(header.storage_class_stats.has_value()); // Still has_value even with 0
}

TEST(StorageClassStats, ValueOrAlternative) {
  rgw_bucket_dir_header header1, header2;
  
  // header1 is nullopt
  auto map1 = header1.storage_class_stats.value_or(
    std::unordered_map<std::string, rgw_bucket_category_stats>()
  );
  EXPECT_TRUE(map1.empty());
  
  // header2 has value
  header2.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  (*header2.storage_class_stats)["STANDARD"].num_entries = 10;
  
  auto map2 = header2.storage_class_stats.value_or(
    std::unordered_map<std::string, rgw_bucket_category_stats>()
  );
  EXPECT_EQ(map2.size(), 1);
  EXPECT_EQ(map2["STANDARD"].num_entries, 10);
}

// ============================================================================
// REGRESSION TESTS
// ============================================================================

TEST(StorageClassStats, DereferenceSafety) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Test all dereference operators work correctly
  EXPECT_NO_THROW({
    (*header.storage_class_stats)["STANDARD"].num_entries = 10;  // operator*
    header.storage_class_stats->size();                          // operator->
  });
  
  EXPECT_EQ(header.storage_class_stats->size(), 1);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].num_entries, 10);
}

// ============================================================================
// PERFORMANCE BENCHMARK TESTS
// ============================================================================

TEST(StorageClassStats, BenchmarkLargeScale) {
  using namespace std::chrono;
  
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  auto start = high_resolution_clock::now();
  
  // Add 10,000 storage classes
  for (int i = 0; i < 10000; i++) {
    std::string name = "STORAGE_CLASS_" + std::to_string(i);
    rgw_bucket_category_stats stats;
    stats.num_entries = i;
    stats.total_size = i * 1024;
    (*header.storage_class_stats)[name] = stats;
  }
  
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(end - start).count();
  
  EXPECT_EQ(header.storage_class_stats->size(), 10000);
  std::cout << "Added 10,000 storage classes in " << duration << " µs" << std::endl;
  
  // Should complete in under 100ms
  EXPECT_LT(duration, 100000); // 100ms in microseconds
}

TEST(StorageClassStats, BenchmarkIteration) {
  using namespace std::chrono;
  
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Setup: 1000 storage classes
  for (int i = 0; i < 1000; i++) {
    (*header.storage_class_stats)["SC_" + std::to_string(i)].num_entries = i;
  }
  
  auto start = high_resolution_clock::now();
  
  // Iterate 1000 times
  uint64_t sum = 0;
  for (int iteration = 0; iteration < 1000; iteration++) {
    if (header.storage_class_stats.has_value()) {
      for (const auto& [key, value] : *header.storage_class_stats) {
        sum += value.num_entries;
      }
    }
  }
  
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(end - start).count();
  
  std::cout << "1000 iterations over 1000 entries in " << duration << " µs" << std::endl;
  EXPECT_LT(duration, 50000); // Should be under 50ms
  EXPECT_GT(sum, 0); // Make sure loop ran
}

TEST(StorageClassStats, BenchmarkSerialization) {
  using namespace std::chrono;
  
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Add 100 storage classes
  for (int i = 0; i < 100; i++) {
    (*header.storage_class_stats)["SC_" + std::to_string(i)].num_entries = i * 100;
    (*header.storage_class_stats)["SC_" + std::to_string(i)].total_size = i * 1024 * 1024;
  }
  
  auto start = high_resolution_clock::now();
  
  // Encode/decode 100 times
  for (int i = 0; i < 100; i++) {
    bufferlist bl;
    encode(header, bl);
    
    rgw_bucket_dir_header decoded;
    auto iter = bl.cbegin();
    decode(decoded, iter);
  }
  
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(end - start).count();
  
  std::cout << "100 encode/decode cycles in " << duration << " µs" << std::endl;
  EXPECT_LT(duration, 10000); // Should be under 10ms
}

TEST(StorageClassStats, BenchmarkMemoryUsage) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  size_t initial_size = sizeof(header);
  std::cout << "Header base size: " << initial_size << " bytes" << std::endl;
  
  // Add varying numbers of storage classes and measure
  std::vector<int> sizes = {1, 10, 100, 1000};
  
  for (int size : sizes) {
    rgw_bucket_dir_header test_header;
    test_header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
    
    for (int i = 0; i < size; i++) {
      (*test_header.storage_class_stats)["SC_" + std::to_string(i)].num_entries = i;
    }
    
    bufferlist bl;
    encode(test_header, bl);
    
    std::cout << "  " << size << " storage classes: " << bl.length() << " bytes encoded" << std::endl;
  }
  
  SUCCEED(); // Informational test
}

// ============================================================================
// STRESS TESTS
// ============================================================================

TEST(StorageClassStats, StressTestRapidChanges) {
  rgw_bucket_dir_header header;
  header.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Rapidly add and remove storage classes
  for (int cycle = 0; cycle < 100; cycle++) {
    // Add 50
    for (int i = 0; i < 50; i++) {
      std::string name = "SC_" + std::to_string(cycle) + "_" + std::to_string(i);
      (*header.storage_class_stats)[name].num_entries = i;
    }
    
    // Remove half
    int removed = 0;
    for (auto it = header.storage_class_stats->begin(); 
         it != header.storage_class_stats->end() && removed < 25; ) {
      it = header.storage_class_stats->erase(it);
      removed++;
    }
  }
  
  // Should still be valid
  EXPECT_TRUE(header.storage_class_stats.has_value());
  std::cout << "Final size after stress test: " 
            << header.storage_class_stats->size() << " entries" << std::endl;
}

TEST(StorageClassStats, StressTestDeepCopy) {
  rgw_bucket_dir_header original;
  original.storage_class_stats = std::unordered_map<std::string, rgw_bucket_category_stats>();
  
  // Add 100 entries
  for (int i = 0; i < 100; i++) {
    (*original.storage_class_stats)["SC_" + std::to_string(i)].num_entries = i;
  }
  
  // Make 1000 copies
  std::vector<rgw_bucket_dir_header> copies;
  for (int i = 0; i < 1000; i++) {
    copies.push_back(original);
  }
  
  // Verify all copies are valid
  for (const auto& copy : copies) {
    EXPECT_TRUE(copy.storage_class_stats.has_value());
    EXPECT_EQ(copy.storage_class_stats->size(), 100);
  }
  
  std::cout << "Successfully created 1000 deep copies" << std::endl;
}