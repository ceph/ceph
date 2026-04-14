// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * TestBackendBasics - Unified parameterized test harness for EC and Replicated
 * backend operations.
 *
 * Two fixture classes are defined, each parameterized over the full set of
 * backend configurations:
 *
 * TestBackendBasics
 *   Parameterized over BackendWriteReadParam (BackendConfig × WriteReadParam).
 *   13 backends × 8 data sizes = 104 instances per test body.
 *
 *   WriteThenRead  – write data, verify protocol messages, read back, verify
 *                    data integrity.
 *   PartialWrite   – create an object, perform a partial write at a non-zero
 *                    offset, read back and verify all three regions.
 *
 * TestECFailover
 *   Parameterized over BackendConfig (EC configs only, 12 instances).
 *   Failover is an EC-specific concept (shard-based primary election).
 *
 *   BasicOSDMapUpdate – write, update OSDMap epoch, verify read still works.
 *   PrimaryFailover   – write, fail OSD 0, verify new primary and degraded
 *                       read with EC reconstruction.
 */

#include <gtest/gtest.h>
#include "test/osd/PGBackendTestFixture.h"
#include "test/osd/TestCommon.h"
#include "messages/MOSDECSubOpWrite.h"

using namespace std;

// ---------------------------------------------------------------------------
// TestBackendBasics fixture
// ---------------------------------------------------------------------------

/**
 * TestBackendBasics - single fixture parameterized over BackendWriteReadParam.
 *
 * The constructor reads the BackendConfig portion of the parameter and
 * configures the base fixture fields (pool_type, k, m, stripe_unit, ec_plugin,
 * ec_technique, ec_optimizations, num_replicas, min_size) before SetUp() is
 * called by GTest.
 */
class TestBackendBasics : public PGBackendTestFixture,
                          public ::testing::WithParamInterface<BackendWriteReadParam> {
public:
  TestBackendBasics() : PGBackendTestFixture() {
    const auto& config = GetParam().backend;
    pool_type = config.pool_type;
    if (pool_type == EC) {
      k = config.k;
      m = config.m;
      stripe_unit = config.stripe_unit;
      ec_plugin = config.ec_plugin;
      ec_technique = config.ec_technique;
      pool_flags = config.pool_flags;
      num_zones = config.num_zones;
    } else {
      num_replicas = 3;
      min_size = 2;
    }
  }

  void SetUp() override {
    PGBackendTestFixture::SetUp();
  }

  /**
   * Simulate failure of multiple OSDs by marking them down in the OSDMap.
   * This is similar to TestECFailover::simulate_osd_failure but handles
   * multiple failures at once.
   */
  void simulate_multiple_osd_failures(const std::vector<int>& failed_osds) {
    auto new_osdmap = std::make_shared<OSDMap>();
    new_osdmap->deepish_copy_from(*osdmap);

    // Build new acting set with failed OSDs replaced by CRUSH_ITEM_NONE
    std::vector<int> new_acting;
    int total_osds = (num_zones > 0) ? (num_zones * (k + m)) : (k + m);

    for (int i = 0; i < total_osds; i++) {
      bool is_failed = std::find(failed_osds.begin(), failed_osds.end(), i) != failed_osds.end();
      new_acting.push_back(is_failed ? CRUSH_ITEM_NONE : i);
    }

    // Get the pool to use pgtemp_primaryfirst transformation
    const pg_pool_t* pool = new_osdmap->get_pg_pool(pgid.pool());
    ceph_assert(pool != nullptr);

    // For EC pools with optimizations, pgtemp_primaryfirst reorders the acting set
    std::vector<int> transformed_acting = new_osdmap->pgtemp_primaryfirst(*pool, new_acting);

    // Use OSDMap::Incremental to set pg_temp and mark OSDs as down
    OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
    inc.fsid = new_osdmap->get_fsid();

    for (int failed_osd : failed_osds) {
      inc.new_state[failed_osd] = CEPH_OSD_EXISTS;  // Mark as down (exists but not UP)
    }

    // Convert to mempool vector for pg_temp
    mempool::osdmap::vector<int> pg_temp_vec(transformed_acting.begin(), transformed_acting.end());
    inc.new_pg_temp[pgid] = pg_temp_vec;

    new_osdmap->apply_incremental(inc);

    // Finalize the CRUSH map
    new_osdmap->crush->finalize();

    // Update listener shardsets to remove failed shards
    for (int failed_osd : failed_osds) {
      pg_shard_t failed_shard(failed_osd, shard_id_t(failed_osd));
      for (auto& [instance_id, list] : listeners) {
        list->shardset.erase(failed_shard);
        list->acting_recovery_backfill_shard_id_set.erase(shard_id_t(failed_osd));
      }
    }

    // update_osdmap will query the OSDMap to determine the primary
    update_osdmap(new_osdmap);
  }
};

// ---------------------------------------------------------------------------
// TestBackendBasics: WriteThenRead
// ---------------------------------------------------------------------------

/**
 * WriteThenRead - write data of the parameterized size, verify protocol
 * messages were sent, read back, and verify data integrity.
 *
 * For EC backends: asserts that MSG_OSD_EC_WRITE messages were sent and that
 * read messages are sent to shards.
 * For Replicated backends: asserts that at least one message was sent.
 */
TEST_P(TestBackendBasics, WriteThenRead) {
  const auto& param = GetParam().write_read;
  const auto& backend_config = GetParam().backend;

  std::string test_data(param.size, param.fill);
  std::string obj_name = "test_backend_" + backend_config.label + "_" + param.label;

  // Execute create+write operation and verify
  create_and_write_verify(obj_name, test_data);

  // Verify messages were sent to replicas/shards
  auto* primary_listener = get_primary_listener();
  ASSERT_TRUE(primary_listener != nullptr) << "Primary listener should exist";
  ASSERT_GT(primary_listener->sent_messages.size(), 0u)
    << "Should send messages to replicas/shards";

  // For EC backends: verify EC write messages were sent
  if (backend_config.pool_type == EC) {
    int write_messages_sent = 0;
    for (auto msg : primary_listener->sent_messages) {
      if (msg->get_type() == MSG_OSD_EC_WRITE) {
        write_messages_sent++;
      }
    }
    ASSERT_GT(write_messages_sent, 0) << "Should send EC write messages";
  }

  // Clear sent messages before read to distinguish read messages
  primary_listener->sent_messages.clear();
  primary_listener->sent_messages_with_dest.clear();

  // Verify object can be read back correctly
  verify_object(obj_name, test_data, 0, test_data.size());

  // For EC backends: verify read messages were sent to shards
  if (backend_config.pool_type == EC) {
    primary_listener = get_primary_listener();
    ASSERT_TRUE(primary_listener != nullptr) << "Primary listener should exist";
    ASSERT_GT(primary_listener->sent_messages.size(), 0u)
      << "Should send read messages to EC shards";
  }

  // All events should be processed by now
  ASSERT_FALSE(event_loop->has_events()) << "Event loop should be idle after read";

  primary_listener = get_primary_listener();
  if (primary_listener) {
    primary_listener->sent_messages.clear();
  }
}

// ---------------------------------------------------------------------------
// TestBackendBasics: PartialWrite
// ---------------------------------------------------------------------------

/**
 * PartialWrite - create an object of the parameterized size (rounded up to a
 * multiple of the stripe width for EC, or used directly for replicated), write
 * a partial region at a non-zero offset, read back and verify that:
 *   - the region before the partial write is unchanged,
 *   - the partial-write region contains the new data,
 *   - the region after the partial write is unchanged.
 */
TEST_P(TestBackendBasics, PartialWrite) {
  const auto& param = GetParam().write_read;
  const auto& backend_config = GetParam().backend;

  std::string obj_name = "test_partial_" + backend_config.label + "_" + param.label;

  // Use the parameterized size as the initial object size, but ensure it is
  // large enough to accommodate a non-trivial partial write.  We need at least
  // 3 regions: prefix, modified, suffix.  Use max(param.size, 3 * 4096) so
  // that even the smallest size parameters produce a meaningful test.
  const size_t initial_size = std::max(param.size, size_t(3 * 4096));

  // Partial write covers the middle third of the object (aligned to 4 KB).
  const size_t region = (initial_size / 3) & ~size_t(4095);  // round down to 4 KB
  const size_t partial_offset = region ? region : 4096;
  const size_t partial_size   = region ? region : 4096;

  // Create initial data filled with the parameterized fill character
  std::string initial_data(initial_size, param.fill);

  int result = create_and_write(obj_name, initial_data);
  EXPECT_EQ(result, 0) << param.label << " initial write should complete successfully";

  // Partial write data uses the next fill character (wraps around 'z' -> 'a')
  char partial_fill = (param.fill == 'z') ? 'a' : (param.fill + 1);
  std::string partial_data(partial_size, partial_fill);

  result = write(
    obj_name,
    partial_offset,
    partial_data,
    initial_size       // object_size
  );
  EXPECT_EQ(result, 0) << param.label << " partial write should complete successfully";

  // Read back the entire object
  bufferlist read_data;
  int read_result = read_object(obj_name, 0, initial_size, read_data, initial_size);
  EXPECT_GE(read_result, 0)
    << param.label << " read after partial write should complete successfully";

  ASSERT_EQ(read_data.length(), initial_size)
    << param.label << " read data length should match object size";

  const char* buf = read_data.c_str();

  // Region before the partial write should be unchanged
  for (size_t i = 0; i < partial_offset; i++) {
    ASSERT_EQ(buf[i], param.fill)
      << param.label << " data before partial write offset should be unchanged at position " << i;
  }

  // Partial-write region should contain the new fill character
  for (size_t i = partial_offset; i < partial_offset + partial_size; i++) {
    ASSERT_EQ(buf[i], partial_fill)
      << param.label << " data at partial write region should be '" << partial_fill
      << "' at position " << i;
  }

  // Region after the partial write should be unchanged
  for (size_t i = partial_offset + partial_size; i < initial_size; i++) {
    ASSERT_EQ(buf[i], param.fill)
      << param.label << " data after partial write region should be unchanged at position " << i;
  }
}

// ---------------------------------------------------------------------------
// TestBackendBasics: DirectRead
// ---------------------------------------------------------------------------

/**
 * DirectRead - test EC direct reads to individual shards.
 *
 * This test:
 * 1. Skips non-optimized EC (we don't support sync reads there)
 * 2. Writes patterned data covering an entire stripe
 * 3. Performs sync reads to each data shard with EC_DIRECT_READ flag
 * 4. Verifies data integrity for each shard
 */
TEST_P(TestBackendBasics, DirectRead) {
  const auto& param = GetParam().write_read;
  const auto& backend_config = GetParam().backend;

  // Skip test for non-EC backends
  if (backend_config.pool_type != EC) {
    GTEST_SKIP() << "DirectRead test only applies to EC backends";
  }

  // Skip test for non-optimized EC - we don't support sync reads
  if (!(backend_config.pool_flags & pg_pool_t::FLAG_EC_OPTIMIZATIONS)) {
    GTEST_SKIP() << "DirectRead test requires optimized EC";
  }

  std::string obj_name = "test_direct_read_" + backend_config.label + "_" + param.label;

  // Get stripe width from the pool
  uint64_t stripe_width = get_stripe_width();

  // Create patterned data where each stripe_unit has a distinct pattern
  // This allows us to verify we're reading the correct shard
  std::string test_data;
  test_data.reserve(stripe_width);
  
  for (size_t i = 0; i < stripe_width; i++) {
    // Pattern: each stripe_unit gets a different character based on its shard position
    size_t shard_index = i / stripe_unit;
    char fill_char = 'A' + (shard_index % 26);
    test_data.push_back(fill_char);
  }

  // Write the data (one full stripe)
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << param.label << " write should complete successfully";

  hobject_t hoid = make_test_object(obj_name);

  // Perform direct reads to each data shard (skip coding shards)
  for (auto& [shard_id, backend] : backends) {
    // Skip coding shards - only test data shards
    if (shard_id >= k) {
      continue;
    }

    ASSERT_TRUE(backend != nullptr) << "Backend for shard " << shard_id << " should not be null";
    
    ECSwitch* ec_switch = dynamic_cast<ECSwitch*>(backend.get());
    ASSERT_TRUE(ec_switch != nullptr) << "Backend should be ECSwitch for EC pools";

    bufferlist shard_data;
    
    // Perform sync read with EC_DIRECT_READ flag
    // Read the entire stripe - we expect only this shard's data back
    int read_result = ec_switch->objects_read_sync(
      hoid,
      0,                                    // offset
      stripe_width,                         // length (full stripe)
      CEPH_OSD_RMW_FLAG_EC_DIRECT_READ,    // op_flags with direct read flag
      &shard_data
    );

    EXPECT_GE(read_result, 0)
      << param.label << " direct read to shard " << shard_id << " should complete successfully";

    // For direct reads, we expect to get back only the data for this shard
    // which is one stripe_unit
    ASSERT_EQ(shard_data.length(), stripe_unit)
      << param.label << " shard " << shard_id << " should return " << stripe_unit << " bytes";

    // Verify data integrity: this shard should contain the expected pattern
    const char* buf = shard_data.c_str();
    char expected_char = 'A' + (shard_id % 26);
    
    for (size_t i = 0; i < stripe_unit; i++) {
      ASSERT_EQ(buf[i], expected_char)
        << param.label << " shard " << shard_id << " byte " << i
        << " should be '" << expected_char << "'";
    }
  }

  // Clean up
  auto* primary_listener = get_primary_listener();
  if (primary_listener) {
    primary_listener->sent_messages.clear();
  }
}

// ---------------------------------------------------------------------------
// TestBackendBasics: MultiZoneWriteThenRead
// ---------------------------------------------------------------------------

/**
 * MultiZoneWriteThenRead - test write then read with multiple zones.
 *
 * This test verifies that EC pools configured with multiple zones (num_zones > 0)
 * can successfully write and read data. The test:
 * 1. Skips non-EC backends and EC backends without zones
 * 2. Writes data to an object
 * 3. Reads the data back
 * 4. Verifies data integrity
 *
 * With zones enabled, the pool size is num_zones * (k+m), so there are more
 * shards distributed across multiple failure domains.
 */
TEST_P(TestBackendBasics, MultiZoneWriteThenRead) {
  const auto& param = GetParam().write_read;
  const auto& backend_config = GetParam().backend;

  // Skip test for non-EC backends
  if (backend_config.pool_type != EC) {
    GTEST_SKIP() << "MultiZoneWriteThenRead test only applies to EC backends";
  }

  // Skip test if zones are not configured
  if (backend_config.num_zones == 0) {
    GTEST_SKIP() << "MultiZoneWriteThenRead test requires num_zones > 0";
  }

  std::string test_data(param.size, param.fill);
  std::string obj_name = "test_multizone_" + backend_config.label + "_" + param.label;

  // Execute create+write operation and verify
  create_and_write_verify(obj_name, test_data);

  // Verify messages were sent to shards across zones
  auto* primary_listener = get_primary_listener();
  ASSERT_TRUE(primary_listener != nullptr) << "Primary listener should exist";
  ASSERT_GT(primary_listener->sent_messages.size(), 0u)
    << "Should send messages to shards across zones";

  // Verify EC write messages were sent
  int write_messages_sent = 0;
  for (auto msg : primary_listener->sent_messages) {
    if (msg->get_type() == MSG_OSD_EC_WRITE) {
      write_messages_sent++;
    }
  }
  ASSERT_GT(write_messages_sent, 0) << "Should send EC write messages to multiple zones";

  // With zones, we expect messages to be sent to num_zones * (k+m) shards
  // However, the actual number of messages may vary based on the write pattern
  // and optimization flags, so we just verify that messages were sent

  // Clear sent messages before read to distinguish read messages
  primary_listener->sent_messages.clear();
  primary_listener->sent_messages_with_dest.clear();

  // Verify object can be read back correctly across zones
  verify_object(obj_name, test_data, 0, test_data.size());

  // Verify read messages were sent to shards across zones
  primary_listener = get_primary_listener();
  ASSERT_TRUE(primary_listener != nullptr) << "Primary listener should exist";
  ASSERT_GT(primary_listener->sent_messages.size(), 0u)
    << "Should send read messages to EC shards across zones";

  // All events should be processed by now
  ASSERT_FALSE(event_loop->has_events()) << "Event loop should be idle after read";

  primary_listener = get_primary_listener();
  if (primary_listener) {
    primary_listener->sent_messages.clear();
  }
}

// ---------------------------------------------------------------------------
// TestBackendBasics: MultiZoneFailover
// ---------------------------------------------------------------------------

/**
 * MultiZoneFailover - test write, fail first half of OSDs, then degraded read.
 *
 * This test verifies that EC pools with multiple zones can handle zone failures
 * and perform degraded reads with EC reconstruction. The test:
 * 1. Requires num_zones > 1 (multiple zones)
 * 2. Writes data to an object
 * 3. Fails the first half of the OSDs (simulating a zone failure)
 * 4. Performs a degraded read and verifies data integrity via EC reconstruction
 *
 * With 2 zones and k=4, m=2, we have 12 total shards (2 zones × 6 shards).
 * Failing the first 6 OSDs (one complete zone) should still allow reads
 * because we have k=4 data chunks and m=2 coding chunks, and the remaining
 * 6 shards provide sufficient redundancy.
 */
TEST_P(TestBackendBasics, MultiZoneFailover) {
  const auto& param = GetParam().write_read;
  const auto& backend_config = GetParam().backend;

  // Skip test for non-EC backends
  if (backend_config.pool_type != EC) {
    GTEST_SKIP() << "MultiZoneFailover test only applies to EC backends";
  }

  // Skip test if zones are not configured or only one zone
  if (backend_config.num_zones <= 1) {
    GTEST_SKIP() << "MultiZoneFailover test requires num_zones > 1";
  }

  std::string test_data(param.size, param.fill);
  std::string obj_name = "test_zone_failover_" + backend_config.label + "_" + param.label;

  // Write data before failure and verify
  create_and_write_verify(obj_name, test_data);

  // Calculate how many OSDs to fail (first half)
  int total_osds = backend_config.num_zones * (k + m);
  int osds_to_fail = total_osds / 2;

  // Build list of OSDs to fail (first half)
  std::vector<int> failed_osds;
  for (int i = 0; i < osds_to_fail; i++) {
    failed_osds.push_back(i);
  }

  // Simulate failure of first half of OSDs
  simulate_multiple_osd_failures(failed_osds);

  // Verify the primary has changed (OSD 0 was in the first half)
  auto* new_primary_listener = get_primary_listener();
  ASSERT_TRUE(new_primary_listener != nullptr) << "Should have a primary after failover";

  // The new primary should not be one of the failed OSDs
  bool primary_is_valid = true;
  for (auto& [instance_id, listener] : listeners) {
    if (listener.get() == new_primary_listener) {
      primary_is_valid = (std::find(failed_osds.begin(), failed_osds.end(), instance_id) == failed_osds.end());
      break;
    }
  }
  EXPECT_TRUE(primary_is_valid) << "New primary should not be a failed OSD";

  // Perform degraded read after failover and verify data integrity
  verify_object(obj_name, test_data, 0, test_data.size());

  // Verify OSDMap epoch incremented
  EXPECT_GT(new_primary_listener->osdmap->get_epoch(), 1)
    << "OSDMap epoch should have incremented after failover";

  // Clean up
  if (new_primary_listener) {
    new_primary_listener->sent_messages.clear();
  }
}

// ---------------------------------------------------------------------------
// Backend configurations and size parameters
// ---------------------------------------------------------------------------

namespace {

const std::vector<BackendConfig> kBackendConfigs = {
  {PGBackendTestFixture::REPLICATED, "", "", 0, 4096, 4, 2, 0, "Replicated"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 1, "EC_ISA_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  8192,  4, 2, 1, "EC_ISA_Opt_k4m2_su8k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  16384, 4, 2, 1, "EC_ISA_Opt_k4m2_su16k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, 1, "EC_ISA_Opt_k2m1_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  8, 3, 1, "EC_ISA_Opt_k8m3_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES, 4096,  4, 2, 0, "EC_ISA_NonOpt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 1, "EC_Jerasure_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  8192,  4, 2, 1, "EC_Jerasure_Opt_k4m2_su8k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  16384, 4, 2, 1, "EC_Jerasure_Opt_k4m2_su16k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, 1, "EC_Jerasure_Opt_k2m1_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  8, 3, 1, "EC_Jerasure_Opt_k8m3_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES, 4096,  4, 2, 0, "EC_Jerasure_NonOpt_k4m2_su4k"},
  // Test configuration with num_zones set to 2 (size will be 2 * (4+2) = 12)
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 2, "EC_ISA_Opt_k4m2_zones2"},
};

const std::vector<WriteReadParam> kSizeParams = {
  {4  * 1024,       'A', "4k"},
  {8  * 1024,       'B', "8k"},
  {12 * 1024,       'C', "12k"},
  {12 * 1024 + 512, 'D', "12_5k"},
  {16 * 1024,       'E', "16k"},
  {31 * 1024 + 512, 'F', "31_5k"},
  {32 * 1024,       'G', "32k"},
  {32 * 1024 + 512, 'H', "32_5k"},
};

/**
 * Build the cross-product of kBackendConfigs × kSizeParams.
 */
std::vector<BackendWriteReadParam> make_cross_product() {
  std::vector<BackendWriteReadParam> result;
  result.reserve(kBackendConfigs.size() * kSizeParams.size());
  for (const auto& backend : kBackendConfigs) {
    for (const auto& size : kSizeParams) {
      result.push_back({backend, size});
    }
  }
  return result;
}

}  // namespace

// ---------------------------------------------------------------------------
// Instantiate TestBackendBasics with the full cross-product
// ---------------------------------------------------------------------------

INSTANTIATE_TEST_SUITE_P(
  BackendSizes,
  TestBackendBasics,
  ::testing::ValuesIn(make_cross_product()),
  [](const ::testing::TestParamInfo<BackendWriteReadParam>& info) {
    return info.param.backend.label + "_" + info.param.write_read.label;
  }
);

// ---------------------------------------------------------------------------
// TestECFailover fixture and tests
// ---------------------------------------------------------------------------

/**
 * TestECFailover - tests OSDMap updates and primary failover, parameterized
 * over all EC backend configurations.
 *
 * Failover is an EC-specific concept (shard-based primary election), so only
 * EC configs are included.  The fixture reads k/m/stripe_unit/plugin/technique
 * from the BackendConfig parameter so that every EC variant is exercised.
 */
class TestECFailover : public PGBackendTestFixture,
                       public ::testing::WithParamInterface<BackendConfig> {
public:
  TestECFailover() : PGBackendTestFixture(PGBackendTestFixture::EC) {
    const auto& config = GetParam();
    k = config.k;
    m = config.m;
    stripe_unit = config.stripe_unit;
    ec_plugin = config.ec_plugin;
    ec_technique = config.ec_technique;
    pool_flags = config.pool_flags;
  }

  void SetUp() override {
    PGBackendTestFixture::SetUp();
  }

  void simulate_osd_failure(int failed_osd, int new_primary_instance)
  {
    auto new_osdmap = std::make_shared<OSDMap>();
    new_osdmap->deepish_copy_from(*osdmap);

    // Build new acting set with the failed OSD replaced by CRUSH_ITEM_NONE
    std::vector<int> new_acting;
    for (int i = 0; i < k+m; i++) {
      new_acting.push_back((i == failed_osd) ? CRUSH_ITEM_NONE : i);
    }
    
    // Get the pool to use pgtemp_primaryfirst transformation
    const pg_pool_t* pool = new_osdmap->get_pg_pool(pgid.pool());
    ceph_assert(pool != nullptr);
    
    // For EC pools with optimizations, pgtemp_primaryfirst reorders the acting set
    // to put primary-eligible shards first. We need to apply this transformation
    // before setting pg_temp so that the OSDMap will correctly identify the primary.
    std::vector<int> transformed_acting = new_osdmap->pgtemp_primaryfirst(*pool, new_acting);
    
    // Use OSDMap::Incremental to set pg_temp with the transformed acting set
    OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
    inc.fsid = new_osdmap->get_fsid();
    inc.new_state[failed_osd] = CEPH_OSD_EXISTS;  // Mark as down (exists but not UP)
    
    // Convert to mempool vector for pg_temp
    mempool::osdmap::vector<int> pg_temp_vec(transformed_acting.begin(), transformed_acting.end());
    inc.new_pg_temp[pgid] = pg_temp_vec;

    new_osdmap->apply_incremental(inc);
    
    // Finalize the CRUSH map to ensure working_size is calculated
    new_osdmap->crush->finalize();

    pg_shard_t failed_shard(failed_osd, shard_id_t(failed_osd));
    for (auto& [instance_id, list] : listeners) {
      list->shardset.erase(failed_shard);
      list->acting_recovery_backfill_shard_id_set.erase(shard_id_t(failed_osd));
    }

    // update_osdmap will query the OSDMap to determine the primary
    update_osdmap(new_osdmap);
  }
};

TEST_P(TestECFailover, BasicOSDMapUpdate) {
  const std::string obj_name = "test_failover_object";
  const std::string test_data = "Initial data before OSDMap change";

  // Write and verify initial data
  create_and_write_verify(obj_name, test_data);

  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  new_osdmap->inc_epoch();

  update_osdmap(new_osdmap);

  EXPECT_EQ(osdmap, new_osdmap) << "OSDMap should be updated";
  auto* primary_listener = get_primary_listener();
  ASSERT_TRUE(primary_listener != nullptr) << "Primary listener should exist";
  EXPECT_EQ(primary_listener->osdmap, new_osdmap) << "Listener OSDMap should be updated";

  // Verify data can still be read after OSDMap update
  verify_object(obj_name, test_data, 0, test_data.size());
}

TEST_P(TestECFailover, PrimaryFailover) {
  const std::string obj_name = "test_primary_failover";
  const std::string test_data = "Data written before primary failover";

  // Write and verify initial data
  create_and_write_verify(obj_name, test_data);

  EXPECT_TRUE(listeners[0]->pgb_is_primary())
    << "Instance 0 should be primary before failover";
  EXPECT_FALSE(listeners[k]->pgb_is_primary())
    << "Instance " << k << " should not be primary before failover";

  // Determine expected new primary based on pool optimization
  // For optimized EC: shards 1 to k-1 are nonprimary, so new primary will be shard k
  // For non-optimized EC: any shard can be primary, so new primary will be shard 1
  const pg_pool_t& pool = get_pool();
  bool is_optimized = pool.has_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  int expected_new_primary = is_optimized ? k : 1;
  
  simulate_osd_failure(0, expected_new_primary);

  EXPECT_FALSE(listeners[0]->pgb_is_primary())
    << "Instance 0 should not be primary after failover";
  EXPECT_TRUE(listeners[expected_new_primary]->pgb_is_primary())
    << "Instance " << expected_new_primary << " should be primary after failover";

  // Verify the query functions return the correct primary
  auto* new_primary_listener = get_primary_listener();
  auto* new_primary_backend = get_primary_backend();
  EXPECT_EQ(new_primary_listener, listeners[expected_new_primary].get())
    << "get_primary_listener() should return the new primary";
  EXPECT_EQ(new_primary_backend, backends[expected_new_primary].get())
    << "get_primary_backend() should return the new primary";

  // Verify degraded read works after failover with EC reconstruction
  verify_object(obj_name, test_data, 0, test_data.size());

  EXPECT_TRUE(new_primary_listener != nullptr) << "Primary listener should exist after failover";
  EXPECT_GT(new_primary_listener->osdmap->get_epoch(), 1)
    << "OSDMap epoch should have incremented after failover";
}

// ---------------------------------------------------------------------------
// Instantiate TestECFailover with EC-only backend configurations
// ---------------------------------------------------------------------------

namespace {

std::vector<BackendConfig> make_ec_configs() {
  std::vector<BackendConfig> ec_configs;
  for (const auto& cfg : kBackendConfigs) {
    if (cfg.pool_type == PGBackendTestFixture::EC) {
      ec_configs.push_back(cfg);
    }
  }
  return ec_configs;
}

}  // namespace

INSTANTIATE_TEST_SUITE_P(
  ECBackends,
  TestECFailover,
  ::testing::ValuesIn(make_ec_configs()),
  [](const ::testing::TestParamInfo<BackendConfig>& info) {
    return info.param.label;
  }
);
