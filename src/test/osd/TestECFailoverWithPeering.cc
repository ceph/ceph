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

#include <gtest/gtest.h>
#include "test/osd/ECPeeringTestFixture.h"
#include "test/osd/TestCommon.h"
#include "osd/ECSwitch.h"

using namespace std;

/**
 * TestECFailoverWithPeering - parameterized EC peering and failover tests.
 *
 * This fixture is parameterized over BackendConfig to test multiple EC
 * configurations (different k/m values, stripe units, plugins, and optimizations).
 * Only EC configurations are tested since peering and failover are EC-specific.
 */
class TestECFailoverWithPeering : public ECPeeringTestFixture,
                                   public ::testing::WithParamInterface<BackendConfig> {
public:
  TestECFailoverWithPeering() : ECPeeringTestFixture() {
    const auto& config = GetParam();
    k = config.k;
    m = config.m;
    stripe_unit = config.stripe_unit;
    ec_plugin = config.ec_plugin;
    ec_technique = config.ec_technique;
    pool_flags = config.pool_flags;
  }
  
  void SetUp() override {
    ECPeeringTestFixture::SetUp();
  }
};

TEST_P(TestECFailoverWithPeering, BasicPeeringCycle) {
  pg_t pgid = get_peering_state(0)->get_info().pgid.pgid;
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(pgid, &acting_osds, &acting_primary);
  
  EXPECT_TRUE(get_peering_state(acting_primary)->is_clean())
    << "Primary should be clean after peering";
  
  // Verify primary is shard 0
  EXPECT_TRUE(get_peering_listener(0)->backend_listener->pgb_is_primary())
    << "Shard 0 should be primary";
  
  for (int i = 1; i < k + m; i++) {
    EXPECT_FALSE(get_peering_listener(i)->backend_listener->pgb_is_primary())
      << "Shard " << i << " should not be primary";
  }
}

TEST_P(TestECFailoverWithPeering, WriteWithPeering) {

  const std::string obj_name = "test_write_with_peering";
  const std::string test_data = "Data written with full peering support";
  
  create_and_write_verify(obj_name, test_data);

  auto* primary_ps = get_peering_state(0);
  EXPECT_GT(primary_ps->get_pg_log().get_log().log.size(), 0)
    << "Primary should have log entries after write";
}

TEST_P(TestECFailoverWithPeering, OSDFailureWithPeering) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_osd_failure";
  uint64_t object_size = k * stripe_unit;
  const std::string test_data_full(object_size, 'X');
  const size_t read_length = 2 * stripe_unit;
  const std::string test_data_read(read_length, 'X');
  int failed_osd = 1;  // Fail shard 1 which contains part of the data

  create_and_write_verify(obj_name, test_data_full);
  event_loop->reset_stats();
  bufferlist pre_failover_read;
  verify_object(obj_name, test_data_read, 0, object_size);
  EXPECT_EQ(4, event_loop->get_stats_by_type().at(EventLoop::EventType::OSD_MESSAGE));

  // Use fixture helper to mark OSD as down
  mark_osd_down(failed_osd);
  
  // Reset EventLoop stats before post-failover read
  event_loop->reset_stats();
  verify_object(obj_name, test_data_read, 0, object_size);
  EXPECT_EQ(k * 2, event_loop->get_stats_by_type().at(EventLoop::EventType::OSD_MESSAGE));
}

TEST_P(TestECFailoverWithPeering, PrimaryFailoverWithPeering) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_primary_failover";
  const std::string test_data = "Data before primary failover";
  
  create_and_write_verify(obj_name, test_data);
  
  // Mark OSD 0 (the initial primary) as down
  // PeeringState will automatically determine the new primary
  mark_osd_down(0);
  
  // Determine the actual new primary from the OSDMap
  int new_primary_shard = get_primary_shard_from_osdmap();
  ASSERT_GE(new_primary_shard, 0) << "Should have a valid new primary after failover";
  
  // For an optimized EC pool (k=4, m=2), the new primary should be a coding shard (>= k)
  // For a non-optimized pool, it would be shard 1
  const pg_pool_t& pool = get_pool();
  if (pool.allows_ecoptimizations()) {
    EXPECT_GE(new_primary_shard, k)
      << "New primary should be a coding shard (>= k) for optimized pool";
  } else {
    EXPECT_EQ(new_primary_shard, 1)
      << "New primary should be shard 1 for non-optimized pool";
  }
  
  EXPECT_TRUE(get_peering_listener(new_primary_shard)->backend_listener->pgb_is_primary())
    << "Shard " << new_primary_shard << " should be new primary";
  
  EXPECT_FALSE(get_peering_listener(0)->backend_listener->pgb_is_primary())
    << "Failed shard should not be primary";
  
  std::string state = get_state_name(new_primary_shard);
  EXPECT_TRUE(state.find("Active") != std::string::npos)
    << "New primary should be Active after failover, got: " << state;
  
  // Verify the PG reached Active state
  EXPECT_TRUE(get_peering_state(new_primary_shard)->is_active())
    << "New primary should be in Active state";
  
  // Verify reads work after primary failover (with EC reconstruction)
  verify_object(obj_name, test_data, 0, test_data.length());
}

TEST_P(TestECFailoverWithPeering, MultipleOSDFailuresWithPeering) {
  // This test only runs for configurations with m=2
  if (m != 2) {
    GTEST_SKIP() << "MultipleOSDFailuresWithPeering only runs for m=2";
  }
  
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_multiple_failures";
  const std::string test_data = "Data before multiple failures";
  
  create_and_write_verify(obj_name, test_data);
  
  std::vector<int> failed_osds = {1, 2};  // Fail 2 data shards
  ASSERT_EQ(failed_osds.size(), static_cast<size_t>(m))
    << "Should fail exactly m OSDs";
  
  // Use fixture helper to mark multiple OSDs as down
  mark_osds_down(failed_osds);
  
  auto* primary_ps = get_peering_state(0);
  for (int failed_osd : failed_osds) {
    EXPECT_TRUE(primary_ps->get_acting_recovery_backfill().count(
      pg_shard_t(failed_osd, shard_id_t(failed_osd))) == 0)
      << "Failed OSD " << failed_osd << " should not be in acting set";
  }
  
  std::string primary_state = get_state_name(0);
  EXPECT_TRUE(primary_state.find("Peering") != std::string::npos ||
              primary_state.find("Active") != std::string::npos ||
              primary_state.find("Recovery") != std::string::npos)
    << "Primary should be operational, got: " << primary_state;
}

TEST_P(TestECFailoverWithPeering, RecoveryWithPeering) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj1_name = "test_recovery_obj1";
  const std::string obj1_data = "First object data for recovery test";
  
  const std::string obj2_name = "test_recovery_obj2";
  const std::string obj2_data = "Second object data for recovery test";
  
  int result = create_and_write(obj1_name, obj1_data);
  EXPECT_EQ(result, 0) << "First pre-failure write should complete";
  
  result = create_and_write(obj2_name, obj2_data);
  EXPECT_EQ(result, 0) << "Second pre-failure write should complete";
  
  EXPECT_TRUE(all_shards_clean()) << "All shards should be clean before recovery test";
  
  auto* primary_ps = get_peering_state(0);
  eversion_t pre_failure_log_head = primary_ps->get_pg_log().get_log().head;
  EXPECT_GT(pre_failure_log_head.version, 0u)
    << "Primary should have log entries before failure";
  
  int failed_osd = k - 1;  // Last data shard
  
  // Use fixture helper to mark OSD as down
  mark_osd_down(failed_osd);
  
  std::string state_after_failure = get_state_name(0);
  ASSERT_TRUE(all_shards_active() ||
              state_after_failure.find("Recovery") != std::string::npos ||
              state_after_failure.find("Peering") != std::string::npos)
    << "PG should be active, recovering, or peering after OSD failure, got: "
    << state_after_failure;
  
  // EC can reconstruct data from remaining k shards even with one shard missing
  bufferlist obj1_read;
  int read_result = read_object(obj1_name, 0, obj1_data.length(),
                                obj1_read, obj1_data.length());
  EXPECT_GE(read_result, 0) << "First object should be readable after OSD failure";
  ASSERT_EQ(obj1_read.length(), obj1_data.length())
    << "First object read length should match after failure";
  {
    std::string read_str(obj1_read.c_str(), obj1_read.length());
    EXPECT_EQ(read_str, obj1_data)
      << "First object data should be correct after OSD failure (EC reconstruction)";
  }
  
  bufferlist obj2_read;
  read_result = read_object(obj2_name, 0, obj2_data.length(),
                            obj2_read, obj2_data.length());
  EXPECT_GE(read_result, 0) << "Second object should be readable after OSD failure";
  ASSERT_EQ(obj2_read.length(), obj2_data.length())
    << "Second object read length should match after failure";
  {
    std::string read_str(obj2_read.c_str(), obj2_read.length());
    EXPECT_EQ(read_str, obj2_data)
      << "Second object data should be correct after OSD failure (EC reconstruction)";
  }
  
  const std::string post_recovery_obj = "test_post_recovery";
  const std::string post_recovery_data = "Data written after OSD failure and recovery";
  
  result = create_and_write(post_recovery_obj, post_recovery_data);
  EXPECT_EQ(result, 0) << "Write after OSD failure should complete successfully";
  
  bufferlist post_recovery_read;
  read_result = read_object(post_recovery_obj, 0, post_recovery_data.length(),
                            post_recovery_read, post_recovery_data.length());
  EXPECT_GE(read_result, 0) << "Post-recovery object should be readable";
  ASSERT_EQ(post_recovery_read.length(), post_recovery_data.length())
    << "Post-recovery read length should match";
  {
    std::string read_str(post_recovery_read.c_str(), post_recovery_read.length());
    EXPECT_EQ(read_str, post_recovery_data)
      << "Post-recovery data should match what was written";
  }
  
  eversion_t post_recovery_log_head = primary_ps->get_pg_log().get_log().head;
  EXPECT_GT(post_recovery_log_head.version, pre_failure_log_head.version)
    << "Primary PG log head should advance after post-recovery write";
  
  // Even though the OSD is "down", its PeeringState still holds the log
  // from before it went down.
  auto* failed_ps = get_peering_state(failed_osd);
  EXPECT_TRUE(failed_ps != nullptr) << "Failed OSD's PeeringState should still exist";
  
  size_t primary_log_size = primary_ps->get_pg_log().get_log().log.size();
  size_t failed_log_size = failed_ps->get_pg_log().get_log().log.size();
  EXPECT_LE(failed_log_size, primary_log_size)
    << "Failed OSD's PG log size should not exceed primary's log size";
  // The primary wrote 3 objects (obj1, obj2, post_recovery_obj), so its log must be non-empty.
  EXPECT_GT(primary_log_size, 0u)
    << "Primary PG log should have entries after 3 writes";
  
  auto* listener_ptr = get_peering_listener(0);
  EXPECT_TRUE(listener_ptr != nullptr) << "Peering listener should exist";
  EXPECT_TRUE(listener_ptr->activate_complete_called)
    << "on_activate_complete should have been called during peering";
}

// ---------------------------------------------------------------------------
// EC backend configurations for parameterized tests
// ---------------------------------------------------------------------------

namespace {

/**
 * EC-only backend configurations for TestECFailoverWithPeering.
 * These configurations test various EC parameters:
 * - Different k/m ratios (2+1, 4+2, 8+3)
 * - Different stripe units (4k, 8k, 16k)
 * - Different plugins (isa, jerasure)
 * - Optimized vs non-optimized EC
 * - Multi-zone configurations
 */
const std::vector<BackendConfig> kECPeeringConfigs = {
  // ISA plugin with optimizations (modern EC)
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, "EC_ISA_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  8192,  4, 2, "EC_ISA_Opt_k4m2_su8k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  16384, 4, 2, "EC_ISA_Opt_k4m2_su16k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, "EC_ISA_Opt_k2m1_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  8, 3, "EC_ISA_Opt_k8m3_su4k"},
  
  // Jerasure plugin with optimizations (modern EC)
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, "EC_Jerasure_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  8192,  4, 2, "EC_Jerasure_Opt_k4m2_su8k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  16384, 4, 2, "EC_Jerasure_Opt_k4m2_su16k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, "EC_Jerasure_Opt_k2m1_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  8, 3, "EC_Jerasure_Opt_k8m3_su4k"},
};

}  // namespace

/**
 * Test OSD failure and recovery with peering.
 *
 * This test simulates the following scenario:
 * 1. Write full stripe with pattern A (committed to all shards)
 * 2. Write full stripe with pattern B (committed to all shards)
 * 3. Mark OSD 5 as down (forcing peering)
 * 4. Trigger peering - PG should remain active/recovering
 * 5. Read data back - should get pattern B (latest write)
 *
 * This verifies that the test infrastructure properly handles OSD failures
 * and peering without leaving OSDs in a suspended state that would block
 * teardown.
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackAfterOSDFailure
) {
  // GTEST_SKIP(); // Temporary
  int failing_shard = k + m - 1;
  int blocked_shard = 1;
  const std::string obj_name = "test";
  const size_t data_size = stripe_unit * k;  // One full stripe.
  std::string pattern_a(data_size, 'A');
  std::string pattern_b(data_size, 'B');
  std::string pattern_c(data_size, 'C');

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  create_and_write_verify(obj_name, pattern_a);
  suspend_primary_to_osd(blocked_shard);
  int result = write(obj_name, 0, pattern_b, data_size);
  ASSERT_EQ(-EINPROGRESS, result);
  result = write(obj_name, 0, pattern_c, data_size);
  ASSERT_EQ(-EINPROGRESS, result);
  mark_osd_down(failing_shard);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();
  
  // Ensure all shards have completed peering and applied rollback transactions
  ASSERT_TRUE(all_shards_active()) << "All shards should be active after peering";
  
  verify_object(obj_name, pattern_a, 0, pattern_a.length());

  std::cout << "\n=== RollbackAfterOSDFailure Test Complete ===" << std::endl;
}
/**
 * ECRecoveryTest - Test EC recovery scenario with missing objects
 *
 * This test verifies the EC recovery mechanism by:
 * 1. Writing and verifying an object
 * 2. Removing an OSD from the acting set (simulating OSD failure)
 * 3. Performing an overwrite to the object (creating a version mismatch)
 * 4. Adding the OSD back to the acting set
 * 5. Inspecting the missing list to verify the object is marked as missing
 * 6. Demonstrating that the primary can open a recovery operation
 *
 * The test runs multiple times, once for each OSD to fail:
 * - OSD 1 (always)
 */
TEST_P(TestECFailoverWithPeering, ECRecoveryTest) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  std::vector<int> osds_to_test;
  osds_to_test.push_back(1); // Non-primary
  osds_to_test.push_back(0); // Primary
  osds_to_test.push_back(k); // First coding shard

  // Run the test for each OSD
  for (int removed_osd : osds_to_test) {
    const std::string obj_name = "test_ec_recovery_osd" + std::to_string(removed_osd);
    const size_t data_size = stripe_unit * k;  // One full stripe.
    std::string pattern_a(data_size, 'A');
    std::string pattern_b(data_size, 'B');

    create_and_write_verify(obj_name, pattern_a);
    mark_osd_down(removed_osd);
    write_verify(obj_name, 0, pattern_b, data_size);
    mark_osd_up(removed_osd);

    // Use the fixture helper to run recovery and verify callbacks
    run_recovery_and_verify_callbacks(obj_name, removed_osd, pattern_b);

    std::cout << "=== Recovery test with OSD " << removed_osd << " completed successfully ===" << std::endl;
  }
}

/**
 * ECSequentialOSDFailoverTest - Test sequential OSD failure and recovery
 *
 * This test verifies the EC recovery mechanism by sequentially failing and
 * recovering each OSD in the cluster:
 * 1. Create an object and write initial data
 * 2. For each OSD (0 to (k+m)*num_zones - 1):
 *    a. Fail the OSD
 *    b. Write new data to the object (overwrite)
 *    c. Recover the OSD
 *    d. Verify recovery completes
 * 3. Verify final data is correct
 *
 * Unlike ECRecoveryTest which creates a new object for each OSD failure,
 * this test performs a new write to the same object on each cycle.
 */
TEST_P(TestECFailoverWithPeering, ECSequentialOSDFailoverTest) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_sequential_failover";
  const size_t data_size = stripe_unit * k;  // One full stripe

  // Calculate total number of OSDs to test
  int total_osds = (k + m);

  std::cout << "\n=== Testing sequential OSD failover for " << total_osds
            << " OSDs (k=" << k << ", m=" << m << ") ===" << std::endl;

  // Create object with initial pattern
  std::string initial_pattern(data_size, 'A');
  create_and_write_verify(obj_name, initial_pattern);

  // Cycle through each OSD, failing and recovering it
  for (int osd_to_fail = 0; osd_to_fail < total_osds; osd_to_fail++) {
    char pattern_char = 'B' + (osd_to_fail % 25);  // Cycle through B-Z, then wrap
    std::string cycle_pattern(data_size, pattern_char);
    mark_osd_down(osd_to_fail);
    write_verify(obj_name, 0, cycle_pattern, data_size);
    mark_osd_up(osd_to_fail);
    run_recovery_and_verify_callbacks(obj_name, osd_to_fail, cycle_pattern);
  }

  std::cout << "\n=== Sequential OSD failover test completed successfully ===" << std::endl;
}

/**
 * ECZoneRecoveryTest - Test zone-level EC recovery scenario (zone 0 fails first)
 *
 * This test reproduces a bug whereby a full write, following a partial write
 * will rollback to an OI with an incorrect previous version.
 *
 * Recreate https://tracker.ceph.com/issues/76213
 */
TEST_P(TestECFailoverWithPeering, DISABLED_RollbackVersionMismatch) {
  if (k < 3) {
    GTEST_SKIP() << "SnapshotTrimRollbackVersionMismatch requires at least 3 data shards";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_attr_rollback";
  int temp_failing_shard = 2;     // Temporarily fail shard 2 for peering interval change

  create_and_write_verify(obj_name, "initial_data");
  eversion_t v1 = read_shard_object_info(obj_name, 0).version;
  ASSERT_EQ(v1, read_shard_object_info(obj_name, 1).version);
  ASSERT_EQ(v1, read_shard_object_info(obj_name, k).version);

  int result = write_attribute(obj_name, "test_attr", "value1", false);
  ASSERT_EQ(0, result);
  event_loop->run_until_idle();

  eversion_t v2 = read_shard_object_info(obj_name, 0).version;
  ASSERT_GT(v2, v1);
  ASSERT_EQ(v1, read_shard_object_info(obj_name, 1).version);
  ASSERT_EQ(v2, read_shard_object_info(obj_name, k).version);

  suspend_primary_to_osd(k);
  result = write_attribute(obj_name, "test_attr", "value2", true);
  ASSERT_NE(0, result);
  mark_osd_down(temp_failing_shard);
  unsuspend_primary_to_osd(k);
  event_loop->run_until_idle();
  ASSERT_EQ(v2, read_shard_object_info(obj_name, 0).version);
  ASSERT_EQ(v1, read_shard_object_info(obj_name, 1).version);
  ASSERT_EQ(v2, read_shard_object_info(obj_name, k).version);

}

/**
 * TEST: MultiObjectRecoveryReadCrash
 *
 * This test reproduces Bug 75432: Assertion failure in ECCommon::ReadPipeline::do_read_op()
 * when handling multi-object EC reads with partial failures.
 *
 * The bug occurs when:
 * 1. Multiple objects of different sizes are read simultaneously
 * 2. Smaller objects complete successfully (shard_reads cleared)
 * 3. A larger object needs additional reads due to a shard failure (need_resend = true)
 * 4. do_read_op() is called with both completed and incomplete objects
 */
TEST_P(TestECFailoverWithPeering, MultiObjectRecoveryReadCrash) {
  // This test requires k >= 3 and m >= 2
  if (k < 3 || m < 2) {
    GTEST_SKIP() << "Test requires k >= 3 and m >= 2";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  // Create objects of different sizes with initial pattern
  const std::string obj1_name = "crash_test_obj1";
  const std::string obj1_pattern_a(stripe_unit, 'A');  // 1 chunk

  const std::string obj2_name = "crash_test_obj2";
  const std::string obj2_pattern_a(2 * stripe_unit, 'A');  // 2 chunks

  const std::string obj3_name = "crash_test_obj3";
  const std::string obj3_pattern_a(3 * stripe_unit, 'A');  // 3 chunks

  // Write initial pattern to all objects
  int result = create_and_write(obj1_name, obj1_pattern_a);
  EXPECT_EQ(result, 0) << "First object write should complete";

  result = create_and_write(obj2_name, obj2_pattern_a);
  EXPECT_EQ(result, 0) << "Second object write should complete";

  result = create_and_write(obj3_name, obj3_pattern_a);
  EXPECT_EQ(result, 0) << "Third object write should complete";

  EXPECT_TRUE(all_shards_clean()) << "All shards should be clean";

  // Mark shard 1 as down - this will require recovery
  int failed_osd = 1;
  mark_osd_down(failed_osd);

  // Write new pattern to all objects while OSD 1 is down
  // This creates objects that need recovery on OSD 1
  const std::string obj1_pattern_b(stripe_unit, 'B');
  const std::string obj2_pattern_b(2 * stripe_unit, 'B');
  const std::string obj3_pattern_b(3 * stripe_unit, 'B');

  result = write(obj1_name, 0, obj1_pattern_b, obj1_pattern_b.length());
  EXPECT_EQ(result, 0) << "First object update should complete";

  result = write(obj2_name, 0, obj2_pattern_b, obj2_pattern_b.length());
  EXPECT_EQ(result, 0) << "Second object update should complete";

  result = write(obj3_name, 0, obj3_pattern_b, obj3_pattern_b.length());
  EXPECT_EQ(result, 0) << "Third object update should complete";

  // Bring OSD back up to trigger peering
  // Peering will detect that OSD 1 has stale data and populate peer_missing
  mark_osd_up(failed_osd);

  // Inject read error on shard 2 for object 3 only
  // This will cause object 3's recovery to fail and need resend
  inject_read_error_for_shard(obj3_name, 2, -EIO);

  // Now trigger recovery for all 3 objects simultaneously
  // This is the key: recovery reads multiple objects in a single operation
  // obj1: 1 chunk - reads shard 0 only -> succeeds -> shard_reads cleared
  // obj2: 2 chunks - reads shards 0, k -> succeeds -> shard_reads cleared
  // obj3: 3 chunks - reads shards 0, 2, k -> shard 2 fails -> needs resend
  // BUG: do_read_op() called with obj1/obj2 having empty shard_reads

  std::cout << "Starting recovery for all 3 objects..." << std::endl;

  run_recovery_and_verify_callbacks(obj1_name, failed_osd, obj1_pattern_b);
  run_recovery_and_verify_callbacks(obj2_name, failed_osd, obj2_pattern_b);
  run_recovery_and_verify_callbacks(obj3_name, failed_osd, obj3_pattern_b);

  // If the bug is present, we'll crash before getting here
  // If the bug is fixed, recovery should complete successfully
  std::cout << "Recovery completed for all objects" << std::endl;

  SUCCEED() << "Multi-object recovery completed without crash";
}

/**
 * TEST: MultiObjectParallelRecoveryCrash
 *
 * This test reproduces Bug 75432 by recovering multiple objects in parallel
 * within a single recovery operation (not sequentially).
 *
 * The bug occurs when:
 * 1. Multiple objects are recovered in a single operation (parallel recovery)
 * 2. Smaller objects complete successfully (shard_reads cleared)
 * 3. A larger object needs additional reads due to a shard failure (need_resend = true)
 * 4. do_read_op() is called with both completed and incomplete objects
 *
 * Recreate for tracker https://tracker.ceph.com/issues/75432
 *
 * Expected behavior WITH fix: Test completes successfully.
 */
TEST_P(TestECFailoverWithPeering, DISABLED_MultiObjectParallelRecoveryCrash) {
  // This test requires k >= 3 and m >= 2
  if (k < 3 || m < 2) {
    GTEST_SKIP() << "Test requires k >= 3 and m >= 2";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  // Create objects of different sizes with initial pattern
  const std::string obj1_name = "crash_test_obj1";
  const std::string obj1_pattern_a(stripe_unit, 'A');  // 1 chunk

  const std::string obj2_name = "crash_test_obj2";
  const std::string obj2_pattern_a(2 * stripe_unit, 'A');  // 2 chunks

  const std::string obj3_name = "crash_test_obj3";
  const std::string obj3_pattern_a(3 * stripe_unit, 'A');  // 3 chunks

  // Write initial pattern to all objects
  int result = create_and_write(obj1_name, obj1_pattern_a);
  EXPECT_EQ(result, 0) << "First object write should complete";

  result = create_and_write(obj2_name, obj2_pattern_a);
  EXPECT_EQ(result, 0) << "Second object write should complete";

  result = create_and_write(obj3_name, obj3_pattern_a);
  EXPECT_EQ(result, 0) << "Third object write should complete";

  EXPECT_TRUE(all_shards_clean()) << "All shards should be clean";

  // Mark shard 1 as down - this will require recovery
  int failed_osd = 1;
  mark_osd_down(failed_osd);

  // Write new pattern to all objects while OSD 1 is down
  // This creates objects that need recovery on OSD 1
  const std::string obj1_pattern_b(stripe_unit, 'B');
  const std::string obj2_pattern_b(2 * stripe_unit, 'B');
  const std::string obj3_pattern_b(3 * stripe_unit, 'B');

  result = write(obj1_name, 0, obj1_pattern_b, obj1_pattern_b.length());
  EXPECT_EQ(result, 0) << "First object update should complete";

  result = write(obj2_name, 0, obj2_pattern_b, obj2_pattern_b.length());
  EXPECT_EQ(result, 0) << "Second object update should complete";

  result = write(obj3_name, 0, obj3_pattern_b, obj3_pattern_b.length());
  EXPECT_EQ(result, 0) << "Third object update should complete";

  // Bring OSD back up to trigger peering
  // Peering will detect that OSD 1 has stale data and populate peer_missing
  mark_osd_up(failed_osd);

  // Inject read error on shard 2 for object 3 only
  // This will cause object 3's recovery to fail and need resend
  inject_read_error_for_shard(obj3_name, 2, -EIO);

  // Now trigger recovery for all 3 objects in parallel (single operation)
  // This is the key difference from the sequential test
  std::cout << "Starting parallel recovery for all 3 objects..." << std::endl;

  std::vector<std::string> obj_names = {obj1_name, obj2_name, obj3_name};
  std::vector<std::string> expected_data = {obj1_pattern_b, obj2_pattern_b, obj3_pattern_b};
  run_parallel_recovery_and_verify_callbacks(obj_names, failed_osd, expected_data);

  // If the bug is present, we'll crash before getting here
  // If the bug is fixed, recovery should complete successfully
  std::cout << "Parallel recovery completed for all objects" << std::endl;

  SUCCEED() << "Multi-object parallel recovery completed without crash";
}

/**
 * Test rollback after a sequence of blocked full-stripe and chunk writes.
 * Recreate for tracker https://tracker.ceph.com/issues/75211
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackAfterMixedBlockedWritesWithOSDFailure
) {
  if (m < 2) {
    GTEST_SKIP() << "RollbackAfterMixedBlockedWritesWithOSDFailure requires m >= 2";
  }

  // Set osd_async_recovery_min_cost to 0 to ensure even single-object
  // recovery uses async recovery. This is necessary because the test
  // harness doesn't block writes during synchronous recovery, which
  // would cause writes to missing objects to crash.
  set_config("osd_async_recovery_min_cost", "0");

  const int blocked_shard = k + 1;
  const int recovery_target_shard = 1;
  const std::string obj_name = "test_mixed_blocked_writes";
  const size_t full_stripe_size = stripe_unit * k;
  const std::string pattern_p1(full_stripe_size, 'A');
  const std::string pattern_p2(full_stripe_size, 'B');

  // Trigger an async recovery on shard 1.
  mark_osd_down(recovery_target_shard);
  create_and_write_verify(obj_name, pattern_p1);
  mark_osd_up(recovery_target_shard);

  // Create a dummy object. This is purely here to be the first write in a
  // new interval, which has some special behavior.
  create_and_write_verify("dummy", pattern_p1);

  // This has the effect of preventing ops from completing.
  suspend_primary_to_osd(blocked_shard);

  // Force next partial write to go to all shards (including non-primary)
  // This uses a side effect of call_write_ordered() which causes the next op
  // to be sent to all shards, even if it is a partial write.
  ECSwitch* ec_switch = dynamic_cast<ECSwitch*>(get_primary_backend());
  ASSERT_NE(nullptr, ec_switch) << "Primary backend must be ECSwitch";
  ec_switch->call_write_ordered([] {});

  // This is a partial write that will be sent to all shards due to the above
  // above mechanism. NOTE: This is different to the force_all_shards boolean
  // below, which generates a full write, rather than a partial write sent to
  // all shards!
  int result = write_attribute(obj_name, "test_attr", "value2", false);
  ASSERT_EQ(-EINPROGRESS, result);

  // Add a full write. In the defect, the diverge log "merge" code ended up
  // using this version in the missing list - which is wrong.
  result = write(obj_name, 0, pattern_p2, full_stripe_size);
  ASSERT_EQ(-EINPROGRESS, result);

  // Mark an otherwise-uninvolved shard as down to trigger the rollback of
  // above
  mark_osd_down(2);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();

  // Now run the recovery - the target shard asserts it is being written with
  // the object version it is expecting. In the defect, this assert failed.
  run_recovery_and_verify_callbacks(obj_name, recovery_target_shard, pattern_p1);

  // Undo our config change!
  set_config("osd_async_recovery_min_cost", "100");
}

/**
 * Test rollback after a sequence of blocked full-stripe and chunk writes.
 * This is a similar scenario to the previous test, but we force the shard
 * to do a sync, rather than async recovery at the end.
 * Recreate for tracker https://tracker.ceph.com/issues/75211
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackAfterMixedBlockedWritesWithOSDFailure2
) {
  if (m < 2) {
    GTEST_SKIP() << "RollbackAfterMixedBlockedWritesWithOSDFailure requires m >= 2";
  }

  // Set osd_async_recovery_min_cost to 0 to ensure even single-object
  // recovery uses async recovery. This is necessary because the test
  // harness doesn't block writes during synchronous recovery, which
  // would cause writes to missing objects to crash.
  set_config("osd_async_recovery_min_cost", "0");

  const int blocked_shard = k + 1;
  const int recovery_target_shard = 1;
  const std::string obj_name = "test_mixed_blocked_writes";
  const size_t full_stripe_size = stripe_unit * k;
  const std::string pattern_p1(full_stripe_size, 'A');
  const std::string pattern_p2(full_stripe_size, 'B');

  // Trigger an async recovery on shard 1.
  mark_osd_down(recovery_target_shard);
  create_and_write_verify(obj_name, pattern_p1);
  mark_osd_up(recovery_target_shard);

  // Create a dummy object. This is purely here to be the first write in a
  // new interval, which has some special behavior.
  create_and_write_verify("dummy", pattern_p1);

  // This has the effect of preventing ops from completing.
  suspend_primary_to_osd(blocked_shard);

  // Force next partial write to go to all shards (including non-primary)
  // This uses a side effect of call_write_ordered() which causes the next op
  // to be sent to all shards, even if it is a partial write.
  ECSwitch* ec_switch = dynamic_cast<ECSwitch*>(get_primary_backend());
  ASSERT_NE(nullptr, ec_switch) << "Primary backend must be ECSwitch";
  ec_switch->call_write_ordered([] {});

  // This is a partial write that will be sent to all shards due to the above
  // above mechanism. NOTE: This is different to the force_all_shards boolean
  // below, which generates a full write, rather than a partial write sent to
  // all shards!
  int result = write_attribute(obj_name, "test_attr", "value2", false);
  ASSERT_EQ(-EINPROGRESS, result);

  // Add a full write. In the defect, the diverge log "merge" code ended up
  // using this version in the missing list - which is wrong.
  result = write(obj_name, 0, pattern_p2, full_stripe_size);
  ASSERT_EQ(-EINPROGRESS, result);

  set_config("osd_async_recovery_min_cost", "100");

  // Mark an otherwise-uninvolved shard as down to trigger the rollback of
  // above
  mark_osd_down(2);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();

  // Now run the recovery - the target shard asserts it is being written with
  // the object version it is expecting. In the defect, this assert failed.
  run_recovery_and_verify_callbacks(obj_name, recovery_target_shard, pattern_p1);
}

// ---------------------------------------------------------------------------
// Instantiate TestECFailoverWithPeering with EC configurations
// ---------------------------------------------------------------------------

INSTANTIATE_TEST_SUITE_P(
  ECConfigs,
  TestECFailoverWithPeering,
  ::testing::ValuesIn(kECPeeringConfigs),
  [](const ::testing::TestParamInfo<BackendConfig>& info) {
    return info.param.label;
  }
);

