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

TEST_P(TestECFailoverWithPeering, ZeroSizeObjectWithAttributesRecovery) {
  //  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_primary_failover";
  const std::string test_data;
  
  create_and_write(obj_name, test_data);
  
  // Mark OSD 0 (the initial primary) as down
  // PeeringState will automatically determine the new primary
  mark_osd_down(0);
  
  write_attribute(obj_name, "key", "value", false);
  
  // Determine the actual new primary from the OSDMap
  int new_primary_shard = get_primary_shard_from_osdmap();
  ASSERT_GE(new_primary_shard, 0) << "Should have a valid new primary after failover";
  
  // For an optimized EC pool (k=4, m=2), the new primary should be a coding shard (>= k)
  // For a non-optimized pool, it would be shard 1
  const pg_pool_t& pool = get_pool();
  if (pool.allows_ecoptimizations()) {
    ASSERT_GE(new_primary_shard, k)
      << "New primary should be a coding shard (>= k) for optimized pool";
  } else {
    ASSERT_EQ(new_primary_shard, 1)
      << "New primary should be shard 1 for non-optimized pool";
  }
  
  ASSERT_TRUE(get_peering_listener(new_primary_shard)->backend_listener->pgb_is_primary())
    << "Shard " << new_primary_shard << " should be new primary";
  
  ASSERT_FALSE(get_peering_listener(0)->backend_listener->pgb_is_primary())
    << "Failed shard should not be primary";
  
  std::string state = get_state_name(new_primary_shard);
  ASSERT_TRUE(state.find("Active") != std::string::npos)
    << "New primary should be Active after failover, got: " << state;
  
  // Verify the PG reached Active state
  ASSERT_TRUE(get_peering_state(new_primary_shard)->is_active())
    << "New primary should be in Active state";
  
  mark_osd_up(0);
  
  run_recovery_and_verify_callbacks(obj_name, 0, test_data);
  
  // Verify that the attribute was recovered on shard 0
  hobject_t hoid = make_test_object(obj_name);
  ghobject_t ghoid = ghobject_t(hoid, ghobject_t::NO_GEN, shard_id_t(0));
  
  ceph::buffer::ptr attr_value;
  int r = store->getattr(chs[0], ghoid, "key", attr_value);
  ASSERT_GE(r, 0) << "Attribute 'key' should exist on recovered shard 0";
  ASSERT_EQ(std::string(attr_value.c_str(), attr_value.length()), "value")
    << "Attribute 'key' should have value 'value' after recovery";
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
TEST_P(TestECFailoverWithPeering, RollbackVersionMismatch) {
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
TEST_P(TestECFailoverWithPeering, MultiObjectParallelRecoveryCrash) {
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

/**
 * Test rollback after a sequence of blocked full-stripe and chunk writes.
 * This is a similar scenario to the previous test, but we force the shard
 * to do a sync, rather than async recovery at the end.
 * Recreate for tracker https://tracker.ceph.com/issues/75962
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackAfterMixedBlockedWritesWithOSDFailure3
) {
  if (m < 2) {
    GTEST_SKIP() << "RollbackAfterMixedBlockedWritesWithOSDFailure requires m >= 2";
  }
  set_config("osd_async_recovery_min_cost", "0");

  const int blocked_shard = k + 1;
  const int recovery_target_shard = 1;
  const std::string obj_name = "test_mixed_blocked_writes";
  const size_t full_stripe_size = stripe_unit * k;
  const std::string pattern_p1(full_stripe_size, 'A');
  mark_osd_down(recovery_target_shard);
  create_and_write_verify(obj_name, pattern_p1);
  mark_osd_up(recovery_target_shard);
  create_and_write_verify("dummy", pattern_p1);
  suspend_primary_to_osd(blocked_shard);
  int result = write_attribute(obj_name, "test_attr", "value2", false);
  ASSERT_EQ(-EINPROGRESS, result);
  mark_osd_down(2);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();

  run_recovery_and_verify_callbacks(obj_name, recovery_target_shard, pattern_p1);

  set_config("osd_async_recovery_min_cost", "100");
}

TEST_P(TestECFailoverWithPeering, ScrubClean) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_scrub_corruption";
  uint64_t object_size = k * stripe_unit;

  bufferlist bl = create_random_buffer(object_size);
  std::string test_data(bl.c_str(), bl.length());

  std::cout << "Writing full-stripe object (" << object_size << " bytes of random data)" << std::endl;
  create_and_write_verify(obj_name, test_data);

  std::cout << "Scrubbing object to verify data integrity" << std::endl;
  bool corruption_detected = scrub_object(obj_name);

  ASSERT_FALSE(corruption_detected)
    << "scrub_object() should NOT detect corruption when data is valid";

  std::cout << "=== ScrubDetectsCorruption test completed successfully ===" << std::endl;
}

TEST_P(TestECFailoverWithPeering, ScrubDetectsCorruption) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const uint64_t object_size = k * stripe_unit;
  const std::vector<int> shard_offsets = {/*0, 1, */k};
  const bool supports_crc = ec_plugin == "isa";

  for (int zone = 0; zone < 1; ++zone) {
    for (int shard_offset : shard_offsets) {
      const int absolute_shard = shard_offset;
      const std::string obj_name =
        "test_obj_zone_" + std::to_string(zone) +
        "_shard_" + std::to_string(shard_offset);

      bufferlist bl = create_random_buffer(object_size);
      std::string test_data(bl.c_str(), bl.length());

      std::cout << "\n=== ScrubDetectsCorruption: testing zone " << zone
                << ", shard offset " << shard_offset
                << " (absolute shard " << absolute_shard << ") ===" << std::endl;

      std::cout << "Writing object " << obj_name << " (" << object_size
                << " bytes of random data)" << std::endl;
      create_and_write_verify(obj_name, test_data);

      std::cout << "Corrupting object " << obj_name
                << " for zone iteration " << zone
                << " on relative shard " << shard_offset
                << " using absolute shard " << absolute_shard << std::endl;
      hobject_t hoid = make_test_object(obj_name);
      corrupt_shard_data(hoid,
                         pg_shard_t(absolute_shard, shard_id_t(absolute_shard)));

      std::cout << "Scrubbing object " << obj_name
                << " to verify corruption detection for zone iteration " << zone
                << ", shard offset " << shard_offset << std::endl;
      bool corruption_detected = scrub_object(obj_name);

      std::cout << "Zone iteration " << zone
                << " corruption result for shard offset " << shard_offset
                << ": " << (corruption_detected ? "detected" : "not detected")
                << " (absolute shard " << absolute_shard
                << ", supports_crc=" << (supports_crc ? "true" : "false")
                << ")" << std::endl;

      if (supports_crc) {
        EXPECT_TRUE(corruption_detected)
          << "scrub_object() should detect corruption for object " << obj_name
          << " during zone iteration " << zone
          << ", shard offset " << shard_offset
          << " (absolute shard " << absolute_shard << ")";
      } else {
        EXPECT_FALSE(corruption_detected)
            << "scrub_object() should not report corruption for object "
            << obj_name << " when CRC-based detection is unsupported"
            << " during zone iteration " << zone << ", shard offset "
            << shard_offset << " (absolute shard " << absolute_shard << ")";
      }
    }
  }

  std::cout << "=== ScrubDetectsCorruption test completed successfully ===" << std::endl;
}

TEST_P(TestECFailoverWithPeering, ScrubPartialWrite) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_scrub_partial_write";

  uint64_t partial_size = stripe_unit / 2;

  std::cout << "Creating partial write object with size " << partial_size
            << " bytes (stripe_unit=" << stripe_unit << ", full stripe would be "
            << (k * stripe_unit) << " bytes)" << std::endl;

  bufferlist bl = create_random_buffer(partial_size);
  std::string test_data(bl.c_str(), bl.length());

  std::cout << "Writing partial object (" << partial_size << " bytes)" << std::endl;
  create_and_write_verify(obj_name, test_data);

  write(obj_name, 0, test_data, test_data.size());

  // NOTE: Partial writes may expose scrub issues with EC pools
  std::cout << "Scrubbing partial write object to test scrub behavior" << std::endl;
  bool corruption_detected = scrub_object(obj_name);

  std::cout << "Scrub result for partial write: "
            << (corruption_detected ? "corruption detected" : "no corruption detected")
            << std::endl;

  EXPECT_FALSE(corruption_detected)
    << "scrub_object() should NOT detect corruption on valid partial write";

  std::cout << "=== ScrubPartialWrite test completed ===" << std::endl;
}

/**
 * TEST: ECRollbackShardVersions
 *
 * Verifies that shard_versions is handled correctly through a partial-write
 * rollback and a subsequent partial-write recovery push.
 *
 * Write history (stripe_unit = S, k >= 3):
 *
 *   vA: full write  -- shard_versions = {}
 *   vB: partial, skip shard 1 (offset (2%k)*S, len (k-1)*S)
 *           primary OI: OI = {vB, sv={1=vA}}
 *           shard 1 OI: OI = {vA, sv={}}
 *           shard 2 OI: OI = {vB, sv={1=vA}}
 *   vC: partial, skip shard 2 (offset (3%k)*S, len (k-1)*S)
 *           primary OI: OI = {vC, sv={2=vB}}
 *           shard 1 OI: OI = {vC, sv={2=vB}} (caught up)
 *           shard 2 OI: OI = {vB, sv={1=vA}} (now behind)
 *
 * Step 4: attempt full write vD with the parity shard (shard k+m-1) blocked.
 * Then drop shard k to open a new peering interval.
 * vD is rolled back on every shard that received it.
 * The new interval recovers shard 2 (which the primary knows is at vB) via the
 * partial-write recovery push in ECCommon.cc:
 *
 *   recovery push to shard 2: oi = {vC, sv={2=vB}}
 *                              set oi.version = vB
 *                              erase_if(sv[x] >= vB) => removes {2=vB}
 *                   result: shard 2 OI = {vB, sv={}}
 *
 * Note on technical inaccuracy: the recovery push gives shard 2
 * shard_versions={} rather than what was truly on-disk before vC ({1=vA})
 * as it has to build using the knowledge from the primary rather than what
 * would have existed on shard 2 at the time.
 *
 * Step 5: restore shard 1, then drop/recover shard 2 again to confirm
 * the recovery push produces the same clean state a second time.
 *
 * Requires k >= 3 so shards 0, 1, and 2 are distinct data shards.
 */
TEST_P(TestECFailoverWithPeering, ECRollbackShardVersions) {
  if (!(pool_flags & pg_pool_t::FLAG_EC_OPTIMIZATIONS)) {
    GTEST_SKIP() << "ECRollbackShardVersions requires optimized EC";
  }
  if (k < 3) {
    GTEST_SKIP() << "ECRollbackShardVersions requires k >= 3";
  }
  if (m < 2) {
    GTEST_SKIP() << "ECRollbackShardVersions requires m >= 2";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_rollback_shard_versions";
  const size_t object_size = stripe_unit * k * 2;

  // Helper: print version and shard_versions for every live shard.
  auto print_shard_versions = [&](const std::string& label) {
    std::cout << label << std::endl;
    for (auto& [shard_id, backend] : backends) {
      if (backend == nullptr) continue;
      object_info_t oi = read_shard_object_info(obj_name, shard_id);
      std::cout << "  shard " << shard_id << ": v=" << oi.version;
      if (oi.shard_versions.empty()) {
        std::cout << " sv={}";
      } else {
        std::cout << " sv={";
        bool first = true;
        for (auto& [sid, sv] : oi.shard_versions) {
          if (!first) std::cout << ", ";
          std::cout << sid << "=" << sv;
          first = false;
        }
        std::cout << "}";
      }
      std::cout << std::endl;
    }
  };

  create_and_write_verify(obj_name, std::string(object_size, 'A'));
  print_shard_versions("After write A (all shards):");

  // Write B: partial, skips shard 1.
  int result = write(obj_name,
                     (2 % k) * stripe_unit,
                     std::string((k - 1) * stripe_unit, 'B'),
                     object_size);
  ASSERT_EQ(0, result);
  eversion_t vB = read_shard_object_info(obj_name, 0).version;
  print_shard_versions("After write B (excl shard 1):");

  // Write C: partial, skips shard 2.  After this the primary OI must
  // record shard 2 as stale at vB.
  result = write(obj_name,
                 (3 % k) * stripe_unit,
                 std::string((k - 1) * stripe_unit, 'C'),
                 object_size);
  ASSERT_EQ(0, result);
  eversion_t vC = read_shard_object_info(obj_name, 0).version;
  print_shard_versions("After write C (excl shard 2):");

  // Verify the accumulated shard_versions on the primary after writes B and C.
  {
    object_info_t oi = read_shard_object_info(obj_name, 0);
    ASSERT_EQ(oi.version, vC);
    ASSERT_EQ(oi.shard_versions,
              (std::map<shard_id_t, eversion_t>{{shard_id_t(2), vB}}))
      << "After writes B and C the primary must record shard 2 as stale at vB";
  }

  // Drop a separate coding shard (interval_trigger_shard=k) to open a new
  // peering interval, blocking the last coding shard (blocked_shard=k+m-1)
  // so write D cannot commit before the interval changes.
  const int interval_trigger_shard = k;
  const int blocked_shard = k + m - 1;
  suspend_primary_to_osd(blocked_shard);
  result = write(obj_name, 0, std::string(object_size, 'D'), object_size);
  ASSERT_EQ(-EINPROGRESS, result);
  mark_osd_down(interval_trigger_shard);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();
  ASSERT_TRUE(all_shards_active());
  print_shard_versions("After write D + rollback on shard 2:");

  // After the new interval has started and vD is rolled back, shard 2 is
  // recovered via the partial-write recovery push.
  // The recovery push sets oi.version = vB and runs erase_if, yielding
  // shard_versions={}. This is an accepted difference to what was on disk
  // as we are recovering based on the primary's shard versions at the point
  // before the rollback.
  for (auto& [shard_id, backend] : backends) {
    if (backend == nullptr) continue;
    if (shard_id == interval_trigger_shard) continue;
    object_info_t oi = read_shard_object_info(obj_name, shard_id);
    if (shard_id == 2) {
      EXPECT_EQ(oi.version, vB);
      EXPECT_TRUE(oi.shard_versions.empty())
        << "shard 2 recovered to vB; shard_versions={}";
    } else {
      EXPECT_EQ(oi.version, vC);
      EXPECT_EQ(oi.shard_versions,
                (std::map<shard_id_t, eversion_t>{{shard_id_t(2), vB}}));
    }
  }

  mark_osd_up(interval_trigger_shard);
  event_loop->run_until_idle();
  print_shard_versions("After recovery of interval_trigger_shard:");

  // Drop and recover shard 2 again: the recovery push must produce the
  // same clean result.
  mark_osd_down(2);
  event_loop->run_until_idle();
  mark_osd_up(2);
  event_loop->run_until_idle();
  ASSERT_TRUE(all_shards_active());
  print_shard_versions("After recovery of shard 2 (step 5):");

  for (auto& [shard_id, backend] : backends) {
    if (backend == nullptr) continue;
    object_info_t oi = read_shard_object_info(obj_name, shard_id);
    if (shard_id == 2) {
      EXPECT_EQ(oi.version, vB);
      EXPECT_TRUE(oi.shard_versions.empty())
        << "shard 2 recovered to vB; shard_versions={}";
    } else {
      EXPECT_EQ(oi.version, vC);
      EXPECT_EQ(oi.shard_versions,
                (std::map<shard_id_t, eversion_t>{{shard_id_t(2), vB}}));
    }
  }

  std::cout << "=== ECRollbackShardVersions completed successfully ===" << std::endl;
}

/**
 * TEST: ECRollbackPreservesOlderShardVersions
 *
 * Specifically verifies that the erase_if in ECCommon.cc (partial-write
 * recovery push) preserves shard_versions entries that are OLDER than the
 * shard being recovered, i.e. that the condition is ">=" not "always clear".
 *
 * This test is the minimal case that distinguishes erase_if(kv.second >= v)
 * from a simple shard_versions.clear(): with clear() the older entry for
 * shard 2 would be wrongly discarded; with erase_if it is kept.
 *
 * Write history (stripe_unit = S, k >= 4, m >= 2):
 *
 *   vA: full write  -- shard_versions = {}
 *
 *   vB: single-chunk partial at offset 3*S (raw shard 3 only)
 *       written shards: {0(primary), 3, parities}
 *       non-written non-primaries: {1, 2, ..., k-1} except {3}
 *       (OI examples below shown for k=4)
 *           primary OI: OI = {vB, sv={1=vA, 2=vA}}
 *           shard 1 OI: OI = {vA, sv={}}
 *           shard 2 OI: OI = {vA, sv={}}
 *           shard 3 OI: OI = {vB, sv={1=vA, 2=vA}}
 *
 *   vC: single-chunk partial at offset 1*S (raw shard 1 only)
 *       written shards: {0(primary), 1, parities}
 *       shard 1 entry erased; shard 2 still stale (already-out-of-date path);
 *       shard 3 is now newly stale: shard_versions[3] = prior_version = vB
 *           primary OI: OI = {vC, sv={2=vA, 3=vB}}
 *           shard 1 OI: OI = {vC, sv={2=vA, 3=vB}}
 *           shard 2 OI: OI = {vA, sv={}}
 *           shard 3 OI: OI = {vB, sv={1=vA, 2=vA}}
 *
 *   attempt full write vD with the parity shard (shard k+m-1) blocked.
 *       Then drop shard k to open a new peering interval.
 *       vD is rolled back on every shard that received it.
 *       The new interval recovers shard 3 (which the primary knows is at vB)
 *       via the partial-write recovery push in ECCommon.cc:
 *
 *   recovery push to shard 3: oi = {vC, sv={2=vA, 3=vB}}
 *                              set oi.version = vB
 *                              erase_if(sv[x] >= vB):
 *                                removes {3=vB} because (vB >= vB)
 *                                keeps {2=vA} because (vA < vB)
 *                   result: shard 3 OI = {vB, sv={2=vA}}
 *
 *
 *
 * Requires k >= 4 (so single-chunk writes skip exactly the intended two
 * non-primaries) and m >= 2 (for the two distinct coding shards needed for
 * the blocked-write/interval-trigger trick).
 */
TEST_P(TestECFailoverWithPeering, ECRollbackPreservesOlderShardVersions) {
  if (!(pool_flags & pg_pool_t::FLAG_EC_OPTIMIZATIONS)) {
    GTEST_SKIP() << "ECRollbackPreservesOlderShardVersions requires optimized EC";
  }
  if (k < 4) {
    GTEST_SKIP() << "ECRollbackPreservesOlderShardVersions requires k >= 4";
  }
  if (m < 2) {
    GTEST_SKIP() << "ECRollbackPreservesOlderShardVersions requires m >= 2";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_rollback_preserves_older";
  const size_t object_size = stripe_unit * k * 2;

  // Helper: print version and shard_versions for every live shard.
  auto print_shard_versions = [&](const std::string& label) {
    std::cout << label << std::endl;
    for (auto& [shard_id, backend] : backends) {
      if (backend == nullptr) continue;
      object_info_t oi = read_shard_object_info(obj_name, shard_id);
      std::cout << "  shard " << shard_id << ": v=" << oi.version;
      if (oi.shard_versions.empty()) {
        std::cout << " sv={}";
      } else {
        std::cout << " sv={";
        bool first = true;
        for (auto& [sid, sv] : oi.shard_versions) {
          if (!first) std::cout << ", ";
          std::cout << sid << "=" << sv;
          first = false;
        }
        std::cout << "}";
      }
      std::cout << std::endl;
    }
  };

  create_and_write_verify(obj_name, std::string(object_size, 'A'));
  eversion_t vA = read_shard_object_info(obj_name, 0).version;
  print_shard_versions("After write A (full, all shards):");

  // Write B: single-chunk write touching only raw shard 3 (offset 3*S).
  // Non-written non-primaries are all data shards except 0 and 3,
  // i.e. shards 1, 2, and (for k>4) 4..k-1.
  int result = write(obj_name,
                     3 * stripe_unit,
                     std::string(stripe_unit, 'B'),
                     object_size);
  ASSERT_EQ(0, result);
  eversion_t vB = read_shard_object_info(obj_name, 0).version;
  print_shard_versions("After write B (shard 3 only):");

  for (auto& [shard_id, backend] : backends) {
    if (backend == nullptr) continue;
    object_info_t oi = read_shard_object_info(obj_name, shard_id);
    if (shard_id == 0 || shard_id == 3 || shard_id >= k) {
      // Build expected shard_versions: every data shard except primary (0)
      // and shard 3 was skipped by write B and is stale at vA.
      std::map<shard_id_t, eversion_t> expected_shard_versions;
      for (int s = 1; s < k; ++s) {
        if (s != 3) {
          expected_shard_versions[shard_id_t(s)] = vA;
        }
      }
      EXPECT_EQ(oi.version, vB);
      EXPECT_EQ(oi.shard_versions, expected_shard_versions)
        << "After write B: all non-primary data shards except 3 should be stale at vA";
    } else {
      EXPECT_EQ(oi.version, vA);
      EXPECT_TRUE(oi.shard_versions.empty())
        << "shard " << shard_id << ": shard_versions={}";
    }
  }

  // Write C: single-chunk write touching only raw shard 1 (offset 1*S).
  // Shard 1's entry is erased (caught up); shard 3 is newly stale at vB;
  // all other previously-skipped shards (2, 4..k-1) carry forward at vA.
  result = write(obj_name,
                 1 * stripe_unit,
                 std::string(stripe_unit, 'C'),
                 object_size);
  ASSERT_EQ(0, result);
  eversion_t vC = read_shard_object_info(obj_name, 0).version;
  print_shard_versions("After write C (shard 1 only):");

  for (auto& [shard_id, backend] : backends) {
    if (backend == nullptr) continue;
    object_info_t oi = read_shard_object_info(obj_name, shard_id);
    if (shard_id == 0 || shard_id == 1 || shard_id >= k) {
      // skipped_by_B minus shard 1 (caught up), plus shard 3 (newly stale at vB)
      std::map<shard_id_t, eversion_t> expected_shard_versions;
      for (int s = 1; s < k; ++s) {
        if (s != 1 && s != 3) {
          expected_shard_versions[shard_id_t(s)] = vA;
        }
      }
      expected_shard_versions[shard_id_t(3)] = vB;
      EXPECT_EQ(oi.version, vC);
      EXPECT_EQ(oi.shard_versions, expected_shard_versions)
        << "After write C: shard 3 should be stale at vB, remaining skipped shards at vA";
    } else if (shard_id == 3) {
      // shard 3 was not written in C; it still holds its write-B OI
      std::map<shard_id_t, eversion_t> expected_shard_versions;
      for (int s = 1; s < k; ++s) {
        if (s != 3) {
          expected_shard_versions[shard_id_t(s)] = vA;
        }
      }
      EXPECT_EQ(oi.version, vB);
      EXPECT_EQ(oi.shard_versions, expected_shard_versions)
        << "After write C: shard 3 should still hold its write-B OI";
    } else {
      EXPECT_EQ(oi.version, vA);
      EXPECT_TRUE(oi.shard_versions.empty())
        << "shard " << shard_id << ": shard_versions={}";
    }
  }

  // Attempt write D (full), blocked so it cannot commit.  Drop a separate
  // coding shard (interval_trigger_shard=k) to open a new peering interval,
  // forcing vD to be rolled back on shard 3 (and all others that received it).
  // The new interval immediately recovers shard 3 via the ECCommon.cc
  // partial-write push path (same mechanism as ECRollbackShardVersions).
  const int interval_trigger_shard = k;      // first coding shard -- gets dropped
  const int blocked_shard = k + m - 1;       // last coding shard -- ack is blocked
  suspend_primary_to_osd(blocked_shard);
  result = write(obj_name, 0, std::string(object_size, 'D'), object_size);
  ASSERT_EQ(-EINPROGRESS, result);
  mark_osd_down(interval_trigger_shard);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();
  ASSERT_TRUE(all_shards_active());
  print_shard_versions("After write D + rollback + recovery of shard 3:");

  // Key assertion: shard 3 is recovered to vB via the partial-write push
  // path, and the entry for shard 2 (at vA < vB) must be PRESERVED by
  // erase_if.  With a naive clear() the result would be {vB, sv={}}.
  for (auto& [shard_id, backend] : backends) {
    if (backend == nullptr) continue;
    if (shard_id == interval_trigger_shard) continue;
    object_info_t oi = read_shard_object_info(obj_name, shard_id);
    if (shard_id == 0 || shard_id == 1 || shard_id >= k) {
      // Same sv as written shards after write C: skipped-by-B minus shard 1, plus {3=vB}
      std::map<shard_id_t, eversion_t> expected_shard_versions;
      for (int s = 1; s < k; ++s) {
        if (s != 1 && s != 3) {
          expected_shard_versions[shard_id_t(s)] = vA;
        }
      }
      expected_shard_versions[shard_id_t(3)] = vB;
      EXPECT_EQ(oi.version, vC);
      EXPECT_EQ(oi.shard_versions, expected_shard_versions)
        << "After recovery: shard should be at vC";
    } else if (shard_id == 3) {
      // erase_if removes {3=vB} (vB >= vB), keeps remaining skipped-by-B entries (< vB)
      std::map<shard_id_t, eversion_t> expected_shard_versions;
      for (int s = 1; s < k; ++s) {
        if (s != 1 && s != 3) {
          expected_shard_versions[shard_id_t(s)] = vA;
        }
      }
      EXPECT_EQ(oi.version, vB)
        << "Shard 3 should be recovered to vB (its version per shard_versions)";
      EXPECT_EQ(oi.shard_versions, expected_shard_versions)
        << "erase_if must keep skipped-by-B entries (vA < vB); clear() would wrongly drop them";
    } else {
      EXPECT_EQ(oi.version, vA);
      EXPECT_TRUE(oi.shard_versions.empty())
        << "shard " << shard_id << ": shard_versions={}";
    }
  }

  // Restore interval_trigger_shard; all shards should now be at vC.
  mark_osd_up(interval_trigger_shard);
  event_loop->run_until_idle();
  ASSERT_TRUE(all_shards_active());
  print_shard_versions("After restoring interval_trigger_shard:");

  for (auto& [shard_id, backend] : backends) {
    if (backend == nullptr) continue;
    object_info_t oi = read_shard_object_info(obj_name, shard_id);
    if (shard_id == 0 || shard_id == 1 || shard_id >= k) {
      // Same sv as written shards after write C: skipped-by-B minus shard 1, plus {3=vB}
      std::map<shard_id_t, eversion_t> expected_shard_versions;
      for (int s = 1; s < k; ++s) {
        if (s != 1 && s != 3) {
          expected_shard_versions[shard_id_t(s)] = vA;
        }
      }
      expected_shard_versions[shard_id_t(3)] = vB;
      EXPECT_EQ(oi.version, vC);
      EXPECT_EQ(oi.shard_versions, expected_shard_versions)
        << "After restoring interval_trigger_shard: shard should be at vC";
    } else if (shard_id == 3) {
      // erase_if removes {3=vB} (vB >= vB), keeps remaining entries which are < vB
      std::map<shard_id_t, eversion_t> expected_shard_versions;
      for (int s = 1; s < k; ++s) {
        if (s != 1 && s != 3) {
          expected_shard_versions[shard_id_t(s)] = vA;
        }
      }
      EXPECT_EQ(oi.version, vB)
        << "Shard 3 should be recovered to vB (its version per shard_versions)";
      EXPECT_EQ(oi.shard_versions, expected_shard_versions)
        << "erase_if must keep skipped-by-B entries (vA < vB); clear() would wrongly drop them";
    } else {
      EXPECT_EQ(oi.version, vA);
      EXPECT_TRUE(oi.shard_versions.empty())
        << "shard " << shard_id << ": shard_versions={}";
    }
  }

  std::cout << "=== ECRollbackPreservesOlderShardVersions completed successfully ===" << std::endl;
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

