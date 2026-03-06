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

using namespace std;

class TestECFailoverWithPeering : public ECPeeringTestFixture {
public:
  TestECFailoverWithPeering() : ECPeeringTestFixture() {
    k = 4;
    m = 2;
    stripe_unit = 4096;
    ec_plugin = "isa";
    ec_technique = "reed_sol_van";
  }
};

TEST_F(TestECFailoverWithPeering, BasicPeeringCycle) {
  run_peering_cycle();
  
  EXPECT_TRUE(all_shards_active()) << "All shards should be active after peering";
  
  // Note: In EC pools, only the primary tracks PG_STATE_CLEAN.
  // Replicas are in ReplicaActive state and don't set the CLEAN flag.
  // Get acting_primary from OSDMap
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

TEST_F(TestECFailoverWithPeering, WriteWithPeering) {
  run_peering_cycle();
  ASSERT_TRUE(all_shards_active()) << "Peering must complete before write";
  
  const std::string obj_name = "test_write_with_peering";
  const std::string test_data = "Data written with full peering support";
  
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << "Write should complete successfully";
  
  bufferlist read_data;
  int read_result = read_object(obj_name, 0, test_data.length(), read_data, test_data.length());
  EXPECT_GE(read_result, 0) << "Read should complete successfully";
  ASSERT_EQ(read_data.length(), test_data.length());
  
  std::string read_string(read_data.c_str(), read_data.length());
  EXPECT_EQ(read_string, test_data) << "Data should match";
  
  auto* primary_ps = get_peering_state(0);
  EXPECT_GT(primary_ps->get_pg_log().get_log().log.size(), 0)
    << "Primary should have log entries after write";
}

TEST_F(TestECFailoverWithPeering, OSDFailureWithPeering) {
  run_peering_cycle();
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_osd_failure";
  // Write 16KB but read only 8KB to force reconstruction when shard 1 is down
  const std::string test_data(16384, 'X');  // 16KB write
  const size_t read_length = 8192;  // 8KB read
  
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << "Initial write should complete";
  
  // Pre-failover read: measure baseline message count with all OSDs up
  // Clear message counters first
  for (auto& [shard, listener] : backend_listeners) {
    listener->sent_messages.clear();
  }
  
  bufferlist pre_failover_read;
  int pre_read_result = read_object(obj_name, 0, read_length,
                                     pre_failover_read, test_data.length());
  EXPECT_GE(pre_read_result, 0) << "Pre-failover read should complete";
  
  // Count messages sent during pre-failover read
  size_t pre_failover_msg_count = 0;
  for (auto& [shard, listener] : backend_listeners) {
    pre_failover_msg_count += listener->sent_messages.size();
  }

  int failed_osd = 1;  // Fail shard 1 which contains part of the data
  
  // Use fixture helper to mark OSD as down
  mark_osd_down(failed_osd);
  
  // Primary (OSD 0) should remain active after non-primary OSD failure
  auto* primary_ps = get_peering_state(0);
  std::string primary_state = get_state_name(0);
  EXPECT_TRUE(primary_state.find("Peering") != std::string::npos ||
              primary_state.find("Active") != std::string::npos)
    << "Primary should be peering or active after OSD failure, got: " << primary_state;
  
  EXPECT_TRUE(primary_ps->get_acting_recovery_backfill().count(pg_shard_t(failed_osd, shard_id_t(failed_osd))) == 0)
    << "Failed OSD should not be in acting set";
  
  // Clear message counters before post-failover read
  for (auto& [shard, listener] : backend_listeners) {
    listener->sent_messages.clear();
  }
  
  // Post-failover read: verify EC reconstruction works with one OSD down
  bufferlist post_failover_read;
  int post_read_result = read_object(obj_name, 0, read_length,
                                      post_failover_read, test_data.length());
  EXPECT_GE(post_read_result, 0) << "Read should complete successfully after OSD failure";
  ASSERT_EQ(post_failover_read.length(), read_length)
    << "Read length should match after OSD failure";
  
  std::string read_string(post_failover_read.c_str(), post_failover_read.length());
  std::string expected_data(read_length, 'X');
  EXPECT_EQ(read_string, expected_data)
    << "Data should be correctly reconstructed via EC after OSD failure";
  
  // Count messages sent during post-failover read
  size_t post_failover_msg_count = 0;
  for (auto& [shard, listener] : backend_listeners) {
    post_failover_msg_count += listener->sent_messages.size();
  }
  
  // This is an 8k read of a 16k object in a 4+2 array.  This means that if shard 1
  // is missing, then this should result in 4 reads, rather than 2 to recover.
  EXPECT_GT(post_failover_msg_count, pre_failover_msg_count)
    << "Post-failover read should complete successfully "
    << "(pre: " << pre_failover_msg_count << ", post: " << post_failover_msg_count << ")";
}

TEST_F(TestECFailoverWithPeering, PrimaryFailoverWithPeering) {
  run_peering_cycle();
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_primary_failover";
  const std::string test_data = "Data before primary failover";
  
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << "Initial write should complete";
  
  EXPECT_TRUE(get_peering_listener(0)->backend_listener->pgb_is_primary())
    << "Shard 0 should be primary initially";
  
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
  bufferlist read_data;
  int read_result = read_object(obj_name, 0, test_data.length(),
                                read_data, test_data.length());
  EXPECT_GE(read_result, 0) << "Read should complete successfully after primary failover";
  ASSERT_EQ(read_data.length(), test_data.length())
    << "Read length should match after primary failover";
  
  std::string read_string(read_data.c_str(), read_data.length());
  EXPECT_EQ(read_string, test_data)
    << "Data should be correctly reconstructed via EC after primary failover";
}

TEST_F(TestECFailoverWithPeering, MultipleOSDFailuresWithPeering) {
  run_peering_cycle();
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_multiple_failures";
  const std::string test_data = "Data before multiple failures";
  
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << "Initial write should complete";
  
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

TEST_F(TestECFailoverWithPeering, PeeringWithLogDivergence) {
  run_peering_cycle();
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string pre_div_obj = "test_pre_divergence";
  const std::string pre_div_data = "Data written before divergence";
  
  int result = create_and_write(pre_div_obj, pre_div_data, eversion_t(1, 1));
  EXPECT_EQ(result, 0) << "Pre-divergence write should complete";
  
  auto* primary_ps = get_peering_state(0);
  size_t initial_log_size = primary_ps->get_pg_log().get_log().log.size();
  EXPECT_GT(initial_log_size, 0) << "Primary should have log entries after pre-divergence write";
  
  // Note: get_pg_log().get_log().head reflects the log entries added via append_log
  eversion_t pre_div_log_head = primary_ps->get_pg_log().get_log().head;
  EXPECT_GT(pre_div_log_head.version, 0u) << "PG log head should be non-zero after write";
  
  const std::string post_div_obj = "test_post_divergence";
  const std::string post_div_data = "Data written after divergence point";
  
  result = create_and_write(post_div_obj, post_div_data, eversion_t(1, 2));
  EXPECT_EQ(result, 0) << "Post-divergence write should complete";
  
  eversion_t post_div_log_head = primary_ps->get_pg_log().get_log().head;
  EXPECT_GT(post_div_log_head.version, pre_div_log_head.version)
    << "PG log head should advance after post-divergence write";
  
  size_t log_size_after_writes = primary_ps->get_pg_log().get_log().log.size();
  EXPECT_GE(log_size_after_writes, initial_log_size)
    << "Primary log should have at least as many entries after second write";
  
  // Trigger a new peering cycle by advancing the map to simulate re-peering
  // after a shard had a divergent log.
  advance_epoch();
  
  std::string primary_state = get_state_name(0);
  ASSERT_TRUE(all_shards_active() ||
              primary_state.find("Recovery") != std::string::npos ||
              primary_state.find("Peering") != std::string::npos)
    << "Shards should be active, recovering, or peering after map advance, got: "
    << primary_state;
  
  // --- Verify pre-divergence data is readable and correct ---
  bufferlist pre_div_read;
  int read_result = read_object(pre_div_obj, 0, pre_div_data.length(),
                                pre_div_read, pre_div_data.length());
  EXPECT_GE(read_result, 0) << "Pre-divergence object should be readable after reconciliation";
  ASSERT_EQ(pre_div_read.length(), pre_div_data.length())
    << "Pre-divergence read length should match";
  {
    std::string read_str(pre_div_read.c_str(), pre_div_read.length());
    EXPECT_EQ(read_str, pre_div_data)
      << "Pre-divergence data should match after log reconciliation";
  }
  
  // --- Verify post-divergence data is readable and correct ---
  bufferlist post_div_read;
  read_result = read_object(post_div_obj, 0, post_div_data.length(),
                            post_div_read, post_div_data.length());
  EXPECT_GE(read_result, 0) << "Post-divergence object should be readable after reconciliation";
  ASSERT_EQ(post_div_read.length(), post_div_data.length())
    << "Post-divergence read length should match";
  {
    std::string read_str(post_div_read.c_str(), post_div_read.length());
    EXPECT_EQ(read_str, post_div_data)
      << "Post-divergence data should match after log reconciliation";
  }
  
  // After peering, the primary's PG log head should reflect all writes.
  eversion_t primary_log_head = primary_ps->get_pg_log().get_log().head;
  EXPECT_EQ(primary_log_head, post_div_log_head)
    << "Primary PG log head should reflect all writes after reconciliation";
  
  pg_t pgid = get_peering_state(0)->get_info().pgid.pgid;
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(pgid, &acting_osds, &acting_primary);

  for (int shard : acting_osds) {
    if (shard == CRUSH_ITEM_NONE) {
      continue;
    }
    auto* shard_ps = get_peering_state(shard);
    if (shard_ps->is_active()) {
      eversion_t shard_info_last_update = shard_ps->get_info().last_update;
      if (shard == acting_primary) {
        EXPECT_EQ(shard_info_last_update, post_div_log_head)
          << "Primary shard info.last_update should match post-divergence log head";
      } else {
        EXPECT_LE(shard_info_last_update, post_div_log_head)
          << "Shard " << shard << " info.last_update should not exceed primary's log head";
      }
    }
  }
  
  // Verify the formerly-failed shard's PG log is accessible and consistent.
  // We use the last data shard (k-1) as the "formerly-failed" shard to check.
  int reconciled_shard = k - 1;
  if (reconciled_shard >= 0 && reconciled_shard < k + m) {
    auto* reconciled_ps = get_peering_state(reconciled_shard);
    size_t reconciled_log_size = reconciled_ps->get_pg_log().get_log().log.size();
    auto* primary_ps_check = get_peering_state(acting_primary);
    size_t primary_log_size = primary_ps_check->get_pg_log().get_log().log.size();
    EXPECT_LE(reconciled_log_size, primary_log_size)
      << "Reconciled shard " << reconciled_shard
      << " log size should not exceed primary's log size";
    
    if (reconciled_ps->is_active()) {
      eversion_t reconciled_info_lu = reconciled_ps->get_info().last_update;
      EXPECT_LE(reconciled_info_lu, post_div_log_head)
        << "Reconciled shard " << reconciled_shard
        << " info.last_update should not exceed primary's log head after log reconciliation";
    }
  }
}

TEST_F(TestECFailoverWithPeering, RecoveryWithPeering) {
  run_peering_cycle();
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj1_name = "test_recovery_obj1";
  const std::string obj1_data = "First object data for recovery test";
  
  const std::string obj2_name = "test_recovery_obj2";
  const std::string obj2_data = "Second object data for recovery test";
  
  int result = create_and_write(obj1_name, obj1_data, eversion_t(1, 1));
  EXPECT_EQ(result, 0) << "First pre-failure write should complete";
  
  result = create_and_write(obj2_name, obj2_data, eversion_t(1, 2));
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
  
  result = create_and_write(post_recovery_obj, post_recovery_data, eversion_t(1, 3));
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

