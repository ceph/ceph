// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * TestECFailover - Test harness for EC failover scenarios
 *
 * This test suite validates EC backend behavior during OSDMap changes,
 * such as OSD failures, recoveries, and acting set changes. It uses
 * the PGBackendTestFixture infrastructure to simulate failover scenarios
 * and verify that the EC backend properly handles state transitions.
 */

#include <gtest/gtest.h>
#include "test/osd/PGBackendTestFixture.h"

using namespace std;

// Test EC failover scenarios using PGBackendTestFixture
// This class extends PGBackendTestFixture to test OSDMap changes
// and their impact on EC operations.
//
// Configuration:
// - k=4 (data chunks)
// - m=2 (coding chunks)
// - chunk_size=4096 (stripe_width)
// - plugin=isa
// - technique=reed_sol_van
class TestECFailover : public PGBackendTestFixture {
public:
  TestECFailover() : PGBackendTestFixture(PGBackendTestFixture::EC) {
    // Configure EC parameters before SetUp() is called
    k = 4;
    m = 2;
    stripe_width = 4096 * k;
    ec_plugin = "isa";
    ec_technique = "reed_sol_van";
  }
  
  /**
   * Simulate OSD failure and primary failover
   *
   * This helper creates a new OSDMap with the failed OSD marked as down,
   * updates the acting set, and elects a new primary.
   *
   * @param failed_osd The OSD ID to mark as failed
   * @param new_primary_instance The instance ID to elect as new primary
   */
  void simulate_osd_failure(int failed_osd, int new_primary_instance)
  {
    // Create a new OSDMap with the failed OSD marked as down
    auto new_osdmap = std::make_shared<OSDMap>();
    new_osdmap->deepish_copy_from(*osdmap);
    new_osdmap->inc_epoch();
    
    // Mark the OSD as down (exists but not UP)
    new_osdmap->set_state(failed_osd, CEPH_OSD_EXISTS);  // Mark as down (exists but not UP)
    
    // Remove the failed OSD from all listeners' acting sets
    pg_shard_t failed_shard(failed_osd, shard_id_t(failed_osd));
    for (auto& [instance_id, list] : listeners) {
      list->shardset.erase(failed_shard);
      list->acting_recovery_backfill_shard_id_set.erase(shard_id_t(failed_osd));
    }
    
    // Create the new primary pg_shard_t
    pg_shard_t new_primary(new_primary_instance, shard_id_t(new_primary_instance));
    
    // Update the OSDMap and primary
    update_osdmap(new_osdmap, new_primary);
  }
};

// Test basic OSDMap update mechanism
// This test validates that the update_osdmap helper:
// 1. Calls on_change() on all EC switches
// 2. Updates the osdmap reference
// 3. Clears in-flight operations properly
TEST_F(TestECFailover, BasicOSDMapUpdate) {
  // Create and write an object first
  const std::string obj_name = "test_failover_object";
  const std::string test_data = "Initial data before OSDMap change";
  
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << "Initial write should complete successfully";
  
  // Verify the object was written
  bufferlist read_data;
  int read_result = read_object(obj_name, 0, test_data.length(), read_data, test_data.length());
  EXPECT_GE(read_result, 0) << "Read should complete successfully";
  ASSERT_EQ(read_data.length(), test_data.length());
  
  // Create a new OSDMap (simulating a map change)
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  new_osdmap->inc_epoch();
  
  // Update the OSDMap - this should trigger on_change()
  update_osdmap(new_osdmap);
  
  // Verify the osdmap was updated
  EXPECT_EQ(osdmap, new_osdmap) << "OSDMap should be updated";
  EXPECT_EQ(listener->osdmap, new_osdmap) << "Listener OSDMap should be updated";
  
  // After OSDMap update, we should still be able to read the object
  // (assuming no actual OSD failures in this simple test)
  bufferlist read_data2;
  read_result = read_object(obj_name, 0, test_data.length(), read_data2, test_data.length());
  EXPECT_GE(read_result, 0) << "Read after OSDMap update should complete successfully";
  ASSERT_EQ(read_data2.length(), test_data.length());
  
  std::string read_string(read_data2.c_str(), read_data2.length());
  EXPECT_EQ(read_string, test_data) << "Data should match after OSDMap update";
}

// Test primary failover scenario
// This test validates that:
// 1. An object can be written with shard 0 as primary
// 2. Shard 0 can be marked as failed
// 3. A new primary (shard 1) can be elected
// 4. The object can still be read from the new primary
TEST_F(TestECFailover, PrimaryFailover) {
  const std::string obj_name = "test_primary_failover";
  const std::string test_data = "Data written before primary failover";
  
  // Write initial data with shard 0 as primary
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << "Initial write should complete successfully";
  
  // Verify the object was written
  bufferlist read_data;
  int read_result = read_object(obj_name, 0, test_data.length(), read_data, test_data.length());
  EXPECT_GE(read_result, 0) << "Read should complete successfully";
  ASSERT_EQ(read_data.length(), test_data.length());
  
  std::string read_string(read_data.c_str(), read_data.length());
  EXPECT_EQ(read_string, test_data) << "Data should match before failover";
  
  // Verify instance 0 is currently the primary
  EXPECT_TRUE(listeners[0]->pgb_is_primary())
    << "Instance 0 should be primary before failover";
  EXPECT_FALSE(listeners[k]->pgb_is_primary())
    << "Instance " << k << " should not be primary before failover";
  
  // Simulate OSD 0 failure and elect instance k (first parity shard) as new primary
  // In EC, peering will not allow primary to be instances 1 to k-1 when instance 0 fails
  simulate_osd_failure(0, k);
  
  // Verify the primary changed
  EXPECT_FALSE(listeners[0]->pgb_is_primary())
    << "Instance 0 should not be primary after failover";
  EXPECT_TRUE(listeners[k]->pgb_is_primary())
    << "Instance " << k << " should be primary after failover";
  
  // Verify the convenience pointers were updated
  EXPECT_EQ(listener, listeners[k].get())
    << "Listener convenience pointer should point to new primary";
  EXPECT_EQ(backend, backends[k].get())
    << "Backend convenience pointer should point to new primary";
  
  // Read the object after failover to verify degraded reads work
  // The new primary (shard k) should coordinate reading from remaining shards
  // and reconstruct the data using EC (k=4, m=2, so we can lose 2 shards)
  bufferlist read_data_after;
  int read_result_after = read_object(obj_name, 0, test_data.length(), read_data_after, test_data.length());
  EXPECT_GE(read_result_after, 0) << "Degraded read should complete successfully after failover";
  ASSERT_EQ(read_data_after.length(), test_data.length());
  
  std::string read_string_after(read_data_after.c_str(), read_data_after.length());
  EXPECT_EQ(read_string_after, test_data) << "Data should match after failover with EC reconstruction";
  
  // Verify that the OSDMap was updated (epoch should have incremented)
  EXPECT_GT(listener->osdmap->get_epoch(), 1)
    << "OSDMap epoch should have incremented after failover";
}

