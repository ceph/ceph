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
 * TestBackendBasics - Unified parameterized test harness for EC and Replicated
 * backend operations.
 *
 * This file replaces TestECBasics.cc and TestReplicatedBasics.cc by combining
 * their nearly-identical test logic into a single parameterized suite that runs
 * across multiple backend configurations:
 *
 *   - Replicated (3 replicas)
 *   - EC ISA optimised   (FLAG_EC_OPTIMIZATIONS set)
 *   - EC ISA non-optimised (FLAG_EC_OPTIMIZATIONS cleared)
 *   - EC Jerasure optimised
 *   - EC Jerasure non-optimised
 *
 * Two test suites are defined:
 *
 *   TestBackendBasics  – WriteThenRead and PartialWrite, one instance per
 *                        BackendConfig.
 *
 *   TestBackendWriteRead – WriteThenRead parameterized over both BackendConfig
 *                          and WriteReadParam (data sizes), using the combined
 *                          BackendWriteReadParam struct.
 */

#include <gtest/gtest.h>
#include "test/osd/PGBackendTestFixture.h"
#include "test/osd/TestCommon.h"
#include "test/osd/OSDMapTestHelpers.h"
#include "messages/MOSDECSubOpWrite.h"

using namespace std;

// ---------------------------------------------------------------------------
// TestBackendBasics fixture
// ---------------------------------------------------------------------------

/**
 * TestBackendBasics - fixture that inherits PGBackendTestFixture and is
 * parameterized over BackendConfig.
 *
 * The constructor reads the BackendConfig parameter and configures the base
 * fixture fields (pool_type, k, m, stripe_width, ec_plugin, ec_technique,
 * num_replicas, min_size) before SetUp() is called by GTest.
 *
 * For EC non-optimised variants, SetUp() clears FLAG_EC_OPTIMIZATIONS from
 * the pool in the OSDMap after the base SetUp() has run.
 */
class TestBackendBasics : public PGBackendTestFixture,
                          public ::testing::WithParamInterface<BackendConfig> {
public:
  TestBackendBasics() : PGBackendTestFixture() {
    auto config = GetParam();
    pool_type = config.pool_type;
    if (pool_type == EC) {
      k = 4;
      m = 2;
      stripe_width = 4096 * k;
      ec_plugin = config.ec_plugin;
      ec_technique = config.ec_technique;
      // Set ec_optimizations before SetUp() so setup_ec_pool() clears the flag
      // before creating ECSwitch backends, keeping is_optimized_actual consistent.
      ec_optimizations = config.ec_optimizations;
    } else {
      num_replicas = 3;
      min_size = 2;
    }
  }

  void SetUp() override {
    PGBackendTestFixture::SetUp();
  }
};

// ---------------------------------------------------------------------------
// TestBackendBasics: WriteThenRead
// ---------------------------------------------------------------------------

/**
 * WriteThenRead - write data, verify messages sent, read back, verify data.
 *
 * For EC backends: asserts that MSG_OSD_EC_WRITE messages were sent.
 * For Replicated backends: asserts that at least one message was sent.
 * The write/read/verify logic is identical across backends.
 */
TEST_P(TestBackendBasics, WriteThenRead) {
  const std::string test_data = "Backend write test data with message handling";
  const std::string obj_name = "test_backend_write";

  // Execute create+write operation using fixture helper
  int result = create_and_write(obj_name, test_data);

  // Verify successful completion
  EXPECT_EQ(result, 0) << "Transaction should complete successfully";

  // Verify messages were sent to replicas/shards
  ASSERT_GT(listener->sent_messages.size(), 0u) << "Should send messages to replicas";

  // Backend-specific message type assertion
  if (GetParam().pool_type == EC) {
    int write_messages_sent = 0;
    for (auto msg : listener->sent_messages) {
      if (msg->get_type() == MSG_OSD_EC_WRITE) {
        write_messages_sent++;
      }
    }
    ASSERT_GT(write_messages_sent, 0) << "Should send EC write messages";
  }
  // For Replicated: we already asserted sent_messages.size() > 0 above.

  // Clear sent messages before read to distinguish read messages
  listener->sent_messages.clear();
  listener->sent_messages_with_dest.clear();

  // Now perform the read operation
  bufferlist read_data;
  int read_result = read_object(
    obj_name,
    0,                    // offset
    test_data.length(),   // length
    read_data,
    test_data.length()    // object_size
  );

  // Verify read completed successfully
  EXPECT_GE(read_result, 0) << "Read operation should complete successfully";

  // Verify the data read matches what was written
  ASSERT_EQ(read_data.length(), test_data.length())
    << "Read data length should match written data length";

  std::string read_string(read_data.c_str(), read_data.length());
  EXPECT_EQ(read_string, test_data)
    << "Read data should match written data";

  // For EC backends, verify read messages were sent to shards
  if (GetParam().pool_type == EC) {
    ASSERT_GT(listener->sent_messages.size(), 0u)
      << "Should send read messages to EC shards";
  }

  // All events should be processed by now
  ASSERT_FALSE(event_loop->has_events()) << "Event loop should be idle after read";
  listener->sent_messages.clear();
}

// ---------------------------------------------------------------------------
// TestBackendBasics: PartialWrite
// ---------------------------------------------------------------------------

/**
 * PartialWrite - create an object, perform a partial write at a non-zero
 * offset, read back and verify both original and modified data.
 *
 * This test is identical across all backends.
 */
TEST_P(TestBackendBasics, PartialWrite) {
  const std::string obj_name = "test_backend_partial_write";
  const size_t initial_size = 16 * 1024;  // 16KB initial object
  const size_t partial_offset = 4 * 1024;  // Write at 4KB offset
  const size_t partial_size = 4 * 1024;    // Write 4KB

  // Create initial data - fill with 'A'
  std::string initial_data(initial_size, 'A');

  // Create object with initial data
  int result = create_and_write(obj_name, initial_data, eversion_t(1, 1));
  EXPECT_EQ(result, 0) << "Initial write should complete successfully";

  // Prepare partial write data - fill with 'B'
  std::string partial_data(partial_size, 'B');

  // Perform partial write using the write helper
  result = write(
    obj_name,
    partial_offset,
    partial_data,
    eversion_t(1, 1),  // prior_version
    eversion_t(1, 2),  // at_version
    initial_size       // object_size
  );
  EXPECT_EQ(result, 0) << "Partial write should complete successfully";

  // Read back the entire object
  bufferlist read_data;
  int read_result = read_object(obj_name, 0, initial_size, read_data, initial_size);
  EXPECT_GE(read_result, 0) << "Read after partial write should complete successfully";

  // Verify the read data length
  ASSERT_EQ(read_data.length(), initial_size) << "Read data length should match object size";

  // Verify the data content
  std::string read_string(read_data.c_str(), read_data.length());

  // First part (0 to partial_offset) should still be 'A'
  for (size_t i = 0; i < partial_offset; i++) {
    EXPECT_EQ(read_string[i], 'A')
      << "Data before partial write offset should be unchanged at position " << i;
  }

  // Middle part (partial_offset to partial_offset+partial_size) should be 'B'
  for (size_t i = partial_offset; i < partial_offset + partial_size; i++) {
    EXPECT_EQ(read_string[i], 'B')
      << "Data at partial write region should be 'B' at position " << i;
  }

  // Last part (partial_offset+partial_size to end) should still be 'A'
  for (size_t i = partial_offset + partial_size; i < initial_size; i++) {
    ASSERT_EQ(read_string[i], 'A')
      << "Data after partial write region should be unchanged at position " << i;
  }
}

// ---------------------------------------------------------------------------
// Backend configurations shared by both test suites
// ---------------------------------------------------------------------------

namespace {

// All backend configurations
const std::vector<BackendConfig> kBackendConfigs = {
  {PGBackendTestFixture::REPLICATED, "", "", false, "Replicated"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", true,  "EC_ISA_Optimised"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", false, "EC_ISA_NonOptimised"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", true,  "EC_Jerasure_Optimised"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", false, "EC_Jerasure_NonOptimised"},
};

}  // namespace

// ---------------------------------------------------------------------------
// Instantiate TestBackendBasics with all backend configurations
// ---------------------------------------------------------------------------

INSTANTIATE_TEST_SUITE_P(
  Backends,
  TestBackendBasics,
  ::testing::ValuesIn(kBackendConfigs),
  [](const ::testing::TestParamInfo<BackendConfig>& info) {
    return info.param.label;
  }
);

// ---------------------------------------------------------------------------
// TestBackendWriteRead fixture (two-level parameterization)
// ---------------------------------------------------------------------------

/**
 * TestBackendWriteRead - fixture parameterized over BackendWriteReadParam,
 * which combines a BackendConfig with a WriteReadParam (data size).
 *
 * This produces N_backends × N_sizes test instances, e.g.:
 *   BackendSizes/TestBackendWriteRead.WriteThenRead/Replicated_4k
 *   BackendSizes/TestBackendWriteRead.WriteThenRead/EC_ISA_Optimised_4k
 *   ...
 */
class TestBackendWriteRead : public PGBackendTestFixture,
                             public ::testing::WithParamInterface<BackendWriteReadParam> {
public:
  TestBackendWriteRead() : PGBackendTestFixture() {
    auto config = GetParam().backend;
    pool_type = config.pool_type;
    if (pool_type == EC) {
      k = 4;
      m = 2;
      stripe_width = 4096 * k;
      ec_plugin = config.ec_plugin;
      ec_technique = config.ec_technique;
      // Set ec_optimizations before SetUp() so setup_ec_pool() clears the flag
      // before creating ECSwitch backends, keeping is_optimized_actual consistent.
      ec_optimizations = config.ec_optimizations;
    } else {
      num_replicas = 3;
      min_size = 2;
    }
  }

  void SetUp() override {
    PGBackendTestFixture::SetUp();
  }
};

// ---------------------------------------------------------------------------
// TestBackendWriteRead: WriteThenRead (size-parameterized)
// ---------------------------------------------------------------------------

TEST_P(TestBackendWriteRead, WriteThenRead) {
  auto param = GetParam().write_read;
  auto backend_config = GetParam().backend;
  std::string test_data(param.size, param.fill);
  std::string obj_name = "test_backend_" + backend_config.label + "_" + param.label;

  // Write operation
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << param.label << " write should complete successfully";

  // Clear messages between write and read
  listener->sent_messages.clear();
  listener->sent_messages_with_dest.clear();

  // Read operation
  bufferlist read_data;
  int read_result = read_object(obj_name, 0, test_data.length(), read_data, test_data.length());
  EXPECT_GE(read_result, 0) << param.label << " read should complete successfully";

  // Verify data
  ASSERT_EQ(read_data.length(), test_data.length())
    << param.label << " read data length mismatch";
  std::string read_string(read_data.c_str(), read_data.length());
  EXPECT_EQ(read_string, test_data)
    << param.label << " read data should match written data";

  // Clean up
  listener->sent_messages.clear();
}

// ---------------------------------------------------------------------------
// Helper: generate cross-product of backends × sizes
// ---------------------------------------------------------------------------

namespace {

// All data-size configurations
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
// Instantiate TestBackendWriteRead with the cross-product
// ---------------------------------------------------------------------------

INSTANTIATE_TEST_SUITE_P(
  BackendSizes,
  TestBackendWriteRead,
  ::testing::ValuesIn(make_cross_product()),
  [](const ::testing::TestParamInfo<BackendWriteReadParam>& info) {
    // Combine backend label and size label: e.g. "Replicated_4k"
    return info.param.backend.label + "_" + info.param.write_read.label;
  }
);

