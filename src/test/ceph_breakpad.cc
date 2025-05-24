// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 Clyso GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <common/Thread.h>
#include <common/ceph_time.h>
#include <include/uuid.h>

#include <csignal>
#include <exception>
#include <filesystem>
#include <fstream>
#include <include/expected.hpp>
#include <ios>

#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "gtest/gtest.h"
#include "include/ceph_assert.h"
#include <google_breakpad/common/minidump_format.h>

class BreakpadDeathTest : public ::testing::Test {
  const std::filesystem::path crash_dir{
      g_conf().get_val<std::string>("crash_dir")};

  void SetUp() override {
    std::filesystem::create_directories(crash_dir);
    std::cout << "using crash dir: " << crash_dir << std::endl;
  }

  void TearDown() override { std::filesystem::remove_all(crash_dir);}

 public:
  std::optional<std::filesystem::path> minidump() {
    for (auto const& dir_entry :
         std::filesystem::directory_iterator{crash_dir}) {
      if (dir_entry.is_regular_file() &&
          dir_entry.path().extension() == ".dmp") {
        return dir_entry.path();
      }
    }
    return std::nullopt;
  }

  void check_minidump() {
    const auto md = minidump();
    ASSERT_TRUE(md.has_value());
    EXPECT_TRUE(std::filesystem::exists(md.value())) << md.value();

    std::ifstream in(md.value(), std::ios::binary);
    std::array<char, sizeof(MDRawHeader)> buf{0};
    in.read(buf.data(), sizeof(MDRawHeader));

    const auto* header = reinterpret_cast<MDRawHeader*>(buf.data());
    ASSERT_TRUE(header);
    EXPECT_EQ(header->signature, MD_HEADER_SIGNATURE);
    EXPECT_EQ(header->version, MD_HEADER_VERSION);
    EXPECT_GT(header->stream_count, 0);
    EXPECT_GT(header->time_date_stamp, 0);
  }

  std::string expected_minidump_message() {
    return "minidump created in path " + crash_dir.string();
  }
};

TEST_F(BreakpadDeathTest, CephAbortCreatesMinidump) {
  ASSERT_DEATH(ceph_abort(), expected_minidump_message());
  check_minidump();
}

TEST_F(BreakpadDeathTest, AbortCreatesMinidump) {
  ASSERT_DEATH(abort(), expected_minidump_message());
  check_minidump();
}

TEST_F(BreakpadDeathTest, SegfaultCreatesMinidump) {
  EXPECT_EXIT(
      std::raise(SIGSEGV), testing::KilledBySignal(SIGSEGV),
      expected_minidump_message());
  check_minidump();
}

TEST_F(BreakpadDeathTest, TerminateCreatesMinidump) {
  ASSERT_DEATH(std::terminate(), expected_minidump_message());
  check_minidump();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  GTEST_FLAG_SET(death_test_style, "threadsafe");

  const auto dir =
      std::filesystem::temp_directory_path() / "ceph_breakpad_test";
  std::map<std::string, std::string> defaults = {
      {"breakpad", "true"}, {"crash_dir", dir.string()}};
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(
      &defaults, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  return RUN_ALL_TESTS();
}
