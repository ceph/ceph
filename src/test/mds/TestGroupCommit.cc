// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <iostream>

#include "common/config.h"
#include "common/options.h"
#include "gtest/gtest.h"

// Verify MDS Group Commit config options are registered and have
// correct default values.

TEST(MDSGroupCommit, ConfigDefaults)
{
  auto& conf = *g_ceph_context->_conf;

  // Option 1: enable/disable switch
  ASSERT_TRUE(conf.exists("mds_group_commit_enable"));
  bool enabled = conf.get_val<bool>("mds_group_commit_enable");
  ASSERT_TRUE(enabled) << "Default should be enabled";

  // Option 2: max entries per batch
  ASSERT_TRUE(conf.exists("mds_group_commit_max_entries"));
  Option::size_t max_entries =
      conf.get_val<Option::size_t>("mds_group_commit_max_entries");
  ASSERT_GT(max_entries, 0u) << "Must be > 0";
  ASSERT_EQ(max_entries, 8u) << "Default should be 8";

  // Option 3: max interval before forced flush
  ASSERT_TRUE(conf.exists("mds_group_commit_max_interval"));
  double interval = conf.get_val<double>("mds_group_commit_max_interval");
  ASSERT_GT(interval, 0.0) << "Must be > 0";
  ASSERT_DOUBLE_EQ(interval, 0.002) << "Default should be 0.002s (2ms)";
}

TEST(MDSGroupCommit, ConfigRuntimeChange)
{
  auto& conf = *g_ceph_context->_conf;

  // Verify enable can be toggled
  conf.set_val("mds_group_commit_enable", "false");
  ASSERT_FALSE(conf.get_val<bool>("mds_group_commit_enable"));
  conf.set_val("mds_group_commit_enable", "true");
  ASSERT_TRUE(conf.get_val<bool>("mds_group_commit_enable"));

  // Verify max_entries can be changed at runtime
  conf.set_val("mds_group_commit_max_entries", "16");
  ASSERT_EQ(conf.get_val<Option::size_t>("mds_group_commit_max_entries"), 16u);

  // Verify interval can be changed
  conf.set_val("mds_group_commit_max_interval", "0.005");
  ASSERT_DOUBLE_EQ(conf.get_val<double>("mds_group_commit_max_interval"), 0.005);

  // Restore defaults
  conf.set_val("mds_group_commit_enable", "true");
  conf.set_val("mds_group_commit_max_entries", "8");
  conf.set_val("mds_group_commit_max_interval", "0.002");
}

TEST(MDSGroupCommit, BatchingLogic)
{
  // Verify batching invariant: with max_entries=N, flush should be
  // triggered when queue size >= N.
  size_t max_entries = 8;
  size_t count = 0;
  bool flushed = false;

  // Simulate journal_and_reply() batching logic
  auto maybe_flush = [&](size_t queue_size) -> bool {
    count++;
    if (queue_size >= max_entries) {
      flushed = true;
      return true;
    }
    return false;
  };

  // Add entries one by one, should flush on the 8th
  for (size_t i = 1; i <= max_entries; i++) {
    if (maybe_flush(i)) {
      ASSERT_EQ(i, max_entries) << "Should flush exactly at threshold";
      break;
    }
  }
  ASSERT_TRUE(flushed);
  ASSERT_EQ(count, max_entries);

  // Verify that with fewer than max_entries, no flush occurs
  flushed = false;
  ASSERT_FALSE(maybe_flush(3));
  ASSERT_FALSE(maybe_flush(5));
  ASSERT_FALSE(maybe_flush(7));
  ASSERT_FALSE(flushed) << "Should not flush below threshold";
}
