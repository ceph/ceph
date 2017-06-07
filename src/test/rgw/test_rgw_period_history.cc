// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include "rgw/rgw_period_history.h"
#include "rgw/rgw_rados.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include <boost/lexical_cast.hpp>
#include <gtest/gtest.h>

namespace {

// construct a period with the given fields
RGWPeriod make_period(const std::string& id, epoch_t realm_epoch,
                      const std::string& predecessor)
{
  RGWPeriod period(id);
  period.set_realm_epoch(realm_epoch);
  period.set_predecessor(predecessor);
  return period;
}

const auto current_period = make_period("5", 5, "4");

// mock puller that throws an exception if it's called
struct ErrorPuller : public RGWPeriodHistory::Puller {
  int pull(const std::string& id, RGWPeriod& period) override {
    throw std::runtime_error("unexpected call to pull");
  }
};
ErrorPuller puller; // default puller

// mock puller that records the period ids requested and returns an error
using Ids = std::vector<std::string>;
class RecordingPuller : public RGWPeriodHistory::Puller {
  const int error;
 public:
  RecordingPuller(int error) : error(error) {}
  Ids ids;
  int pull(const std::string& id, RGWPeriod& period) override {
    ids.push_back(id);
    return error;
  }
};

// mock puller that returns a fake period by parsing the period id
struct NumericPuller : public RGWPeriodHistory::Puller {
  int pull(const std::string& id, RGWPeriod& period) override {
    // relies on numeric period ids to divine the realm_epoch
    auto realm_epoch = boost::lexical_cast<epoch_t>(id);
    auto predecessor = boost::lexical_cast<std::string>(realm_epoch-1);
    period = make_period(id, realm_epoch, predecessor);
    return 0;
  }
};

} // anonymous namespace

// for ASSERT_EQ()
bool operator==(const RGWPeriod& lhs, const RGWPeriod& rhs)
{
  return lhs.get_id() == rhs.get_id()
      && lhs.get_realm_epoch() == rhs.get_realm_epoch();
}

TEST(PeriodHistory, InsertBefore)
{
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  // inserting right before current_period 5 will attach to history
  auto c = history.insert(make_period("4", 4, "3"));
  ASSERT_TRUE(c);
  ASSERT_FALSE(c.has_prev());
  ASSERT_TRUE(c.has_next());

  // cursor can traverse forward to current_period
  c.next();
  ASSERT_EQ(5u, c.get_epoch());
  ASSERT_EQ(current_period, c.get_period());
}

TEST(PeriodHistory, InsertAfter)
{
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  // inserting right after current_period 5 will attach to history
  auto c = history.insert(make_period("6", 6, "5"));
  ASSERT_TRUE(c);
  ASSERT_TRUE(c.has_prev());
  ASSERT_FALSE(c.has_next());

  // cursor can traverse back to current_period
  c.prev();
  ASSERT_EQ(5u, c.get_epoch());
  ASSERT_EQ(current_period, c.get_period());
}

TEST(PeriodHistory, InsertWayBefore)
{
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  // inserting way before current_period 5 will not attach to history
  auto c = history.insert(make_period("1", 1, ""));
  ASSERT_FALSE(c);
  ASSERT_EQ(0, c.get_error());
}

TEST(PeriodHistory, InsertWayAfter)
{
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  // inserting way after current_period 5 will not attach to history
  auto c = history.insert(make_period("9", 9, "8"));
  ASSERT_FALSE(c);
  ASSERT_EQ(0, c.get_error());
}

TEST(PeriodHistory, PullPredecessorsBeforeCurrent)
{
  RecordingPuller puller{-EFAULT};
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  // create a disjoint history at 1 and verify that periods are requested
  // backwards from current_period
  auto c1 = history.attach(make_period("1", 1, ""));
  ASSERT_FALSE(c1);
  ASSERT_EQ(-EFAULT, c1.get_error());
  ASSERT_EQ(Ids{"4"}, puller.ids);

  auto c4 = history.insert(make_period("4", 4, "3"));
  ASSERT_TRUE(c4);

  c1 = history.attach(make_period("1", 1, ""));
  ASSERT_FALSE(c1);
  ASSERT_EQ(-EFAULT, c1.get_error());
  ASSERT_EQ(Ids({"4", "3"}), puller.ids);

  auto c3 = history.insert(make_period("3", 3, "2"));
  ASSERT_TRUE(c3);

  c1 = history.attach(make_period("1", 1, ""));
  ASSERT_FALSE(c1);
  ASSERT_EQ(-EFAULT, c1.get_error());
  ASSERT_EQ(Ids({"4", "3", "2"}), puller.ids);

  auto c2 = history.insert(make_period("2", 2, "1"));
  ASSERT_TRUE(c2);

  c1 = history.attach(make_period("1", 1, ""));
  ASSERT_TRUE(c1);
  ASSERT_EQ(Ids({"4", "3", "2"}), puller.ids);
}

TEST(PeriodHistory, PullPredecessorsAfterCurrent)
{
  RecordingPuller puller{-EFAULT};
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  // create a disjoint history at 9 and verify that periods are requested
  // backwards down to current_period
  auto c9 = history.attach(make_period("9", 9, "8"));
  ASSERT_FALSE(c9);
  ASSERT_EQ(-EFAULT, c9.get_error());
  ASSERT_EQ(Ids{"8"}, puller.ids);

  auto c8 = history.attach(make_period("8", 8, "7"));
  ASSERT_FALSE(c8);
  ASSERT_EQ(-EFAULT, c8.get_error());
  ASSERT_EQ(Ids({"8", "7"}), puller.ids);

  auto c7 = history.attach(make_period("7", 7, "6"));
  ASSERT_FALSE(c7);
  ASSERT_EQ(-EFAULT, c7.get_error());
  ASSERT_EQ(Ids({"8", "7", "6"}), puller.ids);

  auto c6 = history.attach(make_period("6", 6, "5"));
  ASSERT_TRUE(c6);
  ASSERT_EQ(Ids({"8", "7", "6"}), puller.ids);
}

TEST(PeriodHistory, MergeBeforeCurrent)
{
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  auto c = history.get_current();
  ASSERT_FALSE(c.has_prev());

  // create a disjoint history at 3
  auto c3 = history.insert(make_period("3", 3, "2"));
  ASSERT_FALSE(c3);

  // insert the missing period to merge 3 and 5
  auto c4 = history.insert(make_period("4", 4, "3"));
  ASSERT_TRUE(c4);
  ASSERT_TRUE(c4.has_prev());
  ASSERT_TRUE(c4.has_next());

  // verify that the merge didn't destroy the original cursor's history
  ASSERT_EQ(current_period, c.get_period());
  ASSERT_TRUE(c.has_prev());
}

TEST(PeriodHistory, MergeAfterCurrent)
{
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  auto c = history.get_current();
  ASSERT_FALSE(c.has_next());

  // create a disjoint history at 7
  auto c7 = history.insert(make_period("7", 7, "6"));
  ASSERT_FALSE(c7);

  // insert the missing period to merge 5 and 7
  auto c6 = history.insert(make_period("6", 6, "5"));
  ASSERT_TRUE(c6);
  ASSERT_TRUE(c6.has_prev());
  ASSERT_TRUE(c6.has_next());

  // verify that the merge didn't destroy the original cursor's history
  ASSERT_EQ(current_period, c.get_period());
  ASSERT_TRUE(c.has_next());
}

TEST(PeriodHistory, MergeWithoutCurrent)
{
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  // create a disjoint history at 7
  auto c7 = history.insert(make_period("7", 7, "6"));
  ASSERT_FALSE(c7);

  // create a disjoint history at 9
  auto c9 = history.insert(make_period("9", 9, "8"));
  ASSERT_FALSE(c9);

  // insert the missing period to merge 7 and 9
  auto c8 = history.insert(make_period("8", 8, "7"));
  ASSERT_FALSE(c8); // not connected to current_period yet

  // insert the missing period to merge 5 and 7-9
  auto c = history.insert(make_period("6", 6, "5"));
  ASSERT_TRUE(c);
  ASSERT_TRUE(c.has_next());

  // verify that we merged all periods from 5-9
  c.next();
  ASSERT_EQ(7u, c.get_epoch());
  ASSERT_TRUE(c.has_next());
  c.next();
  ASSERT_EQ(8u, c.get_epoch());
  ASSERT_TRUE(c.has_next());
  c.next();
  ASSERT_EQ(9u, c.get_epoch());
  ASSERT_FALSE(c.has_next());
}

TEST(PeriodHistory, AttachBefore)
{
  NumericPuller puller;
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  auto c1 = history.attach(make_period("1", 1, ""));
  ASSERT_TRUE(c1);

  // verify that we pulled and merged all periods from 1-5
  auto c = history.get_current();
  ASSERT_TRUE(c);
  ASSERT_TRUE(c.has_prev());
  c.prev();
  ASSERT_EQ(4u, c.get_epoch());
  ASSERT_TRUE(c.has_prev());
  c.prev();
  ASSERT_EQ(3u, c.get_epoch());
  ASSERT_TRUE(c.has_prev());
  c.prev();
  ASSERT_EQ(2u, c.get_epoch());
  ASSERT_TRUE(c.has_prev());
  c.prev();
  ASSERT_EQ(1u, c.get_epoch());
  ASSERT_FALSE(c.has_prev());
}

TEST(PeriodHistory, AttachAfter)
{
  NumericPuller puller;
  RGWPeriodHistory history(g_ceph_context, &puller, current_period);

  auto c9 = history.attach(make_period("9", 9, "8"));
  ASSERT_TRUE(c9);

  // verify that we pulled and merged all periods from 5-9
  auto c = history.get_current();
  ASSERT_TRUE(c);
  ASSERT_TRUE(c.has_next());
  c.next();
  ASSERT_EQ(6u, c.get_epoch());
  ASSERT_TRUE(c.has_next());
  c.next();
  ASSERT_EQ(7u, c.get_epoch());
  ASSERT_TRUE(c.has_next());
  c.next();
  ASSERT_EQ(8u, c.get_epoch());
  ASSERT_TRUE(c.has_next());
  c.next();
  ASSERT_EQ(9u, c.get_epoch());
  ASSERT_FALSE(c.has_next());
}

int main(int argc, char** argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
