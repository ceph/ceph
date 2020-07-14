// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/types.h"
#include "osd/osd_types.h"
#include "osd/OSDMap.h"
#include "gtest/gtest.h"
#include "include/coredumpctl.h"
#include "common/Thread.h"
#include "include/stringify.h"
#include "osd/ReplicatedBackend.h"
#include <sstream>

TEST(hobject, prefixes0)
{
  uint32_t mask = 0xE947FA20;
  uint32_t bits = 12;
  int64_t pool = 0;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000000.02A"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes1)
{
  uint32_t mask = 0x0000000F;
  uint32_t bits = 6;
  int64_t pool = 20;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000014.F0"));
  prefixes_correct.insert(string("0000000000000014.F4"));
  prefixes_correct.insert(string("0000000000000014.F8"));
  prefixes_correct.insert(string("0000000000000014.FC"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes2)
{
  uint32_t mask = 0xDEADBEAF;
  uint32_t bits = 25;
  int64_t pool = 0;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000000.FAEBDA0"));
  prefixes_correct.insert(string("0000000000000000.FAEBDA2"));
  prefixes_correct.insert(string("0000000000000000.FAEBDA4"));
  prefixes_correct.insert(string("0000000000000000.FAEBDA6"));
  prefixes_correct.insert(string("0000000000000000.FAEBDA8"));
  prefixes_correct.insert(string("0000000000000000.FAEBDAA"));
  prefixes_correct.insert(string("0000000000000000.FAEBDAC"));
  prefixes_correct.insert(string("0000000000000000.FAEBDAE"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes3)
{
  uint32_t mask = 0xE947FA20;
  uint32_t bits = 32;
  int64_t pool = 0x23;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000023.02AF749E"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes4)
{
  uint32_t mask = 0xE947FA20;
  uint32_t bits = 0;
  int64_t pool = 0x23;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000023."));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes5)
{
  uint32_t mask = 0xDEADBEAF;
  uint32_t bits = 1;
  int64_t pool = 0x34AC5D00;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000034AC5D00.1"));
  prefixes_correct.insert(string("0000000034AC5D00.3"));
  prefixes_correct.insert(string("0000000034AC5D00.5"));
  prefixes_correct.insert(string("0000000034AC5D00.7"));
  prefixes_correct.insert(string("0000000034AC5D00.9"));
  prefixes_correct.insert(string("0000000034AC5D00.B"));
  prefixes_correct.insert(string("0000000034AC5D00.D"));
  prefixes_correct.insert(string("0000000034AC5D00.F"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(pg_interval_t, check_new_interval)
{
// iterate through all 4 combinations
for (unsigned i = 0; i < 4; ++i) {
  //
  // Create a situation where osdmaps are the same so that
  // each test case can diverge from it using minimal code.
  //
  int osd_id = 1;
  epoch_t epoch = 40;
  std::shared_ptr<OSDMap> osdmap(new OSDMap());
  osdmap->set_max_osd(10);
  osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
  osdmap->set_epoch(epoch);
  std::shared_ptr<OSDMap> lastmap(new OSDMap());
  lastmap->set_max_osd(10);
  lastmap->set_state(osd_id, CEPH_OSD_EXISTS);
  lastmap->set_epoch(epoch);
  epoch_t same_interval_since = epoch;
  epoch_t last_epoch_clean = same_interval_since;
  int64_t pool_id = 200;
  int pg_num = 4;
  __u8 min_size = 2;
  boost::scoped_ptr<IsPGRecoverablePredicate> recoverable(new ReplicatedBackend::RPCRecPred());
  {
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    inc.new_pools[pool_id].set_pg_num_pending(pg_num);
    inc.new_up_thru[osd_id] = epoch + 1;
    osdmap->apply_incremental(inc);
    lastmap->apply_incremental(inc);
  }
  vector<int> new_acting;
  new_acting.push_back(osd_id);
  new_acting.push_back(osd_id + 1);
  vector<int> old_acting = new_acting;
  int old_primary = osd_id;
  int new_primary = osd_id;
  vector<int> new_up;
  new_up.push_back(osd_id);
  int old_up_primary = osd_id;
  int new_up_primary = osd_id;
  vector<int> old_up = new_up;
  pg_t pgid;
  pgid.set_pool(pool_id);

  //
  // Do nothing if there are no modifications in
  // acting, up or pool size and that the pool is not
  // being split
  //
  {
    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_FALSE(PastIntervals::check_new_interval(old_primary,
						   new_primary,
						   old_acting,
						   new_acting,
						   old_up_primary,
						   new_up_primary,
						   old_up,
						   new_up,
						   same_interval_since,
						   last_epoch_clean,
						   osdmap,
						   lastmap,
						   pgid,
                                                   *recoverable,
						   &past_intervals));
    ASSERT_TRUE(past_intervals.empty());
  }

  //
  // The acting set has changed
  //
  {
    vector<int> new_acting;
    int _new_primary = osd_id + 1;
    new_acting.push_back(_new_primary);

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals));
    old_primary = new_primary;
  }

  //
  // The up set has changed
  //
  {
    vector<int> new_up;
    int _new_primary = osd_id + 1;
    new_up.push_back(_new_primary);

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // The up primary has changed
  //
  {
    vector<int> new_up;
    int _new_up_primary = osd_id + 1;

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  _new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // PG is splitting
  //
  {
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    int new_pg_num = pg_num ^ 2;
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(new_pg_num);
    osdmap->apply_incremental(inc);

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // PG is pre-merge source
  //
  {
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    inc.new_pools[pool_id].set_pg_num_pending(pg_num - 1);
    osdmap->apply_incremental(inc);
    cout << "pg_num " << pg_num << std::endl;
    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pg_t(pg_num - 1, pool_id),
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // PG was pre-merge source
  //
  {
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    inc.new_pools[pool_id].set_pg_num_pending(pg_num - 1);
    osdmap->apply_incremental(inc);

    cout << "pg_num " << pg_num << std::endl;
    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  lastmap,  // reverse order!
						  osdmap,
						  pg_t(pg_num - 1, pool_id),
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // PG is merge source
  //
  {
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num - 1);
    osdmap->apply_incremental(inc);

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pg_t(pg_num - 1, pool_id),
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // PG is pre-merge target
  //
  {
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num_pending(pg_num - 1);
    osdmap->apply_incremental(inc);

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pg_t(pg_num / 2 - 1, pool_id),
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // PG was pre-merge target
  //
  {
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num_pending(pg_num - 1);
    osdmap->apply_incremental(inc);

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  lastmap,  // reverse order!
						  osdmap,
						  pg_t(pg_num / 2 - 1, pool_id),
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // PG is merge target
  //
  {
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num - 1);
    osdmap->apply_incremental(inc);

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pg_t(pg_num / 2 - 1, pool_id),
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // PG size has changed
  //
  {
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    __u8 new_min_size = min_size + 1;
    inc.new_pools[pool_id].min_size = new_min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    osdmap->apply_incremental(inc);

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals));
  }

  //
  // The old acting set was empty : the previous interval could not
  // have been rw
  //
  {
    vector<int> old_acting;

    PastIntervals past_intervals;

    ostringstream out;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals,
						  &out));
    ASSERT_NE(string::npos, out.str().find("acting set is too small"));
  }

  //
  // The old acting set did not have enough osd : it could
  // not have been rw
  //
  {
    vector<int> old_acting;
    old_acting.push_back(osd_id);

    //
    // see http://tracker.ceph.com/issues/5780
    // the size of the old acting set should be compared
    // with the min_size of the old osdmap
    //
    // The new osdmap is created so that it triggers the
    // bug.
    //
    std::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    __u8 new_min_size = old_acting.size();
    inc.new_pools[pool_id].min_size = new_min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    osdmap->apply_incremental(inc);

    ostringstream out;

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals,
						  &out));
    ASSERT_NE(string::npos, out.str().find("acting set is too small"));
  }

  //
  // The acting set changes. The old acting set primary was up during the
  // previous interval and may have been rw.
  //
  {
    vector<int> new_acting;
    new_acting.push_back(osd_id + 4);
    new_acting.push_back(osd_id + 5);

    ostringstream out;

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals,
						  &out));
    ASSERT_NE(string::npos, out.str().find("includes interval"));
  }
  //
  // The acting set changes. The old acting set primary was not up
  // during the old interval but last_epoch_clean is in the
  // old interval and it may have been rw.
  //
  {
    vector<int> new_acting;
    new_acting.push_back(osd_id + 4);
    new_acting.push_back(osd_id + 5);

    std::shared_ptr<OSDMap> lastmap(new OSDMap());
    lastmap->set_max_osd(10);
    lastmap->set_state(osd_id, CEPH_OSD_EXISTS);
    lastmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    inc.new_up_thru[osd_id] = epoch - 10;
    lastmap->apply_incremental(inc);

    ostringstream out;

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals,
						  &out));
    ASSERT_NE(string::npos, out.str().find("presumed to have been rw"));
  }

  //
  // The acting set changes. The old acting set primary was not up
  // during the old interval and last_epoch_clean is before the
  // old interval : the previous interval could not possibly have
  // been rw.
  //
  {
    vector<int> new_acting;
    new_acting.push_back(osd_id + 4);
    new_acting.push_back(osd_id + 5);

    epoch_t last_epoch_clean = epoch - 10;

    std::shared_ptr<OSDMap> lastmap(new OSDMap());
    lastmap->set_max_osd(10);
    lastmap->set_state(osd_id, CEPH_OSD_EXISTS);
    lastmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    inc.new_up_thru[osd_id] = last_epoch_clean;
    lastmap->apply_incremental(inc);

    ostringstream out;

    PastIntervals past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(PastIntervals::check_new_interval(old_primary,
						  new_primary,
						  old_acting,
						  new_acting,
						  old_up_primary,
						  new_up_primary,
						  old_up,
						  new_up,
						  same_interval_since,
						  last_epoch_clean,
						  osdmap,
						  lastmap,
						  pgid,
                                                  *recoverable,
						  &past_intervals,
						  &out));
    ASSERT_NE(string::npos, out.str().find("does not include interval"));
  }
} // end for, didn't want to reindent
}

TEST(pg_t, get_ancestor)
{
  ASSERT_EQ(pg_t(0, 0), pg_t(16, 0).get_ancestor(16));
  ASSERT_EQ(pg_t(1, 0), pg_t(17, 0).get_ancestor(16));
  ASSERT_EQ(pg_t(0, 0), pg_t(16, 0).get_ancestor(8));
  ASSERT_EQ(pg_t(16, 0), pg_t(16, 0).get_ancestor(80));
  ASSERT_EQ(pg_t(16, 0), pg_t(16, 0).get_ancestor(83));
  ASSERT_EQ(pg_t(1, 0), pg_t(1321, 0).get_ancestor(123).get_ancestor(8));
  ASSERT_EQ(pg_t(3, 0), pg_t(1323, 0).get_ancestor(123).get_ancestor(8));
  ASSERT_EQ(pg_t(3, 0), pg_t(1323, 0).get_ancestor(8));
}

TEST(pg_t, split)
{
  pg_t pgid(0, 0);
  set<pg_t> s;
  bool b;

  s.clear();
  b = pgid.is_split(1, 1, &s);
  ASSERT_TRUE(!b);

  s.clear();
  b = pgid.is_split(2, 4, NULL);
  ASSERT_TRUE(b);
  b = pgid.is_split(2, 4, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(2, 0)));

  s.clear();
  b = pgid.is_split(2, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(2, 0)));
  ASSERT_TRUE(s.count(pg_t(4, 0)));
  ASSERT_TRUE(s.count(pg_t(6, 0)));

  s.clear();
  b = pgid.is_split(3, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(4, 0)));

  s.clear();
  b = pgid.is_split(6, 8, NULL);
  ASSERT_TRUE(!b);
  b = pgid.is_split(6, 8, &s);
  ASSERT_TRUE(!b);
  ASSERT_EQ(0u, s.size());

  pgid = pg_t(1, 0);

  s.clear();
  b = pgid.is_split(2, 4, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0)));

  s.clear();
  b = pgid.is_split(2, 6, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(2u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0)));
  ASSERT_TRUE(s.count(pg_t(5, 0)));

  s.clear();
  b = pgid.is_split(2, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0)));
  ASSERT_TRUE(s.count(pg_t(5, 0)));
  ASSERT_TRUE(s.count(pg_t(7, 0)));

  s.clear();
  b = pgid.is_split(4, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(5, 0)));

  s.clear();
  b = pgid.is_split(3, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0)));
  ASSERT_TRUE(s.count(pg_t(5, 0)));
  ASSERT_TRUE(s.count(pg_t(7, 0)));

  s.clear();
  b = pgid.is_split(6, 8, &s);
  ASSERT_TRUE(!b);
  ASSERT_EQ(0u, s.size());

  pgid = pg_t(3, 0);

  s.clear();
  b = pgid.is_split(7, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0)));

  s.clear();
  b = pgid.is_split(7, 12, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(2u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0)));
  ASSERT_TRUE(s.count(pg_t(11, 0)));

  s.clear();
  b = pgid.is_split(7, 11, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0)));

}

TEST(pg_t, merge)
{
  pg_t pgid, parent;
  bool b;

  pgid = pg_t(7, 0);
  b = pgid.is_merge_source(8, 7, &parent);
  ASSERT_TRUE(b);
  ASSERT_EQ(parent, pg_t(3, 0));
  ASSERT_TRUE(parent.is_merge_target(8, 7));

  b = pgid.is_merge_source(8, 5, &parent);
  ASSERT_TRUE(b);
  ASSERT_EQ(parent, pg_t(3, 0));
  ASSERT_TRUE(parent.is_merge_target(8, 5));

  b = pgid.is_merge_source(8, 4, &parent);
  ASSERT_TRUE(b);
  ASSERT_EQ(parent, pg_t(3, 0));
  ASSERT_TRUE(parent.is_merge_target(8, 4));

  b = pgid.is_merge_source(8, 3, &parent);
  ASSERT_TRUE(b);
  ASSERT_EQ(parent, pg_t(1, 0));
  ASSERT_TRUE(parent.is_merge_target(8, 4));

  b = pgid.is_merge_source(9, 8, &parent);
  ASSERT_FALSE(b);
  ASSERT_FALSE(parent.is_merge_target(9, 8));
}

TEST(ObjectCleanRegions, mark_data_region_dirty)
{
  ObjectCleanRegions clean_regions;
  uint64_t offset_1, len_1, offset_2, len_2;
  offset_1 = 4096;
  len_1 = 8192;
  offset_2 = 40960;
  len_2 = 4096;

  interval_set<uint64_t> expect_dirty_region;
  EXPECT_EQ(expect_dirty_region, clean_regions.get_dirty_regions());
  expect_dirty_region.insert(offset_1, len_1);
  expect_dirty_region.insert(offset_2, len_2);

  clean_regions.mark_data_region_dirty(offset_1, len_1);
  clean_regions.mark_data_region_dirty(offset_2, len_2);
  EXPECT_EQ(expect_dirty_region, clean_regions.get_dirty_regions());
}

TEST(ObjectCleanRegions, mark_omap_dirty)
{
  ObjectCleanRegions clean_regions;

  EXPECT_FALSE(clean_regions.omap_is_dirty());
  clean_regions.mark_omap_dirty();
  EXPECT_TRUE(clean_regions.omap_is_dirty());
}

TEST(ObjectCleanRegions, merge)
{
  ObjectCleanRegions cr1, cr2;
  interval_set<uint64_t> cr1_expect;
  interval_set<uint64_t> cr2_expect;
  ASSERT_EQ(cr1_expect, cr1.get_dirty_regions());
  ASSERT_EQ(cr2_expect, cr2.get_dirty_regions());

  cr1.mark_data_region_dirty(4096, 4096);
  cr1_expect.insert(4096, 4096);
  ASSERT_EQ(cr1_expect, cr1.get_dirty_regions());
  cr1.mark_data_region_dirty(12288, 8192);
  cr1_expect.insert(12288, 8192);
  ASSERT_TRUE(cr1_expect.subset_of(cr1.get_dirty_regions()));
  cr1.mark_data_region_dirty(32768, 10240);
  cr1_expect.insert(32768, 10240);
  cr1_expect.erase(4096, 4096);
  ASSERT_TRUE(cr1_expect.subset_of(cr1.get_dirty_regions()));

  cr2.mark_data_region_dirty(20480, 12288);
  cr2_expect.insert(20480, 12288);
  ASSERT_EQ(cr2_expect, cr2.get_dirty_regions());
  cr2.mark_data_region_dirty(102400, 4096);
  cr2_expect.insert(102400, 4096);
  cr2.mark_data_region_dirty(204800, 8192);
  cr2_expect.insert(204800, 8192);
  cr2.mark_data_region_dirty(409600, 4096);
  cr2_expect.insert(409600, 4096);
  ASSERT_TRUE(cr2_expect.subset_of(cr2.get_dirty_regions()));

  ASSERT_FALSE(cr2.omap_is_dirty());
  cr2.mark_omap_dirty();
  ASSERT_FALSE(cr1.omap_is_dirty());
  ASSERT_TRUE(cr2.omap_is_dirty());

  cr1.merge(cr2);
  cr1_expect.insert(204800, 8192);
  ASSERT_TRUE(cr1_expect.subset_of(cr1.get_dirty_regions()));
  ASSERT_TRUE(cr1.omap_is_dirty());
}

TEST(pg_missing_t, constructor)
{
  pg_missing_t missing;
  EXPECT_EQ((unsigned int)0, missing.num_missing());
  EXPECT_FALSE(missing.have_missing());
}

TEST(pg_missing_t, have_missing)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  EXPECT_FALSE(missing.have_missing());
  missing.add(oid, eversion_t(), eversion_t(), false);
  EXPECT_TRUE(missing.have_missing());
}

TEST(pg_missing_t, claim)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  EXPECT_FALSE(missing.have_missing());
  missing.add(oid, eversion_t(), eversion_t(), false);
  EXPECT_TRUE(missing.have_missing());

  pg_missing_t other;
  EXPECT_FALSE(other.have_missing());

  other.claim(missing);
  EXPECT_TRUE(other.have_missing());
}

TEST(pg_missing_t, is_missing)
{
  // pg_missing_t::is_missing(const hobject_t& oid) const
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add(oid, eversion_t(), eversion_t(), false);
    EXPECT_TRUE(missing.is_missing(oid));
  }

  // bool pg_missing_t::is_missing(const hobject_t& oid, eversion_t v) const
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    eversion_t need(10,5);
    EXPECT_FALSE(missing.is_missing(oid, eversion_t()));
    missing.add(oid, need, eversion_t(), false);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_FALSE(missing.is_missing(oid, eversion_t()));
    EXPECT_TRUE(missing.is_missing(oid, need));
  }
}

TEST(pg_missing_t, add_next_event)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  hobject_t oid_other(object_t("other"), "key", 9123, 9456, 0, "");
  eversion_t version(10,5);
  eversion_t prior_version(3,4);
  pg_log_entry_t sample_e(pg_log_entry_t::DELETE, oid, version, prior_version,
			  0, osd_reqid_t(entity_name_t::CLIENT(777), 8, 999),
			  utime_t(8,9), 0);

  // new object (MODIFY)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    e.prior_version = eversion_t();
    EXPECT_TRUE(e.is_update());
    EXPECT_TRUE(e.object_is_indexed());
    EXPECT_TRUE(e.reqid_is_indexed());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(eversion_t(), missing.get_items().at(oid).have);
    EXPECT_EQ(oid, missing.get_rmissing().at(e.version.version));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());

    // adding the same object replaces the previous one
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());
  }

  // new object (CLONE)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::CLONE;
    e.prior_version = eversion_t();
    EXPECT_TRUE(e.is_clone());
    EXPECT_TRUE(e.object_is_indexed());
    EXPECT_FALSE(e.reqid_is_indexed());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(eversion_t(), missing.get_items().at(oid).have);
    EXPECT_EQ(oid, missing.get_rmissing().at(e.version.version));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());

    // adding the same object replaces the previous one
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());
  }

  // existing object (MODIFY)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    e.prior_version = eversion_t();
    EXPECT_TRUE(e.is_update());
    EXPECT_TRUE(e.object_is_indexed());
    EXPECT_TRUE(e.reqid_is_indexed());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(eversion_t(), missing.get_items().at(oid).have);
    EXPECT_EQ(oid, missing.get_rmissing().at(e.version.version));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());

    // adding the same object with a different version
    e.prior_version = prior_version;
    missing.add_next_event(e);
    EXPECT_EQ(eversion_t(), missing.get_items().at(oid).have);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());
  }

  // object with prior version (MODIFY)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    EXPECT_TRUE(e.is_update());
    EXPECT_TRUE(e.object_is_indexed());
    EXPECT_TRUE(e.reqid_is_indexed());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(prior_version, missing.get_items().at(oid).have);
    EXPECT_EQ(version, missing.get_items().at(oid).need);
    EXPECT_EQ(oid, missing.get_rmissing().at(e.version.version));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());
  }

  // adding a DELETE matching an existing event
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    EXPECT_TRUE(e.is_update());
    EXPECT_TRUE(e.object_is_indexed());
    EXPECT_TRUE(e.reqid_is_indexed());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));

    e.op = pg_log_entry_t::DELETE;
    EXPECT_TRUE(e.is_delete());
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_TRUE(missing.get_items().at(oid).is_delete());
    EXPECT_EQ(prior_version, missing.get_items().at(oid).have);
    EXPECT_EQ(version, missing.get_items().at(oid).need);
    EXPECT_EQ(oid, missing.get_rmissing().at(e.version.version));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());
  }

  // adding a LOST_DELETE after an existing event
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    EXPECT_TRUE(e.is_update());
    EXPECT_TRUE(e.object_is_indexed());
    EXPECT_TRUE(e.reqid_is_indexed());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_FALSE(missing.get_items().at(oid).is_delete());

    e.op = pg_log_entry_t::LOST_DELETE;
    e.version.version++;
    EXPECT_TRUE(e.is_delete());
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_TRUE(missing.get_items().at(oid).is_delete());
    EXPECT_EQ(prior_version, missing.get_items().at(oid).have);
    EXPECT_EQ(e.version, missing.get_items().at(oid).need);
    EXPECT_EQ(oid, missing.get_rmissing().at(e.version.version));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.get_rmissing().size());
  }
}

TEST(pg_missing_t, revise_need)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  // create a new entry
  EXPECT_FALSE(missing.is_missing(oid));
  eversion_t need(10,10);
  missing.revise_need(oid, need, false);
  EXPECT_TRUE(missing.is_missing(oid));
  EXPECT_EQ(eversion_t(), missing.get_items().at(oid).have);
  EXPECT_EQ(need, missing.get_items().at(oid).need);
  // update an existing entry and preserve have
  eversion_t have(1,1);
  missing.revise_have(oid, have);
  eversion_t new_need(10,12);
  EXPECT_EQ(have, missing.get_items().at(oid).have);
  missing.revise_need(oid, new_need, false);
  EXPECT_EQ(have, missing.get_items().at(oid).have);
  EXPECT_EQ(new_need, missing.get_items().at(oid).need);
}

TEST(pg_missing_t, revise_have)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  // a non existing entry means noop
  EXPECT_FALSE(missing.is_missing(oid));
  eversion_t have(1,1);
  missing.revise_have(oid, have);
  EXPECT_FALSE(missing.is_missing(oid));
  // update an existing entry
  eversion_t need(10,12);
  missing.add(oid, need, have, false);
  EXPECT_TRUE(missing.is_missing(oid));
  eversion_t new_have(2,2);
  EXPECT_EQ(have, missing.get_items().at(oid).have);
  missing.revise_have(oid, new_have);
  EXPECT_EQ(new_have, missing.get_items().at(oid).have);
  EXPECT_EQ(need, missing.get_items().at(oid).need);
}

TEST(pg_missing_t, add)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  EXPECT_FALSE(missing.is_missing(oid));
  eversion_t have(1,1);
  eversion_t need(10,10);
  missing.add(oid, need, have, false);
  EXPECT_TRUE(missing.is_missing(oid));
  EXPECT_EQ(have, missing.get_items().at(oid).have);
  EXPECT_EQ(need, missing.get_items().at(oid).need);
}

TEST(pg_missing_t, rm)
{
  // void pg_missing_t::rm(const hobject_t& oid, eversion_t v)
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    EXPECT_FALSE(missing.is_missing(oid));
    epoch_t epoch = 10;
    eversion_t need(epoch,10);
    missing.add(oid, need, eversion_t(), false);
    EXPECT_TRUE(missing.is_missing(oid));
    // rm of an older version is a noop
    missing.rm(oid, eversion_t(epoch / 2,20));
    EXPECT_TRUE(missing.is_missing(oid));
    // rm of a later version removes the object
    missing.rm(oid, eversion_t(epoch * 2,20));
    EXPECT_FALSE(missing.is_missing(oid));
  }
  // void pg_missing_t::rm(const std::map<hobject_t, pg_missing_item>::iterator &m)
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add(oid, eversion_t(), eversion_t(), false);
    EXPECT_TRUE(missing.is_missing(oid));
    auto m = missing.get_items().find(oid);
    missing.rm(m);
    EXPECT_FALSE(missing.is_missing(oid));
  }
}

TEST(pg_missing_t, got)
{
  // void pg_missing_t::got(const hobject_t& oid, eversion_t v)
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    // assert if the oid does not exist
    {
      PrCtl unset_dumpable;
      EXPECT_DEATH(missing.got(oid, eversion_t()), "");
    }
    EXPECT_FALSE(missing.is_missing(oid));
    epoch_t epoch = 10;
    eversion_t need(epoch,10);
    missing.add(oid, need, eversion_t(), false);
    EXPECT_TRUE(missing.is_missing(oid));
    // assert if that the version to be removed is lower than the version of the object
    {
      PrCtl unset_dumpable;
      EXPECT_DEATH(missing.got(oid, eversion_t(epoch / 2,20)), "");
    }
    // remove of a later version removes the object
    missing.got(oid, eversion_t(epoch * 2,20));
    EXPECT_FALSE(missing.is_missing(oid));
  }
  // void pg_missing_t::got(const std::map<hobject_t, pg_missing_item>::iterator &m)
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add(oid, eversion_t(), eversion_t(), false);
    EXPECT_TRUE(missing.is_missing(oid));
    auto m = missing.get_items().find(oid);
    missing.got(m);
    EXPECT_FALSE(missing.is_missing(oid));
  }
}

TEST(pg_missing_t, split_into)
{
  uint32_t hash1 = 1;
  hobject_t oid1(object_t("objname"), "key1", 123, hash1, 0, "");
  uint32_t hash2 = 2;
  hobject_t oid2(object_t("objname"), "key2", 123, hash2, 0, "");
  pg_missing_t missing;
  missing.add(oid1, eversion_t(), eversion_t(), false);
  missing.add(oid2, eversion_t(), eversion_t(), false);
  pg_t child_pgid;
  child_pgid.m_seed = 1;
  pg_missing_t child;
  unsigned split_bits = 1;
  missing.split_into(child_pgid, split_bits, &child);
  EXPECT_TRUE(child.is_missing(oid1));
  EXPECT_FALSE(child.is_missing(oid2));
  EXPECT_FALSE(missing.is_missing(oid1));
  EXPECT_TRUE(missing.is_missing(oid2));
}

TEST(pg_pool_t_test, get_pg_num_divisor) {
  pg_pool_t p;
  p.set_pg_num(16);
  p.set_pgp_num(16);

  for (int i = 0; i < 16; ++i)
    ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(i, 1)));

  p.set_pg_num(12);
  p.set_pgp_num(12);

  ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(0, 1)));
  ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(1, 1)));
  ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(2, 1)));
  ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(3, 1)));
  ASSERT_EQ(8u, p.get_pg_num_divisor(pg_t(4, 1)));
  ASSERT_EQ(8u, p.get_pg_num_divisor(pg_t(5, 1)));
  ASSERT_EQ(8u, p.get_pg_num_divisor(pg_t(6, 1)));
  ASSERT_EQ(8u, p.get_pg_num_divisor(pg_t(7, 1)));
  ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(8, 1)));
  ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(9, 1)));
  ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(10, 1)));
  ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(11, 1)));
}

TEST(pg_pool_t_test, get_random_pg_position) {
  srand(getpid());
  for (int i = 0; i < 100; ++i) {
    pg_pool_t p;
    p.set_pg_num(1 + (rand() % 1000));
    p.set_pgp_num(p.get_pg_num());
    pg_t pgid(rand() % p.get_pg_num(), 1);
    uint32_t h = p.get_random_pg_position(pgid, rand());
    uint32_t ps = p.raw_hash_to_pg(h);
    cout << p.get_pg_num() << " " << pgid << ": "
	 << h << " -> " << pg_t(ps, 1) << std::endl;
    ASSERT_EQ(pgid.ps(), ps);
  }
}

TEST(shard_id_t, iostream) {
    set<shard_id_t> shards;
    shards.insert(shard_id_t(0));
    shards.insert(shard_id_t(1));
    shards.insert(shard_id_t(2));
    ostringstream out;
    out << shards;
    ASSERT_EQ(out.str(), "0,1,2");

    shard_id_t noshard = shard_id_t::NO_SHARD;
    shard_id_t zero(0);
    ASSERT_GT(zero, noshard);
}

TEST(spg_t, parse) {
  spg_t a(pg_t(1,2), shard_id_t::NO_SHARD);
  spg_t aa, bb;
  spg_t b(pg_t(3,2), shard_id_t(2));
  std::string s = stringify(a);
  ASSERT_TRUE(aa.parse(s.c_str()));
  ASSERT_EQ(a, aa);

  s = stringify(b);
  ASSERT_TRUE(bb.parse(s.c_str()));
  ASSERT_EQ(b, bb);
}

TEST(coll_t, parse) {
  const char *ok[] = {
    "meta",
    "1.2_head",
    "1.2_TEMP",
    "1.2s3_head",
    "1.3s2_TEMP",
    "1.2s0_head",
    0
  };
  const char *bad[] = {
    "foo",
    "1.2_food",
    "1.2_head ",
    //" 1.2_head",   // hrm, this parses, which is not ideal.. pg_t's fault?
    "1.2_temp",
    "1.2_HEAD",
    "1.xS3_HEAD",
    "1.2s_HEAD",
    "1.2sfoo_HEAD",
    0
  };
  coll_t a;
  for (int i = 0; ok[i]; ++i) {
    cout << "check ok " << ok[i] << std::endl;
    ASSERT_TRUE(a.parse(ok[i]));
    ASSERT_EQ(string(ok[i]), a.to_str());
  }
  for (int i = 0; bad[i]; ++i) {
    cout << "check bad " << bad[i] << std::endl;
    ASSERT_FALSE(a.parse(bad[i]));
  }
}

TEST(coll_t, temp) {
  spg_t pgid;
  coll_t foo(pgid);
  ASSERT_EQ(foo.to_str(), string("0.0_head"));

  coll_t temp = foo.get_temp();
  ASSERT_EQ(temp.to_str(), string("0.0_TEMP"));

  spg_t pgid2;
  ASSERT_TRUE(temp.is_temp());
  ASSERT_TRUE(temp.is_temp(&pgid2));
  ASSERT_EQ(pgid, pgid2);
}

TEST(coll_t, assigment) {
  spg_t pgid;
  coll_t right(pgid);
  ASSERT_EQ(right.to_str(), string("0.0_head"));

  coll_t left, middle;

  ASSERT_EQ(left.to_str(), string("meta"));
  ASSERT_EQ(middle.to_str(), string("meta"));

  left = middle = right;

  ASSERT_EQ(left.to_str(), string("0.0_head"));
  ASSERT_EQ(middle.to_str(), string("0.0_head"));
  
  ASSERT_NE(middle.c_str(), right.c_str());
  ASSERT_NE(left.c_str(), middle.c_str());
}

TEST(hobject_t, parse) {
  const char *v[] = {
    "MIN",
    "MAX",
    "-1:60c2fa6d:::inc_osdmap.1:0",
    "-1:60c2fa6d:::inc_osdmap.1:333",
    "0:00000000::::head",
    "1:00000000:nspace:key:obj:head",
    "-40:00000000:nspace::obj:head",
    "20:00000000::key:obj:head",
    "20:00000000:::o%fdj:head",
    "20:00000000:::o%02fdj:head",
    "20:00000000:::_zero_%00_:head",
    NULL
  };

  for (unsigned i=0; v[i]; ++i) {
    hobject_t o;
    bool b = o.parse(v[i]);
    if (!b) {
      cout << "failed to parse " << v[i] << std::endl;
      ASSERT_TRUE(false);
    }
    string s = stringify(o);
    if (s != v[i]) {
      cout << v[i] << " -> " << o << " -> " << s << std::endl;
      ASSERT_EQ(s, string(v[i]));
    }
  }
}

TEST(ghobject_t, cmp) {
  ghobject_t min;
  ghobject_t sep;
  sep.set_shard(shard_id_t(1));
  sep.hobj.pool = -1;
  cout << min << " < " << sep << std::endl;
  ASSERT_TRUE(min < sep);

  sep.set_shard(shard_id_t::NO_SHARD);
  cout << "sep shard " << sep.shard_id << std::endl;
  ghobject_t o(hobject_t(object_t(), string(), CEPH_NOSNAP, 0x42,
			 1, string()));
  cout << "o " << o << std::endl;
  ASSERT_TRUE(o > sep);
}

TEST(ghobject_t, parse) {
  const char *v[] = {
    "GHMIN",
    "GHMAX",
    "13#0:00000000::::head#",
    "13#0:00000000::::head#deadbeef",
    "#-1:60c2fa6d:::inc_osdmap.1:333#deadbeef",
    "#-1:60c2fa6d:::inc%02osdmap.1:333#deadbeef",
    "#-1:60c2fa6d:::inc_osdmap.1:333#",
    "1#MIN#deadbeefff",
    "1#MAX#",
    "#MAX#123",
    "#-40:00000000:nspace::obj:head#",
    NULL
  };

  for (unsigned i=0; v[i]; ++i) {
    ghobject_t o;
    bool b = o.parse(v[i]);
    if (!b) {
      cout << "failed to parse " << v[i] << std::endl;
      ASSERT_TRUE(false);
    }
    string s = stringify(o);
    if (s != v[i]) {
      cout << v[i] << " -> " << o << " -> " << s << std::endl;
      ASSERT_EQ(s, string(v[i]));
    }
  }
}

TEST(pool_opts_t, invalid_opt) {
  EXPECT_FALSE(pool_opts_t::is_opt_name("INVALID_OPT"));
  PrCtl unset_dumpable;
  EXPECT_DEATH(pool_opts_t::get_opt_desc("INVALID_OPT"), "");
}

TEST(pool_opts_t, scrub_min_interval) {
  EXPECT_TRUE(pool_opts_t::is_opt_name("scrub_min_interval"));
  EXPECT_EQ(pool_opts_t::get_opt_desc("scrub_min_interval"),
            pool_opts_t::opt_desc_t(pool_opts_t::SCRUB_MIN_INTERVAL,
                                    pool_opts_t::DOUBLE));

  pool_opts_t opts;
  EXPECT_FALSE(opts.is_set(pool_opts_t::SCRUB_MIN_INTERVAL));
  {
    PrCtl unset_dumpable;
    EXPECT_DEATH(opts.get(pool_opts_t::SCRUB_MIN_INTERVAL), "");
  }
  double val;
  EXPECT_FALSE(opts.get(pool_opts_t::SCRUB_MIN_INTERVAL, &val));
  opts.set(pool_opts_t::SCRUB_MIN_INTERVAL, static_cast<double>(2015));
  EXPECT_TRUE(opts.get(pool_opts_t::SCRUB_MIN_INTERVAL, &val));
  EXPECT_EQ(val, 2015);
  opts.unset(pool_opts_t::SCRUB_MIN_INTERVAL);
  EXPECT_FALSE(opts.is_set(pool_opts_t::SCRUB_MIN_INTERVAL));
}

TEST(pool_opts_t, scrub_max_interval) {
  EXPECT_TRUE(pool_opts_t::is_opt_name("scrub_max_interval"));
  EXPECT_EQ(pool_opts_t::get_opt_desc("scrub_max_interval"),
            pool_opts_t::opt_desc_t(pool_opts_t::SCRUB_MAX_INTERVAL,
                                    pool_opts_t::DOUBLE));

  pool_opts_t opts;
  EXPECT_FALSE(opts.is_set(pool_opts_t::SCRUB_MAX_INTERVAL));
  {
    PrCtl unset_dumpable;
    EXPECT_DEATH(opts.get(pool_opts_t::SCRUB_MAX_INTERVAL), "");
  }
  double val;
  EXPECT_FALSE(opts.get(pool_opts_t::SCRUB_MAX_INTERVAL, &val));
  opts.set(pool_opts_t::SCRUB_MAX_INTERVAL, static_cast<double>(2015));
  EXPECT_TRUE(opts.get(pool_opts_t::SCRUB_MAX_INTERVAL, &val));
  EXPECT_EQ(val, 2015);
  opts.unset(pool_opts_t::SCRUB_MAX_INTERVAL);
  EXPECT_FALSE(opts.is_set(pool_opts_t::SCRUB_MAX_INTERVAL));
}

TEST(pool_opts_t, deep_scrub_interval) {
  EXPECT_TRUE(pool_opts_t::is_opt_name("deep_scrub_interval"));
  EXPECT_EQ(pool_opts_t::get_opt_desc("deep_scrub_interval"),
            pool_opts_t::opt_desc_t(pool_opts_t::DEEP_SCRUB_INTERVAL,
                                    pool_opts_t::DOUBLE));

  pool_opts_t opts;
  EXPECT_FALSE(opts.is_set(pool_opts_t::DEEP_SCRUB_INTERVAL));
  {
    PrCtl unset_dumpable;
    EXPECT_DEATH(opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL), "");
  }
  double val;
  EXPECT_FALSE(opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL, &val));
  opts.set(pool_opts_t::DEEP_SCRUB_INTERVAL, static_cast<double>(2015));
  EXPECT_TRUE(opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL, &val));
  EXPECT_EQ(val, 2015);
  opts.unset(pool_opts_t::DEEP_SCRUB_INTERVAL);
  EXPECT_FALSE(opts.is_set(pool_opts_t::DEEP_SCRUB_INTERVAL));
}

struct RequiredPredicate : IsPGRecoverablePredicate {
  unsigned required_size;
  explicit RequiredPredicate(unsigned required_size) : required_size(required_size) {}
  bool operator()(const set<pg_shard_t> &have) const override {
    return have.size() >= required_size;
  }
};

using namespace std;
struct MapPredicate {
  map<int, pair<PastIntervals::osd_state_t, epoch_t>> states;
  explicit MapPredicate(
    const vector<pair<int, pair<PastIntervals::osd_state_t, epoch_t>>> &_states)
   : states(_states.begin(), _states.end()) {}
  PastIntervals::osd_state_t operator()(epoch_t start, int osd, epoch_t *lost_at) {
    auto val = states.at(osd);
    if (lost_at)
      *lost_at = val.second;
    return val.first;
  }
};

using sit = shard_id_t;
using PI = PastIntervals;
using pst = pg_shard_t;
using ival = PastIntervals::pg_interval_t;
using ivallst = std::list<ival>;
const int N = 0x7fffffff /* CRUSH_ITEM_NONE, can't import crush.h here */;

struct PITest : ::testing::Test {
  PITest() {}
  void run(
    bool ec_pool,
    ivallst intervals,
    epoch_t last_epoch_started,
    unsigned min_to_peer,
    vector<pair<int, pair<PastIntervals::osd_state_t, epoch_t>>> osd_states,
    vector<int> up,
    vector<int> acting,
    set<pg_shard_t> probe,
    set<int> down,
    map<int, epoch_t> blocked_by,
    bool pg_down) {
    RequiredPredicate rec_pred(min_to_peer);
    MapPredicate map_pred(osd_states);

    PI::PriorSet correct(
      ec_pool,
      probe,
      down,
      blocked_by,
      pg_down,
      new RequiredPredicate(rec_pred));

    PastIntervals compact;
    for (auto &&i: intervals) {
      compact.add_interval(ec_pool, i);
    }
    PI::PriorSet compact_ps = compact.get_prior_set(
      ec_pool,
      last_epoch_started,
      new RequiredPredicate(rec_pred),
      map_pred,
      up,
      acting,
      nullptr);
    ASSERT_EQ(correct, compact_ps);
  }
};

TEST_F(PITest, past_intervals_rep) {
  run(
    /* ec_pool    */ false,
    /* intervals  */
    { ival{{0, 1, 2}, {0, 1, 2}, 10, 20,  true, 0, 0}
    , ival{{   1, 2}, {   1, 2}, 21, 30,  true, 1, 1}
    , ival{{      2}, {      2}, 31, 35, false, 2, 2}
    , ival{{0,    2}, {0,    2}, 36, 50,  true, 0, 0}
    },
    /* les        */ 5,
    /* min_peer   */ 1,
    /* osd states at end */
    { make_pair(0, make_pair(PI::UP   , 0))
    , make_pair(1, make_pair(PI::UP   , 0))
    , make_pair(2, make_pair(PI::DOWN , 0))
    },
    /* acting     */ {0, 1   },
    /* up         */ {0, 1   },
    /* probe      */ {pst(0), pst(1)},
    /* down       */ {2},
    /* blocked_by */ {},
    /* pg_down    */ false);
}

TEST_F(PITest, past_intervals_ec) {
  run(
    /* ec_pool    */ true,
    /* intervals  */
    { ival{{0, 1, 2}, {0, 1, 2}, 10, 20,  true, 0, 0}
    , ival{{N, 1, 2}, {N, 1, 2}, 21, 30,  true, 1, 1}
    },
    /* les        */ 5,
    /* min_peer   */ 2,
    /* osd states at end */
    { make_pair(0, make_pair(PI::DOWN , 0))
    , make_pair(1, make_pair(PI::UP   , 0))
    , make_pair(2, make_pair(PI::UP   , 0))
    },
    /* acting     */ {N, 1, 2},
    /* up         */ {N, 1, 2},
    /* probe      */ {pst(1, sit(1)), pst(2, sit(2))},
    /* down       */ {0},
    /* blocked_by */ {},
    /* pg_down    */ false);
}

TEST_F(PITest, past_intervals_rep_down) {
  run(
    /* ec_pool    */ false,
    /* intervals  */
    { ival{{0, 1, 2}, {0, 1, 2}, 10, 20,  true, 0, 0}
    , ival{{   1, 2}, {   1, 2}, 21, 30,  true, 1, 1}
    , ival{{      2}, {      2}, 31, 35,  true, 2, 2}
    , ival{{0,    2}, {0,    2}, 36, 50,  true, 0, 0}
    },
    /* les        */ 5,
    /* min_peer   */ 1,
    /* osd states at end */
    { make_pair(0, make_pair(PI::UP   , 0))
    , make_pair(1, make_pair(PI::UP   , 0))
    , make_pair(2, make_pair(PI::DOWN , 0))
    },
    /* acting     */ {0, 1   },
    /* up         */ {0, 1   },
    /* probe      */ {pst(0), pst(1)},
    /* down       */ {2},
    /* blocked_by */ {{2, 0}},
    /* pg_down    */ true);
}

TEST_F(PITest, past_intervals_ec_down) {
  run(
    /* ec_pool    */ true,
    /* intervals  */
    { ival{{0, 1, 2}, {0, 1, 2}, 10, 20,  true, 0, 0}
    , ival{{N, 1, 2}, {N, 1, 2}, 21, 30,  true, 1, 1}
    , ival{{N, N, 2}, {N, N, 2}, 31, 35, false, 2, 2}
    },
    /* les        */ 5,
    /* min_peer   */ 2,
    /* osd states at end */
    { make_pair(0, make_pair(PI::UP   , 0))
    , make_pair(1, make_pair(PI::DOWN , 0))
    , make_pair(2, make_pair(PI::UP   , 0))
    },
    /* acting     */ {0, N, 2},
    /* up         */ {0, N, 2},
    /* probe      */ {pst(0, sit(0)), pst(2, sit(2))},
    /* down       */ {1},
    /* blocked_by */ {{1, 0}},
    /* pg_down    */ true);
}

TEST_F(PITest, past_intervals_rep_no_subsets) {
  run(
    /* ec_pool    */ false,
    /* intervals  */
    { ival{{0,    2}, {0,    2}, 10, 20,  true, 0, 0}
    , ival{{   1, 2}, {   1, 2}, 21, 30,  true, 1, 1}
    , ival{{0, 1   }, {0, 1   }, 31, 35,  true, 0, 0}
    },
    /* les        */ 5,
    /* min_peer   */ 1,
    /* osd states at end */
    { make_pair(0, make_pair(PI::UP   , 0))
    , make_pair(1, make_pair(PI::UP   , 0))
    , make_pair(2, make_pair(PI::DOWN , 0))
    },
    /* acting     */ {0, 1   },
    /* up         */ {0, 1   },
    /* probe      */ {pst(0), pst(1)},
    /* down       */ {2},
    /* blocked_by */ {},
    /* pg_down    */ false);
}

TEST_F(PITest, past_intervals_ec_no_subsets) {
  run(
    /* ec_pool    */ true,
    /* intervals  */
    { ival{{0, N, 2}, {0, N, 2}, 10, 20,  true, 0, 0}
    , ival{{N, 1, 2}, {N, 1, 2}, 21, 30,  true, 1, 1}
    , ival{{0, 1, N}, {0, 1, N}, 31, 35,  true, 0, 0}
    },
    /* les        */ 5,
    /* min_peer   */ 2,
    /* osd states at end */
    { make_pair(0, make_pair(PI::UP   , 0))
    , make_pair(1, make_pair(PI::DOWN , 0))
    , make_pair(2, make_pair(PI::UP   , 0))
    },
    /* acting     */ {0, N, 2},
    /* up         */ {0, N, 2},
    /* probe      */ {pst(0, sit(0)), pst(2, sit(2))},
    /* down       */ {1},
    /* blocked_by */ {{1, 0}},
    /* pg_down    */ true);
}

TEST_F(PITest, past_intervals_ec_no_subsets2) {
  run(
    /* ec_pool    */ true,
    /* intervals  */
    { ival{{N, 1, 2}, {N, 1, 2}, 10, 20,  true, 0, 0}
    , ival{{0, N, 2}, {0, N, 2}, 21, 30,  true, 1, 1}
    , ival{{0, 3, N}, {0, 3, N}, 31, 35,  true, 0, 0}
    },
    /* les        */ 31,
    /* min_peer   */ 2,
    /* osd states at end */
    { make_pair(0, make_pair(PI::UP   , 0))
    , make_pair(1, make_pair(PI::DOWN , 0))
    , make_pair(2, make_pair(PI::UP   , 0))
    , make_pair(3, make_pair(PI::UP   , 0))
    },
    /* acting     */ {0, N, 2},
    /* up         */ {0, N, 2},
    /* probe      */ {pst(0, sit(0)), pst(2, sit(2)), pst(3, sit(1))},
    /* down       */ {1},
    /* blocked_by */ {},
    /* pg_down    */ false);
}

TEST_F(PITest, past_intervals_rep_lost) {
  run(
    /* ec_pool    */ false,
    /* intervals  */
    { ival{{0, 1, 2}, {0, 1, 2}, 10, 20,  true, 0, 0}
    , ival{{   1, 2}, {   1, 2}, 21, 30,  true, 1, 1}
    , ival{{      2}, {      2}, 31, 35,  true, 2, 2}
    , ival{{0,    2}, {0,    2}, 36, 50,  true, 0, 0}
    },
    /* les        */ 5,
    /* min_peer   */ 1,
    /* osd states at end */
    { make_pair(0, make_pair(PI::UP   , 0))
    , make_pair(1, make_pair(PI::UP   , 0))
    , make_pair(2, make_pair(PI::LOST , 55))
    },
    /* acting     */ {0, 1   },
    /* up         */ {0, 1   },
    /* probe      */ {pst(0), pst(1)},
    /* down       */ {2},
    /* blocked_by */ {},
    /* pg_down    */ false);
}

TEST_F(PITest, past_intervals_ec_lost) {
  run(
    /* ec_pool    */ true,
    /* intervals  */
    { ival{{0, N, 2}, {0, N, 2}, 10, 20,  true, 0, 0}
    , ival{{N, 1, 2}, {N, 1, 2}, 21, 30,  true, 1, 1}
    , ival{{0, 1, N}, {0, 1, N}, 31, 35,  true, 0, 0}
    },
    /* les        */ 5,
    /* min_peer   */ 2,
    /* osd states at end */
    { make_pair(0, make_pair(PI::UP   , 0))
    , make_pair(1, make_pair(PI::LOST , 36))
    , make_pair(2, make_pair(PI::UP   , 0))
    },
    /* acting     */ {0, N, 2},
    /* up         */ {0, N, 2},
    /* probe      */ {pst(0, sit(0)), pst(2, sit(2))},
    /* down       */ {1},
    /* blocked_by */ {},
    /* pg_down    */ false);
}

void ci_ref_test(
  object_manifest_t l,
  object_manifest_t to_remove,
  object_manifest_t g,
  object_ref_delta_t expected_delta)
{
  {
    object_ref_delta_t delta;
    to_remove.calc_refs_to_drop_on_removal(
      &l,
      &g,
      delta);
    ASSERT_EQ(
      expected_delta,
      delta);
  }

  // calc_refs_to_drop specifically handles nullptr identically to empty
  // chunk_map
  if (l.chunk_map.empty() || g.chunk_map.empty()) {
    object_ref_delta_t delta;
    to_remove.calc_refs_to_drop_on_removal(
      l.chunk_map.empty() ? nullptr : &l,
      g.chunk_map.empty() ? nullptr : &g,
      delta);
    ASSERT_EQ(
      expected_delta,
      delta);
  }
}

hobject_t mk_hobject(string name)
{
  return hobject_t(
    std::move(name),
    string(),
    CEPH_NOSNAP,
    0x42,
    1,
    string());
}

object_manifest_t mk_manifest(
  std::map<uint64_t, std::tuple<uint64_t, uint64_t, string>> m)
{
  object_manifest_t ret;
  ret.type = object_manifest_t::TYPE_CHUNKED;
  for (auto &[offset, tgt] : m) {
    auto &[tgt_off, length, name] = tgt;
    auto &ci = ret.chunk_map[offset];
    ci.offset = tgt_off;
    ci.length = length;
    ci.oid = mk_hobject(name);
  }
  return ret;
}

object_ref_delta_t mk_delta(std::map<string, int> _m) {
  std::map<hobject_t, int> m;
  for (auto &[name, delta] : _m) {
    m.insert(
      std::make_pair(
	mk_hobject(name),
	delta));
  }
  return object_ref_delta_t(std::move(m));
}

TEST(chunk_info_test, calc_refs_to_drop) {
  ci_ref_test(
    mk_manifest({}),
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_manifest({}),
    mk_delta({{"foo", -1}}));

}


TEST(chunk_info_test, calc_refs_to_drop_match) {
  ci_ref_test(
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_delta({}));

}

TEST(chunk_info_test, calc_refs_to_drop_head_match) {
  ci_ref_test(
    mk_manifest({}),
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_delta({}));

}

TEST(chunk_info_test, calc_refs_to_drop_tail_match) {
  ci_ref_test(
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_manifest({}),
    mk_delta({}));

}

TEST(chunk_info_test, calc_refs_to_drop_second_reference) {
  ci_ref_test(
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_manifest({{0, {0, 1024, "foo"}}, {4<<10, {0, 1<<10, "foo"}}}),
    mk_manifest({}),
    mk_delta({{"foo", -1}}));

}

TEST(chunk_info_test, calc_refs_offsets_dont_match) {
  ci_ref_test(
    mk_manifest({{0, {0, 1024, "foo"}}}),
    mk_manifest({{512, {0, 1024, "foo"}}, {(4<<10) + 512, {0, 1<<10, "foo"}}}),
    mk_manifest({}),
    mk_delta({{"foo", -2}}));

}

TEST(chunk_info_test, calc_refs_g_l_match) {
  ci_ref_test(
    mk_manifest({{4096, {0, 1024, "foo"}}}),
    mk_manifest({{0, {0, 1024, "foo"}}, {4096, {0, 1024, "bar"}}}),
    mk_manifest({{4096, {0, 1024, "foo"}}}),
    mk_delta({{"foo", -2}, {"bar", -1}}));

}

TEST(chunk_info_test, calc_refs_g_l_match_no_this) {
  ci_ref_test(
    mk_manifest({{4096, {0, 1024, "foo"}}}),
    mk_manifest({{0, {0, 1024, "bar"}}}),
    mk_manifest({{4096, {0, 1024, "foo"}}}),
    mk_delta({{"foo", -1}, {"bar", -1}}));

}

/*
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make unittest_osd_types ;
 *   ./unittest_osd_types # --gtest_filter=pg_missing_t.constructor
 * "
 * End:
 */
