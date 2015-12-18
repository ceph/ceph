// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
  //
  // Create a situation where osdmaps are the same so that
  // each test case can diverge from it using minimal code.
  //
  int osd_id = 1;
  epoch_t epoch = 40;
  ceph::shared_ptr<OSDMap> osdmap(new OSDMap());
  osdmap->set_max_osd(10);
  osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
  osdmap->set_epoch(epoch);
  ceph::shared_ptr<OSDMap> lastmap(new OSDMap());
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
    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_FALSE(pg_interval_t::check_new_interval(old_primary,
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
                                                   recoverable.get(),
						   &past_intervals));
    ASSERT_TRUE(past_intervals.empty());
  }

  //
  // pool did not exist in the old osdmap
  //
  {
    ceph::shared_ptr<OSDMap> lastmap(new OSDMap());
    lastmap->set_max_osd(10);
    lastmap->set_state(osd_id, CEPH_OSD_EXISTS);
    lastmap->set_epoch(epoch);

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_EQ(same_interval_since, past_intervals[same_interval_since].first);
    ASSERT_EQ(osdmap->get_epoch() - 1, past_intervals[same_interval_since].last);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].acting[0]);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].up[0]);
  }

  //
  // The acting set has changed
  //
  {
    vector<int> new_acting;
    int _new_primary = osd_id + 1;
    new_acting.push_back(_new_primary);

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals));
    old_primary = new_primary;
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_EQ(same_interval_since, past_intervals[same_interval_since].first);
    ASSERT_EQ(osdmap->get_epoch() - 1, past_intervals[same_interval_since].last);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].acting[0]);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].up[0]);
  }

  //
  // The up set has changed
  //
  {
    vector<int> new_up;
    int _new_primary = osd_id + 1;
    new_up.push_back(_new_primary);

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_EQ(same_interval_since, past_intervals[same_interval_since].first);
    ASSERT_EQ(osdmap->get_epoch() - 1, past_intervals[same_interval_since].last);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].acting[0]);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].up[0]);
  }

  //
  // The up primary has changed
  //
  {
    vector<int> new_up;
    int _new_up_primary = osd_id + 1;

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_EQ(same_interval_since, past_intervals[same_interval_since].first);
    ASSERT_EQ(osdmap->get_epoch() - 1, past_intervals[same_interval_since].last);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].acting[0]);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].up[0]);
  }

  //
  // PG is splitting
  //
  {
    ceph::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    int new_pg_num = pg_num ^ 2;
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(new_pg_num);
    osdmap->apply_incremental(inc);

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_EQ(same_interval_since, past_intervals[same_interval_since].first);
    ASSERT_EQ(osdmap->get_epoch() - 1, past_intervals[same_interval_since].last);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].acting[0]);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].up[0]);
  }

  //
  // PG size has changed
  //
  {
    ceph::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    __u8 new_min_size = min_size + 1;
    inc.new_pools[pool_id].min_size = new_min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    osdmap->apply_incremental(inc);

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_EQ(same_interval_since, past_intervals[same_interval_since].first);
    ASSERT_EQ(osdmap->get_epoch() - 1, past_intervals[same_interval_since].last);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].acting[0]);
    ASSERT_EQ(osd_id, past_intervals[same_interval_since].up[0]);
  }

  //
  // The old acting set was empty : the previous interval could not
  // have been rw
  //
  {
    vector<int> old_acting;

    map<epoch_t, pg_interval_t> past_intervals;

    ostringstream out;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals,
						  &out));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_FALSE(past_intervals[same_interval_since].maybe_went_rw);
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
    ceph::shared_ptr<OSDMap> osdmap(new OSDMap());
    osdmap->set_max_osd(10);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);
    osdmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    __u8 new_min_size = old_acting.size();
    inc.new_pools[pool_id].min_size = new_min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    osdmap->apply_incremental(inc);

    ostringstream out;

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals,
						  &out));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_FALSE(past_intervals[same_interval_since].maybe_went_rw);
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

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals,
						  &out));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_TRUE(past_intervals[same_interval_since].maybe_went_rw);
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

    ceph::shared_ptr<OSDMap> lastmap(new OSDMap());
    lastmap->set_max_osd(10);
    lastmap->set_state(osd_id, CEPH_OSD_EXISTS);
    lastmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    inc.new_up_thru[osd_id] = epoch - 10;
    lastmap->apply_incremental(inc);

    ostringstream out;

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals,
						  &out));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_TRUE(past_intervals[same_interval_since].maybe_went_rw);
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

    ceph::shared_ptr<OSDMap> lastmap(new OSDMap());
    lastmap->set_max_osd(10);
    lastmap->set_state(osd_id, CEPH_OSD_EXISTS);
    lastmap->set_epoch(epoch);
    OSDMap::Incremental inc(epoch + 1);
    inc.new_pools[pool_id].min_size = min_size;
    inc.new_pools[pool_id].set_pg_num(pg_num);
    inc.new_up_thru[osd_id] = last_epoch_clean;
    lastmap->apply_incremental(inc);

    ostringstream out;

    map<epoch_t, pg_interval_t> past_intervals;

    ASSERT_TRUE(past_intervals.empty());
    ASSERT_TRUE(pg_interval_t::check_new_interval(old_primary,
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
                                                  recoverable.get(),
						  &past_intervals,
						  &out));
    ASSERT_EQ((unsigned int)1, past_intervals.size());
    ASSERT_FALSE(past_intervals[same_interval_since].maybe_went_rw);
    ASSERT_NE(string::npos, out.str().find("does not include interval"));
  }
}

TEST(pg_t, get_ancestor)
{
  ASSERT_EQ(pg_t(0, 0, -1), pg_t(16, 0, -1).get_ancestor(16));
  ASSERT_EQ(pg_t(1, 0, -1), pg_t(17, 0, -1).get_ancestor(16));
  ASSERT_EQ(pg_t(0, 0, -1), pg_t(16, 0, -1).get_ancestor(8));
  ASSERT_EQ(pg_t(16, 0, -1), pg_t(16, 0, -1).get_ancestor(80));
  ASSERT_EQ(pg_t(16, 0, -1), pg_t(16, 0, -1).get_ancestor(83));
  ASSERT_EQ(pg_t(1, 0, -1), pg_t(1321, 0, -1).get_ancestor(123).get_ancestor(8));
  ASSERT_EQ(pg_t(3, 0, -1), pg_t(1323, 0, -1).get_ancestor(123).get_ancestor(8));
  ASSERT_EQ(pg_t(3, 0, -1), pg_t(1323, 0, -1).get_ancestor(8));
}

TEST(pg_t, split)
{
  pg_t pgid(0, 0, -1);
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
  ASSERT_TRUE(s.count(pg_t(2, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(2, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(4, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(6, 0, -1)));

  s.clear();
  b = pgid.is_split(3, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(4, 0, -1)));

  s.clear();
  b = pgid.is_split(6, 8, NULL);
  ASSERT_TRUE(!b);
  b = pgid.is_split(6, 8, &s);
  ASSERT_TRUE(!b);
  ASSERT_EQ(0u, s.size());

  pgid = pg_t(1, 0, -1);

  s.clear();
  b = pgid.is_split(2, 4, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 6, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(2u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(4, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));

  s.clear();
  b = pgid.is_split(3, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(6, 8, &s);
  ASSERT_TRUE(!b);
  ASSERT_EQ(0u, s.size());

  pgid = pg_t(3, 0, -1);

  s.clear();
  b = pgid.is_split(7, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(7, 12, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(2u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(11, 0, -1)));

  s.clear();
  b = pgid.is_split(7, 11, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

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
  missing.add(oid, eversion_t(), eversion_t());
  EXPECT_TRUE(missing.have_missing());
}

TEST(pg_missing_t, swap)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  EXPECT_FALSE(missing.have_missing());
  missing.add(oid, eversion_t(), eversion_t());
  EXPECT_TRUE(missing.have_missing());

  pg_missing_t other;
  EXPECT_FALSE(other.have_missing());

  other.swap(missing);
  EXPECT_FALSE(missing.have_missing());
  EXPECT_TRUE(other.have_missing());
}

TEST(pg_missing_t, is_missing)
{
  // pg_missing_t::is_missing(const hobject_t& oid) const
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add(oid, eversion_t(), eversion_t());
    EXPECT_TRUE(missing.is_missing(oid));
  }

  // bool pg_missing_t::is_missing(const hobject_t& oid, eversion_t v) const
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    eversion_t need(10,5);
    EXPECT_FALSE(missing.is_missing(oid, eversion_t()));
    missing.add(oid, need, eversion_t());
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_FALSE(missing.is_missing(oid, eversion_t()));
    EXPECT_TRUE(missing.is_missing(oid, need));
  }
}

TEST(pg_missing_t, have_old)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  EXPECT_EQ(eversion_t(), missing.have_old(oid));
  missing.add(oid, eversion_t(), eversion_t());
  EXPECT_EQ(eversion_t(), missing.have_old(oid));
  eversion_t have(1,1);
  missing.revise_have(oid, have);
  EXPECT_EQ(have, missing.have_old(oid));
}

TEST(pg_missing_t, add_next_event)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  hobject_t oid_other(object_t("other"), "key", 9123, 9456, 0, "");
  eversion_t version(10,5);
  eversion_t prior_version(3,4);
  pg_log_entry_t sample_e(pg_log_entry_t::DELETE, oid, version, prior_version,
			  0, osd_reqid_t(entity_name_t::CLIENT(777), 8, 999),
			  utime_t(8,9));

  // new object (MODIFY)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    e.prior_version = eversion_t();
    EXPECT_TRUE(e.is_update());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(eversion_t(), missing.missing[oid].have);
    EXPECT_EQ(oid, missing.rmissing[e.version.version]);
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.rmissing.size());

    // adding the same object replaces the previous one
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.rmissing.size());
  }

  // new object (CLONE)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::CLONE;
    e.prior_version = eversion_t();
    EXPECT_TRUE(e.is_clone());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(eversion_t(), missing.missing[oid].have);
    EXPECT_EQ(oid, missing.rmissing[e.version.version]);
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.rmissing.size());

    // adding the same object replaces the previous one
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.rmissing.size());
  }

  // existing object (MODIFY)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    e.prior_version = eversion_t();
    EXPECT_TRUE(e.is_update());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(eversion_t(), missing.missing[oid].have);
    EXPECT_EQ(oid, missing.rmissing[e.version.version]);
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.rmissing.size());

    // adding the same object with a different version
    e.prior_version = prior_version;
    missing.add_next_event(e);
    EXPECT_EQ(eversion_t(), missing.missing[oid].have);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.rmissing.size());
  }

  // object with prior version (MODIFY)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    EXPECT_TRUE(e.is_update());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));
    EXPECT_EQ(prior_version, missing.missing[oid].have);
    EXPECT_EQ(version, missing.missing[oid].need);
    EXPECT_EQ(oid, missing.rmissing[e.version.version]);
    EXPECT_EQ(1U, missing.num_missing());
    EXPECT_EQ(1U, missing.rmissing.size());
  }

  // obsolete (BACKLOG)
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::BACKLOG;
    EXPECT_TRUE(e.is_backlog());
    EXPECT_FALSE(missing.is_missing(oid));
    EXPECT_DEATH(missing.add_next_event(e), "");
  }

  // adding a DELETE matching an existing event
  {
    pg_missing_t missing;
    pg_log_entry_t e = sample_e;

    e.op = pg_log_entry_t::MODIFY;
    EXPECT_TRUE(e.is_update());
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add_next_event(e);
    EXPECT_TRUE(missing.is_missing(oid));

    e.op = pg_log_entry_t::DELETE;
    EXPECT_TRUE(e.is_delete());
    missing.add_next_event(e);
    EXPECT_FALSE(missing.have_missing());
  }
}

TEST(pg_missing_t, revise_need)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  // create a new entry
  EXPECT_FALSE(missing.is_missing(oid));
  eversion_t need(10,10);
  missing.revise_need(oid, need);
  EXPECT_TRUE(missing.is_missing(oid));
  EXPECT_EQ(eversion_t(), missing.missing[oid].have);
  EXPECT_EQ(need, missing.missing[oid].need);
  // update an existing entry and preserve have
  eversion_t have(1,1);
  missing.revise_have(oid, have);
  eversion_t new_need(10,12);
  EXPECT_EQ(have, missing.missing[oid].have);
  missing.revise_need(oid, new_need);
  EXPECT_EQ(have, missing.missing[oid].have);
  EXPECT_EQ(new_need, missing.missing[oid].need);
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
  missing.add(oid, need, have);
  EXPECT_TRUE(missing.is_missing(oid));
  eversion_t new_have(2,2);
  EXPECT_EQ(have, missing.missing[oid].have);
  missing.revise_have(oid, new_have);
  EXPECT_EQ(new_have, missing.missing[oid].have);
  EXPECT_EQ(need, missing.missing[oid].need);
}

TEST(pg_missing_t, add)
{
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  pg_missing_t missing;
  EXPECT_FALSE(missing.is_missing(oid));
  eversion_t have(1,1);
  eversion_t need(10,10);
  missing.add(oid, need, have);
  EXPECT_TRUE(missing.is_missing(oid));
  EXPECT_EQ(have, missing.missing[oid].have);
  EXPECT_EQ(need, missing.missing[oid].need);
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
    missing.add(oid, need, eversion_t());
    EXPECT_TRUE(missing.is_missing(oid));
    // rm of an older version is a noop
    missing.rm(oid, eversion_t(epoch / 2,20));
    EXPECT_TRUE(missing.is_missing(oid));
    // rm of a later version removes the object
    missing.rm(oid, eversion_t(epoch * 2,20));
    EXPECT_FALSE(missing.is_missing(oid));
  }
  // void pg_missing_t::rm(const std::map<hobject_t, pg_missing_t::item>::iterator &m)
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add(oid, eversion_t(), eversion_t());
    EXPECT_TRUE(missing.is_missing(oid));
    const std::map<hobject_t, pg_missing_t::item>::iterator m = missing.missing.find(oid);
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
    EXPECT_DEATH(missing.got(oid, eversion_t()), "");
    EXPECT_FALSE(missing.is_missing(oid));
    epoch_t epoch = 10;
    eversion_t need(epoch,10);
    missing.add(oid, need, eversion_t());
    EXPECT_TRUE(missing.is_missing(oid));
    // assert if that the version to be removed is lower than the version of the object
    EXPECT_DEATH(missing.got(oid, eversion_t(epoch / 2,20)), "");
    // remove of a later version removes the object
    missing.got(oid, eversion_t(epoch * 2,20));
    EXPECT_FALSE(missing.is_missing(oid));
  }
  // void pg_missing_t::got(const std::map<hobject_t, pg_missing_t::item>::iterator &m)
  {
    hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
    pg_missing_t missing;
    EXPECT_FALSE(missing.is_missing(oid));
    missing.add(oid, eversion_t(), eversion_t());
    EXPECT_TRUE(missing.is_missing(oid));
    const std::map<hobject_t, pg_missing_t::item>::iterator m = missing.missing.find(oid);
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
  missing.add(oid1, eversion_t(), eversion_t());
  missing.add(oid2, eversion_t(), eversion_t());
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

class ObjectContextTest : public ::testing::Test {
protected:

  static const useconds_t DELAY_MAX = 20 * 1000 * 1000;

  class Thread_read_lock : public Thread {
  public:
    ObjectContext &obc;

    Thread_read_lock(ObjectContext& _obc) :
      obc(_obc)
    {
    }

    virtual void *entry() {
      obc.ondisk_read_lock();
      return NULL;
    }
  };

  class Thread_write_lock : public Thread {
  public:
    ObjectContext &obc;

    Thread_write_lock(ObjectContext& _obc) :
      obc(_obc)
    {
    }

    virtual void *entry() {
      obc.ondisk_write_lock();
      return NULL;
    }
  };

};

TEST_F(ObjectContextTest, read_write_lock)
{
  {
    ObjectContext obc;

    //
    // write_lock
    // write_lock
    // write_unlock
    // write_unlock
    //
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_write_lock();

    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    obc.ondisk_write_lock();

    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(2, obc.unstable_writes);

    obc.ondisk_write_unlock();

    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    obc.ondisk_write_unlock();

    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);
  }

  useconds_t delay = 0;

  {
    ObjectContext obc;

    //
    // write_lock
    // read_lock => wait
    // write_unlock => signal
    // read_unlock
    //
    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_write_lock();

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    Thread_read_lock t(obc);
    t.create();

    do {
      cout << "Trying (1) with delay " << delay << "us\n";
      usleep(delay);
    } while (obc.readers_waiting == 0 &&
	     ( delay = delay * 2 + 1) < DELAY_MAX);

    EXPECT_EQ(1, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    obc.ondisk_write_unlock();

    do {
      cout << "Trying (2) with delay " << delay << "us\n";
      usleep(delay);
    } while ((obc.readers == 0 || obc.readers_waiting == 1) &&
	     ( delay = delay * 2 + 1) < DELAY_MAX);
    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(1, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_read_unlock();

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    t.join();
  }

  {
    ObjectContext obc;

    //
    // read_lock
    // write_lock => wait
    // read_unlock => signal
    // write_unlock
    //
    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_read_lock();

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(1, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    Thread_write_lock t(obc);
    t.create();

    do {
      cout << "Trying (3) with delay " << delay << "us\n";
      usleep(delay);
    } while ((obc.writers_waiting == 0) &&
	     ( delay = delay * 2 + 1) < DELAY_MAX);

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(1, obc.readers);
    EXPECT_EQ(1, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_read_unlock();

    do {
      cout << "Trying (4) with delay " << delay << "us\n";
      usleep(delay);
    } while ((obc.unstable_writes == 0 || obc.writers_waiting == 1) &&
	     ( delay = delay * 2 + 1) < DELAY_MAX);

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    obc.ondisk_write_unlock();

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    t.join();
  }

}

TEST(pg_pool_t_test, get_pg_num_divisor) {
  pg_pool_t p;
  p.set_pg_num(16);
  p.set_pgp_num(16);

  for (int i = 0; i < 16; ++i)
    ASSERT_EQ(16u, p.get_pg_num_divisor(pg_t(i, 1)));

  p.set_pg_num(12);
  p.set_pgp_num(12);
  //cout << "num " << p.get_pg_num()
  //     << " mask " << p.get_pg_num_mask() << std::endl;
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

TEST(ghobject_t, cmp) {
  ghobject_t min;
  ghobject_t sep;
  sep.set_shard(shard_id_t(1));
  sep.hobj.pool = -1;
  cout << min << " < " << sep << std::endl;
  ASSERT_TRUE(cmp_bitwise(min, sep) < 0);

  sep.set_shard(shard_id_t::NO_SHARD);
  cout << "sep shard " << sep.shard_id << std::endl;
  ghobject_t o(hobject_t(object_t(), string(), CEPH_NOSNAP, 0x42,
			 1, string()));
  cout << "o " << o << std::endl;
  ASSERT_TRUE(cmp_bitwise(o, sep) > 0);
}

TEST(pool_opts_t, invalid_opt) {
  EXPECT_FALSE(pool_opts_t::is_opt_name("INVALID_OPT"));
  EXPECT_DEATH(pool_opts_t::get_opt_desc("INVALID_OPT"), "");
}

TEST(pool_opts_t, scrub_min_interval) {
  EXPECT_TRUE(pool_opts_t::is_opt_name("scrub_min_interval"));
  EXPECT_EQ(pool_opts_t::get_opt_desc("scrub_min_interval"),
            pool_opts_t::opt_desc_t(pool_opts_t::SCRUB_MIN_INTERVAL,
                                    pool_opts_t::DOUBLE));

  pool_opts_t opts;
  EXPECT_FALSE(opts.is_set(pool_opts_t::SCRUB_MIN_INTERVAL));
  EXPECT_DEATH(opts.get(pool_opts_t::SCRUB_MIN_INTERVAL), "");
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
  EXPECT_DEATH(opts.get(pool_opts_t::SCRUB_MAX_INTERVAL), "");
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
  EXPECT_DEATH(opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL), "");
  double val;
  EXPECT_FALSE(opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL, &val));
  opts.set(pool_opts_t::DEEP_SCRUB_INTERVAL, static_cast<double>(2015));
  EXPECT_TRUE(opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL, &val));
  EXPECT_EQ(val, 2015);
  opts.unset(pool_opts_t::DEEP_SCRUB_INTERVAL);
  EXPECT_FALSE(opts.is_set(pool_opts_t::DEEP_SCRUB_INTERVAL));
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make unittest_osd_types ;
 *   ./unittest_osd_types # --gtest_filter=pg_missing_t.constructor
 * "
 * End:
 */
