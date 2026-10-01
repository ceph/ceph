// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank <info@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "mon/PGMap.h"
#include "gtest/gtest.h"

#include "common/TextTable.h"
#include "include/stringify.h"

using namespace std;

namespace {
  class CheckTextTable : public TextTable {
  public:
    explicit CheckTextTable(bool verbose) {
      for (int i = 0; i < 5; i++) {
        define_column("", TextTable::LEFT, TextTable::LEFT);
      }
      if (verbose) {
        for (int i = 0; i < 9; i++) {
          define_column("", TextTable::LEFT, TextTable::LEFT);
        }
      }
    }
    const string& get(unsigned r, unsigned c) const {
      ceph_assert(r < row.size());
      ceph_assert(c < row[r].size());
      return row[r][c];
    }
  };

  // copied from PGMap.cc
  string percentify(float a) {
    stringstream ss;
    if (a < 0.01)
      ss << "0";
    else
      ss << std::fixed << std::setprecision(2) << a;
    return ss.str();
  }
}

// dump_object_stat_sum() is called by "ceph df" command
// with table, without formatter, verbose = true, not empty, avail > 0
TEST(pgmap, dump_object_stat_sum_0)
{
  bool verbose = true;
  CheckTextTable tbl(verbose);
  pool_stat_t pool_stat;
  object_stat_sum_t& sum = pool_stat.stats.sum;
  sum.num_bytes = 42 * 1024 * 1024;
  sum.num_objects = 42;
  sum.num_objects_degraded = 13; // there are 13 missings + not_yet_backfilled
  sum.num_objects_dirty = 2;
  sum.num_rd = 100;
  sum.num_rd_kb = 123;
  sum.num_wr = 101;
  sum.num_wr_kb = 321;    
  pool_stat.num_store_stats = 3;
  store_statfs_t &statfs = pool_stat.store_stats;
  statfs.data_stored = 40 * 1024 * 1024;
  statfs.allocated = 41 * 1024 * 1024 * 2;
  statfs.data_compressed_allocated = 4334;
  statfs.data_compressed_original = 1213;

  sum.calc_copies(3); // assuming we have 3 copies for each obj
  // nominal amount of space available for new objects in this pool
  uint64_t avail = 2016 * 1024 * 1024;
  pg_pool_t pool;
  pool.quota_max_objects = 2000;
  pool.quota_max_bytes = 2000 * 1024 * 1024;
  pool.size = 2;
  pool.type = pg_pool_t::TYPE_REPLICATED;
  pool.tier_of = 0;
  PGMap::dump_object_stat_sum(tbl, nullptr, pool_stat, avail,
			      pool.get_size(), verbose, true, true, &pool);

  float used_percent = (float)statfs.allocated /
    (statfs.allocated + avail) * 100;

  unsigned col = 0;
  ASSERT_EQ(stringify(byte_u_t(statfs.data_stored/pool.get_size())), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(statfs.data_stored/pool.get_size())), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(sum.num_objects)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(statfs.allocated)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(statfs.allocated)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(percentify(used_percent), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(avail/pool.get_size())), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(pool.quota_max_objects)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(pool.quota_max_bytes)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(sum.num_objects_dirty)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(statfs.data_compressed_allocated)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(statfs.data_compressed_original)), tbl.get(0, col++));
}

// with table, without formatter, verbose = true, empty, avail > 0
TEST(pgmap, dump_object_stat_sum_1)
{
  bool verbose = true;
  CheckTextTable tbl(verbose);
  pool_stat_t pool_stat;
  object_stat_sum_t& sum = pool_stat.stats.sum; // zero by default
  ASSERT_TRUE(sum.is_zero());
  // nominal amount of space available for new objects in this pool
  uint64_t avail = 2016 * 1024 * 1024;
  pg_pool_t pool;
  pool.quota_max_objects = 2000;
  pool.quota_max_bytes = 2000 * 1024 * 1024;
  pool.size = 2;
  pool.type = pg_pool_t::TYPE_REPLICATED;
  pool.tier_of = 0;
  PGMap::dump_object_stat_sum(tbl, nullptr, pool_stat, avail,
			      pool.get_size(), verbose, true, true, &pool);
  unsigned col = 0;
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(percentify(0), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(avail/pool.size)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(pool.quota_max_objects)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(pool.quota_max_bytes)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
}

// with table, without formatter, verbose = false, empty, avail = 0
TEST(pgmap, dump_object_stat_sum_2)
{
  bool verbose = false;
  CheckTextTable tbl(verbose);
  pool_stat_t pool_stat;
  object_stat_sum_t& sum = pool_stat.stats.sum; // zero by default
  ASSERT_TRUE(sum.is_zero());
  // nominal amount of space available for new objects in this pool
  uint64_t avail = 0;
  pg_pool_t pool;
  pool.quota_max_objects = 2000;
  pool.quota_max_bytes = 2000 * 1024 * 1024;
  pool.size = 2;
  pool.type = pg_pool_t::TYPE_REPLICATED;

  PGMap::dump_object_stat_sum(tbl, nullptr, pool_stat, avail,
			      pool.get_size(), verbose, true, true, &pool);  
  unsigned col = 0;
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(percentify(0), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(avail/pool.size)), tbl.get(0, col++));
}

// Helper: build a pg_stat_t with the given state and completed_rollbacks
static pg_stat_t make_pg_stat(uint64_t state,
                               std::initializer_list<snapid_t> snaps)
{
  pg_stat_t s;
  s.state = state;
  for (auto snap : snaps) {
    s.completed_rollbacks.insert(snap);
  }
  return s;
}

// 1. Basic intersection: two PGs in the same pool
TEST(pgmap, calc_completed_rollbacks_intersection)
{
  PGMap pgmap;
  // pool 1, pg 0: {1,2}
  pgmap.pg_stat[pg_t(0, 1)] = make_pg_stat(1, {1, 2});
  // pool 1, pg 1: {2,3}
  pgmap.pg_stat[pg_t(1, 1)] = make_pg_stat(1, {2, 3});

  mempool::pgmap::map<int64_t, snap_interval_set_t> result;
  pgmap.calc_completed_rollbacks(result);

  ASSERT_EQ(1u, result.size());
  ASSERT_TRUE(result.count(1));
  snap_interval_set_t expected;
  expected.insert(2);
  ASSERT_EQ(expected, result[1]);
}

// 2. Single PG (seed): result equals that PG's set
TEST(pgmap, calc_completed_rollbacks_single_pg)
{
  PGMap pgmap;
  pgmap.pg_stat[pg_t(0, 2)] = make_pg_stat(1, {1, 2, 3});

  mempool::pgmap::map<int64_t, snap_interval_set_t> result;
  pgmap.calc_completed_rollbacks(result);

  ASSERT_EQ(1u, result.size());
  ASSERT_TRUE(result.count(2));
  snap_interval_set_t expected;
  expected.insert(1, 3); // snapids 1,2,3
  ASSERT_EQ(expected, result[2]);
}

// 3. Unknown state PG exclusion: a PG with state==0 causes the pool to be excluded
TEST(pgmap, calc_completed_rollbacks_unknown_state)
{
  PGMap pgmap;
  // pool 3 has one good PG and one unknown PG
  pgmap.pg_stat[pg_t(0, 3)] = make_pg_stat(1, {1, 2});
  pgmap.pg_stat[pg_t(1, 3)] = make_pg_stat(0, {1, 2}); // unknown

  mempool::pgmap::map<int64_t, snap_interval_set_t> result;
  pgmap.calc_completed_rollbacks(result);

  // pool 3 should be absent from result
  ASSERT_EQ(0u, result.count(3));
}

// 4. Empty sets: all PGs report empty completed_rollbacks
TEST(pgmap, calc_completed_rollbacks_empty_sets)
{
  PGMap pgmap;
  pgmap.pg_stat[pg_t(0, 4)] = make_pg_stat(1, {});
  pgmap.pg_stat[pg_t(1, 4)] = make_pg_stat(1, {});

  mempool::pgmap::map<int64_t, snap_interval_set_t> result;
  pgmap.calc_completed_rollbacks(result);

  // pool 4 is present but its set is empty
  ASSERT_TRUE(result.count(4));
  ASSERT_TRUE(result[4].empty());
}

// 5. Multiple pools: per-pool intersections are independent
TEST(pgmap, calc_completed_rollbacks_multiple_pools)
{
  PGMap pgmap;
  // pool 5: {1,2} ∩ {2,3} = {2}
  pgmap.pg_stat[pg_t(0, 5)] = make_pg_stat(1, {1, 2});
  pgmap.pg_stat[pg_t(1, 5)] = make_pg_stat(1, {2, 3});
  // pool 6: {4,5} ∩ {5,6} = {5}
  pgmap.pg_stat[pg_t(0, 6)] = make_pg_stat(1, {4, 5});
  pgmap.pg_stat[pg_t(1, 6)] = make_pg_stat(1, {5, 6});

  mempool::pgmap::map<int64_t, snap_interval_set_t> result;
  pgmap.calc_completed_rollbacks(result);

  ASSERT_EQ(2u, result.size());

  snap_interval_set_t exp5, exp6;
  exp5.insert(2);
  exp6.insert(5);
  ASSERT_EQ(exp5, result[5]);
  ASSERT_EQ(exp6, result[6]);
}
