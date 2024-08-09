// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include "include/stringify.h"

using namespace std;

namespace {
  class CheckTextTable : public TextTable {
  public:
    explicit CheckTextTable(bool verbose) {
      for (int i = 0; i < 6; i++) {
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
  int64_t max_used_osd = 123;
  float max_used_rate = 0.75;
  stringstream max_used;
  max_used << "OSD." << max_used_osd << "/" << percentify(max_used_rate * 100) << "%";

  PGMap::dump_object_stat_sum(tbl, nullptr, pool_stat, avail,
			      pool.get_size(), max_used_osd, max_used_rate,
			      verbose, true, true, &pool);
  float copies_rate =
    (static_cast<float>(sum.num_object_copies - sum.num_objects_degraded) /
      sum.num_object_copies) * pool.get_size();
  float used_percent = (float)statfs.allocated /
    (statfs.allocated + avail) * 100;
  uint64_t stored = statfs.data_stored / copies_rate;

  unsigned col = 0;
  ASSERT_EQ(stringify(byte_u_t(stored)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(stored)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(sum.num_objects)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(statfs.allocated)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(statfs.allocated)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(percentify(used_percent), tbl.get(0, col++));
  ASSERT_EQ(max_used.str(), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(avail/copies_rate)), tbl.get(0, col++));
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
  int64_t max_used_osd = 123;
  float max_used_rate = 0.75;
  stringstream max_used;
  max_used << "OSD." << max_used_osd << "/" << percentify(max_used_rate * 100) << "%";
  PGMap::dump_object_stat_sum(tbl, nullptr, pool_stat, avail,
			      pool.get_size(), max_used_osd, max_used_rate,
                              verbose, true, true, &pool);
  unsigned col = 0;
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(percentify(0), tbl.get(0, col++));
  ASSERT_EQ(max_used.str(), tbl.get(0, col++));
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
  int64_t max_used_osd = -1; // causes N/A in MAX RAW USED col
  float max_used_rate = 0.75;
  PGMap::dump_object_stat_sum(tbl, nullptr, pool_stat, avail,
			      pool.get_size(), max_used_osd, max_used_rate,
                              verbose, true, true, &pool);
  unsigned col = 0;
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(0)), tbl.get(0, col++));
  ASSERT_EQ(percentify(0), tbl.get(0, col++));
  ASSERT_EQ("N/A", tbl.get(0, col++));
  ASSERT_EQ(stringify(byte_u_t(avail/pool.size)), tbl.get(0, col++));
}
