// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "common/TextTable.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "include/stringify.h"
#include "mon/PGMonitor.h"

namespace {
  class CheckTextTable : public TextTable {
  public:
    CheckTextTable(bool verbose) {
      for (int i = 0; i < 4; i++) {
        define_column("", TextTable::LEFT, TextTable::LEFT);
      }
      if (verbose) {
        for (int i = 0; i < 4; i++) {
          define_column("", TextTable::LEFT, TextTable::LEFT);
        }
      }
    }
    const string& get(unsigned r, unsigned c) const {
      assert(r < row.size());
      assert(c < row[r].size());
      return row[r][c];
    }
  };

  // copied from PGMonitor.cc
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
TEST(pgmonitor, dump_object_stat_sum_0)
{
  bool verbose = true;
  CheckTextTable tbl(verbose);
  object_stat_sum_t sum;
  sum.num_bytes = 42 * 1024 * 1024;
  sum.num_objects = 42;
  sum.num_objects_degraded = 13; // there are 13 missings + not_yet_backfilled
  sum.num_objects_dirty = 2;
  sum.num_rd = 100;
  sum.num_rd_kb = 123;
  sum.num_wr = 101;
  sum.num_wr_kb = 321;    

  sum.calc_copies(3);           // assuming we have 3 copies for each obj
  // nominal amount of space available for new objects in this pool
  uint64_t avail = 2016 * 1024 * 1024;
  pg_pool_t pool;
  pool.quota_max_objects = 2000;
  pool.quota_max_bytes = 2000 * 1024 * 1024;
  pool.size = 2;
  pool.type = pg_pool_t::TYPE_REPLICATED;
  PGMonitor::dump_object_stat_sum(tbl, nullptr, sum, avail,
                                  pool.get_size(), verbose, &pool);  
  ASSERT_EQ(stringify(si_t(sum.num_bytes)), tbl.get(0, 0));
  float copies_rate =
    (static_cast<float>(sum.num_object_copies - sum.num_objects_degraded) /
     sum.num_object_copies);
  float used_bytes = sum.num_bytes * copies_rate;
  float used_percent = used_bytes / (used_bytes + avail) * 100;
  unsigned col = 0;
  ASSERT_EQ(stringify(si_t(sum.num_bytes)), tbl.get(0, col++));
  ASSERT_EQ(percentify(used_percent), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(avail)), tbl.get(0, col++));
  ASSERT_EQ(stringify(sum.num_objects), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(sum.num_objects_dirty)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(sum.num_rd)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(sum.num_wr)), tbl.get(0, col++));
  // we can use pool.size for raw_used_rate if it is a replica pool
  uint64_t raw_bytes_used = sum.num_bytes * pool.get_size() * copies_rate;
  ASSERT_EQ(stringify(si_t(raw_bytes_used)), tbl.get(0, col++));
}

// with table, without formatter, verbose = true, empty, avail > 0
TEST(pgmonitor, dump_object_stat_sum_1)
{
  bool verbose = true;
  CheckTextTable tbl(verbose);
  object_stat_sum_t sum;        // zero by default
  ASSERT_TRUE(sum.is_zero());
  // nominal amount of space available for new objects in this pool
  uint64_t avail = 2016 * 1024 * 1024;
  pg_pool_t pool;
  pool.quota_max_objects = 2000;
  pool.quota_max_bytes = 2000 * 1024 * 1024;
  pool.size = 2;
  pool.type = pg_pool_t::TYPE_REPLICATED;
  PGMonitor::dump_object_stat_sum(tbl, nullptr, sum, avail,
                                  pool.get_size(), verbose, &pool);  
  ASSERT_EQ(stringify(si_t(0)), tbl.get(0, 0));
  unsigned col = 0;
  ASSERT_EQ(stringify(si_t(0)), tbl.get(0, col++));
  ASSERT_EQ(percentify(0), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(avail)), tbl.get(0, col++));
  ASSERT_EQ(stringify(0), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(0)), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(0)), tbl.get(0, col++));
}

// with table, without formatter, verbose = false, empty, avail = 0
TEST(pgmonitor, dump_object_stat_sum_2)
{
  bool verbose = false;
  CheckTextTable tbl(verbose);
  object_stat_sum_t sum;        // zero by default
  ASSERT_TRUE(sum.is_zero());
  // nominal amount of space available for new objects in this pool
  uint64_t avail = 0;
  pg_pool_t pool;
  pool.quota_max_objects = 2000;
  pool.quota_max_bytes = 2000 * 1024 * 1024;
  pool.size = 2;
  pool.type = pg_pool_t::TYPE_REPLICATED;

  PGMonitor::dump_object_stat_sum(tbl, nullptr, sum, avail,
                                  pool.get_size(), verbose, &pool);  
  ASSERT_EQ(stringify(si_t(0)), tbl.get(0, 0));
  unsigned col = 0;
  ASSERT_EQ(stringify(si_t(0)), tbl.get(0, col++));
  ASSERT_EQ(percentify(0), tbl.get(0, col++));
  ASSERT_EQ(stringify(si_t(avail)), tbl.get(0, col++));
  ASSERT_EQ(stringify(0), tbl.get(0, col++));
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  vector<const char*> def_args;
  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
