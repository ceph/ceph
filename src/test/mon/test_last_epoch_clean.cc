// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"

#include "global/global_context.h"
#include "mon/OSDMonitor.h"
#include "osd/OSDMap.h"

class LastEpochCleanTest : public ::testing::Test {
protected:
  OSDMap osdmap;
  int64_t pool = -1;

  void SetUp() override {
    uuid_d fsid;
    osdmap.build_simple(g_ceph_context, 100, fsid, 3);
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    inc.new_pool_max = osdmap.get_pool_max();
    pool = ++inc.new_pool_max;
    pg_pool_t empty;
    pg_pool_t *p = inc.get_new_pool(pool, &empty);
    p->size = 3;
    p->set_pg_num(4);
    p->set_pgp_num(4);
    p->type = pg_pool_t::TYPE_REPLICATED;
    p->crush_rule = 0;
    inc.new_pool_names[pool] = "pool";
    osdmap.apply_incremental(inc);
  }

  void set_pg_num(unsigned pg_num) {
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    pg_pool_t *p = inc.get_new_pool(pool, osdmap.get_pg_pool(pool));
    p->set_pg_num(pg_num);
    p->set_pgp_num(pg_num);
    osdmap.apply_incremental(inc);
  }

  void report(LastEpochClean& lec, unsigned ps, epoch_t last_epoch_clean) {
    lec.report(osdmap.get_pg_pool(pool)->get_pg_num(), pg_t(ps, pool),
	       last_epoch_clean);
  }
};

TEST_F(LastEpochCleanTest, floor_moves_after_merge)
{
  LastEpochClean lec;
  // the oldest epoch belongs to PG 3, which the merge removes
  report(lec, 0, 30);
  report(lec, 1, 40);
  report(lec, 2, 50);
  report(lec, 3, 10);
  EXPECT_EQ(10u, lec.get_lower_bound_by_pool(osdmap));

  set_pg_num(2);
  report(lec, 0, 60);
  report(lec, 1, 70);
  EXPECT_EQ(60u, lec.get_lower_bound_by_pool(osdmap));
}

TEST_F(LastEpochCleanTest, split_after_merge_waits_for_new_pgs)
{
  LastEpochClean lec;
  report(lec, 0, 20);
  report(lec, 1, 21);
  report(lec, 2, 22);
  report(lec, 3, 23);

  set_pg_num(2);
  report(lec, 0, 30);
  EXPECT_EQ(21u, lec.get_lower_bound_by_pool(osdmap));

  // PGs 2 and 3 are new again and have not reported yet
  set_pg_num(4);
  EXPECT_EQ(0u, lec.get_lower_bound_by_pool(osdmap));
  report(lec, 2, 40);
  EXPECT_EQ(0u, lec.get_lower_bound_by_pool(osdmap));
  report(lec, 3, 50);
  EXPECT_EQ(21u, lec.get_lower_bound_by_pool(osdmap));
}
