// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/PyOSDMap.h"
#include "osd/OSDMap.h"

TEST_F(PyOSDMapTest, OSDMapInitialized) {
  ASSERT_EQ(osd_map.get_epoch(), 1);
}

TEST_F(PyOSDMapTest, GetEpoch) {
  EXPECT_EQ(osd_map.get_epoch(), 1);
  EXPECT_GT(osd_map.get_epoch(), 0);
}

TEST_F(PyOSDMapTest, GetMaxOSD) {
  EXPECT_EQ(osd_map.get_max_osd(), 0);
}

TEST_F(PyOSDMapTest, GetNumPools) {
  EXPECT_EQ(osd_map.get_pools().size(), 0);
}

TEST_F(PyOSDMapTest, GetFlags) {
  EXPECT_EQ(osd_map.get_flags(), 0);
}

TEST_F(PyOSDMapTest, GetCreated) {
  auto created = osd_map.get_created();
  EXPECT_GE(created.sec(), 0);
}

TEST_F(PyOSDMapTest, GetModified) {
  auto modified = osd_map.get_modified();
  EXPECT_GE(modified.sec(), 0);
}

TEST_F(PyOSDMapTest, ApplyIncremental) {
  OSDMap::Incremental inc(osd_map.get_epoch() + 1);
  osd_map.apply_incremental(inc);
  EXPECT_EQ(osd_map.get_epoch(), 2);
}

TEST_F(PyOSDMapTest, PoolOperations) {
  OSDMap::Incremental inc(osd_map.get_epoch() + 1);
  pg_pool_t pool;
  pool.set_pg_num(8);
  pool.set_pgp_num(8);
  inc.new_pool_max = 1;
  inc.new_pools[1] = pool;
  osd_map.apply_incremental(inc);

  EXPECT_EQ(osd_map.get_pools().size(), 1);
  EXPECT_TRUE(osd_map.have_pg_pool(1));
}

TEST_F(PyOSDMapTest, OSDOperations) {
  OSDMap::Incremental inc(osd_map.get_epoch() + 1);
  inc.new_max_osd = 1;
  inc.new_state[0] = CEPH_OSD_EXISTS | CEPH_OSD_UP;
  inc.new_weight[0] = CEPH_OSD_IN;
  inc.new_up_client[0] = entity_addrvec_t();
  inc.new_up_cluster[0] = entity_addrvec_t();
  inc.new_hb_back_up[0] = entity_addrvec_t();
  inc.new_hb_front_up[0] = entity_addrvec_t();
  osd_map.apply_incremental(inc);

  EXPECT_EQ(osd_map.get_max_osd(), 1);
  EXPECT_TRUE(osd_map.exists(0));
  EXPECT_TRUE(osd_map.is_up(0));
}

TEST_F(PyOSDMapTest, GetPoolsType) {
  const auto& pools = osd_map.get_pools();
  EXPECT_TRUE(pools.empty());
}

TEST_F(PyOSDMapTest, GetFSMapEpoch) {
  EXPECT_EQ(osd_map.get_fsid(), uuid_d());
}

TEST_F(PyOSDMapTest, GetClusterSnapshot) {
  EXPECT_EQ(osd_map.get_cluster_snapshot(), "");
}

TEST_F(PyOSDMapTest, PoolNameOperations) {
  OSDMap::Incremental inc(osd_map.get_epoch() + 1);
  pg_pool_t pool;
  pool.set_pg_num(8);
  inc.new_pool_max = 1;
  inc.new_pools[1] = pool;
  inc.new_pool_names[1] = "test_pool";
  osd_map.apply_incremental(inc);

  EXPECT_EQ(osd_map.get_pool_name(1), "test_pool");
  EXPECT_TRUE(osd_map.have_pg_pool(1));
}

TEST_F(PyOSDMapTest, MultiplePools) {
  OSDMap::Incremental inc(osd_map.get_epoch() + 1);
  pg_pool_t pool1, pool2;
  pool1.set_pg_num(8);
  pool2.set_pg_num(16);
  inc.new_pool_max = 2;
  inc.new_pools[1] = pool1;
  inc.new_pools[2] = pool2;
  inc.new_pool_names[1] = "pool1";
  inc.new_pool_names[2] = "pool2";
  osd_map.apply_incremental(inc);

  EXPECT_EQ(osd_map.get_pools().size(), 2);
  EXPECT_TRUE(osd_map.have_pg_pool(1));
  EXPECT_TRUE(osd_map.have_pg_pool(2));
  EXPECT_EQ(osd_map.get_pool_name(1), "pool1");
  EXPECT_EQ(osd_map.get_pool_name(2), "pool2");
}

TEST_F(PyOSDMapTest, EpochProgression) {
  EXPECT_EQ(osd_map.get_epoch(), 1);
  for (int i = 0; i < 5; i++) {
    OSDMap::Incremental inc(osd_map.get_epoch() + 1);
    osd_map.apply_incremental(inc);
  }
  EXPECT_EQ(osd_map.get_epoch(), 6);
}

TEST_F(PyOSDMapTest, OSDUpDown) {
  OSDMap::Incremental inc(osd_map.get_epoch() + 1);
  inc.new_max_osd = 1;
  inc.new_state[0] = CEPH_OSD_EXISTS | CEPH_OSD_UP;
  inc.new_weight[0] = CEPH_OSD_IN;
  inc.new_up_client[0] = entity_addrvec_t();
  inc.new_up_cluster[0] = entity_addrvec_t();
  inc.new_hb_back_up[0] = entity_addrvec_t();
  inc.new_hb_front_up[0] = entity_addrvec_t();
  osd_map.apply_incremental(inc);

  EXPECT_TRUE(osd_map.is_up(0));
  EXPECT_FALSE(osd_map.is_down(0));
}

TEST_F(PyOSDMapTest, GetNumUpOSDs) {
  EXPECT_EQ(osd_map.get_num_up_osds(), 0);

  OSDMap::Incremental inc(osd_map.get_epoch() + 1);
  inc.new_max_osd = 1;
  inc.new_state[0] = CEPH_OSD_EXISTS | CEPH_OSD_UP;
  inc.new_weight[0] = CEPH_OSD_IN;
  inc.new_up_client[0] = entity_addrvec_t();
  inc.new_up_cluster[0] = entity_addrvec_t();
  inc.new_hb_back_up[0] = entity_addrvec_t();
  inc.new_hb_front_up[0] = entity_addrvec_t();
  osd_map.apply_incremental(inc);

  EXPECT_EQ(osd_map.get_num_up_osds(), 1);
}

TEST_F(PyOSDMapTest, GetNumInOSDs) {
  EXPECT_EQ(osd_map.get_num_in_osds(), 0);

  OSDMap::Incremental inc(osd_map.get_epoch() + 1);
  inc.new_max_osd = 1;
  inc.new_state[0] = CEPH_OSD_EXISTS | CEPH_OSD_UP;
  inc.new_weight[0] = CEPH_OSD_IN;
  inc.new_up_client[0] = entity_addrvec_t();
  inc.new_up_cluster[0] = entity_addrvec_t();
  inc.new_hb_back_up[0] = entity_addrvec_t();
  inc.new_hb_front_up[0] = entity_addrvec_t();
  osd_map.apply_incremental(inc);

  EXPECT_EQ(osd_map.get_num_in_osds(), 1);
}
