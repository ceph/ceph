// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"
#include "osd/OSDMap.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include <iostream>

using namespace std;

int main(int argc, char **argv) {
  std::vector<const char *> preargs;
  std::vector<const char*> args(argv, argv+argc);
  global_init(&preargs, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
              CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  // make sure we have 3 copies, or some tests won't work
  g_ceph_context->_conf->set_val("osd_pool_default_size", "3", false);
  // our map is flat, so just try and split across OSDs, not hosts or whatever
  g_ceph_context->_conf->set_val("osd_crush_chooseleaf_type", "0", false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class OSDMapTest : public testing::Test {
  const static int num_osds = 6;
public:
  OSDMap osdmap;
  OSDMapTest() {}

  void set_up_map() {
    uuid_d fsid;
    osdmap.build_simple(g_ceph_context, 0, fsid, num_osds, 6, 6);
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.fsid = osdmap.get_fsid();
    entity_addr_t sample_addr;
    uuid_d sample_uuid;
    for (int i = 0; i < num_osds; ++i) {
      sample_uuid.uuid[i] = i;
      sample_addr.nonce = i;
      pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
      pending_inc.new_up_client[i] = sample_addr;
      pending_inc.new_up_cluster[i] = sample_addr;
      pending_inc.new_hb_back_up[i] = sample_addr;
      pending_inc.new_hb_front_up[i] = sample_addr;
      pending_inc.new_weight[i] = CEPH_OSD_IN;
      pending_inc.new_uuid[i] = sample_uuid;
    }
    osdmap.apply_incremental(pending_inc);
  }
  unsigned int get_num_osds() { return num_osds; }
};

TEST_F(OSDMapTest, Create) {
  set_up_map();
  ASSERT_EQ(get_num_osds(), (unsigned)osdmap.get_max_osd());
  ASSERT_EQ(get_num_osds(), osdmap.get_num_in_osds());
}

TEST_F(OSDMapTest, MapPG) {
  set_up_map();

  pg_t rawpg(0, 0, -1);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  vector<int> old_up_osds, old_acting_osds;
  osdmap.pg_to_up_acting_osds(pgid, old_up_osds, old_acting_osds);
  ASSERT_EQ(old_up_osds, up_osds);
  ASSERT_EQ(old_acting_osds, acting_osds);

  ASSERT_EQ(osdmap.get_pg_pool(0)->get_size(), up_osds.size());
}

TEST_F(OSDMapTest, MapFunctionsMatch) {
  // TODO: make sure pg_to_up_acting_osds and pg_to_acting_osds match
  set_up_map();

  pg_t rawpg(0, 0, -1);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  vector<int> up_osds_two, acting_osds_two;

  osdmap.pg_to_up_acting_osds(pgid, up_osds_two, acting_osds_two);

  ASSERT_EQ(up_osds, up_osds_two);
  ASSERT_EQ(acting_osds, acting_osds_two);

  int acting_primary_two;
  osdmap.pg_to_acting_osds(pgid, &acting_osds_two, &acting_primary_two);
  EXPECT_EQ(acting_osds, acting_osds_two);
  EXPECT_EQ(acting_primary, acting_primary_two);
  osdmap.pg_to_acting_osds(pgid, acting_osds_two);
  EXPECT_EQ(acting_osds, acting_osds_two);
}

/** This test must be removed or modified appropriately when we allow
 * other ways to specify a primary. */
TEST_F(OSDMapTest, PrimaryIsFirst) {
  set_up_map();

  pg_t rawpg(0, 0, -1);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);
  EXPECT_EQ(up_osds[0], up_primary);
  EXPECT_EQ(acting_osds[0], acting_primary);
}

TEST_F(OSDMapTest, PGTempRespected) {
  set_up_map();

  pg_t rawpg(0, 0, -1);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  // copy and swap first and last element in acting_osds
  vector<int> new_acting_osds(acting_osds);
  int first = new_acting_osds[0];
  new_acting_osds[0] = *new_acting_osds.rbegin();
  *new_acting_osds.rbegin() = first;

  // apply pg_temp to osdmap
  OSDMap::Incremental pgtemp_map(osdmap.get_epoch() + 1);
  pgtemp_map.new_pg_temp[pgid] = new_acting_osds;
  osdmap.apply_incremental(pgtemp_map);

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);
  EXPECT_EQ(new_acting_osds, acting_osds);
}

TEST_F(OSDMapTest, PrimaryTempRespected) {
  set_up_map();

  pg_t rawpg(0, 0, -1);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  // make second OSD primary via incremental
  OSDMap::Incremental pgtemp_map(osdmap.get_epoch() + 1);
  pgtemp_map.new_primary_temp[pgid] = acting_osds[1];
  osdmap.apply_incremental(pgtemp_map);

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);
  EXPECT_EQ(acting_primary, acting_osds[1]);
}

TEST_F(OSDMapTest, RemovesRedundantTemps) {
  set_up_map();

  pg_t rawpg(0, 0, -1);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  // stick calculated values in to temps
  OSDMap::Incremental pgtemp_map(osdmap.get_epoch() + 1);
  pgtemp_map.new_pg_temp[pgid] = up_osds;
  pgtemp_map.new_primary_temp[pgid] = up_primary;
  osdmap.apply_incremental(pgtemp_map);

  OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
  OSDMap::remove_redundant_temporaries(g_ceph_context, osdmap, &pending_inc);

  EXPECT_TRUE(pending_inc.new_pg_temp.count(pgid) &&
	      pending_inc.new_pg_temp[pgid].size() == 0);
  EXPECT_EQ(-1, pending_inc.new_primary_temp[pgid]);
}

TEST_F(OSDMapTest, KeepsNecessaryTemps) {
  set_up_map();

  pg_t rawpg(0, 0, -1);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  // find unused OSD and stick it in there
  OSDMap::Incremental pgtemp_map(osdmap.get_epoch() + 1);
  // find an unused osd and put it in place of the first one
  int i = 0;
  for(; i != (int)get_num_osds(); ++i) {
    bool in_use = false;
    for (vector<int>::iterator osd_it = up_osds.begin();
	 osd_it != up_osds.end();
	 ++osd_it) {
      if (i == *osd_it) {
	in_use = true;
        break;
      }
    }
    if (!in_use) {
      up_osds[1] = i;
      break;
    }
  }
  if (i == (int)get_num_osds())
    ASSERT_EQ(0, "did not find unused OSD for temp mapping");

  pgtemp_map.new_pg_temp[pgid] = up_osds;
  pgtemp_map.new_primary_temp[pgid] = up_osds[1];
  osdmap.apply_incremental(pgtemp_map);

  OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);

  OSDMap::remove_redundant_temporaries(g_ceph_context, osdmap, &pending_inc);
  EXPECT_FALSE(pending_inc.new_pg_temp.count(pgid));
  EXPECT_FALSE(pending_inc.new_primary_temp.count(pgid));
}
