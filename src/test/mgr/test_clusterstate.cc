// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <memory>

#include "global/global_init.h"
#include "gtest/gtest.h"
#include "messages/MMgrDigest.h"
#include "messages/MPGStats.h"
#include "mgr/ClusterState.h"
#include "mon/PGMap.h"
#include "osd/osd_types.h"
#include "test/mgr/TestMgr.h"

TEST_F(TestMgr, ClusterState_Construct)
{
  cs->with_mgrmap([&](const MgrMap& m) { ASSERT_EQ(m.epoch, mgr_map.epoch); });

  cs->with_osdmap([&](const OSDMap& o) { ASSERT_EQ(o.get_epoch(), 0); });

  cs->with_fsmap([&](const FSMap& f) { ASSERT_EQ(f.get_epoch(), 0); });

  cs->with_servicemap([&](const ServiceMap& sm) { ASSERT_EQ(sm.epoch, 0); });

  cs->with_pgmap([&](const PGMap& pm) { ASSERT_EQ(pm.version, 0); });
}

TEST_F(TestMgr, ClusterState_Setters)
{
  Objecter new_objecter(cct.get(), messenger.get(), mc.get(), *icp);
  cs->set_objecter(&new_objecter);
  ASSERT_EQ(cs->test_get_objecter(), &new_objecter);
  ASSERT_NE(cs->test_get_objecter(), objecter.get());

  cs->with_osdmap_and_pgmap([&](const OSDMap& old_map, const PGMap& pg_map) {
    cs->notify_osdmap(osd_map);
  });
  cs->with_osdmap([&](const OSDMap& o) {
    ASSERT_EQ(o.get_epoch(), osd_map.get_epoch());
  });

  ASSERT_FALSE(cs->have_fsmap());
  const FSMap fs_map = [] {
    FSMap f;
    f.inc_epoch();
    f.inc_epoch();
    return f;
  }();
  cs->set_fsmap(fs_map);
  ASSERT_TRUE(cs->have_fsmap());
  cs->with_fsmap([&](const FSMap& f) {
    ASSERT_EQ(f.get_epoch(), fs_map.get_epoch());
  });

  const MgrMap new_mgr_map = [] {
    MgrMap m;
    m.epoch = 456;
    return m;
  }();
  cs->set_mgr_map(new_mgr_map);
  cs->with_mgrmap([&](const MgrMap& m) {
    ASSERT_EQ(m.epoch, new_mgr_map.epoch);
  });

  const ServiceMap s_map = [] {
    ServiceMap s;
    s.epoch = 777;
    return s;
  }();
  cs->set_service_map(s_map);
  cs->with_servicemap([&](const ServiceMap& sm) {
    ASSERT_EQ(sm.epoch, s_map.epoch);
  });

  //After notify_osdmap, pgmap version should be incremented from 0
  cs->with_pgmap([&](const PGMap& pm) { ASSERT_EQ(pm.version, 1); });
}

TEST_F(TestMgr, ClusterState_LoadDigest)
{
  const bufferlist mon_status_bl = [] {
    bufferlist bl;
    bl.append("{\"monmap\":{\"epoch\":1}}");
    return bl;
  }();

  const bufferlist health_bl = [] {
    bufferlist bl;
    bl.append("{\"status\":\"HEALTH_OK\"}");
    return bl;
  }();

  auto digest = ceph::make_message<MMgrDigest>();
  digest->mon_status_json = mon_status_bl;
  digest->health_json = health_bl;

  cs->load_digest(digest.get());

  cs->with_mon_status([&](const bufferlist& monstat) {
    ASSERT_EQ(monstat.length(), mon_status_bl.length());
    ASSERT_EQ(monstat.to_str(), mon_status_bl.to_str());
  });

  cs->with_health([&](const bufferlist& health) {
    ASSERT_EQ(health.length(), health_bl.length());
    ASSERT_EQ(health.to_str(), health_bl.to_str());
  });
}

TEST_F(ClusterStateTest, IngestPGStats_Valid)
{
  // Test 1: OSD is in the map, PG pool exists, in range, new version
  pg_t pgid(0, 1);
  stats->pg_stat[pgid] = pgstat;
  ingest_and_pginc();

  ASSERT_TRUE(p_inc.pg_stat_updates.contains(pgid));
}

TEST_F(ClusterStateTest, IngestPGStats_PoolDNE)
{
  //Test 2: OSD is in the map, PG pool does not exist
  pg_t pgid(0, 2);
  stats->pg_stat[pgid] = pgstat;
  ingest_and_pginc();

  ASSERT_FALSE(p_inc.pg_stat_updates.contains(pgid));
}

TEST_F(ClusterStateTest, IngestPGStats_OOR)
{
  //Test 3: OSD is in the map, PG pool exists, out of range
  pg_t pgid_out_of_range(2, 1);
  stats->pg_stat[pgid_out_of_range] = pgstat;
  ingest_and_pginc();

  ASSERT_FALSE(p_inc.pg_stat_updates.contains(pgid_out_of_range));
}

TEST_F(ClusterStateTest, IngestPGStats_oldReportedSeq)
{
  pg_t pgid_valid(0, 1);

  stats->set_src(entity_name_t::OSD(0));
  stats->pg_stat[pgid_valid] = pgstat;

  cs->ingest_pgstats(stats);
  cs->update_delta_stats();

  //Test 4: OSD is in the map, PG pool exists, shard in range, old version
  pgstat.reported_epoch = 1;
  pgstat.reported_seq = 1;
  stats->osd_stat.seq = 1;
  pg_t pgid_old_version(0, 1);
  stats->pg_stat[pgid_old_version] = pgstat;
  ingest_and_pginc();

  ASSERT_FALSE(p_inc.pg_stat_updates.contains(pgid_old_version));
}

TEST_F(ClusterStateTest, UpdateDeltaStats)
{
  pg_t pgid(0, 1);

  pgstat.reported_seq = 1;
  stats->pg_stat[pgid] = pgstat;

  cs->ingest_pgstats(stats);
  cs->update_delta_stats();

  p_inc = cs->test_get_pending_inc();
  PGMap pg_map = cs->test_get_pg_map();

  ASSERT_TRUE(p_inc.pg_stat_updates.empty());
  ASSERT_TRUE(pg_map.pg_stat.contains(pgid));
  ASSERT_EQ(pg_map.pg_stat[pgid].reported_seq, pgstat.reported_seq);
}

TEST_F(ClusterStateTest, NotifyOSDMap)
{
  const pg_pool_t pool1 = [] {
    pg_pool_t p;
    p.set_pg_num(2);
    return p;
  }();

  const pg_pool_t pool2 = [] {
    pg_pool_t p;
    p.set_pg_num(4);
    return p;
  }();

  OSDMap::Incremental notify_inc(osd_map.get_epoch() + 1);
  notify_inc.new_pool_max = 2;
  notify_inc.new_pools[1] = pool1;
  notify_inc.new_pools[2] = pool2;
  osd_map.apply_incremental(notify_inc);

  cs->with_osdmap_and_pgmap([&](const OSDMap& old_map, const PGMap& pg_map) {
    cs->notify_osdmap(osd_map);
  });

  const auto& existing_pools = cs->test_get_existing_pools();

  ASSERT_TRUE(existing_pools.contains(1));
  ASSERT_TRUE(existing_pools.contains(2));
  ASSERT_EQ(existing_pools.at(1), pool1.get_pg_num());
  ASSERT_EQ(existing_pools.at(2), pool2.get_pg_num());
}

int
main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
