// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Test maybe_go_degraded_stretch_mode and helper functions
 *
 * Tests the stretch mode failure detection mechanism that monitors both
 * monitor and OSD health to determine when to trigger degraded stretch mode.
 */

#include "gtest/gtest.h"
#include "mon/Monitor.h"
#include "mon/MonMap.h"
#include "osd/OSDMap.h"
#include "crush/CrushWrapper.h"
#include "common/ceph_context.h"

#include <memory>
#include <sstream>

using namespace std;

class StretchModeTest : public ::testing::Test {
protected:
  unique_ptr<CephContext> cct;
  OSDMap osdmap;
  shared_ptr<CrushWrapper> crush;
  MonMap monmap;
  string stretch_bucket_divider;

  void SetUp() override {
    vector<const char*> args;
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_MON));
    crush = make_shared<CrushWrapper>();
    stretch_bucket_divider = "datacenter";
  }

  void setup_crush_three_zones() {
    // Create a CRUSH map with three zones: dc1, dc2 (data)
    crush->create();
    crush->set_max_devices(10);

    crush->set_type_name(10, "root");
    crush->set_type_name(8, "datacenter");
    crush->set_type_name(1, "host");
    crush->set_type_name(0, "osd");

    // DC1 - data zone
    map<string,string> loc_dc1;
    loc_dc1["root"] = "default";
    loc_dc1["datacenter"] = "dc1";
    loc_dc1["host"] = "host1";
    crush->insert_item(cct.get(), 0, 1.0, "osd.0", loc_dc1);
    crush->insert_item(cct.get(), 1, 1.0, "osd.1", loc_dc1);

    // DC2 - data zone
    map<string,string> loc_dc2;
    loc_dc2["root"] = "default";
    loc_dc2["datacenter"] = "dc2";
    loc_dc2["host"] = "host2";
    crush->insert_item(cct.get(), 2, 1.0, "osd.2", loc_dc2);
    crush->insert_item(cct.get(), 3, 1.0, "osd.3", loc_dc2);

     // Tiebreaker zone only has monitors, not OSDs
  }

  void setup_osdmap_all_up() {
    osdmap.set_max_osd(4);
    osdmap.crush = crush;
    
    // Set stretch mode bucket type
    int32_t stretch_divider_id = crush->get_type_id("datacenter");
    osdmap.stretch_mode_bucket = stretch_divider_id;
    
    for (int i = 0; i < 4; i++) {
      osdmap.set_state(i, CEPH_OSD_EXISTS | CEPH_OSD_UP);
      osdmap.set_weight(i, CEPH_OSD_IN);
    }
  }

  void setup_osdmap_dc2_down() {
    // OSDs 0,1 (dc1) are up, OSDs 2,3 (dc2) are down
    osdmap.set_max_osd(4);
    osdmap.crush = crush;
    
    // Set stretch mode bucket type
    int32_t stretch_divider_id = crush->get_type_id("datacenter");
    osdmap.stretch_mode_bucket = stretch_divider_id;
    
    osdmap.set_state(0, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    osdmap.set_weight(0, CEPH_OSD_IN);
    osdmap.set_state(1, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    osdmap.set_weight(1, CEPH_OSD_IN);
    
    // OSDs in dc2 are down
    osdmap.set_state(2, CEPH_OSD_EXISTS);
    osdmap.set_weight(2, CEPH_OSD_IN);
    osdmap.set_state(3, CEPH_OSD_EXISTS);
    osdmap.set_weight(3, CEPH_OSD_IN);
  }

  void setup_monmap_three_zones() {
    // Create monitors in three zones: dc1 (2 mons), dc2 (2 mons), arbiter (1 mon)
    monmap.epoch = 1;
    monmap.stretch_mode_enabled = true;
    monmap.tiebreaker_mon = "mon.arbiter";
    
    // DC1 monitors
    entity_addrvec_t addrs_dc1_mon0;
    addrs_dc1_mon0.v.push_back(entity_addr_t());
    mon_info_t& mon_dc1_0 = monmap.mon_info["mon.dc1-0"];
    mon_dc1_0.name = "mon.dc1-0";
    mon_dc1_0.public_addrs = addrs_dc1_mon0;
    mon_dc1_0.crush_loc = {{"datacenter", "dc1"}};
    monmap.ranks.push_back("mon.dc1-0");
    
    entity_addrvec_t addrs_dc1_mon1;
    addrs_dc1_mon1.v.push_back(entity_addr_t());
    mon_info_t& mon_dc1_1 = monmap.mon_info["mon.dc1-1"];
    mon_dc1_1.name = "mon.dc1-1";
    mon_dc1_1.public_addrs = addrs_dc1_mon1;
    mon_dc1_1.crush_loc = {{"datacenter", "dc1"}};
    monmap.ranks.push_back("mon.dc1-1");
    
    // DC2 monitors
    entity_addrvec_t addrs_dc2_mon0;
    addrs_dc2_mon0.v.push_back(entity_addr_t());
    mon_info_t& mon_dc2_0 = monmap.mon_info["mon.dc2-0"];
    mon_dc2_0.name = "mon.dc2-0";
    mon_dc2_0.public_addrs = addrs_dc2_mon0;
    mon_dc2_0.crush_loc = {{"datacenter", "dc2"}};
    monmap.ranks.push_back("mon.dc2-0");
    
    entity_addrvec_t addrs_dc2_mon1;
    addrs_dc2_mon1.v.push_back(entity_addr_t());
    mon_info_t& mon_dc2_1 = monmap.mon_info["mon.dc2-1"];
    mon_dc2_1.name = "mon.dc2-1";
    mon_dc2_1.public_addrs = addrs_dc2_mon1;
    mon_dc2_1.crush_loc = {{"datacenter", "dc2"}};
    monmap.ranks.push_back("mon.dc2-1");
    
    // Arbiter monitor
    entity_addrvec_t addrs_arbiter;
    addrs_arbiter.v.push_back(entity_addr_t());
    mon_info_t& mon_arbiter = monmap.mon_info["mon.arbiter"];
    mon_arbiter.name = "mon.arbiter";
    mon_arbiter.public_addrs = addrs_arbiter;
    mon_arbiter.crush_loc = {{"datacenter", "arbiter"}};
    monmap.ranks.push_back("mon.arbiter");
  }
};

// Test: DC2 zone has all OSDs down
TEST_F(StretchModeTest, DC2_OSDsDown) {
  setup_crush_three_zones();
  setup_osdmap_dc2_down();
  setup_monmap_three_zones();

  // All monitors in quorum
  set<int> quorum = {0, 1, 2, 3, 4};
  
  // Populate monitor and OSD failure state
  map<string, set<string>> dead_mon_buckets;
  set<string> up_mon_buckets;
  set<string> dead_osd_buckets;
  set<string> up_osd_buckets;
  
  Monitor::populate_dead_mon_buckets(monmap, quorum, stretch_bucket_divider,
                                     dead_mon_buckets, up_mon_buckets);
  Monitor::populate_dead_osd_buckets(osdmap, dead_osd_buckets, up_osd_buckets);
  
  // Compute final degraded mode parameters
  set<string> dead_mons;
  set<string> dead_zones;
  Monitor::compute_dead_zones_and_mons(dead_mon_buckets, dead_osd_buckets,
                                       dead_mons, dead_zones);
  
  // Verify results
  EXPECT_EQ(dead_mons.size(), 0u) << "No monitors should be down";
  EXPECT_EQ(dead_zones.size(), 1u) << "Should have 1 dead zone (dc2)";
  EXPECT_TRUE(dead_zones.count("dc2")) << "dc2 should be in dead_zones";
  EXPECT_EQ(dead_osd_buckets.size(), 1u) << "dc2 OSDs are down";
  EXPECT_EQ(up_osd_buckets.size(), 1u) << "dc1 OSDs are up";
}

// Test: All OSDs and monitors healthy
TEST_F(StretchModeTest, AllHealthy) {
  setup_crush_three_zones();
  setup_osdmap_all_up();
  setup_monmap_three_zones();

  // All monitors in quorum
  set<int> quorum = {0, 1, 2, 3, 4};
  
  // Populate monitor and OSD failure state
  map<string, set<string>> dead_mon_buckets;
  set<string> up_mon_buckets;
  set<string> dead_osd_buckets;
  set<string> up_osd_buckets;
  
  Monitor::populate_dead_mon_buckets(monmap, quorum, stretch_bucket_divider,
                                     dead_mon_buckets, up_mon_buckets);
  Monitor::populate_dead_osd_buckets(osdmap, dead_osd_buckets, up_osd_buckets);
  
  // Compute final degraded mode parameters
  set<string> dead_mons;
  set<string> dead_zones;
  Monitor::compute_dead_zones_and_mons(dead_mon_buckets, dead_osd_buckets,
                                       dead_mons, dead_zones);
  
  // Verify results
  EXPECT_EQ(dead_mons.size(), 0u) << "No monitors should be down";
  EXPECT_EQ(dead_zones.size(), 0u) << "No zones should be dead";
  EXPECT_EQ(dead_mon_buckets.size(), 0u) << "All monitors up";
  EXPECT_EQ(dead_osd_buckets.size(), 0u) << "All OSDs up";
  EXPECT_EQ(up_mon_buckets.size(), 3u) << "dc1, dc2, arbiter all have monitors";
  EXPECT_EQ(up_osd_buckets.size(), 2u) << "dc1, dc2 have OSDs";
}

// ============================================================================
// Monitor Failure Detection Tests
// ============================================================================

// Test: DC2 monitors are all down
TEST_F(StretchModeTest, DC2_MonitorsDown) {
  setup_crush_three_zones();
  setup_osdmap_all_up();
  setup_monmap_three_zones();

  // Only dc1 and arbiter monitors in quorum (dc2 down)
  set<int> quorum = {0, 1, 4};
  
  // Populate monitor and OSD failure state
  map<string, set<string>> dead_mon_buckets;
  set<string> up_mon_buckets;
  set<string> dead_osd_buckets;
  set<string> up_osd_buckets;
  
  Monitor::populate_dead_mon_buckets(monmap, quorum, stretch_bucket_divider,
                                     dead_mon_buckets, up_mon_buckets);
  Monitor::populate_dead_osd_buckets(osdmap, dead_osd_buckets, up_osd_buckets);
  
  // Compute final degraded mode parameters
  set<string> dead_mons;
  set<string> dead_zones;
  Monitor::compute_dead_zones_and_mons(dead_mon_buckets, dead_osd_buckets,
                                       dead_mons, dead_zones);
  
  // Verify results
  EXPECT_EQ(dead_mons.size(), 2u) << "Should have 2 down monitors";
  EXPECT_TRUE(dead_mons.count("mon.dc2-0"));
  EXPECT_TRUE(dead_mons.count("mon.dc2-1"));
  
  EXPECT_EQ(dead_zones.size(), 1u) << "Should have 1 dead zone (dc2)";
  EXPECT_TRUE(dead_zones.count("dc2")) << "dc2 should be in dead_zones";
  
  EXPECT_EQ(dead_mon_buckets.size(), 1u);
  EXPECT_EQ(dead_osd_buckets.size(), 0u) << "All OSDs still up";
}

// Test: Arbiter monitor down (should not trigger degraded mode)
TEST_F(StretchModeTest, ArbiterDown) {
  setup_crush_three_zones();
  setup_osdmap_all_up();
  setup_monmap_three_zones();

  // Arbiter down, both data zones have monitors
  set<int> quorum = {0, 1, 2, 3};
  
  // Populate monitor and OSD failure state
  map<string, set<string>> dead_mon_buckets;
  set<string> up_mon_buckets;
  set<string> dead_osd_buckets;
  set<string> up_osd_buckets;
  
  Monitor::populate_dead_mon_buckets(monmap, quorum, stretch_bucket_divider,
                                     dead_mon_buckets, up_mon_buckets);
  Monitor::populate_dead_osd_buckets(osdmap, dead_osd_buckets, up_osd_buckets);
  
  // Compute final degraded mode parameters
  set<string> dead_mons;
  set<string> dead_zones;
  Monitor::compute_dead_zones_and_mons(dead_mon_buckets, dead_osd_buckets,
                                       dead_mons, dead_zones);
  
  // Verify results - arbiter is filtered out so no dead zones
  EXPECT_EQ(dead_mons.size(), 0u) << "Arbiter filtered out, no data zone monitors down";
  EXPECT_EQ(dead_zones.size(), 0u) << "Arbiter filtered out, no dead zones";
  EXPECT_EQ(dead_mon_buckets.size(), 0u) << "Tiebreaker zone filtered out";
}

// ============================================================================
// Combined Monitor and OSD Failure Tests
// ============================================================================

// Test: Both monitors AND OSDs down in dc2
TEST_F(StretchModeTest, DC2_CompleteFailure) {
  setup_crush_three_zones();
  setup_osdmap_dc2_down();
  setup_monmap_three_zones();

  // DC2 monitors out of quorum
  set<int> quorum = {0, 1, 4};
  
  // Populate monitor and OSD failure state
  map<string, set<string>> dead_mon_buckets;
  set<string> up_mon_buckets;
  set<string> dead_osd_buckets;
  set<string> up_osd_buckets;
  
  Monitor::populate_dead_mon_buckets(monmap, quorum, stretch_bucket_divider,
                                     dead_mon_buckets, up_mon_buckets);
  Monitor::populate_dead_osd_buckets(osdmap, dead_osd_buckets, up_osd_buckets);
  
  // Compute final degraded mode parameters
  set<string> dead_mons;
  set<string> dead_zones;
  Monitor::compute_dead_zones_and_mons(dead_mon_buckets, dead_osd_buckets,
                                       dead_mons, dead_zones);
  
  // Verify results
  EXPECT_EQ(dead_mons.size(), 2u) << "DC2 monitors are down";
  EXPECT_TRUE(dead_mons.count("mon.dc2-0"));
  EXPECT_TRUE(dead_mons.count("mon.dc2-1"));
  
  EXPECT_EQ(dead_zones.size(), 1u) << "DC2 is completely dead (mons + OSDs)";
  EXPECT_TRUE(dead_zones.count("dc2"));
}

// Test: DC1 monitors down, DC2 OSDs down (different zones failed)
TEST_F(StretchModeTest, DifferentZonesDown) {
  setup_crush_three_zones();
  setup_osdmap_dc2_down();
  setup_monmap_three_zones();

  // DC1 monitors out of quorum (only dc2 and arbiter monitors up)
  set<int> quorum = {2, 3, 4};
  
  // Populate monitor and OSD failure state
  map<string, set<string>> dead_mon_buckets;
  set<string> up_mon_buckets;
  set<string> dead_osd_buckets;
  set<string> up_osd_buckets;
  
  Monitor::populate_dead_mon_buckets(monmap, quorum, stretch_bucket_divider,
                                     dead_mon_buckets, up_mon_buckets);
  Monitor::populate_dead_osd_buckets(osdmap, dead_osd_buckets, up_osd_buckets);
  
  // Compute final degraded mode parameters
  set<string> dead_mons;
  set<string> dead_zones;
  Monitor::compute_dead_zones_and_mons(dead_mon_buckets, dead_osd_buckets,
                                       dead_mons, dead_zones);
  
  // Verify results
  EXPECT_EQ(dead_mons.size(), 2u) << "DC1 monitors are down";
  EXPECT_TRUE(dead_mons.count("mon.dc1-0"));
  EXPECT_TRUE(dead_mons.count("mon.dc1-1"));
  
  EXPECT_EQ(dead_zones.size(), 2u) << "Both dc1 (mons) and dc2 (OSDs) are dead";
  EXPECT_TRUE(dead_zones.count("dc1"));
  EXPECT_TRUE(dead_zones.count("dc2"));
}

// Test: One monitor down but zone still up
TEST_F(StretchModeTest, PartialMonitorFailure) {
  setup_crush_three_zones();
  setup_osdmap_all_up();
  setup_monmap_three_zones();

  // One DC2 monitor down, but dc2-1 still in quorum
  set<int> quorum = {0, 1, 3, 4};
  
  // Populate monitor and OSD failure state
  map<string, set<string>> dead_mon_buckets;
  set<string> up_mon_buckets;
  set<string> dead_osd_buckets;
  set<string> up_osd_buckets;
  
  Monitor::populate_dead_mon_buckets(monmap, quorum, stretch_bucket_divider,
                                     dead_mon_buckets, up_mon_buckets);
  Monitor::populate_dead_osd_buckets(osdmap, dead_osd_buckets, up_osd_buckets);
  
  // Compute final degraded mode parameters
  set<string> dead_mons;
  set<string> dead_zones;
  Monitor::compute_dead_zones_and_mons(dead_mon_buckets, dead_osd_buckets,
                                       dead_mons, dead_zones);
  
  // Verify results - partial failure doesn't count as zone down
  EXPECT_EQ(dead_mons.size(), 0u) << "Zone still has live monitors";
  EXPECT_EQ(dead_zones.size(), 0u) << "No complete zone failures";
  EXPECT_EQ(dead_mon_buckets.size(), 0u) << "dc2 still has a live monitor";
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
