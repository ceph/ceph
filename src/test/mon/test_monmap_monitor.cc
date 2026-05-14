// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Test stretch mode logic in MonmapMonitor
 * 
 * Tests tiebreaker validation, auto-selection, and stretch mode enablement
 * for monitor-side stretch cluster configuration.
 */

#include "gtest/gtest.h"
#include "mon/MonMap.h"
#include "mon/MonmapMonitor.h"
#include "mon/Monitor.h"
#include "mon/Paxos.h"
#include "crush/CrushWrapper.h"
#include "common/ceph_context.h"
#include "global/global_init.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"

#include <memory>
#include <sstream>

using namespace std;

class MonmapMonitorStretchTest : public ::testing::Test {
protected:
  unique_ptr<CephContext> cct;
  MonMap monmap;
  CrushWrapper crush;
  
  void SetUp() override {
    vector<const char*> args;
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_MON));
    cct->_conf.set_val("mon_election_default_strategy", "3"); // connectivity
  }

  void setup_basic_monmap_5mons() {
    // Create 5 monitors: a, b (zone1), c, d (zone2), e (zone3)
    monmap.strategy = MonMap::CONNECTIVITY;
    
    entity_addrvec_t addrs_a;
    entity_addr_t addr_a;
    addr_a.parse("127.0.0.1:6789");
    addrs_a.v.push_back(addr_a);
    monmap.add("a", addrs_a);
    monmap.mon_info["a"].crush_loc["zone"] = "zone1";

    entity_addrvec_t addrs_b;
    entity_addr_t addr_b;
    addr_b.parse("127.0.0.2:6789");
    addrs_b.v.push_back(addr_b);
    monmap.add("b", addrs_b);
    monmap.mon_info["b"].crush_loc["zone"] = "zone1";

    entity_addrvec_t addrs_c;
    entity_addr_t addr_c;
    addr_c.parse("127.0.0.3:6789");
    addrs_c.v.push_back(addr_c);
    monmap.add("c", addrs_c);
    monmap.mon_info["c"].crush_loc["zone"] = "zone2";

    entity_addrvec_t addrs_d;
    entity_addr_t addr_d;
    addr_d.parse("127.0.0.4:6789");
    addrs_d.v.push_back(addr_d);
    monmap.add("d", addrs_d);
    monmap.mon_info["d"].crush_loc["zone"] = "zone2";

    entity_addrvec_t addrs_e;
    entity_addr_t addr_e;
    addr_e.parse("127.0.0.5:6789");
    addrs_e.v.push_back(addr_e);
    monmap.add("e", addrs_e);
    monmap.mon_info["e"].crush_loc["zone"] = "zone3";
  }

  void setup_crush_two_zones() {
    // Create a CRUSH map with two zones (zone1, zone2)
    crush.create();
    crush.set_max_devices(10);
    
    // Set up type hierarchy: root (type 10) > zone (type 8) > host (type 1) > osd (type 0)
    crush.set_type_name(10, "root");
    crush.set_type_name(8, "zone");
    crush.set_type_name(1, "host");
    crush.set_type_name(0, "osd");
    
    // Insert OSDs into zone1 and zone2
    // This will automatically create the zone buckets and make them valid subtrees
    map<string,string> loc_zone1;
    loc_zone1["root"] = "default";
    loc_zone1["zone"] = "zone1";
    loc_zone1["host"] = "host1";
    
    map<string,string> loc_zone2;
    loc_zone2["root"] = "default";
    loc_zone2["zone"] = "zone2";
    loc_zone2["host"] = "host2";
    
    // Insert a few OSDs in each zone to make them valid subtrees
    crush.insert_item(cct.get(), 0, 1.0, "osd.0", loc_zone1);
    crush.insert_item(cct.get(), 1, 1.0, "osd.1", loc_zone1);
    crush.insert_item(cct.get(), 2, 1.0, "osd.2", loc_zone2);
    crush.insert_item(cct.get(), 3, 1.0, "osd.3", loc_zone2);
  }

  void setup_monmap_bad_tiebreaker_in_zone1() {
    // Create 5 monitors where 'e' is in zone1 (same as a, b)
    monmap.strategy = MonMap::CONNECTIVITY;
    
    entity_addrvec_t addrs_a;
    entity_addr_t addr_a;
    addr_a.parse("127.0.0.1:6789");
    addrs_a.v.push_back(addr_a);
    monmap.add("a", addrs_a);
    monmap.mon_info["a"].crush_loc["zone"] = "zone1";
    
    entity_addrvec_t addrs_b;
    entity_addr_t addr_b;
    addr_b.parse("127.0.0.2:6789");
    addrs_b.v.push_back(addr_b);
    monmap.add("b", addrs_b);
    monmap.mon_info["b"].crush_loc["zone"] = "zone1";
    
    entity_addrvec_t addrs_c;
    entity_addr_t addr_c;
    addr_c.parse("127.0.0.3:6789");
    addrs_c.v.push_back(addr_c);
    monmap.add("c", addrs_c);
    monmap.mon_info["c"].crush_loc["zone"] = "zone2";
    
    entity_addrvec_t addrs_d;
    entity_addr_t addr_d;
    addr_d.parse("127.0.0.4:6789");
    addrs_d.v.push_back(addr_d);
    monmap.add("d", addrs_d);
    monmap.mon_info["d"].crush_loc["zone"] = "zone2";
    
    entity_addrvec_t addrs_e;
    entity_addr_t addr_e;
    addr_e.parse("127.0.0.5:6789");
    addrs_e.v.push_back(addr_e);
    monmap.add("e", addrs_e);
    monmap.mon_info["e"].crush_loc["zone"] = "zone1"; // BAD: in data zone
  }

  void setup_monmap_multiple_tiebreakers() {
    // Create 6 monitors with 2 in zone3 (e and f)
    monmap.strategy = MonMap::CONNECTIVITY;
    
    entity_addrvec_t addrs_a;
    entity_addr_t addr_a;
    addr_a.parse("127.0.0.1:6789");
    addrs_a.v.push_back(addr_a);
    monmap.add("a", addrs_a);
    monmap.mon_info["a"].crush_loc["zone"] = "zone1";
    
    entity_addrvec_t addrs_b;
    entity_addr_t addr_b;
    addr_b.parse("127.0.0.2:6789");
    addrs_b.v.push_back(addr_b);
    monmap.add("b", addrs_b);
    monmap.mon_info["b"].crush_loc["zone"] = "zone1";
    
    entity_addrvec_t addrs_c;
    entity_addr_t addr_c;
    addr_c.parse("127.0.0.3:6789");
    addrs_c.v.push_back(addr_c);
    monmap.add("c", addrs_c);
    monmap.mon_info["c"].crush_loc["zone"] = "zone2";
    
    entity_addrvec_t addrs_d;
    entity_addr_t addr_d;
    addr_d.parse("127.0.0.4:6789");
    addrs_d.v.push_back(addr_d);
    monmap.add("d", addrs_d);
    monmap.mon_info["d"].crush_loc["zone"] = "zone2";
    
    entity_addrvec_t addrs_e;
    entity_addr_t addr_e;
    addr_e.parse("127.0.0.5:6789");
    addrs_e.v.push_back(addr_e);
    monmap.add("e", addrs_e);
    monmap.mon_info["e"].crush_loc["zone"] = "zone3";
    
    entity_addrvec_t addrs_f;
    entity_addr_t addr_f;
    addr_f.parse("127.0.0.6:6789");
    addrs_f.v.push_back(addr_f);
    monmap.add("f", addrs_f);
    monmap.mon_info["f"].crush_loc["zone"] = "zone3"; // Multiple in zone3
  }

  void setup_monmap_zero_monitors_in_zone2() {
    // Create 3 monitors: a, b (zone1), e (zone3)
    // No monitors in zone2 - should fail validation
    monmap.strategy = MonMap::CONNECTIVITY;

    entity_addrvec_t addrs_a;
    entity_addr_t addr_a;
    addr_a.parse("127.0.0.1:6789");
    addrs_a.v.push_back(addr_a);
    monmap.add("a", addrs_a);
    monmap.mon_info["a"].crush_loc["zone"] = "zone1";

    entity_addrvec_t addrs_b;
    entity_addr_t addr_b;
    addr_b.parse("127.0.0.2:6789");
    addrs_b.v.push_back(addr_b);
    monmap.add("b", addrs_b);
    monmap.mon_info["b"].crush_loc["zone"] = "zone1";

    entity_addrvec_t addrs_e;
    entity_addr_t addr_e;
    addr_e.parse("127.0.0.5:6789");
    addrs_e.v.push_back(addr_e);
    monmap.add("e", addrs_e);
    monmap.mon_info["e"].crush_loc["zone"] = "zone3";
  }

  void setup_monmap_one_monitor_per_zone() {
    // Create 3 monitors: a (zone1), c (zone2), e (zone3)
    // Minimal valid configuration - 1 monitor per zone
    monmap.strategy = MonMap::CONNECTIVITY;

    entity_addrvec_t addrs_a;
    entity_addr_t addr_a;
    addr_a.parse("127.0.0.1:6789");
    addrs_a.v.push_back(addr_a);
    monmap.add("a", addrs_a);
    monmap.mon_info["a"].crush_loc["zone"] = "zone1";

    entity_addrvec_t addrs_c;
    entity_addr_t addr_c;
    addr_c.parse("127.0.0.3:6789");
    addrs_c.v.push_back(addr_c);
    monmap.add("c", addrs_c);
    monmap.mon_info["c"].crush_loc["zone"] = "zone2";

    entity_addrvec_t addrs_e;
    entity_addr_t addr_e;
    addr_e.parse("127.0.0.5:6789");
    addrs_e.v.push_back(addr_e);
    monmap.add("e", addrs_e);
    monmap.mon_info["e"].crush_loc["zone"] = "zone3";
  }
};

// Test success when explicitly supplied tiebreaker
TEST_F(MonmapMonitorStretchTest, ExplicitTiebreakerSuccess) {
  setup_basic_monmap_5mons();
  setup_crush_two_zones();
  
  stringstream ss;
  bool okay = false;
  int errcode = 0;
  
  // Test validation phase (commit=false)
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "e", "zone", crush);
  
  EXPECT_TRUE(okay) << "Validation failed: " << ss.str();
  EXPECT_EQ(errcode, 0);
  EXPECT_FALSE(monmap.stretch_mode_enabled) 
    << "Should not be enabled in validation phase";
  
  // Test commit phase (commit=true)
  ss.str("");
  okay = false;
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, true, "e", "zone", crush);
  
  EXPECT_TRUE(okay) << "Commit failed: " << ss.str();
  EXPECT_EQ(errcode, 0);
  EXPECT_TRUE(monmap.stretch_mode_enabled);
  EXPECT_EQ(monmap.tiebreaker_mon, "e");
  EXPECT_TRUE(monmap.disallowed_leaders.count("e"));
}

// Test success when auto-selecting of tiebreaker monitor
TEST_F(MonmapMonitorStretchTest, AutoSelectTiebreakerSuccess) {
  setup_basic_monmap_5mons();
  setup_crush_two_zones();
  
  stringstream ss;
  bool okay = false;
  int errcode = 0;
  
  // Test with empty tiebreaker_mon string (triggers auto-selection)
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "", "zone", crush);
  
  EXPECT_TRUE(okay) << "Auto-selection validation failed: " << ss.str();
  EXPECT_EQ(errcode, 0);
  
  // Commit with auto-selection
  ss.str("");
  okay = false;
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, true, "", "zone", crush);
  
  EXPECT_TRUE(okay) << "Auto-selection commit failed: " << ss.str();
  EXPECT_EQ(errcode, 0);
  EXPECT_TRUE(monmap.stretch_mode_enabled);
  EXPECT_EQ(monmap.tiebreaker_mon, "e") 
    << "Should auto-select 'e' as tiebreaker";
  EXPECT_TRUE(monmap.disallowed_leaders.count("e"));
}

// Test success when strategy is automatically changed to CONNECTIVITY
TEST_F(MonmapMonitorStretchTest, StrategyAutoChangedToConnectivity) {
  setup_basic_monmap_5mons();
  setup_crush_two_zones();
  
  // Start with classic strategy
  monmap.strategy = MonMap::CLASSIC;
  
  stringstream ss;
  bool okay = false;
  int errcode = 0;
  
  // Validation phase should succeed and prepare strategy change
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "e", "zone", crush);
  
  EXPECT_TRUE(okay) << "Validation should succeed: " << ss.str();
  EXPECT_EQ(errcode, 0);
  EXPECT_FALSE(monmap.stretch_mode_enabled) 
    << "Should not be enabled in validation phase";
  
  // Commit phase should enable stretch mode and change strategy
  ss.str("");
  okay = false;
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, true, "e", "zone", crush);
  
  EXPECT_TRUE(okay) << "Commit should succeed: " << ss.str();
  EXPECT_EQ(errcode, 0);
  EXPECT_TRUE(monmap.stretch_mode_enabled);
  EXPECT_EQ(monmap.strategy, MonMap::CONNECTIVITY) 
    << "Strategy should be changed to CONNECTIVITY";
  EXPECT_EQ(monmap.tiebreaker_mon, "e");
  EXPECT_TRUE(monmap.disallowed_leaders.count("e"));
}

// Test failure when auto-selecting and tiebreaker is in a data zone
TEST_F(MonmapMonitorStretchTest, AutoSelectFailTiebreakerInDataZone) {
  setup_monmap_bad_tiebreaker_in_zone1(); // 'e' is in zone1, not zone3
  setup_crush_two_zones();
  
  stringstream ss;
  bool okay = false;
  int errcode = 0;
  
  // Try auto-selection - should fail because 'e' is in zone1
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "", "zone", crush);
  
  EXPECT_FALSE(okay) << "Should fail when no tiebreaker in third zone";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_FALSE(monmap.stretch_mode_enabled);
  
  string error_msg = ss.str();
  EXPECT_TRUE(error_msg.find("Could not auto-select a tiebreaker monitor") != string::npos)
    << "Error message: " << error_msg;
  EXPECT_TRUE(error_msg.find("third") != string::npos)
    << "Should mention need for third location. Error: " << error_msg;
}

// Test failure when explicitly specifying tiebreaker in a data zone
TEST_F(MonmapMonitorStretchTest, ExplicitTiebreakerInDataZoneFails) {
  setup_monmap_bad_tiebreaker_in_zone1();
  setup_crush_two_zones();
  
  stringstream ss;
  bool okay = false;
  int errcode = 0;
  
  // Explicitly specify 'e' which is in zone1 (a data zone)
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "e", "zone", crush);
  
  EXPECT_FALSE(okay) << "Should fail when tiebreaker is in data zone";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_FALSE(monmap.stretch_mode_enabled);

  string error_msg = ss.str();
  EXPECT_TRUE(error_msg.find("one of the two data zones") != string::npos)
    << "Error message: " << error_msg;
}

// Test failure when multiple potential tiebreakers exist
TEST_F(MonmapMonitorStretchTest, AutoSelectFailMultipleTiebreakers) {
  setup_monmap_multiple_tiebreakers(); // Both 'e' and 'f' are in zone3
  setup_crush_two_zones();
  
  stringstream ss;
  bool okay = false;
  int errcode = 0;
  
  // Try auto-selection - should fail with multiple candidates
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "", "zone", crush);
  
  EXPECT_FALSE(okay) << "Should fail with multiple tiebreaker candidates";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_FALSE(monmap.stretch_mode_enabled);
  
  string error_msg = ss.str();
  EXPECT_TRUE(error_msg.find("Could not auto-select") != string::npos)
    << "Error message: " << error_msg;
  EXPECT_TRUE(error_msg.find("found 2 monitors") != string::npos ||
              error_msg.find("but need exactly 1") != string::npos)
    << "Should mention multiple monitors found. Error: " << error_msg;
}

// Test failure when tiebreaker monitor doesn't exist
TEST_F(MonmapMonitorStretchTest, NonExistentTiebreakerFails) {
  setup_basic_monmap_5mons();
  setup_crush_two_zones();
  
  stringstream ss;
  bool okay = false;
  int errcode = 0;
  
  // Specify a monitor that doesn't exist
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "nonexistent", "zone", crush);
  
  EXPECT_FALSE(okay) << "Should fail with non-existent monitor";
  EXPECT_EQ(errcode, -ENOENT);
  
  string error_msg = ss.str();
  EXPECT_TRUE(error_msg.find("does not seem to exist") != string::npos)
    << "Error message: " << error_msg;
}

// Test failure when one data zone has 0 monitors
TEST_F(MonmapMonitorStretchTest, ZeroMonitorsInDataZoneFails) {
  setup_monmap_zero_monitors_in_zone2(); // No monitors in zone2
  setup_crush_two_zones();

  stringstream ss;
  bool okay = false;
  int errcode = 0;

  // Try to enable stretch mode - should fail due to no monitors in zone2
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "e", "zone", crush);

  EXPECT_FALSE(okay) << "Should fail when a data zone has 0 monitors";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_FALSE(monmap.stretch_mode_enabled);

  string error_msg = ss.str();
  // Validation fails early when detecting monitors aren't in both CRUSH subtrees
  EXPECT_TRUE(error_msg.find("Could not find monitors in both CRUSH subtrees") != string::npos)
    << "Error message: " << error_msg;
  EXPECT_TRUE(error_msg.find("found monitors only in") != string::npos)
    << "Should mention which zones have monitors. Error: " << error_msg;
}

// Test success with minimal valid configuration (1 monitor per zone)
TEST_F(MonmapMonitorStretchTest, OneMonitorPerZoneSucceeds) {
  setup_monmap_one_monitor_per_zone(); // a (zone1), c (zone2), e (zone3)
  setup_crush_two_zones();

  stringstream ss;
  bool okay = false;
  int errcode = 0;

  // Validation phase with explicit tiebreaker
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "e", "zone", crush);

  EXPECT_TRUE(okay) << "Validation should succeed with 1 monitor per zone: " << ss.str();
  EXPECT_EQ(errcode, 0);

  // Commit phase
  ss.str("");
  okay = false;
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, true, "e", "zone", crush);

  EXPECT_TRUE(okay) << "Commit should succeed: " << ss.str();
  EXPECT_EQ(errcode, 0);
  EXPECT_TRUE(monmap.stretch_mode_enabled);
  EXPECT_EQ(monmap.tiebreaker_mon, "e");
}

// Test success with auto-selection and 1 monitor per zone
TEST_F(MonmapMonitorStretchTest, OneMonitorPerZoneAutoSelectSucceeds) {
  setup_monmap_one_monitor_per_zone(); // a (zone1), c (zone2), e (zone3)
  setup_crush_two_zones();

  stringstream ss;
  bool okay = false;
  int errcode = 0;

  // Auto-selection with minimal valid configuration
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, false, "", "zone", crush);

  EXPECT_TRUE(okay) << "Auto-selection should succeed: " << ss.str();
  EXPECT_EQ(errcode, 0);

  // Commit with auto-selection
  ss.str("");
  okay = false;
  MonmapMonitor::validate_and_enable_stretch_mode(
      monmap, monmap, ss, &okay, &errcode, true, "", "zone", crush);

  EXPECT_TRUE(okay) << "Commit should succeed: " << ss.str();
  EXPECT_EQ(errcode, 0);
  EXPECT_TRUE(monmap.stretch_mode_enabled);
  EXPECT_EQ(monmap.tiebreaker_mon, "e") << "Should auto-select 'e' as tiebreaker";
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
