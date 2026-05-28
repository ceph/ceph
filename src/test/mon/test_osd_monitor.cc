// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Test stretch mode logic in OSDMonitor
 * 
 * Tests pool validation for stretch mode enablement via the static
 * validate_stretch_mode_pools() function.
 */

#include "gtest/gtest.h"
#include "mon/OSDMonitor.h"
#include "osd/osd_types.h"
#include "crush/CrushWrapper.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "global/global_context.h"

#include <memory>
#include <sstream>
#include <map>
#include "include/mempool.h"

using namespace std;

class OSDMonitorStretchTest : public ::testing::Test {
protected:
  unique_ptr<CephContext> cct;
  CrushWrapper crush;
  mempool::osdmap::map<int64_t, string> pool_names;
  mempool::osdmap::map<int64_t, pg_pool_t> pools;
  
  void SetUp() override {
    vector<const char*> args;
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_MON));
    g_ceph_context = cct.get();
    common_init_finish(g_ceph_context);
    
    // Set up basic CRUSH map with minimal rules
    setup_basic_crush();
  }
  
  void TearDown() override {
    g_ceph_context = nullptr;
  }

  void setup_basic_crush() {
    crush.create();
    crush.set_max_devices(4);
    
    // Set up minimal type hierarchy
    crush.set_type_name(10, "root");
    crush.set_type_name(1, "host");
    crush.set_type_name(0, "osd");
    
    // Create simple replicated rule (TYPE_REPLICATED) - this is the stretch mode rule
    int rule_id = 0;
    crush_rule *rep_rule = crush_make_rule(2, CEPH_PG_TYPE_REPLICATED);  // 2 steps
    crush_rule_set_step(rep_rule, 0, CRUSH_RULE_TAKE, -1, 0);
    crush_rule_set_step(rep_rule, 1, CRUSH_RULE_EMIT, 0, 0);
    crush_add_rule(crush.get_crush_map(), rep_rule, rule_id);
    crush.set_rule_name(rule_id, "replicated_rule");
    
    // Create simple erasure rule (TYPE_ERASURE)
    rule_id = 1;
    crush_rule *ec_rule = crush_make_rule(2, CEPH_PG_TYPE_ERASURE);  // 2 steps
    crush_rule_set_step(ec_rule, 0, CRUSH_RULE_TAKE, -1, 0);
    crush_rule_set_step(ec_rule, 1, CRUSH_RULE_EMIT, 0, 0);
    crush_add_rule(crush.get_crush_map(), ec_rule, rule_id);
    crush.set_rule_name(rule_id, "ec_rule");
    
    // Create another replicated rule to use as "old" rule before stretch mode
    rule_id = 2;
    crush_rule *old_rule = crush_make_rule(2, CEPH_PG_TYPE_REPLICATED);  // 2 steps
    crush_rule_set_step(old_rule, 0, CRUSH_RULE_TAKE, -1, 0);
    crush_rule_set_step(old_rule, 1, CRUSH_RULE_EMIT, 0, 0);
    crush_add_rule(crush.get_crush_map(), old_rule, rule_id);
    crush.set_rule_name(rule_id, "old_replicated_rule");
  }

  pg_pool_t create_replicated_pool(int64_t pool_id, const string& pool_name, 
                                     uint32_t size = 3, uint32_t min_size = 2,
                                     int crush_rule = 0) {
    pg_pool_t pool;
    pool.type = pg_pool_t::TYPE_REPLICATED;
    pool.size = size;
    pool.min_size = min_size;
    pool.crush_rule = crush_rule;
    pool.set_pg_num(32);
    pool.set_pgp_num(32);
    
    pools[pool_id] = pool;
    pool_names[pool_id] = pool_name;
    
    return pool;
  }

  pg_pool_t create_ec_pool(int64_t pool_id, const string& pool_name,
                            uint32_t k = 2, uint32_t m = 1, int crush_rule = 1) {
    pg_pool_t pool;
    pool.type = pg_pool_t::TYPE_ERASURE;
    pool.size = k + m;
    pool.min_size = k;
    pool.crush_rule = crush_rule;
    pool.set_pg_num(32);
    pool.set_pgp_num(32);
    
    // Set erasure code profile name
    pool.erasure_code_profile = "testprofile";
    
    pools[pool_id] = pool;
    pool_names[pool_id] = pool_name;
    
    return pool;
  }

  void validate_pools(const string& rule_name, bool *okay, int *errcode, stringstream& ss) {
    OSDMonitor::validate_stretch_mode_pools(crush, pool_names, pools, ss, okay, errcode, rule_name);
  }
};

class OSDMonitorValidateStretchModeNewPoolTest : public ::testing::Test {
protected:
  unique_ptr<CephContext> cct;
  CrushWrapper crush;
  mempool::osdmap::map<int64_t, pg_pool_t> pools;
  string root_name = "default";
  string zone_failure_domain_name = "zone";
  string osd_failure_domain_name = "host";
  string mode = "firstn";
  bool force = false;
  int stretch_replica_rule;
  int stretch_ec_rule;

  stringstream ss;
  void SetUp() override {
    vector<const char*> args;
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_MON));
    g_ceph_context = cct.get();
    common_init_finish(g_ceph_context);

    setup_basic_crush();
  }

  void TearDown() override {
    g_ceph_context = nullptr;
  }

  void setup_basic_crush() {
    crush.create();
    crush.set_max_devices(12);
    
    crush.set_type_name(10, "root");
    crush.set_type_name(9, "zone");
    crush.set_type_name(8, "datacenter");
    crush.set_type_name(1, "host");
    crush.set_type_name(0, "osd");

    int default_root = 0;
    crush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		10, 0, NULL, NULL, &default_root);
    crush.set_item_name(default_root, "default");

    int zone1 = 0;
    crush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		9, 0, NULL, NULL, &zone1);
    crush.set_item_name(zone1, "zone1");

    int zone2 = 0;
    crush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		9, 0, NULL, NULL, &zone2);
    crush.set_item_name(zone2, "zone2");

    for (int i = 0; i < 6; i++) {
      int host_id = 0;
      crush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
          1, 0, NULL, NULL, &host_id);
      string host = "zone1-host" + std::to_string(i);
      crush.set_item_name(host_id, host);
      int osd_id = i;
      string osd_str = "osd." + std::to_string(osd_id);
      crush.set_item_name(osd_id, osd_str);
      crush.insert_item(g_ceph_context, osd_id, 1.0, osd_str,
          map<string, string>{{"host", host}, {"zone", "zone1"}, {"root", "default"}});
      
      crush.move_bucket(g_ceph_context, host_id, map<string, string>{{"zone", "zone1"}});
    }

    for (int i = 0; i < 6; i++) {
      int host_id = 0;
      crush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
          1, 0, NULL, NULL, &host_id);
      string host = "zone2-host" + std::to_string(i);
      crush.set_item_name(host_id, host);
      
      // Add OSD to this host
      int osd_id = i + 6;
      string osd_str = "osd." + std::to_string(osd_id);
      crush.set_item_name(osd_id, osd_str);
      crush.insert_item(g_ceph_context, osd_id, 1.0, osd_str,
          map<string, string>{{"host", host}, {"zone", "zone2"}, {"root", "default"}});
      
      crush.move_bucket(g_ceph_context, host_id, map<string, string>{{"zone", "zone2"}});
    }

    crush.move_bucket(g_ceph_context, zone1, map<string, string>{{"root", "default"}});
    crush.move_bucket(g_ceph_context, zone2, map<string, string>{{"root", "default"}});

    stretch_replica_rule = crush.add_simple_stretch_rule("stretch_replica_rule", root_name, zone_failure_domain_name,
      osd_failure_domain_name, 2, 2, "", mode, pg_pool_t::TYPE_REPLICATED, force, &ss);
    stretch_ec_rule = crush.add_simple_stretch_rule("stretch_ec_rule", root_name, zone_failure_domain_name,
      osd_failure_domain_name, 2, 6, "", mode, pg_pool_t::TYPE_ERASURE, force, &ss);
  }

  int validate_stretch_mode_new_pool(int new_crush_rule, int stretch_bucket_count, int stretch_mode_bucket,
      const string& zone_failure_domain, ostream *ss) {
    return OSDMonitor::validate_stretch_mode_new_pool(crush, new_crush_rule, stretch_bucket_count, stretch_mode_bucket,
      pools, zone_failure_domain, ss);
  }
};

// Test success when replicated pool has default size=3 and min_size=2
TEST_F(OSDMonitorStretchTest, ReplicatedPoolDefaultSizeSuccess) {
  create_replicated_pool(1, "test_pool", 3, 2, 0);
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("replicated_rule", &okay, &errcode, ss);
  
  EXPECT_TRUE(okay) << "Validation failed: " << ss.str();
  EXPECT_EQ(errcode, 0);
}

// Test failure when replicated pool has non-default size
TEST_F(OSDMonitorStretchTest, ReplicatedPoolWrongSizeFails) {
  create_replicated_pool(1, "test_pool", 5, 2, 2);  // Use old_replicated_rule (ID 2)
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("replicated_rule", &okay, &errcode, ss);
  
  EXPECT_FALSE(okay) << "Should have failed with size != 3";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_NE(ss.str().find("default size/min_size"), string::npos);
}

// Test failure when replicated pool has non-default min_size
TEST_F(OSDMonitorStretchTest, ReplicatedPoolWrongMinSizeFails) {
  create_replicated_pool(1, "test_pool", 3, 1, 2);  // Use old_replicated_rule (ID 2)
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("replicated_rule", &okay, &errcode, ss);
  
  EXPECT_FALSE(okay) << "Should have failed with min_size != 2";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_NE(ss.str().find("default size/min_size"), string::npos);
}

// Test success when EC pool is used (EC pools are allowed in stretch mode)
TEST_F(OSDMonitorStretchTest, ECPoolSuccess) {
  create_ec_pool(1, "test_ec_pool", 2, 1, 1);
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("ec_rule", &okay, &errcode, ss);
  
  EXPECT_TRUE(okay) << "EC pool validation failed: " << ss.str();
  EXPECT_EQ(errcode, 0);
}

// Test failure when specified CRUSH rule does not exist
TEST_F(OSDMonitorStretchTest, NonexistentCrushRuleFails) {
  create_replicated_pool(1, "test_pool", 3, 2, 0);
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("nonexistent_rule", &okay, &errcode, ss);
  
  EXPECT_FALSE(okay) << "Should fail with nonexistent rule";
  EXPECT_LT(errcode, 0) << "Should have negative error code";
  EXPECT_NE(ss.str().find("unrecognized crush rule"), string::npos);
}

// Test failure when replicated pool is paired with erasure-coded CRUSH rule
TEST_F(OSDMonitorStretchTest, WrongRuleTypeReplicatedPoolFails) {
  create_replicated_pool(1, "test_pool", 3, 2, 0);
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  // Try to use EC rule for replicated pool
  validate_pools("ec_rule", &okay, &errcode, ss);
  
  EXPECT_FALSE(okay) << "Should fail with rule type mismatch";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_NE(ss.str().find("replicated but crush rule"), string::npos);
  EXPECT_NE(ss.str().find("not a replicated rule"), string::npos);
}

// Test failure when EC pool is paired with replicated CRUSH rule
TEST_F(OSDMonitorStretchTest, WrongRuleTypeECPoolFails) {
  create_ec_pool(1, "ec_pool", 2, 1, 1);
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  // Try to use replicated rule for EC pool
  validate_pools("replicated_rule", &okay, &errcode, ss);
  
  EXPECT_FALSE(okay) << "Should fail with rule type mismatch";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_NE(ss.str().find("erasure-coded but crush rule"), string::npos);
  EXPECT_NE(ss.str().find("not an erasure-coded rule"), string::npos);
}

// Test success when multiple replicated pools all have correct configuration
TEST_F(OSDMonitorStretchTest, MultipleReplicatedPoolsSuccess) {
  create_replicated_pool(1, "pool1", 3, 2, 0);
  create_replicated_pool(2, "pool2", 3, 2, 0);
  create_replicated_pool(3, "pool3", 3, 2, 0);
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("replicated_rule", &okay, &errcode, ss);
  
  EXPECT_TRUE(okay) << "Multiple pools validation failed: " << ss.str();
  EXPECT_EQ(errcode, 0);
}

// Test success when multiple EC pools all have correct configuration
TEST_F(OSDMonitorStretchTest, MultipleECPoolsSuccess) {
  create_ec_pool(1, "ec_pool1", 2, 1, 1);
  create_ec_pool(2, "ec_pool2", 4, 2, 1);
  create_ec_pool(3, "ec_pool3", 3, 2, 1);
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("ec_rule", &okay, &errcode, ss);
  
  EXPECT_TRUE(okay) << "Multiple EC pools validation failed: " << ss.str();
  EXPECT_EQ(errcode, 0);
}

// Test failure when one pool has invalid configuration in a set of pools
TEST_F(OSDMonitorStretchTest, OneBadPoolFailsAll) {
  create_replicated_pool(1, "good_pool1", 3, 2, 0);
  create_replicated_pool(2, "bad_pool", 5, 2, 2);  // Wrong size, using old_replicated_rule
  create_replicated_pool(3, "good_pool2", 3, 2, 0);
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("replicated_rule", &okay, &errcode, ss);
  
  EXPECT_FALSE(okay) << "Should fail due to one bad pool";
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_NE(ss.str().find("bad_pool"), string::npos);
}

// Test success when validating an empty set of pools
TEST_F(OSDMonitorStretchTest, EmptyPoolSetSuccess) {
  // Don't create any pools
  
  bool okay = false;
  int errcode = 0;
  stringstream ss;
  
  validate_pools("replicated_rule", &okay, &errcode, ss);
  
  EXPECT_TRUE(okay) << "Empty pool set should succeed: " << ss.str();
  EXPECT_EQ(errcode, 0);
}

TEST_F(OSDMonitorValidateStretchModeNewPoolTest, RejectsRuleWithWrongBarrierType) {
  int r = validate_stretch_mode_new_pool(stretch_replica_rule, 2, crush.get_type_id(zone_failure_domain_name), "datacenter", &ss);

  EXPECT_EQ(r, -EINVAL);
  EXPECT_NE(ss.str().find("instead of " + zone_failure_domain_name), string::npos)
    << "RejectRuleWithWrongBarrierType failed: " << ss.str();
}

TEST_F(OSDMonitorValidateStretchModeNewPoolTest, RejectsRuleWithNoTakeOperations) {
  int rule_id = crush.add_rule(0, 0, pg_pool_t::TYPE_REPLICATED);
  crush.set_rule_name(rule_id, "empty_rule");
  
  int r = validate_stretch_mode_new_pool(rule_id, 2, crush.get_type_id(zone_failure_domain_name), zone_failure_domain_name, &ss);

  EXPECT_EQ(r, -EINVAL);
  EXPECT_NE(ss.str().find("has no take operations"), string::npos) 
    << "RejectsRuleWithNoTakeOperations: " << ss.str();
}

TEST_F(OSDMonitorValidateStretchModeNewPoolTest, RejectsRuleWithWrongNumberOfSites) {
  int r = validate_stretch_mode_new_pool(stretch_ec_rule, 3, crush.get_type_id(zone_failure_domain_name), 
      zone_failure_domain_name, &ss);

  EXPECT_EQ(r, -EINVAL);
  EXPECT_NE(ss.str().find("covers 2"), string::npos) 
    << "RejectsRuleWithWrongNumberOfSites: " << ss.str();
  EXPECT_NE(ss.str().find("stretch mode requires exactly 3"), string::npos)
    << "RejectsRuleWithWrongNumberOfSites: " << ss.str();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
