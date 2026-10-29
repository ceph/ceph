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
#include "osd/OSDMap.h"
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

// Test failure when an EC pool exists, as on releases before per-pool
// num_zones
TEST_F(OSDMonitorStretchTest, ECPoolFails) {
  create_ec_pool(1, "test_ec_pool", 2, 1, 1);

  bool okay = false;
  int errcode = 0;
  stringstream ss;

  validate_pools("replicated_rule", &okay, &errcode, ss);

  EXPECT_FALSE(okay);
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_EQ("stretched pools must be replicated; 'test_ec_pool' is erasure-coded",
            ss.str());
}

// Test failure when an individual stretch pool exists, which earlier releases
// took over
TEST_F(OSDMonitorStretchTest, IndividualStretchPoolFails) {
  create_replicated_pool(1, "test_pool", 3, 2, 0);
  pools[1].peering_crush_bucket_count = 2;
  pools[1].peering_crush_bucket_target = 3;
  pools[1].peering_crush_bucket_barrier = 1;

  bool okay = false;
  int errcode = 0;
  stringstream ss;

  validate_pools("replicated_rule", &okay, &errcode, ss);

  EXPECT_FALSE(okay);
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_EQ("pool 'test_pool' is a legacy individual stretch pool; legacy "
            "individual stretch pools and stretch mode cannot be combined, so "
            "unstretch it first with 'ceph osd pool stretch unset'",
            ss.str());
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

// Test failure when an EC pool exists, even with an EC rule
TEST_F(OSDMonitorStretchTest, ECPoolWithECRuleFails) {
  create_ec_pool(1, "ec_pool", 2, 1, 1);

  bool okay = false;
  int errcode = 0;
  stringstream ss;

  validate_pools("ec_rule", &okay, &errcode, ss);

  EXPECT_FALSE(okay);
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_NE(ss.str().find("must be replicated"), string::npos) << ss.str();
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

// Test failure when an EC pool is among replicated pools
TEST_F(OSDMonitorStretchTest, ECPoolAmongReplicatedPoolsFails) {
  create_replicated_pool(1, "pool1", 3, 2, 0);
  create_ec_pool(2, "ec_pool2", 4, 2, 1);
  create_replicated_pool(3, "pool3", 3, 2, 0);

  bool okay = false;
  int errcode = 0;
  stringstream ss;

  validate_pools("replicated_rule", &okay, &errcode, ss);

  EXPECT_FALSE(okay);
  EXPECT_EQ(errcode, -EINVAL);
  EXPECT_NE(ss.str().find("'ec_pool2' is erasure-coded"), string::npos)
    << ss.str();
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

// num_zones=1 was added to the default EC profile string in
// global.yaml.in (commit "mon: Add num_zones into EC profile.") under an
// old design where the erasure-code layer read num_zones back out of the
// ErasureCodeProfile map. That design was superseded: every real consumer
// of num_zones (ErasureCode::create_rule, OSDMonitor's crush_rule_create_*
// helpers, pool_opts_t::NUM_ZONES) now takes it as an explicit function
// parameter or a separate pool option, never a profile-map lookup. Nothing
// reads "num_zones" back out of a profile map any more, so the key is dead
// weight that still leaks into every default/created EC profile and shows
// up in `ceph osd erasure-code-profile get`.
//
// get_erasure_code_profile_default() is exactly the function both the
// mkfs bootstrap path and OSDMonitor::prepare_new_pool's auto-profile path
// use to materialize that default profile map.
TEST_F(OSDMonitorStretchTest, DefaultProfileHasNoDeadNumZonesKey) {
  OSDMap osdmap;
  map<string,string> profile_map;
  stringstream ss;

  int r = osdmap.get_erasure_code_profile_default(cct.get(), profile_map, &ss);

  ASSERT_EQ(r, 0) << ss.str();
  EXPECT_EQ(profile_map.count("num_zones"), 0u)
    << "default EC profile still carries a dead 'num_zones' key that no "
    << "code reads back out of a profile map";
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

// A stretch rule over the configured zones is accepted, with or without a device class.
TEST_F(OSDMonitorValidateStretchModeNewPoolTest, AcceptsRuleOverConfiguredZones) {
  int zone_type = crush.get_type_id(zone_failure_domain_name);
  pg_pool_t existing;
  existing.type = pg_pool_t::TYPE_REPLICATED;
  existing.crush_rule = stretch_replica_rule;
  existing.peering_crush_bucket_count = 2;
  existing.peering_crush_bucket_barrier = zone_type;
  pools[1] = existing;

  EXPECT_EQ(0, validate_stretch_mode_new_pool(stretch_ec_rule, 2, zone_type,
                                              zone_failure_domain_name, &ss))
    << ss.str();

  for (int i = 0; i < 12; i++) {
    ASSERT_GE(crush.update_device_class(i, "ssd", "osd." + std::to_string(i), &ss), 0);
  }
  int ssd_rule = crush.add_simple_stretch_rule("stretch_ec_ssd_rule", root_name,
    zone_failure_domain_name, osd_failure_domain_name, 2, 6, "ssd", "indep",
    pg_pool_t::TYPE_ERASURE, force, &ss);
  ASSERT_GE(ssd_rule, 0) << ss.str();
  EXPECT_EQ(0, validate_stretch_mode_new_pool(ssd_rule, 2, zone_type,
                                              zone_failure_domain_name, &ss))
    << ss.str();
}

// Rules over other zone buckets than existing stretch pools, or unknown types, are rejected.
TEST_F(OSDMonitorValidateStretchModeNewPoolTest, RejectsDifferentZonesAndUnknownType) {
  int zone_type = crush.get_type_id(zone_failure_domain_name);
  pg_pool_t existing;
  existing.type = pg_pool_t::TYPE_REPLICATED;
  existing.crush_rule = stretch_replica_rule;
  existing.peering_crush_bucket_count = 2;
  existing.peering_crush_bucket_barrier = zone_type;
  pools[1] = existing;

  int other_root = 0;
  crush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
                   10, 0, NULL, NULL, &other_root);
  crush.set_item_name(other_root, "other");
  crush.set_max_devices(20);
  for (int osd = 12; osd < 20; osd++) {
    string zone = osd < 16 ? "zone3" : "zone4";
    crush.insert_item(g_ceph_context, osd, 1.0, "osd." + std::to_string(osd),
      map<string, string>{{"host", "other-host" + std::to_string(osd)},
                          {"zone", zone}, {"root", "other"}});
  }
  int other_rule = crush.add_simple_stretch_rule("other_stretch_rule", "other",
    zone_failure_domain_name, osd_failure_domain_name, 2, 2, "", mode,
    pg_pool_t::TYPE_REPLICATED, force, &ss);
  ASSERT_GE(other_rule, 0) << ss.str();

  EXPECT_EQ(-EINVAL, validate_stretch_mode_new_pool(other_rule, 2, zone_type,
                                                    zone_failure_domain_name, &ss));
  EXPECT_NE(ss.str().find("uses different"), string::npos) << ss.str();

  stringstream ss2;
  EXPECT_EQ(-EINVAL, validate_stretch_mode_new_pool(stretch_ec_rule, 2, zone_type,
                                                    "nosuchtype", &ss2));
  EXPECT_NE(ss2.str().find("does not exist"), string::npos) << ss2.str();
}

class OSDMonitorStretchTransitionTest : public ::testing::Test {
protected:
  static constexpr int zone1 = -2;
  static constexpr int zone2 = -3;
  static constexpr epoch_t epoch = 100;
  mempool::osdmap::map<int64_t, pg_pool_t> pools;
  OSDMap::Incremental pending_inc{epoch};

  static pg_pool_t make_pool(uint32_t bucket_count,
                             int mandatory_member = CRUSH_ITEM_NONE) {
    pg_pool_t p;
    p.type = pg_pool_t::TYPE_REPLICATED;
    p.size = 4;
    p.min_size = 2;
    p.peering_crush_bucket_count = bucket_count;
    p.peering_crush_bucket_target = bucket_count ? 2 : 0;
    p.peering_crush_mandatory_member = mandatory_member;
    // a stretch pool is over zones, with min_size per zone
    p.num_zones = bucket_count ? 2 : 1;
    p.replica = p.size / p.num_zones;
    return p;
  }

  // the transitions change only the bucket count and the mandatory member:
  // never size, min_size or the bucket target
  void expect_transitioned(int64_t id, uint32_t bucket_count,
                           int mandatory_member) {
    SCOPED_TRACE("pool " + std::to_string(id));
    ASSERT_TRUE(pending_inc.new_pools.contains(id));
    const pg_pool_t& p = pending_inc.new_pools.at(id);
    EXPECT_EQ(bucket_count, p.peering_crush_bucket_count);
    EXPECT_EQ(mandatory_member, p.peering_crush_mandatory_member);
    EXPECT_EQ(epoch, p.get_last_force_op_resend());
    EXPECT_EQ(4u, p.size);
    EXPECT_EQ(2u, p.min_size);
    EXPECT_EQ(2u, p.peering_crush_bucket_target);
  }
};

// Pool changes prepared in the same incremental as the transition: a pool
// set to num_zones 2 (2), a pool set to num_zones 1 (3), a pool deletion (4)
// and a two-zone pool create (6).
TEST_F(OSDMonitorStretchTransitionTest, DegradedCoversPendingPools) {
  pools[1] = make_pool(2);
  pools[2] = make_pool(0);
  pending_inc.new_pools[2] = make_pool(2);
  pools[3] = make_pool(2);
  pending_inc.new_pools[3] = make_pool(0, 0);
  pools[4] = make_pool(2);
  pending_inc.old_pools.insert(4);
  pools[5] = make_pool(0);
  pending_inc.new_pools[6] = make_pool(2);

  OSDMonitor::apply_stretch_transition_to_pools(pools, pending_inc, 1, zone2);

  expect_transitioned(1, 1, zone2);
  expect_transitioned(2, 1, zone2);
  expect_transitioned(6, 1, zone2);
  const pg_pool_t& unstretched = pending_inc.new_pools.at(3);
  EXPECT_EQ(0u, unstretched.peering_crush_bucket_count);
  EXPECT_EQ(0, unstretched.peering_crush_mandatory_member);
  EXPECT_EQ(0u, unstretched.get_last_force_op_resend());
  EXPECT_FALSE(pending_inc.new_pools.contains(4));
  EXPECT_FALSE(pending_inc.new_pools.contains(5));
}

// A pool created from the degraded committed map before the healthy
// transition commits.
TEST_F(OSDMonitorStretchTransitionTest, HealthyCoversPendingPools) {
  pools[1] = make_pool(1, zone1);
  pending_inc.new_pools[2] = make_pool(1, zone1);

  OSDMonitor::apply_stretch_transition_to_pools(pools, pending_inc, 2,
                                                CRUSH_ITEM_NONE);

  expect_transitioned(1, 2, CRUSH_ITEM_NONE);
  expect_transitioned(2, 2, CRUSH_ITEM_NONE);
}

TEST_F(OSDMonitorStretchTransitionTest, RecoveryCoversPendingPools) {
  pools[1] = make_pool(1, zone1);
  pending_inc.new_pools[2] = make_pool(1, zone1);
  pools[3] = make_pool(1, zone1);
  pending_inc.old_pools.insert(3);

  OSDMonitor::apply_stretch_transition_to_pools(pools, pending_inc);

  expect_transitioned(1, 1, zone1);
  expect_transitioned(2, 1, zone1);
  EXPECT_FALSE(pending_inc.new_pools.contains(3));
}

// Before the upgrade is committed a pool of global stretch mode keeps the
// earlier release's one zone and min_size over all its copies, which that
// release's OSDs read. The transitions must change min_size as its monitor
// did: halved when a zone is lost, mon_stretch_pool_min_size once healthy.
// Otherwise older OSDs require copies in both zones while OSDs of this
// release write with fewer.
class OSDMonitorPreCommitStretchTransitionTest
  : public OSDMonitorStretchTransitionTest {
protected:
  unique_ptr<CephContext> cct;

  void SetUp() override {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_MON));
    g_ceph_context = cct.get();
    common_init_finish(g_ceph_context);
  }

  void TearDown() override {
    g_ceph_context = nullptr;
  }

  static pg_pool_t make_legacy_pool() {
    pg_pool_t p = make_pool(2);
    p.num_zones = 1;
    p.replica = p.size;
    return p;
  }
};

TEST_F(OSDMonitorPreCommitStretchTransitionTest, MinSizeAsEarlierRelease) {
  pools[1] = make_legacy_pool();
  OSDMonitor::apply_stretch_transition_to_pools(pools, pending_inc, 1, zone2);
  ASSERT_TRUE(pending_inc.new_pools.contains(1));
  EXPECT_EQ(1u, pending_inc.new_pools.at(1).min_size);

  // as the earlier release's monitor left the pool in degraded stretch mode
  pg_pool_t degraded = make_legacy_pool();
  degraded.peering_crush_bucket_count = 1;
  degraded.peering_crush_mandatory_member = zone2;
  degraded.min_size = 1;
  pools[1] = degraded;
  OSDMap::Incremental recovery_inc{epoch + 1};
  OSDMonitor::apply_stretch_transition_to_pools(pools, recovery_inc);
  ASSERT_TRUE(recovery_inc.new_pools.contains(1));
  EXPECT_EQ(1u, recovery_inc.new_pools.at(1).min_size);

  OSDMap::Incremental healthy_inc{epoch + 2};
  OSDMonitor::apply_stretch_transition_to_pools(pools, healthy_inc, 2,
                                                CRUSH_ITEM_NONE);
  ASSERT_TRUE(healthy_inc.new_pools.contains(1));
  EXPECT_EQ(cct->_conf.get_val<uint64_t>("mon_stretch_pool_min_size"),
            healthy_inc.new_pools.at(1).min_size);
}

// The update that commits the upgrade gives each pool of global stretch mode
// its two zones and min_size per zone, never fewer copies than before, and
// forces a resend on every stretch pool, as the peering rules change.
class OSDMonitorStretchCommitTest
  : public OSDMonitorValidateStretchModeNewPoolTest {
protected:
  static constexpr int zone_type = 9;
  OSDMap tmp;
  epoch_t commit_epoch = 0;
  OSDMap::Incremental inc;
  int zone1_id = 0;

  void SetUp() override {
    OSDMonitorValidateStretchModeNewPoolTest::SetUp();
    zone1_id = crush.get_item_id("zone1");
  }

  // a pool of global stretch mode as earlier releases have it
  pg_pool_t legacy_pool(unsigned size, unsigned min_size) const {
    pg_pool_t p;
    p.type = pg_pool_t::TYPE_REPLICATED;
    p.size = size;
    p.min_size = min_size;
    p.num_zones = 1;
    p.replica = size;
    p.crush_rule = stretch_replica_rule;
    p.set_pg_num(8);
    p.set_pgp_num(8);
    p.peering_crush_bucket_count = 2;
    p.peering_crush_bucket_target = 2;
    p.peering_crush_bucket_barrier = zone_type;
    p.peering_crush_mandatory_member = CRUSH_ITEM_NONE;
    return p;
  }

  // as the monitor leaves it once zone2 is lost
  pg_pool_t degraded(pg_pool_t p, int mandatory) const {
    p.peering_crush_bucket_count = 1;
    p.peering_crush_mandatory_member = mandatory;
    return p;
  }

  // tmp holds the pools before the commit, in global stretch mode or not,
  // degraded (and recovering) or not
  void make_map(const map<int64_t, pg_pool_t>& pools, bool stretch_mode,
                bool is_degraded = false, bool recovering = false) {
    uuid_d fsid;
    tmp.build_simple(g_ceph_context, 1, fsid, 12);
    bufferlist crush_bl;
    crush.encode(crush_bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
    auto p = crush_bl.cbegin();
    tmp.crush->decode(p);
    tmp.stretch_mode_enabled = stretch_mode;
    tmp.stretch_bucket_count = stretch_mode ? 2 : 0;
    tmp.degraded_stretch_mode = is_degraded ? 1 : 0;
    tmp.recovering_stretch_mode = recovering ? 1 : 0;
    tmp.stretch_mode_bucket = stretch_mode ? zone_type : 0;
    OSDMap::Incremental setup(tmp.get_epoch() + 1);
    setup.fsid = tmp.get_fsid();
    setup.new_flags = tmp.get_flags() & ~CEPH_OSDMAP_STRETCH_ZONES;
    for (const auto& [id, pool] : pools) {
      setup.new_pools[id] = pool;
      setup.new_pool_names[id] = "pool" + std::to_string(id);
    }
    ASSERT_EQ(0, tmp.apply_incremental(setup));
    commit_epoch = tmp.get_epoch() + 1;
    inc = OSDMap::Incremental(commit_epoch);
  }

  void convert(unsigned stretch_pool_min_size = 2) {
    OSDMonitor::convert_stretch_pools_at_commit(tmp, inc,
                                                stretch_pool_min_size);
  }

  const pg_pool_t& converted(int64_t id) {
    EXPECT_TRUE(inc.new_pools.contains(id)) << "pool " << id;
    return inc.new_pools[id];
  }

  void expect_zones(int64_t id, unsigned min_size) {
    SCOPED_TRACE("pool " + std::to_string(id));
    const pg_pool_t& p = converted(id);
    EXPECT_EQ(2u, p.get_num_zones());
    EXPECT_EQ(2u, p.get_replica());
    EXPECT_EQ(4u, p.get_size());
    EXPECT_EQ(min_size, p.get_min_size());
    EXPECT_EQ(commit_epoch, p.get_last_force_op_resend());
  }

  void expect_one_zone(int64_t id, const pg_pool_t& before) {
    SCOPED_TRACE("pool " + std::to_string(id));
    const pg_pool_t& p = converted(id);
    EXPECT_EQ(1u, p.get_num_zones());
    EXPECT_EQ(before.get_replica(), p.get_replica());
    EXPECT_EQ(before.get_size(), p.get_size());
    EXPECT_EQ(before.get_min_size(), p.get_min_size());
    EXPECT_EQ(commit_epoch, p.get_last_force_op_resend());
  }
};

// min_size 2 over four copies becomes 1 per zone, and an odd total rounds up
TEST_F(OSDMonitorStretchCommitTest, HealthyCommitGivesZones) {
  make_map({{1, legacy_pool(4, 1)}, {2, legacy_pool(4, 2)},
            {3, legacy_pool(4, 3)}, {4, legacy_pool(4, 4)}}, true);
  convert();
  expect_zones(1, 1);
  expect_zones(2, 1);
  expect_zones(3, 2);
  expect_zones(4, 2);
  EXPECT_EQ(CRUSH_ITEM_NONE, converted(2).peering_crush_mandatory_member);
}

// Degraded, the pool's min_size was halved and becoming healthy no longer
// restores it, so it comes from mon_stretch_pool_min_size
TEST_F(OSDMonitorStretchCommitTest, DegradedCommitUsesStretchPoolMinSize) {
  for (unsigned stretch_min_size : {2u, 3u, 4u}) {
    SCOPED_TRACE("mon_stretch_pool_min_size " +
                 std::to_string(stretch_min_size));
    make_map({{1, degraded(legacy_pool(4, stretch_min_size / 2), zone1_id)}},
             true, true);
    convert(stretch_min_size);
    expect_zones(1, (stretch_min_size + 1) / 2);
    EXPECT_EQ(1u, converted(1).peering_crush_bucket_count);
    EXPECT_EQ(zone1_id, converted(1).peering_crush_mandatory_member);
  }
}

TEST_F(OSDMonitorStretchCommitTest, RecoveringCommitUsesStretchPoolMinSize) {
  make_map({{1, degraded(legacy_pool(4, 1), zone1_id)}}, true, true, true);
  convert(3);
  expect_zones(1, 2);
}

// A pool created while degraded has no mandatory zone; per zone it would
// need copies in the lost zone too, so it gets the surviving one
TEST_F(OSDMonitorStretchCommitTest, PoolCreatedWhileDegradedGetsSurvivingZone) {
  make_map({{1, degraded(legacy_pool(4, 1), zone1_id)},
            {2, degraded(legacy_pool(4, 1), CRUSH_ITEM_NONE)}}, true, true);
  convert();
  expect_zones(2, 1);
  EXPECT_EQ(zone1_id, converted(2).peering_crush_mandatory_member);
}

// Pools whose copies are not evenly in two zones keep one zone and their
// min_size, but are forced to resend as the peering rules change
TEST_F(OSDMonitorStretchCommitTest, OtherStretchPoolsKeepOneZone) {
  pg_pool_t odd = legacy_pool(3, 2);
  pg_pool_t one_bucket = legacy_pool(2, 1);  // an earlier leader, degraded
  one_bucket.peering_crush_bucket_count = 1;
  one_bucket.peering_crush_bucket_target = 1;
  pg_pool_t erasure = legacy_pool(4, 3);
  erasure.type = pg_pool_t::TYPE_ERASURE;
  erasure.crush_rule = stretch_ec_rule;
  pg_pool_t uneven = legacy_pool(6, 3);  // the rule puts 2 in each zone
  make_map({{1, odd}, {2, one_bucket}, {3, erasure}, {4, uneven}}, true);
  convert();
  expect_one_zone(1, odd);
  expect_one_zone(2, one_bucket);
  expect_one_zone(3, erasure);
  expect_one_zone(4, uneven);
}

// Without global stretch mode a stretch pool is an individual stretch pool
TEST_F(OSDMonitorStretchCommitTest, IndividualStretchPoolKeepsOneZone) {
  const pg_pool_t p = legacy_pool(4, 2);
  make_map({{1, p}}, false);
  convert();
  expect_one_zone(1, p);
}

TEST_F(OSDMonitorStretchCommitTest, PlainAndDeletedPoolsUntouched) {
  pg_pool_t plain = legacy_pool(3, 2);
  plain.peering_crush_bucket_count = 0;
  plain.peering_crush_bucket_target = 0;
  plain.peering_crush_bucket_barrier = 0;
  make_map({{1, plain}, {2, legacy_pool(4, 2)}}, true);
  inc.old_pools.insert(2);
  convert();
  EXPECT_FALSE(inc.new_pools.contains(1));
  EXPECT_FALSE(inc.new_pools.contains(2));
}

// A change the pending update made after tmp was built is kept
TEST_F(OSDMonitorStretchCommitTest, KeepsPendingPoolChanges) {
  make_map({{1, legacy_pool(4, 2)}}, true);
  pg_pool_t pending = *tmp.get_pg_pool(1);
  pending.flags |= pg_pool_t::FLAG_OMAP;
  inc.new_pools[1] = pending;
  convert();
  expect_zones(1, 1);
  EXPECT_TRUE(converted(1).has_flag(pg_pool_t::FLAG_OMAP));
}

class OSDMonitorStretchPlanTest : public ::testing::Test {
protected:
  mempool::osdmap::map<int64_t, pg_pool_t> pools;

  void add_pool(int64_t id, int type, int num_zones, int replica) {
    pg_pool_t p;
    p.type = type;
    p.num_zones = num_zones;
    p.replica = replica;
    p.size = type == pg_pool_t::TYPE_REPLICATED ? num_zones * replica
                                                : num_zones * 3;
    pools[id] = p;
  }

  map<int64_t, OSDMonitor::StretchModeChange> plan(bool enable) {
    return OSDMonitor::plan_stretch_mode_changes(pools, enable);
  }
};

using Change = OSDMonitor::StretchModeChange;

// Test nothing changes when there are no pools
TEST_F(OSDMonitorStretchPlanTest, NoPools) {
  EXPECT_TRUE(plan(true).empty());
  EXPECT_TRUE(plan(false).empty());
}

// Test enabling stretches a local replicated pool of any size
TEST_F(OSDMonitorStretchPlanTest, EnableStretchesLocalReplicated) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 1, 3);
  add_pool(2, pg_pool_t::TYPE_REPLICATED, 1, 1);
  add_pool(3, pg_pool_t::TYPE_REPLICATED, 1, 4);
  const map<int64_t, Change> expected = {
    {1, Change::STRETCH}, {2, Change::STRETCH}, {3, Change::STRETCH}};
  EXPECT_EQ(expected, plan(true));
}

// Test enabling stretches a local EC pool
TEST_F(OSDMonitorStretchPlanTest, EnableStretchesLocalErasure) {
  add_pool(1, pg_pool_t::TYPE_ERASURE, 1, 0);
  const map<int64_t, Change> expected = {{1, Change::STRETCH}};
  EXPECT_EQ(expected, plan(true));
}

// Test enabling leaves a stretched replicated pool with 2 replicas alone
TEST_F(OSDMonitorStretchPlanTest, EnableKeepsStretchedReplicaTwo) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 2, 2);
  EXPECT_TRUE(plan(true).empty());
}

// Test enabling gives a stretched replicated pool 2 replicas per zone
TEST_F(OSDMonitorStretchPlanTest, EnableResetsStretchedReplica) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 2, 3);
  add_pool(2, pg_pool_t::TYPE_REPLICATED, 2, 1);
  const map<int64_t, Change> expected = {
    {1, Change::REPLICA}, {2, Change::REPLICA}};
  EXPECT_EQ(expected, plan(true));
}

// Test enabling leaves a stretched EC pool alone
TEST_F(OSDMonitorStretchPlanTest, EnableKeepsStretchedErasure) {
  add_pool(1, pg_pool_t::TYPE_ERASURE, 2, 0);
  EXPECT_TRUE(plan(true).empty());
}

// Test disabling unstretches every stretched pool
TEST_F(OSDMonitorStretchPlanTest, DisableUnstretchesStretchedPools) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 2, 2);
  add_pool(2, pg_pool_t::TYPE_ERASURE, 2, 0);
  add_pool(3, pg_pool_t::TYPE_REPLICATED, 2, 3);
  const map<int64_t, Change> expected = {
    {1, Change::UNSTRETCH}, {2, Change::UNSTRETCH}, {3, Change::UNSTRETCH}};
  EXPECT_EQ(expected, plan(false));
}

// Test disabling leaves local pools alone, whatever their size
TEST_F(OSDMonitorStretchPlanTest, DisableKeepsLocalPools) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 1, 2);
  add_pool(2, pg_pool_t::TYPE_ERASURE, 1, 0);
  EXPECT_TRUE(plan(false).empty());
}

// Test a mixture of pools
TEST_F(OSDMonitorStretchPlanTest, MixedPools) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 1, 3);
  add_pool(2, pg_pool_t::TYPE_REPLICATED, 2, 2);
  add_pool(3, pg_pool_t::TYPE_ERASURE, 1, 0);
  add_pool(4, pg_pool_t::TYPE_ERASURE, 2, 0);
  const map<int64_t, Change> enable = {
    {1, Change::STRETCH}, {3, Change::STRETCH}};
  EXPECT_EQ(enable, plan(true));
  const map<int64_t, Change> disable = {
    {2, Change::UNSTRETCH}, {4, Change::UNSTRETCH}};
  EXPECT_EQ(disable, plan(false));
}

class OSDMonitorEnableStretchModeTest
  : public OSDMonitorValidateStretchModeNewPoolTest {
protected:
  mempool::osdmap::map<int64_t, string> pool_names;

  void add_pool(int64_t id, const string& name, int type, bool fast_ec = false) {
    pg_pool_t p;
    p.type = type;
    p.num_zones = 1;
    p.replica = type == pg_pool_t::TYPE_REPLICATED ? 3 : 0;
    p.size = 3;
    if (fast_ec) {
      p.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
    }
    pools[id] = p;
    pool_names[id] = name;
  }

  int validate(const string& rule, const string& dividing_bucket) {
    ss.str("");
    return OSDMonitor::validate_enable_stretch_mode(
      crush, pool_names, pools, rule, dividing_bucket, &ss);
  }
};

// Test success with a replicated stretch rule across the dividing bucket
TEST_F(OSDMonitorEnableStretchModeTest, ReplicatedStretchRuleSucceeds) {
  add_pool(1, "rbd", pg_pool_t::TYPE_REPLICATED);
  EXPECT_EQ(0, validate("stretch_replica_rule", "zone")) << ss.str();
}

// Test success with no pools at all
TEST_F(OSDMonitorEnableStretchModeTest, NoPoolsSucceeds) {
  EXPECT_EQ(0, validate("stretch_replica_rule", "zone")) << ss.str();
}

// Test success with a FastEC pool
TEST_F(OSDMonitorEnableStretchModeTest, FastECPoolSucceeds) {
  add_pool(1, "rbd", pg_pool_t::TYPE_REPLICATED);
  add_pool(2, "ecpool", pg_pool_t::TYPE_ERASURE, true);
  EXPECT_EQ(0, validate("stretch_replica_rule", "zone")) << ss.str();
}

// Test failure with a legacy EC pool, which can never be stretched
TEST_F(OSDMonitorEnableStretchModeTest, LegacyECPoolFails) {
  add_pool(1, "rbd", pg_pool_t::TYPE_REPLICATED);
  add_pool(2, "oldec", pg_pool_t::TYPE_ERASURE);
  EXPECT_EQ(-EINVAL, validate("stretch_replica_rule", "zone"));
  EXPECT_EQ("pool 'oldec' is a legacy EC pool, which cannot be stretched; "
            "convert it to FastEC first with 'ceph osd pool set oldec "
            "allow_ec_optimizations true'", ss.str());
}

// Test failure when the dividing bucket is not a CRUSH type
TEST_F(OSDMonitorEnableStretchModeTest, UnknownDividingBucketFails) {
  EXPECT_EQ(-ENOENT, validate("stretch_replica_rule", "rack"));
  EXPECT_EQ("rack is not a valid crush bucket type", ss.str());
}

// Test failure when the rule does not exist
TEST_F(OSDMonitorEnableStretchModeTest, UnknownRuleFails) {
  EXPECT_GT(0, validate("no_such_rule", "zone"));
  EXPECT_EQ("unrecognized crush rule no_such_rule", ss.str());
}

// Test failure when the rule is an EC rule
TEST_F(OSDMonitorEnableStretchModeTest, ErasureRuleFails) {
  EXPECT_EQ(-EINVAL, validate("stretch_ec_rule", "zone"));
  EXPECT_EQ("crush rule stretch_ec_rule is not a replicated rule", ss.str());
}

// Test failure when the rule is stretched across another bucket type
TEST_F(OSDMonitorEnableStretchModeTest, RuleAcrossOtherTypeFails) {
  EXPECT_EQ(-EINVAL, validate("stretch_replica_rule", "datacenter"));
  EXPECT_NE(ss.str().find("covers 0 datacenter buckets, but stretch mode "
                          "requires exactly 2"), string::npos) << ss.str();
}

class OSDMonitorDisableStretchModeTest
  : public OSDMonitorValidateStretchModeNewPoolTest {
protected:
  int local_rule = -1;

  void SetUp() override {
    OSDMonitorValidateStretchModeNewPoolTest::SetUp();
    local_rule = crush.add_simple_rule("local_rule", root_name,
                                       osd_failure_domain_name, 0, "",
                                       "firstn", pg_pool_t::TYPE_REPLICATED,
                                       &ss);
    ASSERT_GE(local_rule, 0) << ss.str();
  }

  void add_pool(int64_t id, int type, int num_zones, int crush_rule) {
    pg_pool_t p;
    p.type = type;
    p.num_zones = num_zones;
    p.crush_rule = crush_rule;
    pools[id] = p;
  }

  int validate(const string& rule, bool recovering = false) {
    ss.str("");
    return OSDMonitor::validate_disable_stretch_mode(crush, pools, rule,
                                                     recovering, &ss);
  }
};

// Test success without a rule
TEST_F(OSDMonitorDisableStretchModeTest, NoRuleSucceeds) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 2, stretch_replica_rule);
  EXPECT_EQ(0, validate("")) << ss.str();
}

// Test success with a replicated rule that no stretched pool uses
TEST_F(OSDMonitorDisableStretchModeTest, NewReplicatedRuleSucceeds) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 2, stretch_replica_rule);
  EXPECT_EQ(0, validate("local_rule")) << ss.str();
}

// Test success when only a local pool already uses the rule
TEST_F(OSDMonitorDisableStretchModeTest, RuleOfLocalPoolSucceeds) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 2, stretch_replica_rule);
  add_pool(2, pg_pool_t::TYPE_REPLICATED, 1, local_rule);
  EXPECT_EQ(0, validate("local_rule")) << ss.str();
}

// Test failure while stretch mode recovers
TEST_F(OSDMonitorDisableStretchModeTest, RecoveringFails) {
  EXPECT_EQ(-EBUSY, validate("", true));
  EXPECT_EQ("stretch mode is currently recovering and cannot be disabled",
            ss.str());
}

// Test failure when the rule does not exist
TEST_F(OSDMonitorDisableStretchModeTest, UnknownRuleFails) {
  EXPECT_EQ(-EINVAL, validate("no_such_rule"));
  EXPECT_EQ("unrecognized crush rule no_such_rule", ss.str());
}

// Test failure when the rule is an EC rule
TEST_F(OSDMonitorDisableStretchModeTest, ErasureRuleFails) {
  EXPECT_EQ(-EINVAL, validate("stretch_ec_rule"));
  EXPECT_EQ("crush rule stretch_ec_rule type does not match pool type",
            ss.str());
}

// Test failure when a stretched replicated pool already uses the rule
TEST_F(OSDMonitorDisableStretchModeTest, SameRuleFails) {
  add_pool(1, pg_pool_t::TYPE_REPLICATED, 2, stretch_replica_rule);
  EXPECT_EQ(-EINVAL, validate("stretch_replica_rule"));
  EXPECT_EQ("You can't disable stretch mode with the same crush rule you are "
            "using", ss.str());
}

class OSDMonitorStretchSetMinSizeTest : public ::testing::Test {
protected:
  stringstream ss;

  int check(int64_t min_size, int64_t size) {
    ss.str("");
    return OSDMonitor::stretch_set_min_size(min_size, size, &ss);
  }
};

// Test any min_size from 1 to the pool size is accepted: an individual stretch
// pool keeps it as a total over all its copies, as in earlier releases
TEST_F(OSDMonitorStretchSetMinSizeTest, OneToSize) {
  for (int64_t min_size : {1, 2, 3, 4}) {
    EXPECT_EQ(0, check(min_size, 4)) << ss.str();
  }
}

// Test failure for a min_size above the pool size
TEST_F(OSDMonitorStretchSetMinSizeTest, AboveSizeFails) {
  EXPECT_EQ(-EINVAL, check(5, 4));
  EXPECT_EQ("pool min_size must be from 1 to the pool size 4", ss.str());
}

// Test failure for a min_size of 0 or less
TEST_F(OSDMonitorStretchSetMinSizeTest, BelowOneFails) {
  EXPECT_EQ(-EINVAL, check(0, 4));
  EXPECT_EQ(-EINVAL, check(-1, 4));
}

// A k=2 m=1 EC pool
class OSDMonitorStretchUnsetECTest : public ::testing::Test {
protected:
  stringstream ss;

  int check(int num_zones, int64_t size, int64_t min_size) {
    ss.str("");
    return OSDMonitor::check_stretch_unset_ec("ec", num_zones, 2, 3, size,
                                              min_size, &ss);
  }
};

// Test an older release's stretch is cleared with size k+m and min_size k
TEST_F(OSDMonitorStretchUnsetECTest, OneZoneMinSizeKSucceeds) {
  EXPECT_EQ(0, check(1, 3, 2)) << ss.str();
}

// Test min_size k+m is accepted
TEST_F(OSDMonitorStretchUnsetECTest, OneZoneMinSizeKPlusMSucceeds) {
  EXPECT_EQ(0, check(1, 3, 3)) << ss.str();
}

// Test failure for a multi-zone pool, which num_zones 1 unstretches
TEST_F(OSDMonitorStretchUnsetECTest, TwoZonesFails) {
  EXPECT_EQ(-EOPNOTSUPP, check(2, 3, 2));
  EXPECT_EQ("osd pool stretch unset is not supported for EC pools with "
            "num_zones > 1; use 'ceph osd pool set ec num_zones 1' instead",
            ss.str());
}

// Test failure for a multi-zone pool even with its own size
TEST_F(OSDMonitorStretchUnsetECTest, TwoZonesWithZoneSizeFails) {
  EXPECT_EQ(-EOPNOTSUPP, check(2, 6, 2));
}

// Test failure when the size is not k+m
TEST_F(OSDMonitorStretchUnsetECTest, SizeNotKPlusMFails) {
  EXPECT_EQ(-EINVAL, check(1, 6, 2));
  EXPECT_EQ("'ec' is erasure-coded: size must be 3 (k+m) and min_size "
            "between 2 (k) and 3", ss.str());
}

// Test failure when min_size is below k
TEST_F(OSDMonitorStretchUnsetECTest, MinSizeBelowKFails) {
  EXPECT_EQ(-EINVAL, check(1, 3, 1));
}

// Test failure when min_size is left out (0)
TEST_F(OSDMonitorStretchUnsetECTest, MinSizeZeroFails) {
  EXPECT_EQ(-EINVAL, check(1, 3, 0));
}

// Test failure when min_size is above k+m
TEST_F(OSDMonitorStretchUnsetECTest, MinSizeAboveKPlusMFails) {
  EXPECT_EQ(-EINVAL, check(1, 3, 4));
}

class OSDMonitorCommitDefaultsTest
  : public OSDMonitorValidateStretchModeNewPoolTest {
protected:
  map<string, string> defaults(bool stretch_mode_enabled, int bucket_type,
                               uint64_t stretch_pool_size = 4) {
    return OSDMonitor::stretch_mode_defaults_at_commit(
      stretch_mode_enabled, crush, bucket_type, stretch_pool_size);
  }
};

// Test nothing is converted without stretch mode
TEST_F(OSDMonitorCommitDefaultsTest, NoStretchModeNoDefaults) {
  EXPECT_TRUE(defaults(false, crush.get_type_id("zone")).empty());
}

// Test global stretch mode becomes two-zone defaults with half the pool size
TEST_F(OSDMonitorCommitDefaultsTest, StretchModeDefaults) {
  const map<string, string> expected = {
    {"osd_pool_default_num_zones", "2"},
    {"osd_pool_default_size", "2"},
    {"osd_pool_default_zone_failure_domain", "zone"}};
  EXPECT_EQ(expected, defaults(true, crush.get_type_id("zone")));
}

// Test the zone failure domain is the type of the stretch bucket
TEST_F(OSDMonitorCommitDefaultsTest, DatacenterStretchBucket) {
  EXPECT_EQ("datacenter",
            defaults(true, crush.get_type_id("datacenter"))
              .at("osd_pool_default_zone_failure_domain"));
}

// Test a stretch pool size of 6 gives 3 replicas per zone
TEST_F(OSDMonitorCommitDefaultsTest, StretchPoolSizeSix) {
  EXPECT_EQ("3", defaults(true, crush.get_type_id("zone"), 6)
                   .at("osd_pool_default_size"));
}

// Test an odd stretch pool size rounds the replicas per zone down
TEST_F(OSDMonitorCommitDefaultsTest, StretchPoolSizeFive) {
  EXPECT_EQ("2", defaults(true, crush.get_type_id("zone"), 5)
                   .at("osd_pool_default_size"));
}

// Test an unknown stretch bucket type leaves the zone failure domain alone
TEST_F(OSDMonitorCommitDefaultsTest, UnknownBucketTypeKeepsZoneDefault) {
  const auto d = defaults(true, 77);
  EXPECT_FALSE(d.contains("osd_pool_default_zone_failure_domain"));
  EXPECT_EQ("2", d.at("osd_pool_default_num_zones"));
}

// Test enable_stretch_mode's defaults name the zone failure domain
TEST(OSDMonitorStretchPoolDefaultsTest, TwoZones) {
  const map<string, string> expected = {
    {"osd_pool_default_num_zones", "2"},
    {"osd_pool_default_size", "2"},
    {"osd_pool_default_zone_failure_domain", "datacenter"}};
  EXPECT_EQ(expected, OSDMonitor::stretch_pool_defaults(2, 2, "datacenter"));
}

// Test disable_stretch_mode's defaults leave the zone failure domain alone
TEST(OSDMonitorStretchPoolDefaultsTest, OneZone) {
  const map<string, string> expected = {
    {"osd_pool_default_num_zones", "1"},
    {"osd_pool_default_size", "3"}};
  EXPECT_EQ(expected, OSDMonitor::stretch_pool_defaults(1, 3, ""));
}

class OSDMonitorProfileCopyNameTest : public ::testing::Test {
protected:
  const map<string, string> contents = {{"plugin", "isa"}, {"k", "2"},
                                        {"m", "2"}};
  const map<string, string> other = {{"plugin", "jerasure"}, {"k", "2"},
                                     {"m", "2"}};
  map<string, map<string, string>> profiles;

  string copy_name(const string& base) {
    return OSDMonitor::profile_copy_name(
      base, contents, [this](const string& name) -> const map<string, string>* {
        auto p = profiles.find(name);
        return p == profiles.end() ? nullptr : &p->second;
      });
  }
};

// Test a free name is used as it is
TEST_F(OSDMonitorProfileCopyNameTest, FreeName) {
  EXPECT_EQ("pool-k2-m2", copy_name("pool-k2-m2"));
}

// Test a profile of that name with the same contents is shared
TEST_F(OSDMonitorProfileCopyNameTest, SameContents) {
  profiles["pool-k2-m2"] = contents;
  EXPECT_EQ("pool-k2-m2", copy_name("pool-k2-m2"));
}

// Test a profile of that name with other contents gets a suffix
TEST_F(OSDMonitorProfileCopyNameTest, OtherContents) {
  profiles["pool-k2-m2"] = other;
  EXPECT_EQ("pool-k2-m2-1", copy_name("pool-k2-m2"));
  profiles["pool-k2-m2-1"] = other;
  EXPECT_EQ("pool-k2-m2-2", copy_name("pool-k2-m2"));
  profiles["pool-k2-m2-2"] = contents;
  EXPECT_EQ("pool-k2-m2-2", copy_name("pool-k2-m2"));
}

// Test the commit hint names the command and the release
TEST(OSDMonitorUpgradeHintTest, NamesCommandAndRelease) {
  EXPECT_EQ("the upgrade is committed with 'ceph osd require-osd-release "
            "umbrella'", OSDMonitor::num_zones_upgrade_hint());
}

class OSDMonitorCrushRuleSuitsPoolTest
  : public OSDMonitorValidateStretchModeNewPoolTest {
protected:
  int local_rule = -1;
  int local_ec_rule = -1;

  void SetUp() override {
    OSDMonitorValidateStretchModeNewPoolTest::SetUp();
    local_rule = crush.add_simple_rule("local_rule", root_name,
                                       osd_failure_domain_name, 0, "",
                                       "firstn", pg_pool_t::TYPE_REPLICATED,
                                       &ss);
    ASSERT_GE(local_rule, 0) << ss.str();
    local_ec_rule = crush.add_simple_rule("local_ec_rule", root_name,
                                          osd_failure_domain_name, 0, "",
                                          "indep", pg_pool_t::TYPE_ERASURE,
                                          &ss);
    ASSERT_GE(local_ec_rule, 0) << ss.str();
  }

  bool suits(int64_t rule, int pool_type, int64_t num_zones,
             const string& zone = "zone", unsigned zone_size = 0) {
    return OSDMonitor::crush_rule_suits_pool(crush, rule, pool_type,
                                             num_zones, zone, pools,
                                             zone_size);
  }
};

// Test no rule (-1) suits no pool
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, NoRule) {
  EXPECT_FALSE(suits(-1, pg_pool_t::TYPE_REPLICATED, 1));
  EXPECT_FALSE(suits(-1, pg_pool_t::TYPE_ERASURE, 2));
}

// Test a rule that does not exist suits no pool
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, MissingRule) {
  EXPECT_FALSE(suits(99, pg_pool_t::TYPE_REPLICATED, 1));
}

// Test a single-zone pool takes a rule of its type
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, SingleZoneOfPoolType) {
  EXPECT_TRUE(suits(local_rule, pg_pool_t::TYPE_REPLICATED, 1));
  EXPECT_TRUE(suits(local_ec_rule, pg_pool_t::TYPE_ERASURE, 1));
}

// Test a rule of the other type suits no pool
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, OtherType) {
  EXPECT_FALSE(suits(local_ec_rule, pg_pool_t::TYPE_REPLICATED, 1));
  EXPECT_FALSE(suits(local_rule, pg_pool_t::TYPE_ERASURE, 1));
  EXPECT_FALSE(suits(stretch_ec_rule, pg_pool_t::TYPE_REPLICATED, 2));
}

// Test a two-zone pool takes a stretch rule across its zone type
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, TwoZonesStretchRule) {
  EXPECT_TRUE(suits(stretch_replica_rule, pg_pool_t::TYPE_REPLICATED, 2));
  EXPECT_TRUE(suits(stretch_ec_rule, pg_pool_t::TYPE_ERASURE, 2));
}

// Test a two-zone pool takes a rule only if it places the pool's share of
// items in each zone
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, TwoZonesZoneSize) {
  const int zone = crush.get_type_id("zone");
  EXPECT_EQ(2, OSDMonitor::rule_items_per_zone(crush, stretch_replica_rule,
                                               zone, 4));
  EXPECT_EQ(6, OSDMonitor::rule_items_per_zone(crush, stretch_ec_rule, zone,
                                               12));
  EXPECT_EQ(-1, OSDMonitor::rule_items_per_zone(crush, local_rule, zone, 3));
  EXPECT_TRUE(suits(stretch_ec_rule, pg_pool_t::TYPE_ERASURE, 2, "zone", 6));
  EXPECT_FALSE(suits(stretch_ec_rule, pg_pool_t::TYPE_ERASURE, 2, "zone", 3));
  EXPECT_TRUE(suits(stretch_replica_rule, pg_pool_t::TYPE_REPLICATED, 2,
                    "zone", 2));
  EXPECT_FALSE(suits(stretch_replica_rule, pg_pool_t::TYPE_REPLICATED, 2,
                     "zone", 3));

  // choosing 0 hosts in each zone takes the pool's size from each
  const int root = crush.get_item_id("default");
  const int host = crush.get_type_id("host");
  int rule = crush.add_rule(-1, 4, pg_pool_t::TYPE_REPLICATED);
  ASSERT_GE(rule, 0);
  crush.set_rule_step_take(rule, 0, root);
  crush.set_rule_step_choose_firstn(rule, 1, 0, zone);
  crush.set_rule_step_choose_leaf_firstn(rule, 2, 0, host);
  crush.set_rule_step_emit(rule, 3);
  crush.set_rule_name(rule, "all_hosts_per_zone");
  EXPECT_EQ(4, OSDMonitor::rule_items_per_zone(crush, rule, zone, 4));
  EXPECT_FALSE(suits(rule, pg_pool_t::TYPE_REPLICATED, 2, "zone", 2));
}

// Test a two-zone pool does not take a rule that keeps to one zone
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, TwoZonesLocalRule) {
  EXPECT_FALSE(suits(local_rule, pg_pool_t::TYPE_REPLICATED, 2));
}

// Test a two-zone pool does not take a rule across another zone type
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, TwoZonesOtherZoneType) {
  EXPECT_FALSE(suits(stretch_replica_rule, pg_pool_t::TYPE_REPLICATED, 2,
                     "datacenter"));
  EXPECT_FALSE(suits(stretch_replica_rule, pg_pool_t::TYPE_REPLICATED, 2,
                     "nosuchtype"));
}

// Test a two-zone pool takes a rule that takes each zone itself
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, TwoZonesTakeEachZone) {
  const int zone1 = crush.get_item_id("zone1");
  const int zone2 = crush.get_item_id("zone2");
  const int host = crush.get_type_id("host");
  int rule = crush.add_rule(-1, 6, pg_pool_t::TYPE_REPLICATED);
  ASSERT_GE(rule, 0);
  crush.set_rule_step_take(rule, 0, zone1);
  crush.set_rule_step_choose_leaf_firstn(rule, 1, 2, host);
  crush.set_rule_step_emit(rule, 2);
  crush.set_rule_step_take(rule, 3, zone2);
  crush.set_rule_step_choose_leaf_firstn(rule, 4, 2, host);
  crush.set_rule_step_emit(rule, 5);
  crush.set_rule_name(rule, "take_each_zone");
  EXPECT_TRUE(suits(rule, pg_pool_t::TYPE_REPLICATED, 2));
}

// Test a two-zone pool does not take a rule that takes only one zone
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, TwoZonesTakeOneZone) {
  const int zone1 = crush.get_item_id("zone1");
  const int host = crush.get_type_id("host");
  int rule = crush.add_rule(-1, 3, pg_pool_t::TYPE_REPLICATED);
  ASSERT_GE(rule, 0);
  crush.set_rule_step_take(rule, 0, zone1);
  crush.set_rule_step_choose_leaf_firstn(rule, 1, 2, host);
  crush.set_rule_step_emit(rule, 2);
  crush.set_rule_name(rule, "take_one_zone");
  EXPECT_FALSE(suits(rule, pg_pool_t::TYPE_REPLICATED, 2));
}

// Test a two-zone pool takes a rule that chooses as many zones as it needs,
// but not one that chooses hosts anywhere under the root
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, TwoZonesChooseZones) {
  const int root = crush.get_item_id("default");
  const int zone = crush.get_type_id("zone");
  const int host = crush.get_type_id("host");
  int rule = crush.add_rule(-1, 4, pg_pool_t::TYPE_REPLICATED);
  ASSERT_GE(rule, 0);
  crush.set_rule_step_take(rule, 0, root);
  crush.set_rule_step_choose_firstn(rule, 1, 0, zone);
  crush.set_rule_step_choose_leaf_firstn(rule, 2, 2, host);
  crush.set_rule_step_emit(rule, 3);
  crush.set_rule_name(rule, "choose_zones");
  EXPECT_TRUE(suits(rule, pg_pool_t::TYPE_REPLICATED, 2));

  int hosts_rule = crush.add_rule(-1, 3, pg_pool_t::TYPE_REPLICATED);
  ASSERT_GE(hosts_rule, 0);
  crush.set_rule_step_take(hosts_rule, 0, root);
  crush.set_rule_step_choose_leaf_firstn(hosts_rule, 1, 0, host);
  crush.set_rule_step_emit(hosts_rule, 2);
  crush.set_rule_name(hosts_rule, "any_hosts");
  EXPECT_FALSE(suits(hosts_rule, pg_pool_t::TYPE_REPLICATED, 2));
}

// Test a three-zone pool does not take a rule across two zones
TEST_F(OSDMonitorCrushRuleSuitsPoolTest, ThreeZonesTwoZoneRule) {
  EXPECT_FALSE(suits(stretch_replica_rule, pg_pool_t::TYPE_REPLICATED, 3));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// A pool with a pg_upmap, pg_temp and primary_temp on its first two PGs
class OSDMonitorSizeChangeMappingsTest : public OSDMonitorStretchTest {
protected:
  OSDMap osdmap;
  static constexpr int64_t pool = 1;

  void SetUp() override {
    OSDMonitorStretchTest::SetUp();
    uuid_d fsid;
    fsid.generate_random();
    ASSERT_EQ(0, osdmap.build_simple(cct.get(), 1, fsid, 6));
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    pg_pool_t p;
    p.type = pg_pool_t::TYPE_REPLICATED;
    p.size = 4;
    p.min_size = 2;
    p.set_pg_num(8);
    p.set_pgp_num(8);
    inc.new_pool_max = pool;
    inc.new_pools[pool] = p;
    inc.new_pool_names[pool] = "p";
    ASSERT_EQ(0, osdmap.apply_incremental(inc));
    OSDMap::Incremental maps(osdmap.get_epoch() + 1);
    maps.fsid = osdmap.get_fsid();
    maps.new_pg_upmap[pg_t(0, pool)] = {0, 1, 2, 3};
    maps.new_pg_temp[pg_t(1, pool)] = {5, 4, 3, 2};
    maps.new_primary_temp[pg_t(1, pool)] = 5;
    ASSERT_EQ(0, osdmap.apply_incremental(maps));
  }
};

// Test a size decrease keeps the pg_upmap's first OSDs, so they do not move,
// and drops the pg_temp and primary_temp
TEST_F(OSDMonitorSizeChangeMappingsTest, DecreaseTruncatesUpmap) {
  OSDMap::Incremental pending(osdmap.get_epoch() + 1);
  OSDMonitor::clear_pool_mappings_of_size(osdmap, pending, pool, 4, 2);
  ASSERT_EQ(1u, pending.new_pg_upmap.count(pg_t(0, pool)));
  EXPECT_EQ((mempool::osdmap::vector<int32_t>{0, 1}),
            pending.new_pg_upmap[pg_t(0, pool)]);
  EXPECT_EQ(0u, pending.old_pg_upmap.count(pg_t(0, pool)));
  EXPECT_TRUE(pending.new_pg_temp[pg_t(1, pool)].empty());
  EXPECT_EQ(-1, pending.new_primary_temp[pg_t(1, pool)]);
}

// Test a size increase drops the pg_upmap, which names too few OSDs
TEST_F(OSDMonitorSizeChangeMappingsTest, IncreaseDropsUpmap) {
  OSDMap::Incremental pending(osdmap.get_epoch() + 1);
  OSDMonitor::clear_pool_mappings_of_size(osdmap, pending, pool, 4, 6);
  EXPECT_EQ(1u, pending.old_pg_upmap.count(pg_t(0, pool)));
  EXPECT_EQ(0u, pending.new_pg_upmap.count(pg_t(0, pool)));
  EXPECT_TRUE(pending.new_pg_temp[pg_t(1, pool)].empty());
  EXPECT_EQ(-1, pending.new_primary_temp[pg_t(1, pool)]);
}

// Test a pending pg_upmap is truncated too
TEST_F(OSDMonitorSizeChangeMappingsTest, DecreaseTruncatesPendingUpmap) {
  OSDMap::Incremental pending(osdmap.get_epoch() + 1);
  pending.new_pg_upmap[pg_t(2, pool)] = {5, 4, 3, 2};
  OSDMonitor::clear_pool_mappings_of_size(osdmap, pending, pool, 4, 2);
  EXPECT_EQ((mempool::osdmap::vector<int32_t>{5, 4}),
            pending.new_pg_upmap[pg_t(2, pool)]);
}
