// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Test how ceph osd pool create and ceph osd pool default set build the
 * parameters of a new pool (the defaults, the erasure code profile, the
 * command line) and check them.
 */

#include "gtest/gtest.h"
#include "mon/ConfigMap.h"
#include "mon/PoolCreateParams.h"
#include "osd/osd_types.h"
#include "crush/CrushWrapper.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/config_proxy.h"
#include "global/global_context.h"

#include <memory>
#include <sstream>
#include <string>

using namespace std;

namespace {

const string CRUSH_PARAMS = "crush parameters (crush_root, zone_failure_domain, "
                            "osd_failure_domain, crush_device_class)";

constexpr int REPLICATED = pg_pool_t::TYPE_REPLICATED;
constexpr int ERASURE = pg_pool_t::TYPE_ERASURE;

} // anonymous namespace

// A CephContext for the options, and a CRUSH map with the bucket types root,
// zone, datacenter, host and osd: root default holds zone1 and zone2, each
// with two hosts of one OSD; osd.0 has device class ssd.
class PoolParamsTest : public ::testing::Test {
protected:
  unique_ptr<CephContext> cct;
  CrushWrapper crush;
  ostringstream ss;
  map<string, string> named_profile = {
    {"plugin", "isa"}, {"k", "4"}, {"m", "2"},
    {"crush-zone-failure-domain", "zone"}};
  // what the cluster callbacks saw and should return
  int normalize_calls = 0;
  int normalize_result = 0;
  vector<pair<string, int64_t>> stretch_calls;
  int stretch_result = 0;

  void SetUp() override {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_MON));
    g_ceph_context = cct.get();
    common_init_finish(g_ceph_context);
    build_crush();
  }

  void TearDown() override {
    g_ceph_context = nullptr;
  }

  void build_crush() {
    crush.create();
    crush.set_max_devices(4);
    crush.set_type_name(10, "root");
    crush.set_type_name(9, "zone");
    crush.set_type_name(8, "datacenter");
    crush.set_type_name(1, "host");
    crush.set_type_name(0, "osd");
    int root = 0;
    crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1, 10, 0,
                     nullptr, nullptr, &root);
    crush.set_item_name(root, "default");
    for (int osd = 0; osd < 4; ++osd) {
      const string zone = osd < 2 ? "zone1" : "zone2";
      crush.insert_item(g_ceph_context, osd, 1.0, "osd." + to_string(osd),
        map<string, string>{{"host", "host" + to_string(osd)},
                            {"zone", zone}, {"root", "default"}});
    }
    ASSERT_GE(crush.update_device_class(0, "ssd", "osd.0", &ss), 0);
  }

  void set(const string& name, const string& value) {
    ASSERT_EQ(0, cct->_conf.set_val(name, value));
  }

  PoolCreateParams defaults() {
    return load_pool_defaults(cct->_conf);
  }

  // A command line over the defaults, of the given pool type.
  PoolCreateParams with(const cmdmap_t& cmdmap, int pool_type = REPLICATED) {
    PoolCreateParams p = defaults();
    p.pool_type = pool_type;
    apply_command_line(p, cmdmap);
    return p;
  }

  PoolCreateCluster cluster() {
    PoolCreateCluster c;
    c.crush = &crush;
    c.max_pool_pg_num = 1024;
    c.allow_crimson = false;
    c.normalize_profile = [this](map<string, string>&, ostream *out) {
      ++normalize_calls;
      if (normalize_result) {
        *out << "plugin refuses the profile";
      }
      return normalize_result;
    };
    c.validate_stretch = [this](const string& zone, int64_t num_zones,
                                ostream *out) {
      stretch_calls.emplace_back(zone, num_zones);
      if (stretch_result) {
        *out << "Failed to validate monitor stretch mode: no tiebreaker";
      }
      return stretch_result;
    };
    return c;
  }

  int check(const PoolCreateParams& p) {
    ss.str("");
    return check_pool_params(p, cluster(), &ss);
  }

  int check(const PoolCreateParams& p, const PoolCreateCluster& c) {
    ss.str("");
    return check_pool_params(p, c, &ss);
  }

  int check_line(const PoolCreateParams& p, PoolCommand command) {
    ss.str("");
    return check_command_line(p, command, &ss);
  }
};

// 1. The defaults

// Test the built-in defaults are loaded and nothing counts as given
TEST_F(PoolParamsTest, BuiltInDefaults) {
  const PoolCreateParams p = defaults();
  EXPECT_EQ(REPLICATED, p.pool_type);
  EXPECT_EQ(1, p.num_zones);
  EXPECT_EQ(3, p.replica);
  EXPECT_EQ(0, p.size);
  EXPECT_EQ(0, p.min_size);
  EXPECT_EQ("", p.erasure_code_profile);
  EXPECT_EQ("isa", p.profile.at("plugin"));
  EXPECT_EQ(2, p.k().value());
  EXPECT_EQ(2, p.m().value());
  EXPECT_EQ("", p.rule);
  EXPECT_EQ(-1, p.default_rule);
  EXPECT_EQ("datacenter", p.zone_failure_domain);
  EXPECT_EQ("host", p.osd_failure_domain);
  EXPECT_EQ("default", p.root);
  EXPECT_EQ("", p.device_class);
  EXPECT_EQ(32, p.pg_num);
  EXPECT_EQ(0, p.pgp_num);
  EXPECT_EQ("on", p.autoscale_mode);
  EXPECT_FALSE(p.bulk);
  EXPECT_FALSE(p.crimson);
  EXPECT_TRUE(p.given.empty());
}

// Test an erasure default pool type is loaded
TEST_F(PoolParamsTest, ErasureTypeLoaded) {
  set("osd_pool_default_type", "erasure");
  EXPECT_EQ(ERASURE, defaults().pool_type);
}

// Test the replica default follows the legacy size while replica is 0
TEST_F(PoolParamsTest, ReplicaFromLegacySize) {
  set("osd_pool_default_size", "2");
  EXPECT_EQ(2, defaults().replica);
}

// Test the replica default comes from osd_pool_default_replica when set
TEST_F(PoolParamsTest, ReplicaFromReplicaOption) {
  set("osd_pool_default_size", "3");
  set("osd_pool_default_replica", "2");
  EXPECT_EQ(2, defaults().replica);
}

// Test the default rule is loaded as its id
TEST_F(PoolParamsTest, DefaultRuleLoaded) {
  set("osd_pool_default_crush_rule", "3");
  EXPECT_EQ(3, defaults().default_rule);
}

// Test the default profile is parsed into its keys
TEST_F(PoolParamsTest, DefaultProfileParsed) {
  set("osd_pool_default_erasure_code_profile", "plugin=jerasure k=4 m=3");
  const PoolCreateParams p = defaults();
  EXPECT_EQ("jerasure", p.profile.at("plugin"));
  EXPECT_EQ(4, p.k().value());
  EXPECT_EQ(3, p.m().value());
}

// Test a default profile without k and m gives none
TEST_F(PoolParamsTest, DefaultProfileWithoutKM) {
  set("osd_pool_default_erasure_code_profile", "plugin=isa");
  EXPECT_FALSE(defaults().k().has_value());
  EXPECT_FALSE(defaults().m().has_value());
}

// Test a k or m that is not a whole number gives none
TEST_F(PoolParamsTest, DefaultProfileKMNotNumbers) {
  set("osd_pool_default_erasure_code_profile", "plugin=isa k=3x m=2.5");
  EXPECT_FALSE(defaults().k().has_value());
  EXPECT_FALSE(defaults().m().has_value());
}

// Test the placement defaults are loaded
TEST_F(PoolParamsTest, PlacementLoaded) {
  set("osd_pool_default_zone_failure_domain", "rack");
  set("osd_pool_default_osd_failure_domain", "osd");
  set("osd_pool_default_root", "dc1");
  set("osd_pool_default_class", "ssd");
  const PoolCreateParams p = defaults();
  EXPECT_EQ("rack", p.zone_failure_domain);
  EXPECT_EQ("osd", p.osd_failure_domain);
  EXPECT_EQ("dc1", p.root);
  EXPECT_EQ("ssd", p.device_class);
}

// 2. The profile

// Test a named profile replaces the default one and counts as given
TEST_F(PoolParamsTest, NamedProfileUsed) {
  PoolCreateParams p = defaults();
  ASSERT_EQ(0, use_profile(p, "myprofile", &named_profile, &ss));
  EXPECT_EQ("myprofile", p.erasure_code_profile);
  EXPECT_EQ(named_profile, p.profile);
  EXPECT_TRUE(p.is_given("erasure_code_profile"));
}

// Test failure when a named profile does not exist
TEST_F(PoolParamsTest, MissingNamedProfileFails) {
  PoolCreateParams p = defaults();
  EXPECT_EQ(-ENOENT, use_profile(p, "nosuchprofile", nullptr, &ss));
  EXPECT_EQ("erasure code profile 'nosuchprofile' does not exist", ss.str());
}

// Test a default profile without a name replaces the option's
TEST_F(PoolParamsTest, DefaultProfileUsed) {
  PoolCreateParams p = defaults();
  ASSERT_EQ(0, use_profile(p, "", &named_profile, &ss));
  EXPECT_EQ("", p.erasure_code_profile);
  EXPECT_EQ(named_profile, p.profile);
  EXPECT_FALSE(p.is_given("erasure_code_profile"));
}

// Test no profile keeps the option's
TEST_F(PoolParamsTest, NoProfileKeepsOption) {
  PoolCreateParams p = defaults();
  const auto option_profile = p.profile;
  ASSERT_EQ(0, use_profile(p, "", nullptr, &ss));
  EXPECT_EQ(option_profile, p.profile);
}

// 3. The command line

// Test an empty command line changes nothing
TEST_F(PoolParamsTest, EmptyCommandLine) {
  const PoolCreateParams p = with({});
  EXPECT_TRUE(p.given.empty());
  EXPECT_EQ(3, p.replica);
}

// Test every parameter is applied and recorded as given
TEST_F(PoolParamsTest, EveryParameterApplied) {
  const PoolCreateParams p = with({
    {"pool_type", string("erasure")}, {"num_zones", int64_t(2)},
    {"rule", string("r")}, {"root", string("dc1")},
    {"zone_failure_domain", string("rack")},
    {"osd_failure_domain", string("osd")}, {"class", string("ssd")},
    {"replica", int64_t(2)}, {"size", int64_t(4)}, {"min_size", int64_t(1)},
    {"k", int64_t(6)}, {"m", int64_t(3)}, {"pg_num", int64_t(64)},
    {"pgp_num", int64_t(16)}, {"autoscale_mode", string("warn")},
    {"bulk", true}, {"crimson", true}});
  EXPECT_EQ(ERASURE, p.pool_type);
  EXPECT_EQ(2, p.num_zones);
  EXPECT_EQ("r", p.rule);
  EXPECT_EQ("dc1", p.root);
  EXPECT_EQ("rack", p.zone_failure_domain);
  EXPECT_EQ("osd", p.osd_failure_domain);
  EXPECT_EQ("ssd", p.device_class);
  EXPECT_EQ(2, p.replica);
  EXPECT_EQ(4, p.size);
  EXPECT_EQ(1, p.min_size);
  EXPECT_EQ(6, p.k().value());
  EXPECT_EQ(3, p.m().value());
  EXPECT_EQ(64, p.pg_num);
  EXPECT_EQ(16, p.pgp_num);
  EXPECT_EQ("warn", p.autoscale_mode);
  EXPECT_TRUE(p.bulk);
  EXPECT_TRUE(p.crimson);
  EXPECT_EQ((std::set<string>{"pool_type", "num_zones", "rule", "root",
                         "zone_failure_domain", "osd_failure_domain", "class",
                         "replica", "size", "min_size", "k", "m", "pg_num",
                         "pgp_num", "autoscale_mode", "bulk", "crimson"}),
            p.given);
}

// Test k changes only k in the profile
TEST_F(PoolParamsTest, KChangesProfile) {
  const PoolCreateParams p = with({{"k", int64_t(4)}});
  EXPECT_EQ(4, p.k().value());
  EXPECT_EQ(2, p.m().value());
  EXPECT_EQ("isa", p.profile.at("plugin"));
}

// Test zero counts and empty strings mean not given, as pool create sends them
TEST_F(PoolParamsTest, ZeroAndEmptyNotGiven) {
  const PoolCreateParams p = with({
    {"replica", int64_t(0)}, {"size", int64_t(0)}, {"k", int64_t(0)},
    {"pg_num", int64_t(0)}, {"rule", string("")}, {"root", string("")}});
  EXPECT_TRUE(p.given.empty());
}

// Test false is given for the boolean parameters
TEST_F(PoolParamsTest, FalseIsGiven) {
  const PoolCreateParams p = with({{"bulk", false}});
  EXPECT_TRUE(p.is_given("bulk"));
  EXPECT_FALSE(p.bulk);
}

// Test the copies per zone are the legacy size when it is given
TEST_F(PoolParamsTest, CopiesPerZone) {
  EXPECT_EQ(3, with({}).copies_per_zone());
  EXPECT_EQ(5, with({{"size", int64_t(5)}}).copies_per_zone());
  EXPECT_EQ(2, with({{"replica", int64_t(2)}}).copies_per_zone());
}

// Which parameters may be given together

// Test a profile with k or m fails for both commands
TEST_F(PoolParamsTest, ProfileWithKMFails) {
  for (auto command : {PoolCommand::CREATE, PoolCommand::DEFAULT_SET}) {
    for (const char *key : {"k", "m"}) {
      PoolCreateParams p = with({{key, int64_t(4)}}, ERASURE);
      ASSERT_EQ(0, use_profile(p, "myprofile", &named_profile, &ss));
      EXPECT_EQ(-EINVAL, check_line(p, command)) << key;
      EXPECT_EQ("cannot specify both erasure_code_profile and k/m parameters",
                ss.str());
    }
  }
}

// Test a profile with a CRUSH parameter fails for both commands
TEST_F(PoolParamsTest, ProfileWithCrushParamFails) {
  for (auto command : {PoolCommand::CREATE, PoolCommand::DEFAULT_SET}) {
    for (const char *param : {"root", "zone_failure_domain",
                              "osd_failure_domain", "class"}) {
      PoolCreateParams p = with({{param, string("x")}}, ERASURE);
      ASSERT_EQ(0, use_profile(p, "myprofile", &named_profile, &ss));
      EXPECT_EQ(-EINVAL, check_line(p, command)) << param;
      EXPECT_EQ("cannot specify both erasure_code_profile and " + CRUSH_PARAMS,
                ss.str());
    }
  }
}

// Test a rule with a CRUSH parameter fails for both commands
TEST_F(PoolParamsTest, RuleWithCrushParamFails) {
  for (auto command : {PoolCommand::CREATE, PoolCommand::DEFAULT_SET}) {
    const PoolCreateParams p = with(
      {{"rule", string("r")}, {"root", string("default")},
       {"num_zones", int64_t(2)}});
    EXPECT_EQ(-EINVAL, check_line(p, command));
    EXPECT_EQ("cannot specify both crush rule and " + CRUSH_PARAMS, ss.str());
  }
}

// Test rule none with a CRUSH parameter succeeds for the defaults
TEST_F(PoolParamsTest, RuleNoneWithCrushParamSucceeds) {
  const PoolCreateParams p = with(
    {{"rule", string("none")}, {"root", string("default")}});
  EXPECT_EQ(0, check_line(p, PoolCommand::DEFAULT_SET)) << ss.str();
}

// Test size with replica fails for both commands
TEST_F(PoolParamsTest, SizeWithReplicaFails) {
  for (auto command : {PoolCommand::CREATE, PoolCommand::DEFAULT_SET}) {
    const PoolCreateParams p = with(
      {{"size", int64_t(3)}, {"replica", int64_t(3)}});
    EXPECT_EQ(-EINVAL, check_line(p, command));
    EXPECT_EQ("cannot specify both 'size' and 'replica' parameters; use "
              "'replica' and 'num_zones' for new pools", ss.str());
  }
}

// Test k without m fails for a pool, but sets k of the default profile
TEST_F(PoolParamsTest, KWithoutM) {
  const PoolCreateParams p = with({{"k", int64_t(4)}}, ERASURE);
  EXPECT_EQ(-EINVAL, check_line(p, PoolCommand::CREATE));
  EXPECT_EQ("erasure_code_profile requires both k and m", ss.str());
  EXPECT_EQ(0, check_line(p, PoolCommand::DEFAULT_SET)) << ss.str();
}

// Test CRUSH parameters of an EC pool need k and m, but not for the defaults
TEST_F(PoolParamsTest, ErasureCrushParamsWithoutKM) {
  const PoolCreateParams p = with({{"class", string("ssd")}}, ERASURE);
  EXPECT_EQ(-EINVAL, check_line(p, PoolCommand::CREATE));
  EXPECT_EQ(CRUSH_PARAMS + " require k and m", ss.str());
  EXPECT_EQ(0, check_line(p, PoolCommand::DEFAULT_SET)) << ss.str();
}

// Test success for an EC pool with k, m and CRUSH parameters
TEST_F(PoolParamsTest, ErasureCrushParamsWithKMSucceed) {
  const PoolCreateParams p = with(
    {{"k", int64_t(4)}, {"m", int64_t(2)}, {"root", string("default")}},
    ERASURE);
  EXPECT_EQ(0, check_line(p, PoolCommand::CREATE)) << ss.str();
}

// Test k and m fail for a replicated pool, but set the EC defaults
TEST_F(PoolParamsTest, KMForReplicated) {
  const PoolCreateParams p = with({{"k", int64_t(4)}, {"m", int64_t(2)}});
  EXPECT_EQ(-EINVAL, check_line(p, PoolCommand::CREATE));
  EXPECT_EQ("cannot specify k/m parameters for replicated pools", ss.str());
  EXPECT_EQ(0, check_line(p, PoolCommand::DEFAULT_SET)) << ss.str();
}

// Test CRUSH parameters fail for a single-zone replicated pool, but not as
// defaults
TEST_F(PoolParamsTest, CrushParamsForSingleZoneReplicated) {
  const PoolCreateParams p = with({{"root", string("default")}});
  EXPECT_EQ(-EINVAL, check_line(p, PoolCommand::CREATE));
  EXPECT_EQ(CRUSH_PARAMS + " require num_zones > 1 for a replicated pool",
            ss.str());
  EXPECT_EQ(0, check_line(p, PoolCommand::DEFAULT_SET)) << ss.str();
}

// Test success for a two-zone replicated pool with CRUSH parameters
TEST_F(PoolParamsTest, CrushParamsForTwoZoneReplicated) {
  const PoolCreateParams p = with(
    {{"num_zones", int64_t(2)}, {"root", string("default")},
     {"zone_failure_domain", string("zone")}});
  EXPECT_EQ(0, check_line(p, PoolCommand::CREATE)) << ss.str();
}

// 4. The checks of the values

// Test the built-in defaults pass for both pool types
TEST_F(PoolParamsTest, DefaultsPass) {
  EXPECT_EQ(0, check(with({}))) << ss.str();
  EXPECT_EQ(0, check(with({}, ERASURE))) << ss.str();
}

// Test failure when num_zones is 0, naming the option when it is a default
TEST_F(PoolParamsTest, NumZonesZeroFails) {
  EXPECT_EQ(-EINVAL, check(with({{"num_zones", int64_t(0)}})));
  EXPECT_EQ("num_zones must be >= 1", ss.str());
  PoolCreateParams p = defaults();
  p.num_zones = 0;
  EXPECT_EQ(-EINVAL, check(p));
  EXPECT_EQ("num_zones must be >= 1 (osd_pool_default_num_zones)", ss.str());
}

// Test failure when the legacy size is given with two zones
TEST_F(PoolParamsTest, SizeWithTwoZonesFails) {
  EXPECT_EQ(-EINVAL, check(with({{"size", int64_t(4)},
                                 {"num_zones", int64_t(2)}})));
  EXPECT_EQ("cannot specify 'size' with num_zones > 1; use 'replica' parameter "
            "instead", ss.str());
}

// Test failure when the legacy size is given while the default is two zones
TEST_F(PoolParamsTest, SizeWithDefaultTwoZonesFails) {
  set("osd_pool_default_num_zones", "2");
  EXPECT_EQ(-EINVAL, check(with({{"size", int64_t(4)}})));
  EXPECT_EQ("cannot specify 'size' with num_zones > 1; use 'replica' parameter "
            "instead (osd_pool_default_num_zones)", ss.str());
}

// Test success when an EC pool gets the legacy size, which it ignores
TEST_F(PoolParamsTest, ErasureIgnoresSize) {
  EXPECT_EQ(0, check(with({{"size", int64_t(4)}, {"num_zones", int64_t(2)}},
                          ERASURE))) << ss.str();
}

// Test failure when the legacy size is out of range
TEST_F(PoolParamsTest, SizeOutOfRangeFails) {
  EXPECT_EQ(-EINVAL, check(with({{"size", int64_t(11)}})));
  EXPECT_EQ("pool size must be between 1 and 10", ss.str());
}

// Test failure when the replicas per zone are out of range
TEST_F(PoolParamsTest, ReplicaOutOfRangeFails) {
  EXPECT_EQ(-EINVAL, check(with({{"replica", int64_t(11)}})));
  EXPECT_EQ("replica must be between 1 and 10", ss.str());
}

// Test failure when min_size is larger than the copies in a zone
TEST_F(PoolParamsTest, MinSizeAboveReplicaFails) {
  EXPECT_EQ(-EINVAL, check(with({{"replica", int64_t(2)},
                                 {"num_zones", int64_t(2)},
                                 {"min_size", int64_t(3)}})));
  EXPECT_EQ("pool min_size must be between 1 and replica, which is set to 2",
            ss.str());
}

// Test success when min_size equals the legacy size of a single-zone pool
TEST_F(PoolParamsTest, MinSizeWithLegacySizeSucceeds) {
  EXPECT_EQ(0, check(with({{"size", int64_t(4)}, {"min_size", int64_t(4)}})))
    << ss.str();
}

// Test a default min_size is not checked: pool create limits it to the size
TEST_F(PoolParamsTest, DefaultMinSizeNotChecked) {
  set("osd_pool_default_min_size", "3");
  EXPECT_EQ(0, check(with({{"replica", int64_t(2)}}))) << ss.str();
}

// Test an EC pool's profile is normalized and a plugin failure is returned
TEST_F(PoolParamsTest, ErasureProfileNormalized) {
  EXPECT_EQ(0, check(with({}, ERASURE)));
  EXPECT_EQ(1, normalize_calls);
  normalize_result = -EINVAL;
  EXPECT_EQ(-EINVAL, check(with({}, ERASURE)));
  EXPECT_EQ("plugin refuses the profile", ss.str());
}

// Test a replicated pool does not normalize a profile
TEST_F(PoolParamsTest, ReplicatedDoesNotNormalize) {
  EXPECT_EQ(0, check(with({})));
  EXPECT_EQ(0, normalize_calls);
}

// Test failure when k is 1, naming the source of k
TEST_F(PoolParamsTest, ErasureKOneFails) {
  EXPECT_EQ(-EINVAL, check(with({{"k", int64_t(1)}, {"m", int64_t(1)}},
                                ERASURE)));
  EXPECT_EQ("k=1 must be >= 2", ss.str());
  set("osd_pool_default_erasure_code_profile", "plugin=isa k=1 m=1");
  EXPECT_EQ(-EINVAL, check(with({}, ERASURE)));
  EXPECT_EQ("k=1 must be >= 2 (osd_pool_default_erasure_code_profile)",
            ss.str());
}

// Test failure when k of a named profile is 1, naming the profile
TEST_F(PoolParamsTest, NamedProfileKOneFails) {
  PoolCreateParams p = with({}, ERASURE);
  named_profile["k"] = "1";
  ASSERT_EQ(0, use_profile(p, "myprofile", &named_profile, &ss));
  EXPECT_EQ(-EINVAL, check(p));
  EXPECT_EQ("k=1 must be >= 2 (erasure code profile 'myprofile')", ss.str());
}

// Test failure when m is 0
TEST_F(PoolParamsTest, ErasureMZeroFails) {
  set("osd_pool_default_erasure_code_profile", "plugin=isa k=2 m=0");
  EXPECT_EQ(-EINVAL, check(with({}, ERASURE)));
  EXPECT_EQ("m=0 must be >= 1 (osd_pool_default_erasure_code_profile)",
            ss.str());
}

// Test failure when k+m is above 127
TEST_F(PoolParamsTest, ErasureKPlusMTooLargeFails) {
  EXPECT_EQ(-EINVAL, check(with({{"k", int64_t(100)}, {"m", int64_t(28)}},
                                ERASURE)));
  EXPECT_EQ("(k+m)=128 must be <= 127", ss.str());
}

// Test success for k+m at the 127 shard limit
TEST_F(PoolParamsTest, ErasureKPlusMAtLimitSucceeds) {
  EXPECT_EQ(0, check(with({{"k", int64_t(100)}, {"m", int64_t(27)}},
                          ERASURE))) << ss.str();
}

// Test failure when the profile has no k or m and no plugin fills them
TEST_F(PoolParamsTest, ErasureWithoutKMFails) {
  set("osd_pool_default_erasure_code_profile", "plugin=isa");
  PoolCreateCluster c = cluster();
  c.normalize_profile = nullptr;
  EXPECT_EQ(-EINVAL, check(with({}, ERASURE), c));
  EXPECT_EQ("the erasure code profile has no k or m "
            "(osd_pool_default_erasure_code_profile)", ss.str());
}

// Test failure when an EC pool's min_size is outside k..k+m
TEST_F(PoolParamsTest, ErasureMinSizeOutOfRangeFails) {
  for (int64_t min_size : {3, 7}) {
    EXPECT_EQ(-EINVAL, check(with({{"k", int64_t(4)}, {"m", int64_t(2)},
                                   {"min_size", min_size}}, ERASURE)));
    EXPECT_EQ("pool min_size must be between 4 and 6 (k + m)", ss.str());
  }
}

// Test failure when pg_num is above the monitors' limit
TEST_F(PoolParamsTest, PgNumAboveMaxFails) {
  EXPECT_EQ(-ERANGE, check(with({{"pg_num", int64_t(2048)}})));
  EXPECT_EQ("'pg_num' must be greater than 0 and less than or equal to 1024 "
            "(you may adjust 'mon max pool pg num' for higher values)",
            ss.str());
}

// Test failure when pgp_num is above pg_num, naming a default pg_num
TEST_F(PoolParamsTest, PgpNumAbovePgNumFails) {
  EXPECT_EQ(-ERANGE, check(with({{"pgp_num", int64_t(64)}})));
  EXPECT_EQ("'pgp_num' must be greater than 0 and lower or equal than "
            "'pg_num', which in this case is 32", ss.str());
}

// Test pgp_num 0 follows pg_num
TEST_F(PoolParamsTest, PgpNumZeroFollowsPgNum) {
  EXPECT_EQ(0, check(with({{"pgp_num", int64_t(0)}}))) << ss.str();
}

// Test failure when crimson is given with autoscaling on
TEST_F(PoolParamsTest, CrimsonWithAutoscaleOnFails) {
  EXPECT_EQ(-EINVAL, check(with({{"crimson", true},
                                 {"autoscale_mode", string("on")}})));
  EXPECT_EQ("crimson-osd does not support changing pg_num or pgp_num, "
            "pg_autoscale_mode must be set to 'off' (--crimson specified or "
            "osd_pool_default_crimson set)", ss.str());
}

// Test failure when crimson is not allowed on the cluster
TEST_F(PoolParamsTest, CrimsonNotAllowedFails) {
  EXPECT_EQ(-EINVAL, check(with({{"crimson", true}})));
  EXPECT_EQ("set-allow-crimson must be set to create a pool with the crimson "
            "flag (--crimson specified or osd_pool_default_crimson set)",
            ss.str());
}

// Test success for crimson when it is allowed: the default autoscale mode
// does not apply to a crimson pool
TEST_F(PoolParamsTest, CrimsonAllowedSucceeds) {
  PoolCreateCluster c = cluster();
  c.allow_crimson = true;
  EXPECT_EQ(0, check(with({{"crimson", true}}), c)) << ss.str();
}

// Test failure when the zone type of a stretched replicated pool is unknown
TEST_F(PoolParamsTest, UnknownZoneTypeFails) {
  set("osd_pool_default_zone_failure_domain", "rack");
  EXPECT_EQ(-EINVAL, check(with({{"num_zones", int64_t(2)}})));
  EXPECT_EQ("'rack' is not a CRUSH bucket type "
            "(osd_pool_default_zone_failure_domain)", ss.str());
}

// Test failure when the zone type of a stretched EC pool is unknown
TEST_F(PoolParamsTest, ErasureUnknownZoneTypeFails) {
  EXPECT_EQ(-EINVAL, check(with({{"num_zones", int64_t(2)},
                                 {"zone_failure_domain", string("rack")}},
                                ERASURE)));
  EXPECT_EQ("'rack' is not a CRUSH bucket type", ss.str());
}

// Test failure when a given OSD failure domain is unknown
TEST_F(PoolParamsTest, UnknownOsdTypeFails) {
  EXPECT_EQ(-EINVAL, check(with({{"osd_failure_domain", string("chassis")}})));
  EXPECT_EQ("'chassis' is not a CRUSH bucket type", ss.str());
}

// Test failure when the root of a stretched replicated pool does not exist
TEST_F(PoolParamsTest, MissingRootFails) {
  set("osd_pool_default_root", "dc1");
  EXPECT_EQ(-ENOENT, check(with({{"num_zones", int64_t(2)},
                                 {"zone_failure_domain", string("zone")}})));
  EXPECT_EQ("CRUSH root 'dc1' does not exist (osd_pool_default_root)",
            ss.str());
}

// Test failure when a given device class does not exist
TEST_F(PoolParamsTest, MissingClassFails) {
  EXPECT_EQ(-ENOENT, check(with({{"class", string("nvme")}})));
  EXPECT_EQ("device class 'nvme' does not exist", ss.str());
  EXPECT_EQ(0, check(with({{"class", string("ssd")}}))) << ss.str();
}

// Test a single-zone replicated pool does not use the CRUSH defaults
TEST_F(PoolParamsTest, SingleZoneIgnoresCrushDefaults) {
  set("osd_pool_default_root", "dc1");
  set("osd_pool_default_osd_failure_domain", "chassis");
  EXPECT_EQ(0, check(with({}))) << ss.str();
}

// Test a given rule replaces the CRUSH defaults of a stretched pool
TEST_F(PoolParamsTest, RuleSkipsCrushDefaults) {
  set("osd_pool_default_root", "dc1");
  EXPECT_EQ(0, check(with({{"num_zones", int64_t(2)},
                           {"zone_failure_domain", string("zone")},
                           {"rule", string("r")}}))) << ss.str();
  EXPECT_EQ(-ENOENT, check(with({{"num_zones", int64_t(2)},
                                 {"zone_failure_domain", string("zone")},
                                 {"rule", string("none")}})));
}

// Test the checks of CRUSH names need a CRUSH map
TEST_F(PoolParamsTest, NoCrushSkipsNames) {
  PoolCreateCluster c = cluster();
  c.crush = nullptr;
  EXPECT_EQ(0, check(with({{"class", string("nvme")}}), c)) << ss.str();
}

// Test two zones validate stretch mode across the zone type
TEST_F(PoolParamsTest, TwoZonesValidateStretch) {
  EXPECT_EQ(0, check(with({{"num_zones", int64_t(2)},
                           {"zone_failure_domain", string("zone")}})));
  ASSERT_EQ(1u, stretch_calls.size());
  EXPECT_EQ((pair<string, int64_t>{"zone", 2}), stretch_calls[0]);
  stretch_result = -EINVAL;
  EXPECT_EQ(-EINVAL, check(with({{"num_zones", int64_t(2)},
                                 {"zone_failure_domain", string("zone")}})));
  EXPECT_EQ("Failed to validate monitor stretch mode: no tiebreaker", ss.str());
}

// Test one zone does not validate stretch mode
TEST_F(PoolParamsTest, OneZoneDoesNotValidateStretch) {
  EXPECT_EQ(0, check(with({})));
  EXPECT_TRUE(stretch_calls.empty());
}

// Test a stretched EC pool from a named profile uses the profile's zone type
TEST_F(PoolParamsTest, NamedProfileZoneValidated) {
  PoolCreateParams p = with({{"num_zones", int64_t(2)}}, ERASURE);
  ASSERT_EQ(0, use_profile(p, "myprofile", &named_profile, &ss));
  EXPECT_EQ(0, check(p)) << ss.str();
  ASSERT_EQ(1u, stretch_calls.size());
  EXPECT_EQ("zone", stretch_calls[0].first);
}

// The defaults of ceph osd pool default set

// Test the defaults are checked as a pool of the default type
TEST_F(PoolParamsTest, DefaultsCheckDefaultType) {
  set("osd_pool_default_erasure_code_profile", "plugin=isa k=1 m=1");
  EXPECT_EQ(0, check_pool_defaults(with({}), cluster(), &ss)) << ss.str();
  set("osd_pool_default_type", "erasure");
  ss.str("");
  EXPECT_EQ(-EINVAL, check_pool_defaults(defaults(), cluster(), &ss));
  EXPECT_EQ("k=1 must be >= 2 (osd_pool_default_erasure_code_profile)",
            ss.str());
}

// Test given EC parameters are checked as an EC pool too
TEST_F(PoolParamsTest, DefaultsCheckErasureWhenGiven) {
  EXPECT_EQ(-EINVAL, check_pool_defaults(with({{"k", int64_t(1)}}), cluster(),
                                         &ss));
  EXPECT_EQ("k=1 must be >= 2", ss.str());
}

// Test given replicas are checked as a replicated pool too
TEST_F(PoolParamsTest, DefaultsCheckReplicatedWhenGiven) {
  set("osd_pool_default_type", "erasure");
  PoolCreateParams p = defaults();
  apply_command_line(p, {{"replica", int64_t(11)}});
  ASSERT_EQ(ERASURE, p.pool_type);
  EXPECT_EQ(-EINVAL, check_pool_defaults(p, cluster(), &ss));
  EXPECT_EQ("replica must be between 1 and 10", ss.str());
}

// Where values come from

// Test the zone failure domain of a pool
TEST_F(PoolParamsTest, EffectiveZoneFailureDomain) {
  EXPECT_EQ("datacenter", effective_zone_failure_domain(with({})));
  EXPECT_EQ("rack", effective_zone_failure_domain(
    with({{"zone_failure_domain", string("rack")}}, ERASURE)));
  PoolCreateParams p = with({}, ERASURE);
  ASSERT_EQ(0, use_profile(p, "myprofile", &named_profile, &ss));
  EXPECT_EQ("zone", effective_zone_failure_domain(p));
  p.pool_type = REPLICATED;
  EXPECT_EQ("datacenter", effective_zone_failure_domain(p));
  PoolCreateParams q = with({}, ERASURE);
  ASSERT_EQ(0, use_profile(q, "", &named_profile, &ss));
  EXPECT_EQ("datacenter", effective_zone_failure_domain(q));
}

// Test the option named for a value that came from a default
TEST_F(PoolParamsTest, FromDefault) {
  EXPECT_EQ(" (osd_pool_default_replica)", with({}).from_default("replica"));
  EXPECT_EQ("", with({{"replica", int64_t(2)}}).from_default("replica"));
  EXPECT_EQ(" (osd_pool_default_erasure_code_profile)",
            with({}, ERASURE).from_default("k"));
}

// Test every parameter of ceph osd pool default set maps to an option
TEST(PoolDefaultOptionsTest, EveryParameterHasAnOption) {
  const auto& options = pool_default_options();
  for (const char *param : {"pool_type", "num_zones", "rule",
                            "zone_failure_domain", "osd_failure_domain",
                            "root", "class", "replica", "size", "min_size",
                            "erasure_code_profile", "k", "m", "pg_num",
                            "pgp_num", "autoscale_mode", "bulk", "crimson"}) {
    EXPECT_TRUE(options.contains(param)) << param;
  }
}

// Test the legacy size and replica write the same option
TEST(PoolDefaultOptionsTest, SizeAndReplicaShareOneOption) {
  const auto& options = pool_default_options();
  EXPECT_EQ("osd_pool_default_replica", options.at("replica"));
  EXPECT_EQ(options.at("replica"), options.at("size"));
}

// Test the profile, k and m all write the default profile option
TEST(PoolDefaultOptionsTest, ProfileKMShareOneOption) {
  const auto& options = pool_default_options();
  EXPECT_EQ("osd_pool_default_erasure_code_profile",
            options.at("erasure_code_profile"));
  EXPECT_EQ(options.at("erasure_code_profile"), options.at("k"));
  EXPECT_EQ(options.at("erasure_code_profile"), options.at("m"));
}

// Test a profile map is written as key=value items in key order
TEST(ProfileToStringTest, KeyOrder) {
  EXPECT_EQ("k=4 m=2 plugin=isa technique=cauchy",
            profile_to_string({{"plugin", "isa"}, {"technique", "cauchy"},
                               {"k", "4"}, {"m", "2"}}));
}

// Test an empty profile map gives an empty string
TEST(ProfileToStringTest, Empty) {
  EXPECT_EQ("", profile_to_string({}));
}

// Test the pool type, numbers and flags are written as their options hold them
TEST_F(PoolParamsTest, DefaultValueStrings) {
  PoolCreateParams p = with({{"num_zones", int64_t(2)}, {"pg_num", int64_t(64)},
                             {"bulk", true}}, ERASURE);
  EXPECT_EQ("erasure", pool_default_value(p, "pool_type"));
  EXPECT_EQ("2", pool_default_value(p, "num_zones"));
  EXPECT_EQ("64", pool_default_value(p, "pg_num"));
  EXPECT_EQ("true", pool_default_value(p, "bulk"));
  EXPECT_EQ("false", pool_default_value(p, "crimson"));
}

// Test --size is written as the replicas per zone
TEST_F(PoolParamsTest, DefaultValueSizeIsReplica) {
  const PoolCreateParams p = with({{"size", int64_t(2)}});
  EXPECT_EQ("2", pool_default_value(p, "size"));
  EXPECT_EQ("2", pool_default_value(p, "replica"));
}

// Test k is written as the whole profile it changes
TEST_F(PoolParamsTest, DefaultValueKIsProfile) {
  set("osd_pool_default_erasure_code_profile", "plugin=isa k=2 m=1");
  const PoolCreateParams p = with({{"k", int64_t(4)}}, ERASURE);
  EXPECT_EQ("k=4 m=1 plugin=isa", pool_default_value(p, "k"));
  EXPECT_EQ("k=4 m=1 plugin=isa", pool_default_value(p, "erasure_code_profile"));
}

// Test the rule is written as its id
TEST_F(PoolParamsTest, DefaultValueRuleId) {
  set("osd_pool_default_crush_rule", "3");
  EXPECT_EQ("3", pool_default_value(defaults(), "rule"));
}

// Test an unknown parameter gives an empty value
TEST_F(PoolParamsTest, DefaultValueUnknown) {
  EXPECT_EQ("", pool_default_value(defaults(), "no_such_param"));
}

// Test size one is accepted when allowed and confirmed
TEST(CheckSizeOneTest, AllowedAndSure) {
  stringstream ss;
  EXPECT_EQ(0, check_size_one(true, true, &ss)) << ss.str();
}

// Test size one is refused unless mon_allow_pool_size_one is set
TEST(CheckSizeOneTest, NotAllowed) {
  stringstream ss;
  EXPECT_EQ(-EPERM, check_size_one(false, true, &ss));
  EXPECT_EQ("configuring pool size as 1 is disabled by default.", ss.str());
}

// Test size one is refused without --yes-i-really-mean-it
TEST(CheckSizeOneTest, NotSure) {
  stringstream ss;
  EXPECT_EQ(-EPERM, check_size_one(true, false, &ss));
  EXPECT_NE(string::npos, ss.str().find("--yes-i-really-mean-it"));
}

// Test the written options are unique and use the new replica name
TEST(PoolDefaultOptionsTest, OptionNames) {
  const auto names = pool_default_option_names();
  EXPECT_EQ(15u, names.size());
  EXPECT_TRUE(names.contains("osd_pool_default_replica"));
  EXPECT_FALSE(names.contains("osd_pool_default_size"));
  EXPECT_TRUE(names.contains("osd_pool_default_erasure_code_profile"));
}

class PoolDefaultOverridesTest : public ::testing::Test {
protected:
  unique_ptr<CephContext> cct;
  ConfigMap config_map;

  void SetUp() override {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_MON));
    g_ceph_context = cct.get();
    common_init_finish(g_ceph_context);
  }

  void TearDown() override {
    g_ceph_context = nullptr;
  }

  void add(const string& who, const string& name, const string& value) {
    ASSERT_EQ(0, config_map.add_option(
      cct.get(), name, who, value,
      [this](const string& n) { return cct->_conf.find_option(n); }));
  }
};

// Test an unmasked global value is not an override
TEST_F(PoolDefaultOverridesTest, GlobalValueIsNotAnOverride) {
  add("global", "osd_pool_default_replica", "2");
  EXPECT_TRUE(db_pool_default_overrides(config_map).empty());
}

// Test a value in the mon section is an override
TEST_F(PoolDefaultOverridesTest, MonSectionOverrides) {
  add("global", "osd_pool_default_replica", "2");
  add("mon", "osd_pool_default_replica", "3");
  const map<string, string> expected = {{"osd_pool_default_replica", "mon"}};
  EXPECT_EQ(expected, db_pool_default_overrides(config_map));
}

// Test a value for one monitor is an override
TEST_F(PoolDefaultOverridesTest, MonIdOverrides) {
  add("mon.b", "osd_pool_default_num_zones", "1");
  const map<string, string> expected = {{"osd_pool_default_num_zones", "mon.b"}};
  EXPECT_EQ(expected, db_pool_default_overrides(config_map));
}

// Test a masked global value is an override
TEST_F(PoolDefaultOverridesTest, MaskedGlobalOverrides) {
  add("global/host:foo", "osd_pool_default_root", "dc1");
  const map<string, string> expected =
    {{"osd_pool_default_root", "global/host:foo"}};
  EXPECT_EQ(expected, db_pool_default_overrides(config_map));
}

// Test a value for OSDs or for another daemon is not an override
TEST_F(PoolDefaultOverridesTest, OtherDaemonsDoNotOverride) {
  add("osd", "osd_pool_default_replica", "3");
  add("osd.1", "osd_pool_default_replica", "3");
  add("mgr", "osd_pool_default_replica", "3");
  EXPECT_TRUE(db_pool_default_overrides(config_map).empty());
}

// Test a mon section value of an option that is not a pool default is ignored
TEST_F(PoolDefaultOverridesTest, OtherOptionsIgnored) {
  add("mon", "osd_pool_default_size", "3");
  add("mon", "mon_allow_pool_size_one", "true");
  EXPECT_TRUE(db_pool_default_overrides(config_map).empty());
}

// Test every overridden option is listed once
TEST_F(PoolDefaultOverridesTest, SeveralOverrides) {
  add("mon", "osd_pool_default_replica", "3");
  add("mon.a", "osd_pool_default_replica", "4");
  add("mon.a", "osd_pool_default_type", "erasure");
  const map<string, string> expected = {
    {"osd_pool_default_replica", "mon"},
    {"osd_pool_default_type", "mon.a"}};
  EXPECT_EQ(expected, db_pool_default_overrides(config_map));
}

// Test nothing is overridden locally by default
TEST_F(PoolDefaultOverridesTest, NoLocalOverrideByDefault) {
  EXPECT_TRUE(local_pool_default_overrides(
    cct->_conf.get_config_values()).empty());
}

// Test a value from the configuration database is not a local override
TEST_F(PoolDefaultOverridesTest, MonValueIsNotLocalOverride) {
  map<string, string, less<>> mon_vals = {{"osd_pool_default_replica", "2"}};
  cct->_conf.set_mon_vals(cct.get(), mon_vals, nullptr);
  EXPECT_TRUE(local_pool_default_overrides(
    cct->_conf.get_config_values()).empty());
}

// Test a locally set value is a local override
TEST_F(PoolDefaultOverridesTest, LocalValueOverrides) {
  ASSERT_EQ(0, cct->_conf.set_val("osd_pool_default_num_zones", "2"));
  const set<string> expected = {"osd_pool_default_num_zones"};
  EXPECT_EQ(expected,
            local_pool_default_overrides(cct->_conf.get_config_values()));
}

// Test a locally set legacy size is not a local override of replica
TEST_F(PoolDefaultOverridesTest, LocalLegacySizeIsNotOverride) {
  ASSERT_EQ(0, cct->_conf.set_val("osd_pool_default_size", "2"));
  EXPECT_TRUE(local_pool_default_overrides(
    cct->_conf.get_config_values()).empty());
}

