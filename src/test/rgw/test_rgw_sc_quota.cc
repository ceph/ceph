// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Unit tests for the per-storage-class quota feature.
 *
 * We exercise three layers in isolation:
 *
 *   1. The on-the-wire encoding of RGWQuotaInfo at v3 <-> v4 boundaries.
 *
 *   2. The composite-key construction (rgw_sc_quota_key).
 *
 *   3. rgw_check_storage_class_quota() against a deterministic in-test
 *      stats provider, covering:
 *        - no enforcement_mode set => check returns 0 (legacy preservation)
 *        - HYBRID + under limit    => returns 0
 *        - HYBRID + over size      => returns -EDQUOT
 *        - HYBRID + over count     => returns -EDQUOT
 *        - HYBRID + missing stats  => returns 0 (fail-open)
 *        - STORAGE_CLASS mode      => global quota fields ignored
 *        - Different sc_key        => no false positive
 *
 * All cases are pure unit tests: no RADOS, no driver, no asio.
 */

#include <gtest/gtest.h>
#include <cerrno>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/async/yield_context.h"

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "rgw_quota_types.h"
#include "rgw_sc_quota_types.h"
#include "rgw_sc_quota_checker.h"
#include "rgw_placement_types.h"
#include "rgw_basic_types.h"        // for rgw_bucket
#include "include/buffer.h"

using namespace rgw::quota;

namespace {
struct TestDpp : public DoutPrefixProvider {
  CephContext* get_cct() const override { return g_ceph_context; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out; }
};
} // namespace


static rgw_bucket make_bucket(const std::string& name = "test-bucket",
                              const std::string& tenant = "") {
  rgw_bucket b;
  b.name = name;
  b.tenant = tenant;
  return b;
}

static rgw_placement_rule make_placement(const std::string& name,
                                         const std::string& sc) {
  return rgw_placement_rule(name, sc);
}

namespace {
class FakeStatsProvider : public ScUsageStatsProvider {
 public:
  std::map<std::string, ScUsageStats> stats;
  std::optional<ScUsageStats> lookup(const rgw_bucket& /*b*/,
                                     const std::string& sc_key) const override {
    auto it = stats.find(sc_key);
    if (it == stats.end()) return std::nullopt;
    return it->second;
  }
};

// RAII installer for the global provider slot so tests don't leak state.
struct ScopedProvider {
  explicit ScopedProvider(ScUsageStatsProvider* p) {
    set_sc_stats_provider(p);
  }
  ~ScopedProvider() { set_sc_stats_provider(nullptr); }
};
} // namespace

TEST(RGWQuotaInfoV4, RoundtripEmptyScMap) {
  RGWQuotaInfo in;
  in.enabled = true;
  in.max_size = 1024;
  in.max_objects = 10;

  ceph::buffer::list bl;
  encode(in, bl);

  RGWQuotaInfo out;
  auto p = bl.cbegin();
  decode(out, p);

  EXPECT_TRUE(out.enabled);
  EXPECT_EQ(out.max_size, 1024);
  EXPECT_EQ(out.max_objects, 10);
  EXPECT_TRUE(out.storage_class_quotas.empty());
  EXPECT_EQ(out.enforcement_mode, RGWQuotaEnforcementMode::LEGACY);
}

TEST(RGWQuotaInfoV4, RoundtripPopulatedScMap) {
  RGWQuotaInfo in;
  in.enabled = true;
  in.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  in.storage_class_quotas[rgw_sc_quota_key("default", "SSD")] =
    RGWStorageClassQuota{ 1024 * 1024, 100, true };
  in.storage_class_quotas[rgw_sc_quota_key("default", "GLACIER")] =
    RGWStorageClassQuota{ -1, 50, true };

  ceph::buffer::list bl;
  encode(in, bl);

  RGWQuotaInfo out;
  auto p = bl.cbegin();
  decode(out, p);

  EXPECT_EQ(out.enforcement_mode, RGWQuotaEnforcementMode::HYBRID);
  ASSERT_EQ(out.storage_class_quotas.size(), 2u);

  const auto* ssd = out.get_sc_quota(rgw_sc_quota_key("default", "SSD"));
  ASSERT_NE(ssd, nullptr);
  EXPECT_EQ(ssd->max_size, 1024 * 1024);
  EXPECT_EQ(ssd->max_objects, 100);
  EXPECT_TRUE(ssd->enabled);

  const auto* gla = out.get_sc_quota(rgw_sc_quota_key("default", "GLACIER"));
  ASSERT_NE(gla, nullptr);
  EXPECT_EQ(gla->max_size, -1);
  EXPECT_EQ(gla->max_objects, 50);
}

/*
 * Critical backward-compat test: encode a v4 blob with NO sc quotas and
 * an explicit LEGACY mode, decode it, then re-encode it.  The behaviour
 * must be indistinguishable from how a pre-feature RGW would treat the
 * same RGWQuotaInfo -- empty map, legacy mode preserved.
 */
TEST(RGWQuotaInfoV4, RoundtripLegacyDefaultsStable) {
  for (auto& in : RGWQuotaInfo::generate_test_instances()) {
    ceph::buffer::list bl;
    encode(in, bl);
    RGWQuotaInfo out;
    auto p = bl.cbegin();
    decode(out, p);
    EXPECT_EQ(in.enabled, out.enabled);
    EXPECT_EQ(in.max_size, out.max_size);
    EXPECT_EQ(in.max_objects, out.max_objects);
    EXPECT_EQ(in.check_on_raw, out.check_on_raw);
    EXPECT_EQ(in.enforcement_mode, out.enforcement_mode);
    EXPECT_EQ(in.storage_class_quotas.size(), out.storage_class_quotas.size());
  }
}

TEST(RGWScQuotaKey, BasicComposition) {
  EXPECT_EQ(rgw_sc_quota_key("default-placement", "SSD"),
            "default-placement::SSD");
}

TEST(RGWScQuotaKey, EmptyStorageClassMapsToSTANDARD) {
  // get_canonical_storage_class("") -> "STANDARD"
  EXPECT_EQ(rgw_sc_quota_key("default-placement", ""),
            "default-placement::STANDARD");
}

TEST(RGWScQuotaKey, FromRule) {
  EXPECT_EQ(rgw_sc_quota_key(make_placement("p1", "GLACIER")),
            "p1::GLACIER");
}


TEST(RGWScQuotaChecker, LegacyModeReturnsZero) {
  TestDpp dpp;
  FakeStatsProvider prov;
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enabled = true;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::LEGACY;
  // Even with an entry present, LEGACY mode must NOT consult it.
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ 1, 1, true };

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      1ULL << 30, 100, null_yield);
  EXPECT_EQ(r, 0);
}

TEST(RGWScQuotaChecker, NoSCConfiguredFallsThrough) {
  TestDpp dpp;
  FakeStatsProvider prov;
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enabled = true;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  // storage_class_quotas is empty -> sc_enforcement_active() returns false.

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      1ULL << 40, 1'000'000, null_yield);
  EXPECT_EQ(r, 0);
}

TEST(RGWScQuotaChecker, UnderLimitAllows) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "SSD")] = ScUsageStats{ 50, 5 };
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ /*max_size=*/100, /*max_objects=*/10, /*enabled=*/true };

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      /*new_size=*/40, /*new_objects=*/3, null_yield);
  EXPECT_EQ(r, 0);
}

TEST(RGWScQuotaChecker, OverSizeReturnsEDQUOT) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "SSD")] = ScUsageStats{ 90, 5 };
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ 100, -1, true };

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      /*new_size=*/15, /*new_objects=*/1, null_yield);
  EXPECT_EQ(r, -EDQUOT);
}

TEST(RGWScQuotaChecker, OverObjectCountReturnsEDQUOT) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "SSD")] = ScUsageStats{ 10, 9 };
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ -1, /*max_objects=*/10, true };

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      /*new_size=*/1, /*new_objects=*/2, null_yield);
  EXPECT_EQ(r, -EDQUOT);
}

TEST(RGWScQuotaChecker, MissingStatsFailsOpen) {
  TestDpp dpp;
  FakeStatsProvider prov;  // empty
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ 1, 1, true };

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      1'000'000, 1, null_yield);
  EXPECT_EQ(r, 0);  // fail-open even though we are way over limit
}

TEST(RGWScQuotaChecker, NoProviderFailsOpen) {
  TestDpp dpp;
  // No ScopedProvider -> get_sc_stats_provider() returns nullptr.

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ 1, 1, true };

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      1'000'000, 1, null_yield);
  EXPECT_EQ(r, 0);
}

TEST(RGWScQuotaChecker, DifferentScKeyNoFalsePositive) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "GLACIER")] = ScUsageStats{ 9999999, 9999 };
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ 1, 1, true };
  // Writing to GLACIER but only SSD has a quota -> no SC quota applies.
  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "GLACIER"),
      1, 1, null_yield);
  EXPECT_EQ(r, 0);
}

TEST(RGWScQuotaChecker, BucketAndUserCombineTighter) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "SSD")] = ScUsageStats{ 80, 5 };
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ /*max_size=*/200, /*max_objects=*/-1, true };
  q.user_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.user_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
    RGWStorageClassQuota{ /*max_size=*/100, /*max_objects=*/-1, true };

  // 80 + 25 = 105 > 100 (user limit) but < 200 (bucket limit) -> reject.
  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      25, 1, null_yield);
  EXPECT_EQ(r, -EDQUOT);
}

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(RGWScQuotaChecker, StorageClassOnlyModeIgnoresGlobalQuota) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "SSD")] = ScUsageStats{50, 5};
  ScopedProvider sp(&prov);

  RGWQuota q;
  // Global quota would fail (max_size=10, current=50+10=60 > 10)
  q.bucket_quota.max_size    = 10;
  q.bucket_quota.max_objects = 10;
  q.bucket_quota.enabled     = true;
  // But mode is STORAGE_CLASS — global quota is ignored
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::STORAGE_CLASS;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
      RGWStorageClassQuota{200, 100, true};  // generous SC limit

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      10, 1, null_yield);
  // SC check passes (50+10 < 200), global check not run
  EXPECT_EQ(r, 0);
}

TEST(RGWScQuotaChecker, DisabledEntryNotEnforced) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "SSD")] = ScUsageStats{999, 999};
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
      RGWStorageClassQuota{1, 1, /*enabled=*/false};  // tiny limit but disabled

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      1000, 100, null_yield);
  EXPECT_EQ(r, 0);  // disabled entry = no enforcement
}

TEST(RGWScQuotaChecker, ExactLimitBoundaryAllows) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "SSD")] = ScUsageStats{90, 9};
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
      RGWStorageClassQuota{100, 10, true};

  // 90 + 10 == 100 exactly — should PASS (not exceed)
  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      10, 1, null_yield);
  EXPECT_EQ(r, 0);
}

TEST(RGWQuotaInfoV4, JSONRoundtrip) {
  RGWQuotaInfo in;
  in.enabled          = true;
  in.max_size         = 1024 * 1024;
  in.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  in.storage_class_quotas[rgw_sc_quota_key("default", "SSD")] =
      RGWStorageClassQuota{512 * 1024, 50, true};

  // Dump to JSON
  JSONFormatter f;
  f.open_object_section("quota");
  in.dump(&f);
  f.close_section();
  std::stringstream ss;
  f.flush(ss);

  // Parse back
  JSONParser p;
  ASSERT_TRUE(p.parse(ss.str().c_str(), ss.str().size()));
  RGWQuotaInfo out;
  out.decode_json(&p);

  EXPECT_EQ(out.enforcement_mode, RGWQuotaEnforcementMode::HYBRID);
  ASSERT_EQ(out.storage_class_quotas.size(), 1u);
  const auto* ssd = out.get_sc_quota(rgw_sc_quota_key("default", "SSD"));
  ASSERT_NE(ssd, nullptr);
  EXPECT_EQ(ssd->max_size, 512 * 1024);
  EXPECT_EQ(ssd->max_objects, 50);
  EXPECT_TRUE(ssd->enabled);
}

TEST(RGWScQuotaChecker, GlobalOnlyModeReturnsZero) {
  TestDpp dpp;
  FakeStatsProvider prov;
  prov.stats[rgw_sc_quota_key("p", "SSD")] = ScUsageStats{1, 1};
  ScopedProvider sp(&prov);

  RGWQuota q;
  q.bucket_quota.enforcement_mode = RGWQuotaEnforcementMode::GLOBAL_ONLY;
  q.bucket_quota.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] =
      RGWStorageClassQuota{1, 1, true};  // tiny limit

  int r = rgw_check_storage_class_quota(
      &dpp, q, make_bucket(), make_placement("p", "SSD"),
      1000, 100, null_yield);
  EXPECT_EQ(r, 0);  // GLOBAL_ONLY = no SC enforcement
}