#include "rgw/rgw_perf_counters.h"
#include "rgw_perf_counters.h"
#include <common/ceph_argparse.h>
#include <gtest/gtest.h>
#include "rgw_common.h"
#include "rgw_kms.cc"

class TestSSEKMSWithTestingKMS : public ::testing::Test {
 protected:
  CephContext* cct = g_ceph_context;
  const NoDoutPrefix no_dpp{cct, 1};
  std::map<std::string, bufferlist> attrs = {
      {RGW_ATTR_CRYPT_KEYID,
       []() {
         bufferlist bl;
         bl.append("foo");
         return bl;
       }()},
      {RGW_ATTR_CRYPT_KEYSEL, []() {
         // AES_ECB(32*"#").decrypt(32*"*")
         bufferlist bl;
         bl.append(
             "\xc6\xb1/\x12\xdc\xf7"
             "e"
             "\xe3;\xea\x14\xa4x\x1f"
             "bX"
             "\xc6\xb1/\x12\xdc\xf7"
             "e"
             "\xe3;\xea\x14\xa4x\x1f"
             "bX");
         return bl;
       }()}};
  
  void TearDown() override {
    JSONFormatter f(true);
    if (PerfCounters* perf = cache_perf(); perf != nullptr) {
      perf->dump_formatted(&f, false, select_labeled_t::labeled);
      f.flush(std::cout);
    }
    
    KMSContext kctx{cct};
    kctx.clear_cache();
  }

  PerfCounters* cache_perf() {
    PerfCounters* result = nullptr;
    cct->get_perfcounters_collection()->with_counters(
        [&](const PerfCountersCollectionImpl::CounterMap& by_path) {
          for (const auto& i : by_path) {
            const auto& perf_counters = i.second.perf_counters;
            if (perf_counters->get_name() == "kms-cache") {
              result = perf_counters;
              return;
            }
          }
        });
    return result;
  }
  void test_do_reconstitue() {
    std::string actual_key;
    const int ret = reconstitute_actual_key_from_kms(
        &no_dpp, attrs, null_yield, actual_key);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(actual_key, "********************************");
  }
};

TEST_F(
    TestSSEKMSWithTestingKMS,
    TestReconstituteActualKeyFromKMSBasicsDefault) {
  KMSContext kctx{cct};
  ASSERT_TRUE(kctx.cache_enabled());
  test_do_reconstitue();
  PerfCounters* perfcounter = cache_perf();
  ASSERT_NE(perfcounter, nullptr);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::clear)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::capacity)),
            cct->_conf->rgw_crypt_s3_kms_cache_max_size);
}

TEST_F(
    TestSSEKMSWithTestingKMS,
    TestReconstituteActualKeyFromKMSBasicsWithoutCache) {
  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "false");
  test_do_reconstitue();
  PerfCounters* perfcounter = cache_perf();
  ASSERT_NE(perfcounter, nullptr);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 0);
  KMSContext kctx{cct};
  ASSERT_FALSE(kctx.cache_enabled());
}

TEST_F(
    TestSSEKMSWithTestingKMS,
    TestReconstituteActualKeyFromKMSBasicsWithCache) {
  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "true");

  test_do_reconstitue();
  PerfCounters* perfcounter = cache_perf();
  ASSERT_NE(perfcounter, nullptr);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 1);

  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 1);

  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 2);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 1);
}

TEST_F(
    TestSSEKMSWithTestingKMS,
    TestRuntimeEnableDisable) {
  PerfCounters* perfcounter = cache_perf();
  ASSERT_NE(perfcounter, nullptr);
  KMSContext kctx{cct};

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "true");
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 1);

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "false");
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 1);

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "true");
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 1);

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "false");
  kctx.clear_cache();
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::miss)), 0);
  EXPECT_EQ(perfcounter->get(static_cast<int>(webcache::Metric::size)), 0);
}

int main(int argc, char** argv) {
  auto args = argv_to_vec(argc, argv);
  std::map<std::string, std::string> defaults{
      {"rgw_crypt_s3_kms_backend", RGW_SSE_KMS_BACKEND_TESTING},
      {"rgw_crypt_s3_kms_encryption_keys",
       "foo=IyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyM="},
      // make it very unlikely that the TTL reaper runs during unittests
      {"rgw_crypt_s3_kms_cache_positive_ttl", "3600"},
      {"rgw_crypt_s3_kms_cache_transient_error_ttl", "3600"},
      {"rgw_crypt_s3_kms_cache_negative_ttl", "3600"},
      {"debug_rgw", "20"}
  };
  auto cct = global_init(
      &defaults, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  rgw_perf_start(g_ceph_context);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
