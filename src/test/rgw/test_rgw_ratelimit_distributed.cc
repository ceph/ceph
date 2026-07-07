// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>

#include "rgw_ratelimit.h"
#include "rgw_ratelimit_core.h"
#include "rgw_ratelimit_shared.h"
#include "rgw_ratelimit_store.h"

using namespace std::chrono_literals;

class RateLimitBackendFixture : public ::testing::Test {
protected:
  void SetUp() override {
    rgw::ratelimit::SharedCounterRegistry::instance().clear_for_tests();
  }
};

static void run_reject_op_over_limit(RateLimitStore& store)
{
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  const std::string key = "uuser123";
  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store.should_rate_limit("GET", key, time, &info, ""));
  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(60, store.should_rate_limit("GET", key, time, &info, ""));
}

static void run_giveback(RateLimitStore& store)
{
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  const std::string key = "uuser456";
  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store.should_rate_limit("GET", key, time, &info, ""));
  store.giveback_tokens("GET", key, "", &info);
  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store.should_rate_limit("GET", key, time, &info, ""));
}

static void run_refill(RateLimitStore& store)
{
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  const std::string key = "uuser789";
  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store.should_rate_limit("GET", key, time, &info, ""));
  time += 61s;
  EXPECT_EQ(0, store.should_rate_limit("GET", key, time, &info, ""));
}

static void run_bw_reject(RateLimitStore& store)
{
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 1;
  const std::string key = "uuserbw";
  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store.should_rate_limit("GET", key, time, &info, ""));
  store.decrease_bytes("GET", key, 2, &info);
  time = ceph::coarse_real_clock::now();
  EXPECT_GT(store.should_rate_limit("GET", key, time, &info, ""), 0);
}

static void run_list_op(RateLimitStore& store)
{
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  const std::string key = "ubucket1";
  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store.should_rate_limit("GET", key, time, &info, "list-type=2"));
  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(60, store.should_rate_limit("GET", key, time, &info, "list-type=2"));
}

TEST_F(RateLimitBackendFixture, LocalBackendSuite)
{
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter local(g_ceph_context, replacing, cv);
  run_reject_op_over_limit(local);
  run_giveback(local);
  run_refill(local);
  run_bw_reject(local);
  run_list_op(local);
}

TEST_F(RateLimitBackendFixture, RedisBackendSuite)
{
  rgw::ratelimit::RedisRateLimitStore redis(g_ceph_context, "test-redis");
  run_reject_op_over_limit(redis);
  run_giveback(redis);
  run_refill(redis);
  run_bw_reject(redis);
  run_list_op(redis);
}

TEST_F(RateLimitBackendFixture, ClusterWideTwoGateways)
{
  rgw::ratelimit::RedisRateLimitStore rgw_a(g_ceph_context, "cluster");
  rgw::ratelimit::RedisRateLimitStore rgw_b(g_ceph_context, "cluster");

  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 2;
  const std::string key = "ucluster-user";

  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, rgw_a.should_rate_limit("GET", key, time, &info, ""));
  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, rgw_b.should_rate_limit("GET", key, time, &info, ""));
  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(60, rgw_a.should_rate_limit("GET", key, time, &info, ""));
  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(60, rgw_b.should_rate_limit("GET", key, time, &info, ""));
}

TEST(RGWRateLimitCore, consume_matches_entry_semantics)
{
  RGWRateLimitCounterState state;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  const int64_t interval = g_ceph_context->_conf->rgw_ratelimit_interval;
  auto ts = ceph::coarse_real_clock::now().time_since_epoch();

  EXPECT_EQ(0, rgw::ratelimit::consume(state, OpType::Read, &info, ts, interval));
  EXPECT_EQ(60, rgw::ratelimit::consume(state, OpType::Read, &info, ts, interval));
}

TEST(RGWRateLimitService, backend_factory_local_default)
{
  g_ceph_context->_conf.set_val("rgw_ratelimit_backend", "local");
  RateLimitService svc(g_ceph_context);
  svc.start();
  auto store = svc.get_active();
  ASSERT_NE(nullptr, store);

  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_write_ops = 5;
  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store->should_rate_limit("PUT", "uabc", time, &info, ""));
}

TEST(RGWRateLimitService, backend_factory_redis)
{
  g_ceph_context->_conf.set_val("rgw_ratelimit_backend", "redis");
  g_ceph_context->_conf.set_val("rgw_ratelimit_redis_key_prefix", "svc-test");
  rgw::ratelimit::SharedCounterRegistry::instance().clear_for_tests();

  RateLimitService svc(g_ceph_context);
  svc.start();
  auto store = svc.get_active();
  ASSERT_NE(nullptr, store);

  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  const std::string key = "u-redis-svc";
  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store->should_rate_limit("GET", key, time, &info, ""));
  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(60, store->should_rate_limit("GET", key, time, &info, ""));

  g_ceph_context->_conf.set_val("rgw_ratelimit_backend", "local");
}

TEST(RGWRateLimitService, backend_factory_rados_fallback_without_pool)
{
  g_ceph_context->_conf.set_val("rgw_ratelimit_backend", "rados");
  g_ceph_context->_conf.set_val("rgw_ratelimit_rados_pool", "");
  rgw::ratelimit::SharedCounterRegistry::instance().clear_for_tests();

  RateLimitService svc(g_ceph_context);
  svc.start();
  auto store = svc.get_active();
  ASSERT_NE(nullptr, store);

  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  const std::string key = "u-rados-fallback";
  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store->should_rate_limit("GET", key, time, &info, ""));

  g_ceph_context->_conf.set_val("rgw_ratelimit_backend", "local");
}

TEST(RGWRateLimitCore, op_type_classification)
{
  RGWRateLimitInfo info;
  info.max_list_ops = 10;
  info.max_delete_ops = 10;
  EXPECT_EQ(OpType::List,
            rgw_ratelimit_op_type("GET", "list-type=2&prefix=a", &info));
  EXPECT_EQ(OpType::Read, rgw_ratelimit_op_type("GET", "", &info));
  EXPECT_EQ(OpType::Write, rgw_ratelimit_op_type("PUT", "", &info));
  EXPECT_EQ(OpType::Delete, rgw_ratelimit_op_type("DELETE", "obj", &info));
}

TEST(RGWRateLimitCore, state_encode_roundtrip)
{
  RGWRateLimitCounterState in;
  in.read_ops = 5000;
  in.write_bytes = 9000;
  in.first_run = false;
  in.ts_ns = 123456789;

  bufferlist bl;
  in.encode(bl);

  RGWRateLimitCounterState out;
  auto iter = bl.cbegin();
  out.decode(iter);

  EXPECT_EQ(in.read_ops, out.read_ops);
  EXPECT_EQ(in.write_bytes, out.write_bytes);
  EXPECT_EQ(in.first_run, out.first_run);
  EXPECT_EQ(in.ts_ns, out.ts_ns);
}
