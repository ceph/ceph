// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>

#include "cls/rgw_ratelimit/cls_rgw_ratelimit_client.h"
#include "global/global_context.h"
#include "include/rados/librados.hpp"
#include "rgw_ratelimit_core.h"
#include "test/librados/test_cxx.h"

using namespace librados;

static constexpr int64_t ratelimit_interval = 60;

static int wait_for_osd_map()
{
  Rados cluster;
  cluster.init_with_context(g_ceph_context);
  cluster.connect();
  return cluster.wait_for_latest_osdmap();
}

TEST(ClsRgwRatelimit, ConsumeGivebackAndBytes)
{
  ASSERT_EQ(0, wait_for_osd_map());

  Rados cluster;
  std::string pool_name = get_temp_pool_name("rgw-rl");
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  const std::string oid = ".rgw.ratelimit.0";
  const std::string key = "ucls-user";

  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  info.max_read_bytes = 1024;
  const int64_t interval = ratelimit_interval;
  auto ts = ceph::coarse_real_clock::now().time_since_epoch();

  int64_t delay = 0;
  ASSERT_EQ(0, cls::rgw::ratelimit::consume(&ioctx, oid, key, OpType::Read,
                                            info, ts, interval, &delay));
  EXPECT_EQ(0, delay);

  ASSERT_EQ(0, cls::rgw::ratelimit::consume(&ioctx, oid, key, OpType::Read,
                                            info, ts, interval, &delay));
  EXPECT_EQ(rgw::ratelimit::compute_delay(1, 1, interval), delay);

  ASSERT_EQ(0, cls::rgw::ratelimit::giveback(&ioctx, oid, key, OpType::Read));
  ASSERT_EQ(0, cls::rgw::ratelimit::consume(&ioctx, oid, key, OpType::Read,
                                            info, ts, interval, &delay));
  EXPECT_EQ(0, delay);

  ASSERT_EQ(0, cls::rgw::ratelimit::decrease_bytes(&ioctx, oid, key, true, 2048, info));
  ASSERT_EQ(0, cls::rgw::ratelimit::consume(&ioctx, oid, key, OpType::Read,
                                            info, ts, interval, &delay));
  EXPECT_GT(delay, 0);

  ASSERT_EQ(0, ioctx.remove(oid));
  ASSERT_EQ(0, cluster.pool_delete(pool_name.c_str()));
}

TEST(ClsRgwRatelimit, ClusterWideAcrossObjectsSameShard)
{
  ASSERT_EQ(0, wait_for_osd_map());

  Rados cluster;
  std::string pool_name = get_temp_pool_name("rgw-rl-shard");
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  const std::string oid = ".rgw.ratelimit.1";
  const std::string key = "ushared";

  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 2;
  const int64_t interval = ratelimit_interval;
  auto ts = ceph::coarse_real_clock::now().time_since_epoch();

  int64_t delay = 0;
  ASSERT_EQ(0, cls::rgw::ratelimit::consume(&ioctx, oid, key, OpType::Read,
                                            info, ts, interval, &delay));
  EXPECT_EQ(0, delay);
  ASSERT_EQ(0, cls::rgw::ratelimit::consume(&ioctx, oid, key, OpType::Read,
                                            info, ts, interval, &delay));
  EXPECT_EQ(0, delay);
  ASSERT_EQ(0, cls::rgw::ratelimit::consume(&ioctx, oid, key, OpType::Read,
                                            info, ts, interval, &delay));
  EXPECT_EQ(rgw::ratelimit::compute_delay(2, 1, interval), delay);

  ASSERT_EQ(0, ioctx.remove(oid));
  ASSERT_EQ(0, cluster.pool_delete(pool_name.c_str()));
}
