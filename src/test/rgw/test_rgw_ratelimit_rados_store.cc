// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>

#include "cls/rgw_ratelimit/cls_rgw_ratelimit_client.h"
#include "common/ceph_context.h"
#include "include/rados/librados.hpp"
#include "rgw_ratelimit_core.h"
#include "rgw_ratelimit_rados.h"
#include "test/librados/test_cxx.h"

using namespace librados;

TEST(RadosRateLimitStore, cls_backend_cluster_wide)
{
  Rados cluster;
  std::string pool_name = get_temp_pool_name("rgw-rl-store");
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  auto* cct = static_cast<CephContext*>(cluster.cct());
  cct->_conf.set_val("rgw_ratelimit_rados_pool", pool_name);
  cct->_conf.set_val("rgw_ratelimit_rados_oid_prefix", ".rgw.ratelimit");
  cct->_conf.set_val("rgw_ratelimit_rados_num_shards", "4");
  cct->_conf.set_val("rgw_ratelimit_fail_open", "false");

  rgw::ratelimit::RadosRateLimitStore store(
      cct, pool_name, ".rgw.ratelimit", 4);
  store.set_test_ioctx(&ioctx);

  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 2;
  const std::string key = "urados-store";
  const int64_t interval = cct->_conf->rgw_ratelimit_interval;

  auto time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, store.should_rate_limit("GET", key, time, &info, ""));

  rgw::ratelimit::RadosRateLimitStore peer(
      cct, pool_name, ".rgw.ratelimit", 4);
  peer.set_test_ioctx(&ioctx);

  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(0, peer.should_rate_limit("GET", key, time, &info, ""));

  time = ceph::coarse_real_clock::now();
  EXPECT_EQ(rgw::ratelimit::compute_delay(2, 1, interval),
            store.should_rate_limit("GET", key, time, &info, ""));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
