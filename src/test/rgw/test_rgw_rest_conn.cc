// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_conn.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"

#include <gtest/gtest.h>

using namespace std;

static constexpr const char* EP1 = "http://127.0.0.1:8000";
static constexpr const char* EP2 = "http://127.0.0.2:8000";

static RGWRESTConn make_conn(const list<string>& endpoints)
{
  return RGWRESTConn(g_ceph_context, "remote-zone", endpoints,
                     RGWAccessKey("access", "secret"), "zonegroup", nullopt);
}

TEST(RGWRESTConn, get_endpoint_uses_resolved_ip)
{
  auto conn = make_conn({EP1});
  ASSERT_EQ(1u, conn.get_endpoint_count());

  RGWEndpoint ep;
  ASSERT_EQ(0, conn.get_endpoint(ep));
  EXPECT_EQ(EP1, ep.get_url());
  EXPECT_EQ("127.0.0.1:8000:127.0.0.1:8000", ep.get_connect_to());
}

TEST(RGWRESTConn, get_endpoint_without_endpoints)
{
  auto conn = make_conn({});

  RGWEndpoint ep;
  EXPECT_EQ(-EINVAL, conn.get_endpoint(ep));
}

TEST(RGWRESTConn, get_endpoint_when_all_ips_are_down)
{
  auto conn = make_conn({EP1, EP2});

  for (size_t i = 0; i < conn.get_endpoint_count(); ++i) {
    RGWEndpoint ep;
    ASSERT_EQ(0, conn.get_endpoint(ep));
    ASSERT_FALSE(ep.get_connect_to().empty());
    conn.set_endpoint_unconnectable(ep);
  }

  RGWEndpoint ep;
  ASSERT_EQ(0, conn.get_endpoint(ep));
  EXPECT_FALSE(ep.get_url().empty());
  // no connect_to hint:
  EXPECT_TRUE(ep.get_connect_to().empty());
}

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  cct->_conf.set_val_or_die("rgw_rest_conn_connect_to_resolved_ips", "true");
  // never let a marked-down IP recover while a test is running
  cct->_conf.set_val_or_die("rgw_rest_conn_ip_fail_timeout_secs", "60");
  cct->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
