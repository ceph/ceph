// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test_pool_types.h"
#include "common/json/OSDStructures.h"
#include "include/scope_guard.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <iterator>
#include <sstream>
#include <thread>

using namespace librados;

namespace ceph {
namespace test {

// Define static members for PoolTypeTestFixture
librados::Rados PoolTypeTestFixture::rados;
std::map<PoolType, std::string> PoolTypeTestFixture::pool_names;

// Define static member for ECOnlyTestFixture
librados::Rados ECOnlyTestFixture::rados;

void PoolTypeTestFixture::cleanup_namespace(librados::Rados& cluster,
                                            librados::IoCtx& ioctx,
                                            const std::string& ns) {
  ioctx.snap_set_read(librados::SNAP_HEAD);
  ioctx.set_namespace(ns);

  int tries = 20;
  while (--tries) {
    int got_enoent = 0;
    for (librados::NObjectIterator it = ioctx.nobjects_begin();
         it != ioctx.nobjects_end(); ++it) {
      ioctx.locator_set_key(it->get_locator());
      librados::ObjectWriteOperation op;
      op.remove();
      librados::AioCompletion* completion = cluster.aio_create_completion();
      auto sg = make_scope_guard([&] { completion->release(); });
      ASSERT_EQ(0, ioctx.aio_operate(it->get_oid(), completion, &op,
                                     librados::OPERATION_IGNORE_CACHE));
      completion->wait_for_complete();
      if (completion->get_return_value() == -ENOENT) {
        ++got_enoent;
      } else {
        ASSERT_EQ(0, completion->get_return_value());
      }
    }
    if (!got_enoent) {
      break;
    }
    sleep(1);
  }
}

void PoolTypeTestFixture::SetUpTestSuite() {
  ASSERT_EQ("", connect_cluster_pp(rados));

  for (auto type : get_supported_pool_types()) {
    std::string pname = get_temp_pool_name(
      pool_name_prefix() + pool_type_name(type) + "_");
    ASSERT_EQ("", create_pool_by_type(pname, rados, type));
    after_pool_create(type, pname, rados);
    pool_names[type] = pname;
  }
}

void PoolTypeTestFixture::TearDownTestSuite() {
  for (auto& [type, pname] : pool_names) {
    ASSERT_EQ(0, destroy_pool_by_type(pname, rados, type));
  }
  pool_names.clear();
  rados.shutdown();
}

void PoolTypeTestFixture::SetUp() {
  pool_type = GetParam();
  auto it = pool_names.find(pool_type);
  ASSERT_NE(pool_names.end(), it);
  pool_name = it->second;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
  nspace = std::string(test_info->name()) + "_" +
           std::to_string(std::chrono::system_clock::now()
                            .time_since_epoch().count());
  ioctx.set_namespace(nspace);

  cleanup_namespace(rados, ioctx, nspace);
}

void PoolTypeTestFixture::TearDown() {
  cleanup_namespace(rados, ioctx, "");
  cleanup_namespace(rados, ioctx, nspace);
  ioctx.close();
}

void PoolTypeTestFixture::turn_balancing_off() {
  int rc;
  std::ostringstream oss;
  bufferlist outbl;

  oss.str("");
  bufferlist inbl_autoscaler;
  oss << "{\"prefix\": \"osd set\", \"key\": \"noautoscale\"}";
  rc = rados.mon_command(oss.str(), std::move(inbl_autoscaler), &outbl, nullptr);
  EXPECT_EQ(rc, 0);

  oss.str("");
  oss << "{\"prefix\": \"balancer off\"}";
  bufferlist inbl_balancer;
  rc = rados.mon_command(oss.str(), std::move(inbl_balancer), &outbl, nullptr);
  EXPECT_EQ(rc, 0);

  balancing_disabled = true;
}

void PoolTypeTestFixture::turn_balancing_on() {
  int rc;
  std::ostringstream oss;
  bufferlist outbl;

  oss.str("");
  bufferlist inbl_autoscaler;
  oss << "{\"prefix\": \"osd unset\", \"key\": \"noautoscale\"}";
  rc = rados.mon_command(oss.str(), std::move(inbl_autoscaler), &outbl, nullptr);
  EXPECT_EQ(rc, 0);

  oss.str("");
  oss << "{\"prefix\": \"balancer on\"}";
  bufferlist inbl_balancer;
  rc = rados.mon_command(oss.str(), std::move(inbl_balancer), &outbl, nullptr);
  EXPECT_EQ(rc, 0);
}

int PoolTypeTestFixture::request_osd_map(
    const std::string& oid,
    ceph::messaging::osd::OSDMapReply* reply) {
  bufferlist inbl, outbl;
  auto formatter = std::make_unique<JSONFormatter>(false);
  ceph::messaging::osd::OSDMapRequest osdMapRequest{pool_name, oid, nspace};
  encode_json("OSDMapRequest", osdMapRequest, formatter.get());

  std::ostringstream oss;
  formatter.get()->flush(oss);
  int rc = rados.mon_command(oss.str(), std::move(inbl), &outbl, nullptr);
  if (rc != 0) {
    return rc;
  }

  JSONParser p;
  bool success = p.parse(outbl.c_str(), outbl.length());
  if (!success) {
    return -1;
  }

  reply->decode_json(&p);
  return 0;
}

int PoolTypeTestFixture::set_osd_upmap(
    const std::string& pgid,
    const std::vector<int>& up_osds) {
  bufferlist inbl, outbl;
  std::ostringstream oss;
  oss << "{\"prefix\": \"osd pg-upmap\", \"pgid\": \"" << pgid << "\", \"id\": [";
  for (size_t i = 0; i < up_osds.size(); i++) {
    oss << up_osds[i];
    if (i != up_osds.size() - 1) {
      oss << ", ";
    }
  }
  oss << "]}";
  int rc = rados.mon_command(oss.str(), std::move(inbl), &outbl, nullptr);
  return rc;
}

int PoolTypeTestFixture::wait_for_upmap(
    const std::string& oid,
    int desired_primary,
    std::chrono::seconds timeout) {
  bool upmap_in_effect = false;
  auto start_time = std::chrono::steady_clock::now();
  while (!upmap_in_effect &&
         (std::chrono::steady_clock::now() - start_time < timeout)) {
    ceph::messaging::osd::OSDMapReply reply;
    int res = request_osd_map(oid, &reply);
    EXPECT_TRUE(res == 0);
    std::vector<int> acting_osds = reply.acting;
    if (!acting_osds.empty() && acting_osds[0] == desired_primary) {
      print_osd_map("New upmap in effect, acting set: ", acting_osds);
      upmap_in_effect = true;
    } else {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  return upmap_in_effect ? 0 : -ETIMEDOUT;
}

void PoolTypeTestFixture::print_osd_map(
    const std::string& message,
    const std::vector<int>& osd_vec) {
  std::stringstream out_vec;
  std::copy(osd_vec.begin(), osd_vec.end(), std::ostream_iterator<int>(out_vec, " "));
  std::cout << message << out_vec.str().c_str() << std::endl;
}

void PoolTypeTestFixture::setup_and_trigger_recovery(
    const std::string& oid,
    int& new_primary,
    std::chrono::seconds timeout) {
  // Find current up osds
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map(oid, &reply);
  EXPECT_EQ(0, res);

  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);

  // Swap first and last osds to form new upmap
  int prev_primary = prev_up_osds[0];
  std::vector<int> new_up_osds = prev_up_osds;
  std::swap(new_up_osds[0], new_up_osds[new_up_osds.size() - 1]);
  new_primary = new_up_osds[0];

  std::cout << "Previous primary osd: " << prev_primary << std::endl;
  std::cout << "New primary osd: " << new_primary << std::endl;
  print_osd_map("Desired up osds: ", new_up_osds);

  // Set new upmap
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_EQ(0, rc);

  // Wait for new upmap to appear as acting set of osds
  int res2 = wait_for_upmap(oid, new_primary, timeout);
  EXPECT_EQ(0, res2);
}

void ECOnlyTestFixture::SetUp() {
  ASSERT_EQ("", connect_cluster_pp(rados));
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_pool_by_type(pool_name, rados, PoolType::FAST_EC));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
}

void ECOnlyTestFixture::TearDown() {
  ioctx.close();
  ASSERT_EQ(0, destroy_pool_by_type(pool_name, rados, PoolType::FAST_EC));
}

} // namespace test
} // namespace ceph
