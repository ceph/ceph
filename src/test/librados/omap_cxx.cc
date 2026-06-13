// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <common/perf_counters_collection.h>

#include "test/librados/test_cxx.h"
#include "test/librados/test_pool_types.h"
#include "crimson_utils.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/version/cls_version_ops.h"
#include "common/json/OSDStructures.h"
#include "librados/librados_asio.h"

#include <boost/asio/io_context.hpp>

#include <algorithm>
#include <climits>
#include <thread>
#include <chrono>

using namespace std;
using namespace librados;
using ceph::test::PoolType;
using ceph::test::pool_type_name;
using ceph::test::create_pool_by_type;
using ceph::test::destroy_pool_by_type;

// Define static member from base class
librados::Rados ceph::test::PoolTypeTestFixture::rados;

// Parameterized test fixture for omap operations across pool types
class OmapTest : public ceph::test::PoolTypeTestFixture {
protected:
  std::string nspace;
  uint64_t alignment = 0;

  static void SetUpTestSuite() {
    SKIP_IF_CRIMSON();
    ASSERT_EQ("", connect_cluster_pp(rados));
  }

  static void TearDownTestSuite() {
    SKIP_IF_CRIMSON();
    rados.shutdown();
  }

  void SetUp() override {
    SKIP_IF_CRIMSON();
    // Skip EC pools before creating resources
    if (GetParam() == PoolType::FAST_EC) {
      GTEST_SKIP() << "EC pools do not support omap yet";
    }
    // Call base class SetUp to create pool and ioctx
    PoolTypeTestFixture::SetUp();
    
    nspace = get_temp_pool_name();
    ioctx.set_namespace(nspace);
    
    // Enable omap for EC pools
    if (pool_type == PoolType::FAST_EC) {
      enable_omap();
    }
  }
  
  void TearDown() override {
    SKIP_IF_CRIMSON();
    if (GetParam() == PoolType::FAST_EC) {
      GTEST_SKIP() << "EC pools do not support omap yet";
    }
    // Call base class TearDown to clean up pool
    PoolTypeTestFixture::TearDown();
  }

  // Helper methods
  void enable_omap() {
    bufferlist inbl, outbl;
    std::ostringstream oss;
    oss << "{\"prefix\": \"osd pool set\", \"pool\": \"" << pool_name
        << "\", \"var\": \"supports_omap\", \"val\": \"true\"}";
    int ret = rados.mon_command(oss.str(), std::move(inbl), &outbl, nullptr);
    ASSERT_EQ(0, ret);
  }

  void turn_balancing_off() {
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
  }

  void turn_balancing_on() {
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

  int request_osd_map(
      std::string oid,
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

  int set_osd_upmap(
      std::string pgid,
      std::vector<int> up_osds) {
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

  int wait_for_upmap(
      std::string oid,
      int desired_primary,
      std::chrono::seconds timeout) {
    bool upmap_in_effect = false;
    auto start_time = std::chrono::steady_clock::now();
    while (!upmap_in_effect && (std::chrono::steady_clock::now() - start_time < timeout)) {
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

  void print_osd_map(std::string message, std::vector<int> osd_vec) {
    std::stringstream out_vec;
    std::copy(osd_vec.begin(), osd_vec.end(), std::ostream_iterator<int>(out_vec, " "));
    std::cout << message << out_vec.str().c_str() << std::endl;
  }

  void check_omap_read(
      std::string oid,
      std::string first_omap_key,
      std::string first_omap_value,
      int expected_size,
      int expected_err) {
    ObjectReadOperation read;
    int err = 0;
    std::map<std::string,bufferlist> vals_read{ {"_", {}} };
    read.omap_get_vals2("", LONG_MAX, &vals_read, nullptr, &err);
    int ret = ioctx.operate(oid, &read, nullptr);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(err, expected_err);
    EXPECT_EQ(vals_read.size(), expected_size);
    if (vals_read.find(first_omap_key) == vals_read.end()) {
      ADD_FAILURE() << "Missing key " << first_omap_key;
    } else {
      bufferlist val_read_bl = vals_read[first_omap_key];
      std::string val_read;
      decode(val_read, val_read_bl);
      EXPECT_EQ(first_omap_value, val_read);
    }
  }

  // Helper to create test object with omap data
  void create_test_object_with_omap(
      const std::string& oid,
      const std::map<std::string, bufferlist>& omap_map,
      const bufferlist& omap_header);

  // Helper to get standard test omap data
  std::map<std::string, bufferlist> get_test_omap_data();
};

// Helper method to create a test object with omap data
void OmapTest::create_test_object_with_omap(
    const std::string& oid,
    const std::map<std::string, bufferlist>& omap_map,
    const bufferlist& omap_header) {
  bufferlist bl_write;
  bl_write.append("ceph");
  ObjectWriteOperation write_op;
  write_op.write(0, bl_write);
  write_op.omap_set_header(omap_header);
  write_op.omap_set(omap_map);
  int ret = ioctx.operate(oid, &write_op);
  ASSERT_EQ(ret, 0);
}

// Helper method to get standard test omap data
std::map<std::string, bufferlist> OmapTest::get_test_omap_data() {
  const std::string omap_value = "omap_value_1_horse";
  bufferlist omap_val_bl;
  encode(omap_value, omap_val_bl);
  
  return {
    {"omap_key_1_palomino", omap_val_bl},
    {"omap_key_2_chestnut", omap_val_bl},
    {"omap_key_3_bay", omap_val_bl},
    {"omap_key_4_dun", omap_val_bl},
    {"omap_key_5_black", omap_val_bl},
    {"omap_key_6_grey", omap_val_bl}
  };
}

TEST_P(OmapTest, OmapSetAndWrite) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_set";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  
  // Test that we can write an object with omap data
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Verify the object data was written
  bufferlist bl_read;
  ObjectReadOperation read;
  read.read(0, 4, &bl_read, nullptr);
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
}

TEST_P(OmapTest, OmapGetVals) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_get_vals";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_get_vals2 with start_after and max_return
  int err = 0;
  std::map<std::string, bufferlist> returned_vals_1;
  std::map<std::string, bufferlist> returned_vals_2;
  
  ObjectReadOperation read;
  read.omap_get_vals2("omap_key_1_palomino", 1, &returned_vals_1, nullptr, &err);
  read.omap_get_vals2("omap", 4, &returned_vals_2, nullptr, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(1u, returned_vals_1.size());
  ASSERT_EQ(4u, returned_vals_2.size());
}

TEST_P(OmapTest, OmapGetKeys) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_get_keys";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_get_keys2
  int err = 0;
  std::set<std::string> returned_keys;
  
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(6u, returned_keys.size());
}

TEST_P(OmapTest, OmapGetHeader) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_get_header";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_get_header
  int err = 0;
  bufferlist returned_header_bl;
  
  ObjectReadOperation read;
  read.omap_get_header(&returned_header_bl, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  
  std::string returned_header_str;
  decode(returned_header_str, returned_header_bl);
  ASSERT_EQ(returned_header_str, omap_header);
}

TEST_P(OmapTest, OmapGetValsByKeys) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_get_vals_by_keys";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_get_vals_by_keys
  int err = 0;
  std::set<std::string> key_filter = { "omap_key_1_palomino", "omap_key_3_bay" };
  std::map<std::string, bufferlist> returned_vals_by_keys;
  
  ObjectReadOperation read;
  read.omap_get_vals_by_keys(key_filter, &returned_vals_by_keys, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(2u, returned_vals_by_keys.size());
}

TEST_P(OmapTest, OmapCmp) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_cmp";
  const std::string omap_header = "my_omap_header";
  const std::string omap_value = "omap_value_1_horse";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_cmp
  int err = 0;
  map<string, pair<bufferlist, int>> cmp_results;
  bufferlist cmp_val_bl;
  encode(omap_value, cmp_val_bl);
  
  cmp_results["omap_key_1_palomino"] = pair<bufferlist, int>(cmp_val_bl, CEPH_OSD_CMPXATTR_OP_EQ);
  
  ObjectReadOperation read;
  read.omap_cmp(cmp_results, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
}

TEST_P(OmapTest, OmapRmKeys) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_rm_keys";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_rm_keys
  std::set<std::string> keys_to_remove;
  keys_to_remove.insert("omap_key_2_chestnut");
  
  ObjectWriteOperation write_op;
  write_op.omap_rm_keys(keys_to_remove);
  int ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // Verify key was removed
  int err = 0;
  std::set<std::string> returned_keys;
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(5u, returned_keys.size());
  ASSERT_EQ(0u, returned_keys.count("omap_key_2_chestnut"));
}

TEST_P(OmapTest, OmapRmRange) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_rm_range";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_rm_range - removes keys in range [start, end)
  ObjectWriteOperation write_op;
  write_op.omap_rm_range("omap_key_3_bay", "omap_key_5_black");
  int ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // Verify keys in range were removed
  int err = 0;
  std::set<std::string> returned_keys;
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(4u, returned_keys.size());
  // Keys 3 and 4 should be removed, keys 1, 2, 5, 6 should remain
  ASSERT_EQ(0u, returned_keys.count("omap_key_3_bay"));
  ASSERT_EQ(0u, returned_keys.count("omap_key_4_dun"));
  ASSERT_EQ(1u, returned_keys.count("omap_key_1_palomino"));
  ASSERT_EQ(1u, returned_keys.count("omap_key_2_chestnut"));
}

TEST_P(OmapTest, OmapClear) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_clear";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_clear
  ObjectWriteOperation write_op;
  write_op.omap_clear();
  int ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // Verify all omap data was cleared
  int err = 0;
  std::set<std::string> returned_keys;
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_TRUE(returned_keys.empty());
}

TEST_P(OmapTest, OmapRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
  bufferlist bl_write, omap_val_bl, xattr_val_bl;
  const std::string omap_key_1 = "key_a";
  const std::string omap_key_2 = "key_b";
  const std::string omap_key_3 = "key_c";
  const std::string omap_value = "val_12345";
  encode(omap_value, omap_val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1, omap_val_bl},
    {omap_key_2, omap_val_bl},
    {omap_key_3, omap_val_bl}
  };
  const std::string header = "upmap_header_z";
  bufferlist header_bl;
  encode(header, header_bl);
  bl_write.append("ceph");
  
  // 1. Write data to omap
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set(omap_map);
  write1.omap_set_header(header_bl);
  int ret = ioctx.operate("change_upmap_oid", &write1);
  EXPECT_EQ(ret, 0);

  // 2. Find up osds
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map("change_upmap_oid", &reply);
  EXPECT_TRUE(res == 0);
  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);
  
  // 3. Swap first and last osds to form new upmap
  int prev_primary = prev_up_osds[0];
  std::vector<int> new_up_osds = prev_up_osds;
  std::swap(new_up_osds[0], new_up_osds[new_up_osds.size() - 1]);
  int new_primary = new_up_osds[0];
  std::cout << "Previous primary osd: " << prev_primary << std::endl;
  std::cout << "New primary osd: " << new_primary << std::endl;
  print_osd_map("Desired up osds: ", new_up_osds);

  // 4. Set new up map
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_TRUE(rc == 0);

  // 5. Wait for new upmap to appear as acting set of osds
  int res2 = wait_for_upmap("change_upmap_oid", new_primary, 60s);
  EXPECT_TRUE(res2 == 0);
  
  // 6. Read omap
  check_omap_read("change_upmap_oid", omap_key_1, omap_value, 3, 0);

  turn_balancing_on();
}

TEST_P(OmapTest, NoOmapRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
  bufferlist bl_write;
  bl_write.append("ceph");

  // 1. Write data to omap
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  int ret = ioctx.operate("no_omap_oid", &write1);
  EXPECT_EQ(ret, 0);

  // 2. Find up osds
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map("no_omap_oid", &reply);
  EXPECT_TRUE(res == 0);
  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);

  // 3. Swap first and last osds to form new upmap
  int prev_primary = prev_up_osds[0];
  std::vector<int> new_up_osds = prev_up_osds;
  std::swap(new_up_osds[0], new_up_osds[new_up_osds.size() - 1]);
  int new_primary = new_up_osds[0];
  std::cout << "Previous primary osd: " << prev_primary << std::endl;
  std::cout << "New primary osd: " << new_primary << std::endl;
  print_osd_map("Desired up osds: ", new_up_osds);

  // 4. Set new up map
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_TRUE(rc == 0);

  // 5. Wait for new upmap to appear as acting set of osds
  int res2 = wait_for_upmap("no_omap_oid", new_primary, 60s);
  EXPECT_TRUE(res2 == 0);

  // 6. Read data
  bufferlist bl_read;
  ObjectReadOperation read;
  read.read(0, bl_write.length(), &bl_read, nullptr);
  ret = ioctx.operate("no_omap_oid", &read, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));

  turn_balancing_on();
}

TEST_P(OmapTest, LargeOmapRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();

  bufferlist bl_write, header_bl;
  const std::string huge_val(1024, 'x');
  const std::string header = "large_header";
  encode(header, header_bl);
  bl_write.append("ceph");

  // 1. Write data to omap in 100 chunks of 100 keys
  int total_keys = 10000;
  int chunk_size = 100;
  int num_chunks = total_keys / chunk_size;
  int current_key_index = 0;

  for (int chunk = 0; chunk < num_chunks; ++chunk) {
    std::map<std::string, bufferlist> omap_map_chunk;

    for (int i = 0; i < chunk_size; ++i) {
      char key_buf[32];
      snprintf(key_buf, sizeof(key_buf), "key_%06d", current_key_index);
      bufferlist omap_val_bl;
      encode(huge_val, omap_val_bl);
      omap_map_chunk[std::string(key_buf)] = omap_val_bl;
      current_key_index++;
    }

    ObjectWriteOperation write_op;

    // Only write the object payload and omap header on the first transaction
    if (chunk == 0) {
      write_op.write(0, bl_write);
      write_op.omap_set_header(header_bl);
    }

    write_op.omap_set(omap_map_chunk);
    int ret = ioctx.operate("large_oid", &write_op);
    EXPECT_EQ(ret, 0);
  }

  // 2. Find up osds
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map("large_oid", &reply);
  EXPECT_TRUE(res == 0);
  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);

  // 3. Swap first and last osds to form new upmap
  int prev_primary = prev_up_osds[0];
  std::vector<int> new_up_osds = prev_up_osds;
  std::swap(new_up_osds[0], new_up_osds[new_up_osds.size() - 1]);
  int new_primary = new_up_osds[0];
  std::cout << "Previous primary osd: " << prev_primary << std::endl;
  std::cout << "New primary osd: " << new_primary << std::endl;
  print_osd_map("Desired up osds: ", new_up_osds);

  // 4. Set new up map
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_TRUE(rc == 0);

  // 5. Wait for new upmap to appear as acting set of osds
  int res2 = wait_for_upmap("large_oid", new_primary, 60s);
  EXPECT_TRUE(res2 == 0);

  // 6. Read omap
  check_omap_read("large_oid", "key_000000", huge_val, 1024, 0);

  turn_balancing_on();
}

TEST_P(OmapTest, OmapAfterDelete) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_after_delete";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  
  // 1. Create object with omap data
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // 2. Verify omap data exists
  int err = 0;
  std::set<std::string> returned_keys;
  ObjectReadOperation read1;
  read1.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  int ret = ioctx.operate(oid, &read1, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(6u, returned_keys.size());
  
  // 3. Delete the object
  ret = ioctx.remove(oid);
  EXPECT_EQ(ret, 0);
  
  // 4. Try to read omap keys - should fail with -ENOENT
  err = 0;
  returned_keys.clear();
  ObjectReadOperation read2;
  read2.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read2, nullptr);
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(err, -EIO);
  
  // 5. Try to read omap values - should fail with -ENOENT
  err = 0;
  std::map<std::string, bufferlist> returned_vals;
  ObjectReadOperation read3;
  read3.omap_get_vals2("", LONG_MAX, &returned_vals, nullptr, &err);
  ret = ioctx.operate(oid, &read3, nullptr);
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(err, -EIO);
  
  // 6. Try to read omap header - should fail with -ENOENT
  err = 0;
  bufferlist returned_header_bl;
  ObjectReadOperation read4;
  read4.omap_get_header(&returned_header_bl, &err);
  ret = ioctx.operate(oid, &read4, nullptr);
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(err, 0);
  
  // 7. Try to read omap values by keys - should fail with -ENOENT
  err = 0;
  std::set<std::string> key_filter = { "omap_key_1_palomino", "omap_key_3_bay" };
  std::map<std::string, bufferlist> returned_vals_by_keys;
  ObjectReadOperation read5;
  read5.omap_get_vals_by_keys(key_filter, &returned_vals_by_keys, &err);
  ret = ioctx.operate(oid, &read5, nullptr);
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(err, -EIO);
  
  // 8. Try to write omap data to deleted object - should succeed (creates new object)
  bufferlist new_omap_val_bl;
  const std::string new_omap_value = "new_value";
  encode(new_omap_value, new_omap_val_bl);
  std::map<std::string, bufferlist> new_omap_map = {
    {"new_key", new_omap_val_bl}
  };
  ObjectWriteOperation write_op;
  write_op.omap_set(new_omap_map);
  ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // 9. Verify new omap data exists (object was recreated)
  err = 0;
  returned_keys.clear();
  ObjectReadOperation read6;
  read6.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read6, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(1u, returned_keys.size());
  ASSERT_EQ(1u, returned_keys.count("new_key"));
}

TEST_P(OmapTest, CloneBasic) {
  SKIP_IF_CRIMSON();
  
  const std::string oid = "test_omap_clone_basic";
  const std::string original_header = "original_header";
  const std::string original_value = "original_value";
  
  // Create object with OMAP data
  bufferlist header_bl;
  encode(original_header, header_bl);
  
  bufferlist val_bl;
  encode(original_value, val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {"key1", val_bl},
    {"key2", val_bl},
    {"key3", val_bl}
  };
  
  create_test_object_with_omap(oid, omap_map, header_bl);
  
  // Create snapshot (this triggers clone operation)
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger clone creation before modifying OMAP
  bufferlist write_bl;
  write_bl.append("trigger_clone");
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Modify head object OMAP
  const std::string new_header = "modified_header";
  const std::string new_value = "modified_value";
  bufferlist new_header_bl;
  encode(new_header, new_header_bl);
  bufferlist new_val_bl;
  encode(new_value, new_val_bl);
  
  ObjectWriteOperation write_op;
  write_op.omap_set_header(new_header_bl);
  std::map<std::string, bufferlist> new_omap = {{"key4", new_val_bl}};
  write_op.omap_set(new_omap);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has original OMAP data
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  // Check clone header
  bufferlist clone_header_bl;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_header(&clone_header_bl, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  
  std::string clone_header;
  decode(clone_header, clone_header_bl);
  ASSERT_EQ(original_header, clone_header);
  
  // Check clone keys
  std::set<std::string> clone_keys;
  ObjectReadOperation read_clone_keys;
  err = 0;
  read_clone_keys.omap_get_keys2("", LONG_MAX, &clone_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, clone_keys.size());
  ASSERT_EQ(1u, clone_keys.count("key1"));
  ASSERT_EQ(1u, clone_keys.count("key2"));
  ASSERT_EQ(1u, clone_keys.count("key3"));
  ASSERT_EQ(0u, clone_keys.count("key4"));
  
  // Verify head has modified OMAP data
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  bufferlist head_header_bl;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_header(&head_header_bl, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  
  std::string head_header;
  decode(head_header, head_header_bl);
  ASSERT_EQ(new_header, head_header);
  
  // Check head keys
  std::set<std::string> head_keys;
  ObjectReadOperation read_head_keys;
  err = 0;
  read_head_keys.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(4u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("key4"));
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneHeader) {
  SKIP_IF_CRIMSON();
  
  const std::string oid = "test_omap_clone_header";
  const std::string header1 = "header_version_1";
  const std::string header2 = "header_version_2";
  
  // Create object with OMAP header
  bufferlist header1_bl;
  encode(header1, header1_bl);
  std::map<std::string, bufferlist> omap_map;
  create_test_object_with_omap(oid, omap_map, header1_bl);
  
  // Create snapshot
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger clone creation
  bufferlist write_bl;
  write_bl.append("trigger_clone");
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Change header on head
  bufferlist header2_bl;
  encode(header2, header2_bl);
  ObjectWriteOperation write_op;
  write_op.omap_set_header(header2_bl);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has original header
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  bufferlist clone_header_bl;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_header(&clone_header_bl, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  
  std::string clone_header;
  decode(clone_header, clone_header_bl);
  ASSERT_EQ(header1, clone_header);
  
  // Verify head has new header
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  bufferlist head_header_bl;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_header(&head_header_bl, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  
  std::string head_header;
  decode(head_header, head_header_bl);
  ASSERT_EQ(header2, head_header);
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneKeyModifications) {
  SKIP_IF_CRIMSON();
  
  const std::string oid = "test_omap_clone_keys";
  const std::string value = "test_value";
  
  // Create object with OMAP keys
  bufferlist val_bl;
  encode(value, val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {"original_key1", val_bl},
    {"original_key2", val_bl},
    {"to_be_removed", val_bl}
  };
  bufferlist header_bl;
  create_test_object_with_omap(oid, omap_map, header_bl);
  
  // Create snapshot
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger clone creation
  bufferlist write_bl;
  write_bl.append("trigger_clone");
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Modify keys on head: add, remove, and modify
  ObjectWriteOperation write_op;
  std::map<std::string, bufferlist> new_keys = {{"new_key", val_bl}};
  write_op.omap_set(new_keys);
  std::set<std::string> remove_keys = {"to_be_removed"};
  write_op.omap_rm_keys(remove_keys);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has original keys
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  std::set<std::string> clone_keys;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_keys2("", LONG_MAX, &clone_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, clone_keys.size());
  ASSERT_EQ(1u, clone_keys.count("original_key1"));
  ASSERT_EQ(1u, clone_keys.count("original_key2"));
  ASSERT_EQ(1u, clone_keys.count("to_be_removed"));
  ASSERT_EQ(0u, clone_keys.count("new_key"));
  
  // Verify head has modified keys
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  std::set<std::string> head_keys;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("original_key1"));
  ASSERT_EQ(1u, head_keys.count("original_key2"));
  ASSERT_EQ(0u, head_keys.count("to_be_removed"));
  ASSERT_EQ(1u, head_keys.count("new_key"));
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneMultiple) {
  SKIP_IF_CRIMSON();
  
  const std::string oid = "test_omap_clone_multiple";
  const std::string value = "value";
  
  // Create object with initial OMAP data
  bufferlist val_bl;
  encode(value, val_bl);
  std::map<std::string, bufferlist> omap_map1 = {{"key1", val_bl}};
  bufferlist header_bl;
  create_test_object_with_omap(oid, omap_map1, header_bl);
  
  // Create first snapshot
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger first clone creation
  bufferlist write_bl1;
  write_bl1.append("clone1");
  ASSERT_EQ(0, ioctx.write(oid, write_bl1, write_bl1.length(), 0));
  
  // Modify OMAP
  ObjectWriteOperation write_op1;
  std::map<std::string, bufferlist> omap_map2 = {{"key2", val_bl}};
  write_op1.omap_set(omap_map2);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op1));
  
  // Create second snapshot
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger second clone creation
  bufferlist write_bl2;
  write_bl2.append("clone2");
  ASSERT_EQ(0, ioctx.write(oid, write_bl2, write_bl2.length(), 0));
  
  // Modify OMAP again
  ObjectWriteOperation write_op2;
  std::map<std::string, bufferlist> omap_map3 = {{"key3", val_bl}};
  write_op2.omap_set(omap_map3);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op2));
  
  // Verify first clone has only key1
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  std::set<std::string> clone1_keys;
  int err = 0;
  ObjectReadOperation read_clone1;
  read_clone1.omap_get_keys2("", LONG_MAX, &clone1_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone1, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(1u, clone1_keys.size());
  ASSERT_EQ(1u, clone1_keys.count("key1"));
  
  // Verify second clone has key1 and key2
  snap_ioctx.snap_set_read(my_snaps[1]);
  std::set<std::string> clone2_keys;
  ObjectReadOperation read_clone2;
  err = 0;
  read_clone2.omap_get_keys2("", LONG_MAX, &clone2_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone2, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(2u, clone2_keys.size());
  ASSERT_EQ(1u, clone2_keys.count("key1"));
  ASSERT_EQ(1u, clone2_keys.count("key2"));
  
  // Verify head has all three keys
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  std::set<std::string> head_keys;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("key1"));
  ASSERT_EQ(1u, head_keys.count("key2"));
  ASSERT_EQ(1u, head_keys.count("key3"));
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps[1]));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneAfterClear) {
  SKIP_IF_CRIMSON();
  
  const std::string oid = "test_omap_clone_after_clear";
  const std::string value = "value";
  
  // Create object with OMAP data
  bufferlist val_bl;
  encode(value, val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {"key1", val_bl},
    {"key2", val_bl}
  };
  bufferlist header_bl;
  encode("header", header_bl);
  create_test_object_with_omap(oid, omap_map, header_bl);
  
  // Clear OMAP
  ObjectWriteOperation clear_op;
  clear_op.omap_clear();
  ASSERT_EQ(0, ioctx.operate(oid, &clear_op));
  
  // Create snapshot
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger clone creation
  bufferlist write_bl;
  write_bl.append("trigger_clone");
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Add new OMAP data to head
  ObjectWriteOperation write_op;
  std::map<std::string, bufferlist> new_omap = {{"new_key", val_bl}};
  write_op.omap_set(new_omap);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has no OMAP data
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  std::set<std::string> clone_keys;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_keys2("", LONG_MAX, &clone_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_TRUE(clone_keys.empty());
  
  // Verify head has new OMAP data
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  std::set<std::string> head_keys;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(1u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("new_key"));
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneMixedOperations) {
  SKIP_IF_CRIMSON();
  
  const std::string oid = "test_omap_clone_mixed";
  const std::string header1 = "header1";
  const std::string header2 = "header2";
  const std::string value = "value";
  
  // Create object with OMAP data
  bufferlist header1_bl;
  encode(header1, header1_bl);
  bufferlist val_bl;
  encode(value, val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {"keep_key", val_bl},
    {"remove_key", val_bl},
    {"modify_key", val_bl}
  };
  create_test_object_with_omap(oid, omap_map, header1_bl);
  
  // Create snapshot
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger clone creation
  bufferlist write_bl;
  write_bl.append("trigger_clone");
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Perform mixed operations on head
  bufferlist header2_bl;
  encode(header2, header2_bl);
  bufferlist new_val_bl;
  encode("new_value", new_val_bl);
  
  ObjectWriteOperation write_op;
  write_op.omap_set_header(header2_bl);
  std::map<std::string, bufferlist> add_keys = {
    {"add_key", val_bl},
    {"modify_key", new_val_bl}
  };
  write_op.omap_set(add_keys);
  std::set<std::string> remove_keys = {"remove_key"};
  write_op.omap_rm_keys(remove_keys);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has original state
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  // Check clone header
  bufferlist clone_header_bl;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_header(&clone_header_bl, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  
  std::string clone_header;
  decode(clone_header, clone_header_bl);
  ASSERT_EQ(header1, clone_header);
  
  // Check clone keys
  std::set<std::string> clone_keys;
  ObjectReadOperation read_clone_keys;
  err = 0;
  read_clone_keys.omap_get_keys2("", LONG_MAX, &clone_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, clone_keys.size());
  ASSERT_EQ(1u, clone_keys.count("keep_key"));
  ASSERT_EQ(1u, clone_keys.count("remove_key"));
  ASSERT_EQ(1u, clone_keys.count("modify_key"));
  ASSERT_EQ(0u, clone_keys.count("add_key"));
  
  // Check clone value for modify_key
  std::map<std::string, bufferlist> clone_vals;
  ObjectReadOperation read_clone_vals;
  err = 0;
  read_clone_vals.omap_get_vals2("", LONG_MAX, &clone_vals, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone_vals, nullptr));
  ASSERT_EQ(0, err);
  std::string clone_modify_val;
  decode(clone_modify_val, clone_vals["modify_key"]);
  ASSERT_EQ(value, clone_modify_val);
  
  // Verify head has modified state
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  
  // Check head header
  bufferlist head_header_bl;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_header(&head_header_bl, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  
  std::string head_header;
  decode(head_header, head_header_bl);
  ASSERT_EQ(header2, head_header);
  
  // Check head keys
  std::set<std::string> head_keys;
  ObjectReadOperation read_head_keys;
  err = 0;
  read_head_keys.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("keep_key"));
  ASSERT_EQ(0u, head_keys.count("remove_key"));
  ASSERT_EQ(1u, head_keys.count("modify_key"));
  ASSERT_EQ(1u, head_keys.count("add_key"));
  
  // Check head value for modify_key
  std::map<std::string, bufferlist> head_vals;
  ObjectReadOperation read_head_vals;
  err = 0;
  read_head_vals.omap_get_vals2("", LONG_MAX, &head_vals, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head_vals, nullptr));
  ASSERT_EQ(0, err);
  std::string head_modify_val;
  decode(head_modify_val, head_vals["modify_key"]);
  ASSERT_EQ("new_value", head_modify_val);
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneRollback) {
  SKIP_IF_CRIMSON();
  
  const std::string oid = "test_omap_rollback_basic";
  const std::string initial_header = "initial_header";
  const std::string initial_value = "initial_value";
  
  // Create object with initial OMAP data
  bufferlist header_bl;
  encode(initial_header, header_bl);
  
  bufferlist val_bl;
  encode(initial_value, val_bl);
  std::map<std::string, bufferlist> initial_omap = {
    {"key1", val_bl},
    {"key2", val_bl}
  };
  
  create_test_object_with_omap(oid, initial_omap, header_bl);
  
  // Create snapshot (this triggers clone operation)
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger clone creation before modifying OMAP
  bufferlist write_bl;
  write_bl.append("trigger_clone");
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Modify head object OMAP
  const std::string modified_header = "modified_header";
  const std::string modified_value = "modified_value";
  bufferlist mod_header_bl;
  encode(modified_header, mod_header_bl);
  bufferlist mod_val_bl;
  encode(modified_value, mod_val_bl);
  
  ObjectWriteOperation write_op;
  write_op.omap_set_header(mod_header_bl);
  std::map<std::string, bufferlist> modified_omap = {
    {"key3", mod_val_bl}  // Add new key
  };
  write_op.omap_set(modified_omap);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify head has modified OMAP data before rollback
  bufferlist head_header_bl;
  int err = 0;
  ObjectReadOperation read_head;
  read_head.omap_get_header(&head_header_bl, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  
  std::string head_header;
  decode(head_header, head_header_bl);
  ASSERT_EQ(modified_header, head_header);
  
  // Verify head has all three keys
  std::set<std::string> head_keys;
  ObjectReadOperation read_head_keys;
  err = 0;
  read_head_keys.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("key1"));
  ASSERT_EQ(1u, head_keys.count("key2"));
  ASSERT_EQ(1u, head_keys.count("key3"));
  
  // Perform rollback to snapshot
  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback(oid, my_snaps[0]));
  
  // Verify head now has initial OMAP state after rollback
  bufferlist rolled_header_bl;
  ObjectReadOperation read_rolled;
  err = 0;
  read_rolled.omap_get_header(&rolled_header_bl, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_rolled, nullptr));
  ASSERT_EQ(0, err);
  
  std::string rolled_header;
  decode(rolled_header, rolled_header_bl);
  ASSERT_EQ(initial_header, rolled_header);
  
  // Verify head has only initial keys (key3 should be gone)
  std::set<std::string> rolled_keys;
  ObjectReadOperation read_rolled_keys;
  err = 0;
  read_rolled_keys.omap_get_keys2("", LONG_MAX, &rolled_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_rolled_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(2u, rolled_keys.size());
  ASSERT_EQ(1u, rolled_keys.count("key1"));
  ASSERT_EQ(1u, rolled_keys.count("key2"));
  ASSERT_EQ(0u, rolled_keys.count("key3"));
  
  // Verify values are initial values
  std::map<std::string, bufferlist> rolled_vals;
  ObjectReadOperation read_rolled_vals;
  err = 0;
  read_rolled_vals.omap_get_vals2("", LONG_MAX, &rolled_vals, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_rolled_vals, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(2u, rolled_vals.size());
  
  std::string rolled_val1;
  decode(rolled_val1, rolled_vals["key1"]);
  ASSERT_EQ(initial_value, rolled_val1);
  
  std::string rolled_val2;
  decode(rolled_val2, rolled_vals["key2"]);
  ASSERT_EQ(initial_value, rolled_val2);
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
}
TEST_P(OmapTest, OmapCopyFrom) {
  SKIP_IF_CRIMSON();
  
  const std::string src_oid = "test_omap_copy_from_src";
  const std::string dst_oid = "test_omap_copy_from_dst";
  const std::string dst_oid2 = "test_omap_copy_from_dst2";
  
  // Create source object with OMAP data
  const std::string omap_header = "test_omap_header_for_copy";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(src_oid, omap_map, omap_header_bl);
  
  // Get the version of the source object
  version_t src_version = ioctx.get_last_version();
  
  // Test 1: Copy with explicit version
  {
    ObjectWriteOperation write_op;
    write_op.copy_from(src_oid, ioctx, src_version, 0);
    int ret = ioctx.operate(dst_oid, &write_op);
    ASSERT_EQ(0, ret);
    
    // Verify OMAP header was copied
    bufferlist header_read;
    ObjectReadOperation read_header;
    int err = 0;
    read_header.omap_get_header(&header_read, &err);
    ret = ioctx.operate(dst_oid, &read_header, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, err);
    
    std::string header_str;
    decode(header_str, header_read);
    ASSERT_EQ(omap_header, header_str);
    
    // Verify OMAP keys were copied
    std::set<std::string> keys_read;
    ObjectReadOperation read_keys;
    err = 0;
    read_keys.omap_get_keys2("", LONG_MAX, &keys_read, nullptr, &err);
    ret = ioctx.operate(dst_oid, &read_keys, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, err);
    ASSERT_EQ(omap_map.size(), keys_read.size());
    
    // Verify all expected keys are present
    for (const auto& kv : omap_map) {
      ASSERT_EQ(1u, keys_read.count(kv.first));
    }
    
    // Verify OMAP values were copied correctly
    std::map<std::string, bufferlist> vals_read;
    ObjectReadOperation read_vals;
    err = 0;
    read_vals.omap_get_vals2("", LONG_MAX, &vals_read, nullptr, &err);
    ret = ioctx.operate(dst_oid, &read_vals, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, err);
    ASSERT_EQ(omap_map.size(), vals_read.size());
    
    // Verify each value matches the source
    for (const auto& kv : omap_map) {
      ASSERT_EQ(1u, vals_read.count(kv.first));
      ASSERT_TRUE(kv.second.contents_equal(vals_read[kv.first]));
    }
    
    // Verify object data was also copied
    bufferlist data_read;
    ObjectReadOperation read_data;
    read_data.read(0, 4, &data_read, nullptr);
    ret = ioctx.operate(dst_oid, &read_data, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, memcmp(data_read.c_str(), "ceph", 4));
  }
  
  // Test 2: Copy with version=0 (any version)
  {
    ObjectWriteOperation write_op;
    write_op.copy_from(src_oid, ioctx, 0, 0);
    int ret = ioctx.operate(dst_oid2, &write_op);
    ASSERT_EQ(0, ret);
    
    // Verify OMAP header was copied
    bufferlist header_read;
    ObjectReadOperation read_header;
    int err = 0;
    read_header.omap_get_header(&header_read, &err);
    ret = ioctx.operate(dst_oid2, &read_header, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, err);
    
    std::string header_str;
    decode(header_str, header_read);
    ASSERT_EQ(omap_header, header_str);
    
    // Verify OMAP values were copied correctly
    std::map<std::string, bufferlist> vals_read;
    ObjectReadOperation read_vals;
    err = 0;
    read_vals.omap_get_vals2("", LONG_MAX, &vals_read, nullptr, &err);
    ret = ioctx.operate(dst_oid2, &read_vals, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, err);
    ASSERT_EQ(omap_map.size(), vals_read.size());
    
    // Verify each value matches the source
    for (const auto& kv : omap_map) {
      ASSERT_EQ(1u, vals_read.count(kv.first));
      ASSERT_TRUE(kv.second.contents_equal(vals_read[kv.first]));
    }
  }
}

TEST_P(OmapTest, OmapCopyFromOverwritesTarget) {
  SKIP_IF_CRIMSON();
  
  const std::string src_oid = "test_omap_copy_from_src_overwrite";
  const std::string dst_oid = "test_omap_copy_from_dst_overwrite";
  
  // Create source object with OMAP data
  const std::string src_header = "source_header";
  bufferlist src_header_bl;
  encode(src_header, src_header_bl);
  
  std::map<std::string, bufferlist> src_omap_map;
  bufferlist src_val_bl;
  std::string src_value = "source_value";
  encode(src_value, src_val_bl);
  src_omap_map["src_key1"] = src_val_bl;
  src_omap_map["src_key2"] = src_val_bl;
  
  create_test_object_with_omap(src_oid, src_omap_map, src_header_bl);
  
  // Capture source version immediately after creation
  version_t src_version = ioctx.get_last_version();
  
  // Create destination object with DIFFERENT OMAP data
  const std::string dst_header = "destination_header";
  bufferlist dst_header_bl;
  encode(dst_header, dst_header_bl);
  
  std::map<std::string, bufferlist> dst_omap_map;
  bufferlist dst_val_bl;
  std::string dst_value = "destination_value";
  encode(dst_value, dst_val_bl);
  dst_omap_map["dst_key1"] = dst_val_bl;
  dst_omap_map["dst_key2"] = dst_val_bl;
  dst_omap_map["dst_key3"] = dst_val_bl;
  
  create_test_object_with_omap(dst_oid, dst_omap_map, dst_header_bl);
  
  // For EC pools, add another OMAP update to destination to ensure it's in the journal
  if (pool_type == PoolType::FAST_EC) {
    bufferlist updated_dst_header_bl;
    std::string updated_dst_header = "updated_destination_header_in_journal";
    encode(updated_dst_header, updated_dst_header_bl);
    
    ObjectWriteOperation update_op;
    update_op.omap_set_header(updated_dst_header_bl);
    
    // Add a new key that should NOT appear after copy_from
    std::map<std::string, bufferlist> new_dst_keys;
    bufferlist new_dst_val_bl;
    std::string new_dst_value = "new_destination_value_in_journal";
    encode(new_dst_value, new_dst_val_bl);
    new_dst_keys["dst_key_journal_only"] = new_dst_val_bl;
    update_op.omap_set(new_dst_keys);
    
    int ret = ioctx.operate(dst_oid, &update_op);
    ASSERT_EQ(0, ret);
  }
  
  // Perform copy_from to overwrite destination with source
  {
    ObjectWriteOperation write_op;
    write_op.copy_from(src_oid, ioctx, src_version, 0);
    int ret = ioctx.operate(dst_oid, &write_op);
    ASSERT_EQ(0, ret);
  }
  
  // Verify destination now has SOURCE OMAP header, not destination header
  {
    bufferlist header_read;
    ObjectReadOperation read_header;
    int err = 0;
    read_header.omap_get_header(&header_read, &err);
    int ret = ioctx.operate(dst_oid, &read_header, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, err);
    
    std::string header_str;
    decode(header_str, header_read);
    ASSERT_EQ(src_header, header_str) << "Destination should have source header, not destination header";
  }
  
  // Verify destination has SOURCE OMAP keys, not destination keys
  {
    std::set<std::string> keys_read;
    ObjectReadOperation read_keys;
    int err = 0;
    read_keys.omap_get_keys2("", LONG_MAX, &keys_read, nullptr, &err);
    int ret = ioctx.operate(dst_oid, &read_keys, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, err);
    
    // Should have exactly the source keys
    ASSERT_EQ(src_omap_map.size(), keys_read.size()) 
      << "Destination should have " << src_omap_map.size() << " keys from source, not " << keys_read.size();
    
    // Verify source keys are present
    for (const auto& kv : src_omap_map) {
      ASSERT_EQ(1u, keys_read.count(kv.first)) 
        << "Source key '" << kv.first << "' should be present in destination";
    }
    
    // Verify destination keys are NOT present
    ASSERT_EQ(0u, keys_read.count("dst_key1")) 
      << "Old destination key 'dst_key1' should NOT be present after copy_from";
    ASSERT_EQ(0u, keys_read.count("dst_key2")) 
      << "Old destination key 'dst_key2' should NOT be present after copy_from";
    ASSERT_EQ(0u, keys_read.count("dst_key3")) 
      << "Old destination key 'dst_key3' should NOT be present after copy_from";
    
    // For EC pools, verify journal-only key is NOT present
    if (pool_type == PoolType::FAST_EC) {
      ASSERT_EQ(0u, keys_read.count("dst_key_journal_only")) 
        << "Journal-only destination key should NOT be present after copy_from";
    }
  }
  
  // Verify destination has SOURCE OMAP values
  {
    std::map<std::string, bufferlist> vals_read;
    ObjectReadOperation read_vals;
    int err = 0;
    read_vals.omap_get_vals2("", LONG_MAX, &vals_read, nullptr, &err);
    int ret = ioctx.operate(dst_oid, &read_vals, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, err);
    ASSERT_EQ(src_omap_map.size(), vals_read.size());
    
    // Verify each source value is present and correct
    for (const auto& kv : src_omap_map) {
      ASSERT_EQ(1u, vals_read.count(kv.first));
      ASSERT_TRUE(kv.second.contents_equal(vals_read[kv.first])) 
        << "Value for key '" << kv.first << "' should match source value";
    }
  }
}

TEST_P(OmapTest, GenerationalObjectRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
  
  const std::string oid = "gen_recovery_test_oid";
  
  // 1. Create object with omap (generation 0/HEAD)
  std::map<std::string, bufferlist> omap_v1;
  bufferlist val1;
  const std::string original_value = "original_generation_value";
  encode(original_value, val1);
  omap_v1["test_key"] = val1;
  
  bufferlist header1;
  const std::string original_header = "original_generation_header";
  encode(original_header, header1);
  
  ObjectWriteOperation write1;
  write1.create(true);  // exclusive create
  write1.omap_set(omap_v1);
  write1.omap_set_header(header1);
  int ret = ioctx.operate(oid, &write1);
  EXPECT_EQ(ret, 0);
  
  std::cout << "Created object with original omap data" << std::endl;
  
  // 2. Delete object (creates a generational tombstone)
  ObjectWriteOperation del_op;
  del_op.remove();
  ret = ioctx.operate(oid, &del_op);
  EXPECT_EQ(ret, 0);
  
  std::cout << "Deleted object (generational tombstone created)" << std::endl;
  
  // 3. Recreate object with DIFFERENT omap (new HEAD, old gen still exists)
  std::map<std::string, bufferlist> omap_v2;
  bufferlist val2;
  const std::string new_head_value = "new_head_generation_value";
  encode(new_head_value, val2);
  omap_v2["test_key"] = val2;
  
  bufferlist header2;
  const std::string new_head_header = "new_head_generation_header";
  encode(new_head_header, header2);
  
  ObjectWriteOperation write2;
  write2.create(true);
  write2.omap_set(omap_v2);
  write2.omap_set_header(header2);
  ret = ioctx.operate(oid, &write2);
  EXPECT_EQ(ret, 0);
  
  std::cout << "Recreated object with new HEAD omap data" << std::endl;
  
  // 4. Get current OSD mapping
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map(oid, &reply);
  EXPECT_TRUE(res == 0);
  
  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);
  
  // 5. Trigger recovery via upmap IMMEDIATELY (before delete fully completes)
  std::vector<int> new_up_osds = prev_up_osds;
  std::swap(new_up_osds[0], new_up_osds[new_up_osds.size() - 1]);
  int new_primary = new_up_osds[0];
  
  std::cout << "Previous primary osd: " << prev_up_osds[0] << std::endl;
  std::cout << "New primary osd: " << new_primary << std::endl;
  print_osd_map("Desired up osds: ", new_up_osds);
  
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_TRUE(rc == 0);
  
  std::cout << "Set new upmap to trigger recovery" << std::endl;
  
  // 6. Wait for recovery to complete
  int res2 = wait_for_upmap(oid, new_primary, 60s);
  EXPECT_TRUE(res2 == 0);
  
  std::cout << "Recovery completed" << std::endl;
  
  // 7. Read omap - verify we got the NEW head value, not old generation
  // If the bug exists, recovery might have retrieved data from wrong generation
  std::map<std::string, bufferlist> result_omap;
  std::set<std::string> keys{"test_key"};
  ObjectReadOperation read_op;
  int err = 0;
  read_op.omap_get_vals_by_keys(keys, &result_omap, &err);
  ret = ioctx.operate(oid, &read_op, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(err, 0);
  
  ASSERT_EQ(1u, result_omap.size());
  ASSERT_TRUE(result_omap.count("test_key") > 0);
  
  std::string result_val;
  decode(result_val, result_omap["test_key"]);
  
  std::cout << "Retrieved omap value: " << result_val << std::endl;
  std::cout << "Expected value: " << new_head_value << std::endl;
  
  // This assertion will FAIL if the bug exists and wrong generation was recovered
  EXPECT_EQ(result_val, new_head_value) 
    << "ERROR: Recovered wrong generation! Got '" << result_val 
    << "' but expected '" << new_head_value << "'";
  
  // 8. Also verify omap header
  bufferlist result_header;
  ObjectReadOperation read_header_op;
  err = 0;
  read_header_op.omap_get_header(&result_header, &err);
  ret = ioctx.operate(oid, &read_header_op, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(err, 0);
  
  std::string result_header_str;
  decode(result_header_str, result_header);
  
  std::cout << "Retrieved omap header: " << result_header_str << std::endl;
  std::cout << "Expected header: " << new_head_header << std::endl;
  
  // This assertion will FAIL if the bug exists and wrong generation header was recovered
  EXPECT_EQ(result_header_str, new_head_header)
    << "ERROR: Recovered wrong generation header! Got '" << result_header_str
    << "' but expected '" << new_head_header << "'";
  
  turn_balancing_on();
}

// Instantiate tests for both pool types
INSTANTIATE_TEST_SUITE_P(, OmapTest,
  ::testing::Values(
    PoolType::REPLICATED,
    PoolType::FAST_EC
  ),
  [](const ::testing::TestParamInfo<PoolType>& info) {
    return pool_type_name(info.param);
  }
);
