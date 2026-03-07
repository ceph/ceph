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
