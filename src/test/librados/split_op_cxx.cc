#include <common/perf_counters_collection.h>

#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/version/cls_version_ops.h"
#include <fmt/format.h>
#include <json_spirit/json_spirit.h>

#include "common/ceph_json.h"
#include "common/JSONFormatter.h"
#include "common/json/OSDStructures.h"
#include "librados/librados_asio.h"

#include <boost/asio/io_context.hpp>

#include <algorithm>
#include <climits>
#include <thread>
#include <chrono>

using namespace std;
using namespace librados;

typedef RadosTestPP LibRadosSplitOpPP;
typedef RadosTestECPP LibRadosSplitOpECPP;

TEST_P(LibRadosSplitOpECPP, OMAPReads) {
  SKIP_IF_CRIMSON();
  bufferlist bl_write, omap_val_bl, omap_header_bl;
  const std::string omap_key_1 = "omap_key_1_palomino";
  const std::string omap_key_2 = "omap_key_2_chestnut";
  const std::string omap_key_3 = "omap_key_3_bay";
  const std::string omap_key_4 = "omap_key_4_dun";
  const std::string omap_key_5 = "omap_key_5_black";
  const std::string omap_key_6 = "omap_key_6_grey";
  const std::string omap_value = "omap_value_1_horse";
  const std::string omap_header = "my_omap_header";

  encode(omap_value, omap_val_bl);
  encode(omap_header, omap_header_bl);

  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1.c_str(), omap_val_bl},
    {omap_key_2.c_str(), omap_val_bl},
    {omap_key_3.c_str(), omap_val_bl},
    {omap_key_4.c_str(), omap_val_bl},
    {omap_key_5.c_str(), omap_val_bl},
    {omap_key_6.c_str(), omap_val_bl}
  };

  bl_write.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set_header(omap_header_bl);
  write1.omap_set(omap_map);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &write1));

  int err = 0;
  bufferlist bl_read;
  ObjectReadOperation read;

  // OMAP GET VALS TESTING

  read.read(0, bl_write.length(), &bl_read, NULL);

  std::map<std::string, bufferlist> returned_vals_1;
  std::map<std::string, bufferlist> returned_vals_2;

  read.omap_get_vals2(omap_key_1, 1, &returned_vals_1, nullptr, &err);
  read.omap_get_vals2("omap", 4, &returned_vals_2, nullptr, &err);

  ASSERT_TRUE(AssertOperateWithSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_vals_1.size(), (unsigned)1);
  ASSERT_EQ(returned_vals_2.size(), (unsigned)4);

  std::cout << "--- OMap Vals testing passed ---" << std::endl;


  // OMAP GET KEYS TESTING

  std::set<std::string> returned_keys;

  read.omap_get_keys2("", 10, &returned_keys, nullptr, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys.size(), (unsigned)6);

  std::cout << "--- OMap Keys testing passed ---" << std::endl;

  // OMAP GET HEADER TESTING

  bufferlist returned_header_bl;

  read.omap_get_header(&returned_header_bl, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  std::string returned_header_str;
  decode(returned_header_str, returned_header_bl);

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_header_str, omap_header);

  std::cout << "--- OMap Header testing passed ---" << std::endl;

  // OMAP GET VALS BY KEYS TESTING

  std::set<std::string> key_filter = { omap_key_1, omap_key_3 };
  std::map<std::string, bufferlist> returned_vals_by_keys;

  read.omap_get_vals_by_keys(key_filter, &returned_vals_by_keys, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_vals_by_keys.size(), (unsigned)2);

  std::cout << "--- OMap Vals by Keys testing passed ---" << std::endl;

  // OMAP CMP TESTING

  map<string, pair<bufferlist, int>> cmp_results;
  bufferlist cmp_val_bl;

  encode(omap_value, cmp_val_bl);

  cmp_results["omap_key_1_palomino"] = pair<bufferlist, int>(cmp_val_bl, CEPH_OSD_CMPXATTR_OP_EQ);

  read.omap_cmp(cmp_results, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, err);

  std::cout << "--- OMap Cmp testing passed ---" << std::endl;

  // OMAP REMOVE KEYS TESTING

  std::set<std::string> keys_to_remove;
  std::set<std::string> returned_keys_with_removed;

  keys_to_remove.insert("omap_key_2_chestnut");

  write1.omap_rm_keys(keys_to_remove);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &write1));

  read.omap_get_keys2("", 10, &returned_keys_with_removed, nullptr, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys_with_removed.size(), (unsigned)5);

  std::cout << "--- OMap Remove Keys testing passed ---" << std::endl;

  // OMAP CLEAR TESTING

  write1.omap_clear();

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &write1));

  read.omap_get_keys2("", 10, &returned_keys, nullptr, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys.size(), (unsigned)0);

  std::cout << "--- OMap Clear testing passed ---" << std::endl;
}

TEST_P(LibRadosSplitOpECPP, ErrorInject) {
  SKIP_IF_CRIMSON();
  bufferlist bl_write, omap_val_bl, xattr_val_bl;
  const std::string omap_key_1 = "key_a";
  const std::string omap_key_2 = "key_b";
  const std::string omap_value = "val_c";
  encode(omap_value, omap_val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1, omap_val_bl},
    {omap_key_2, omap_val_bl}
  };
  const std::string xattr_key = "xattr_key_1";
  const std::string xattr_value = "xattr_value_2";
  encode(xattr_value, xattr_val_bl);
  bl_write.append("ceph");
  
  // 1a. Write data to omap
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set(omap_map);
  EXPECT_TRUE(AssertOperateWithoutSplitOp(0, "error_inject_oid", &write1));

  // 1b. Write data to xattrs
  write1.setxattr(xattr_key.c_str(), xattr_val_bl);
  EXPECT_TRUE(AssertOperateWithoutSplitOp(0, "error_inject_oid", &write1));

  // 2. Set osd_debug_reject_backfill_probability to 1.0
  CephContext* cct = static_cast<CephContext*>(cluster.cct());
  cct->_conf->osd_debug_reject_backfill_probability = 1.0;

  // 3. Read xattrs before switching primary osd
  read_xattrs("error_inject_oid", xattr_key, xattr_value, 1, 1, 0);

  // 4. Find up osds
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map(pool_name, "error_inject_oid", nspace, &reply);
  EXPECT_TRUE(res == 0);
  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);
  
  // 5. Find unused osd to be new primary
  int prev_primary = prev_up_osds[0];
  int new_primary = 0;
  while (true) {
    auto it = std::find(prev_up_osds.begin(), prev_up_osds.end(), new_primary);
    if (it == prev_up_osds.end()) {
      break;
    }
    new_primary++;
  }
  std::vector<int> new_up_osds = prev_up_osds;
  new_up_osds[0] = new_primary;
  std::cout << "Previous primary osd: " << prev_primary << std::endl;
  std::cout << "New primary osd: " << new_primary << std::endl;
  print_osd_map("Desired up osds: ", new_up_osds);

  // 6. Set new up map
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_TRUE(rc == 0);

  // 7. Wait for new upmap to appear as acting set of osds
  int res2 = wait_for_upmap(pool_name, "error_inject_oid", nspace, new_primary, 30s);
  EXPECT_TRUE(res2 == 0);
  
  // 8a. Read omap
  read_omap("error_inject_oid", omap_key_1, omap_value, 2, 0);

  // 8b. Read xattrs after switching primary osd
  read_xattrs("error_inject_oid", xattr_key, xattr_value, 1, 1, 0);

  // 9. Set osd_debug_reject_backfill_probability to 0.0
  cct->_conf->osd_debug_reject_backfill_probability = 0.0;

  // 10. Reset up map to previous values
  int rc2 = set_osd_upmap(pgid, prev_up_osds);
  EXPECT_TRUE(rc2 == 0);

  // 11. Wait for prev upmap to appear as acting set of osds
  int res3 = wait_for_upmap(pool_name, "error_inject_oid", nspace, prev_primary, 30s);
  EXPECT_TRUE(res3 == 0);
}

INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpECPP);
