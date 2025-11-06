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

#include <climits>

using namespace std;
using namespace librados;

typedef RadosTestPP LibRadosSplitOpPP;
typedef RadosTestECPP LibRadosSplitOpECPP;

TEST_P(LibRadosSplitOpECPP, OMAPReads) {
  SKIP_IF_CRIMSON();
  bufferlist bl_write, omap_val_bl, omap_header_bl;
  const std::string omap_key_1 = "omap_key_1_palomino";
  const std::string omap_key_2 = "omap_key_2_chesnut";
  const std::string omap_key_3 = "omap_key_3_bay";
  const std::string omap_value = "omap_value_1_horse";
  const std::string omap_header = "my_omap_header";

  encode(omap_value, omap_val_bl);
  encode(omap_header, omap_header_bl);

  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1.c_str(), omap_val_bl},
    {omap_key_2.c_str(), omap_val_bl},
    {omap_key_3.c_str(), omap_val_bl}
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
  read.omap_get_vals2("omap", 3, &returned_vals_2, nullptr, &err);

  ASSERT_TRUE(AssertOperateWithSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_vals_1.size(), (unsigned)1);
  ASSERT_EQ(returned_vals_2.size(), (unsigned)3);

  std::cout << "--- OMap Vals testing passed ---" << std::endl;


  // OMAP GET KEYS TESTING

  std::set<std::string> returned_keys;

  read.omap_get_keys2("", 10, &returned_keys, nullptr, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys.size(), (unsigned)3);

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

  keys_to_remove.insert("omap_key_2_chesnut");

  write1.omap_rm_keys(keys_to_remove);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &write1));

  read.omap_get_keys2("", 10, &returned_keys_with_removed, nullptr, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys_with_removed.size(), (unsigned)2);

  std::cout << "--- OMap Remove Keys testing passed ---" << std::endl;

  // // OMAP REMOVE KEYS RANGE TESTING

  // std::set<std::string> returned_keys_with_removed_range;
  // std::string key_begin = "omap_key_1_palomino";
  // std::string key_end = "omap_key_3_bay";

  // write1.omap_rmkeyrange(key_begin, key_end);

  // ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &write1));

  // read.omap_get_keys2("", 10, &returned_keys_with_removed_range, nullptr, &err);

  // ASSERT_TRUE(AssertOperateWithSplitOp(0, "my_object", &read, nullptr));
  
  // ASSERT_EQ(0, err);
  // ASSERT_EQ(returned_keys_with_removed_range.size(), (unsigned)0);

  // std::cout << "--- OMap Remove Key Range testing passed ---" << std::endl; 

  // OMAP CLEAR TESTING

  write1.omap_clear();

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &write1));

  read.omap_get_keys2("", 10, &returned_keys, nullptr, &err);

  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "my_object", &read, nullptr));

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys.size(), (unsigned)0);

  std::cout << "--- OMap Clear testing passed ---" << std::endl;
}

TEST_P(LibRadosSplitOpECPP, ReadWithVersion) {
  SKIP_IF_CRIMSON();
  bufferlist bl, attr_bl, attr_read_bl;
  std::string attr_key = "my_key";
  std::string attr_value = "my_attr";

  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  encode(attr_value, attr_bl);
  write1.setxattr(attr_key.c_str(), attr_bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);

  uint64_t size;
  timespec time;
  time.tv_nsec = 0;
  time.tv_sec = 0;
  int stat_rval;
  read.stat2(&size, &time, &stat_rval);

  ASSERT_TRUE(AssertOperateWithSplitOp(0, "foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
  ASSERT_EQ(0, stat_rval);
  ASSERT_EQ(4, size);
  ASSERT_NE(0, time.tv_nsec);
  ASSERT_NE(0, time.tv_sec);
}

TEST_P(LibRadosSplitOpECPP, OMAPReads) {
  SKIP_IF_CRIMSON();
  bufferlist bl_write, omap_val_bl, omap_header_bl;
  const std::string omap_key_1 = "omap_key_1_elephant";
  const std::string omap_key_2 = "omap_key_2_fox";
  const std::string omap_key_3 = "omap_key_3_squirrel";
  const std::string omap_value = "omap_value_1_giraffe";
  const std::string omap_header = "this is the omap header";
  
  encode(omap_value, omap_val_bl);
  encode(omap_header, omap_header_bl);
  
  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1, omap_val_bl},
    {omap_key_2, omap_val_bl},
    {omap_key_3, omap_val_bl}
  };

  bl_write.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set_header(omap_header_bl);
  write1.omap_set(omap_map);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &write1));

  int err = 0;
  bufferlist bl_read;
  ObjectReadOperation read;

  
  read.read(0, bl_write.length(), &bl_read, nullptr);
  std::map<std::string,bufferlist> vals{ {"_", {}}, {omap_key_1, {}}};
  read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, "omap_oid_axolotl", &read, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
  ASSERT_EQ(3U, vals.size());
  ASSERT_NE(vals.find(omap_key_1), vals.end());
  bufferlist retrieved_val_bl = vals[omap_key_1];
  std::string retrieved_value;
  decode(retrieved_value, retrieved_val_bl);
  ASSERT_EQ(omap_value, retrieved_value);

  bufferlist omap_header_read_bl;
  std::set<std::string> keys;
  read.omap_get_keys2("", LONG_MAX, &keys, nullptr, &err);
  read.omap_get_header(&omap_header_read_bl, &err);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &read, nullptr));
  ASSERT_EQ(0, err);
  std::string omap_header_read;
  decode(omap_header_read, omap_header_read_bl);
  ASSERT_EQ(omap_header, omap_header_read);
  ASSERT_EQ(3U, keys.size());
  
  std::map<std::string,bufferlist> vals_by_keys{ {"_", {}} };
  std::set<std::string> key_filter = {omap_key_1, omap_key_2};
  read.omap_get_vals_by_keys(key_filter, &vals_by_keys, &err);
  std::map<std::string, std::pair<bufferlist, int> > assertions;
  assertions[omap_key_3] = make_pair(omap_val_bl, CEPH_OSD_CMPXATTR_OP_EQ);
  read.omap_cmp(assertions, &err);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &read, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(2U, vals_by_keys.size());

  std::set<std::string> keys_to_remove = {omap_key_2};
  write1.omap_rm_keys(keys_to_remove);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &write1));
  read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &read, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(2U, vals.size());

  write1.omap_clear();
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &write1));
  read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &read, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(0U, vals.size());

  // omap_rmkeyrange has not been tested
}

TEST_P(LibRadosSplitOpECPP, ErrorInject) {
  SKIP_IF_CRIMSON();
  bufferlist bl_write, omap_val_bl, xattr_val_bl;
  const std::string omap_key_1 = "key_a";
  const std::string omap_key_2 = "key_b";
  const std::string omap_value = "val_c";
  const std::string xattr_key = "xattr_key_1";
  const std::string xattr_value = "xattr_value_2";
  encode(omap_value, omap_val_bl);
  encode(xattr_value, xattr_val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1, omap_val_bl},
    {omap_key_2, omap_val_bl}
  };
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
  float prev_value = cct->_conf->osd_debug_reject_backfill_probability;
  cct->_conf->osd_debug_reject_backfill_probability = 1.0;


  // 3. Read from xattr before taking primary osd out
  int err = 0;
  ObjectReadOperation read;
  bufferlist attr_read_bl;
  read.getxattr(xattr_key.c_str(), &attr_read_bl, &err);
  std::map<string, bufferlist> pattrs{ {"_", {}}, {xattr_key, {}}};
  read.getxattrs(&pattrs, &err);
  read.cmpxattr(xattr_key.c_str(), CEPH_OSD_CMPXATTR_OP_EQ, xattr_val_bl);
  int ret = ioctx.operate("error_inject_oid", &read, nullptr);
  EXPECT_TRUE(ret == 1);
  EXPECT_EQ(0, err);
  EXPECT_EQ(1U, pattrs.size());


  // 4. Find primary osd for relevant pg
  int primary_osd = -1;
  bufferlist map_inbl, map_outbl;
  auto map_formatter = std::make_unique<JSONFormatter>(false);
  ceph::messaging::osd::OSDMapRequest osdMapRequest{pool_name, "error_inject_oid", ""};
  encode_json("OSDMapRequest", osdMapRequest, map_formatter.get());

  std::ostringstream map_oss;
  map_formatter.get()->flush(map_oss);
  int rc = cluster.mon_command(map_oss.str(), map_inbl, &map_outbl, nullptr);
  EXPECT_TRUE(rc == 0);

  JSONParser p;
  bool success = p.parse(map_outbl.c_str(), map_outbl.length());
  EXPECT_TRUE(success);
  ceph::messaging::osd::OSDMapReply reply;
  reply.decode_json(&p);
  primary_osd = reply.acting_primary;


  // 5. Take primary osd out
  bufferlist out_inbl, out_outbl;
  std::ostringstream out_oss;
  out_oss << "{\"prefix\": \"osd out\", \"ids\": [\"" << primary_osd << "\"]}";
  rc = cluster.mon_command(out_oss.str(), out_inbl, &out_outbl, nullptr);
  EXPECT_TRUE(rc == 0);
  

  // 6a. Read from omap
  bufferlist bl_read;
  read.read(0, bl_write.length(), &bl_read, nullptr);
  ret = ioctx.operate("error_inject_oid", &read, nullptr);
  EXPECT_TRUE(ret == 0);

  std::map<std::string,bufferlist> vals{ {"_", {}} };
  read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
  EXPECT_TRUE(AssertOperateWithoutSplitOp(0, "error_inject_oid", &read, nullptr));
  EXPECT_EQ(0, err);
  EXPECT_EQ(2U, vals.size());
  EXPECT_NE(vals.find(omap_key_1), vals.end());
  bufferlist retrieved_val_bl = vals[omap_key_1];
  std::string retrieved_value;
  decode(retrieved_value, retrieved_val_bl);
  EXPECT_EQ(omap_value, retrieved_value);

  // 6b. Read from xattr after taking primary osd out
  bufferlist attr_read_bl_after;
  read.getxattr(xattr_key.c_str(), &attr_read_bl_after, &err);
  std::map<string, bufferlist> pattrs_after{ {"_", {}}, {xattr_key, {}}};
  read.getxattrs(&pattrs_after, &err);
  read.cmpxattr(xattr_key.c_str(), CEPH_OSD_CMPXATTR_OP_EQ, xattr_val_bl);
  ret = ioctx.operate("error_inject_oid", &read, nullptr);
  EXPECT_TRUE(ret == 1);
  EXPECT_EQ(0, err);
  EXPECT_EQ(1U, pattrs_after.size());


  // 7. Set osd_debug_reject_backfill_probability to 0.0
  cct->_conf->osd_debug_reject_backfill_probability = 0.0;


  // 8. Take all osds out
  std::vector<int> osds = reply.up;
  bufferlist out_all_inbl, out_all_outbl;
  std::ostringstream out_all_oss;
  out_all_oss << "{\"prefix\": \"osd out\", \"ids\": [";
  for (size_t i = 0; i < osds.size(); i++) {
    out_all_oss << "\"" << osds[i] << "\"";
    if (i != osds.size() - 1) {
      out_all_oss << ", ";
    }
  }
  out_all_oss << "]}";
  rc = cluster.mon_command(out_all_oss.str(), out_all_inbl, &out_all_outbl, nullptr);
  EXPECT_TRUE(rc == 0);


  // 9. Put all osds back in
  bufferlist in_all_inbl, in_all_outbl;
  std::ostringstream in_all_oss;
  in_all_oss << "{\"prefix\": \"osd in\", \"ids\": [";
  for (size_t i = 0; i < osds.size(); i++) {
    in_all_oss << "\"" << osds[i] << "\"";
    if (i != osds.size() - 1) {
      in_all_oss << ", ";
    }
  }
  in_all_oss << "]}";
  rc = cluster.mon_command(in_all_oss.str(), in_all_inbl, &in_all_outbl, nullptr);
  EXPECT_TRUE(rc == 0);


  // 10. Set osd_debug_reject_backfill_probability back to previous value
  cct->_conf->osd_debug_reject_backfill_probability = prev_value;
}

INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpECPP);
