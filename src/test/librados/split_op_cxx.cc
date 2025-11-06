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

// TEST_P(LibRadosSplitOpECPP, XattrReads) {
//   SKIP_IF_CRIMSON();
//   bufferlist bl, attr_bl, attr_read_bl;
//   std::string attr_key = "xattr_key";
//   std::string attr_value = "xattr_value";

//   bl.append("ceph");
//   ObjectWriteOperation write1;
//   write1.write(0, bl);
//   encode(attr_value, attr_bl);
//   write1.setxattr(attr_key.c_str(), attr_bl);
//   ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "xattr_oid_pumpkin", &write1));

//   ObjectReadOperation read;
//   read.read(0, bl.length(), NULL, NULL);

//   int getxattr_rval, getxattrs_rval;
//   read.getxattr(attr_key.c_str(), &attr_read_bl, &getxattr_rval);
//   std::map<string, bufferlist> pattrs{ {"_", {}}, {attr_key, {}}};
//   read.getxattrs(&pattrs, &getxattrs_rval);
//   read.cmpxattr(attr_key.c_str(), CEPH_OSD_CMPXATTR_OP_EQ, attr_bl);

  ASSERT_TRUE(AssertOperateWithSplitOp(1, "xattr_oid_pumpkin", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));

  ASSERT_EQ(0, getxattr_rval);
  ASSERT_EQ(0, getxattrs_rval);
  ASSERT_EQ(1U, pattrs.size());
}

TEST_P(LibRadosSplitOpECPP, ReadWithVersion) {
  SKIP_IF_CRIMSON();
  bufferlist bl, attr_bl, attr_read_bl;
  std::string attr_key = "my_key";
  std::string attr_value = "my_attr";

//   bl.append("ceph");
//   ObjectWriteOperation write1;
//   write1.write(0, bl);
//   encode(attr_value, attr_bl);
//   write1.setxattr(attr_key.c_str(), attr_bl);
//   ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

//   ObjectReadOperation read;
//   read.read(0, bl.length(), NULL, NULL);

//   uint64_t size;
//   timespec time;
//   time.tv_nsec = 0;
//   time.tv_sec = 0;
//   int stat_rval;
//   read.stat2(&size, &time, &stat_rval);

//   ASSERT_TRUE(AssertOperateWithSplitOp(0, "foo", &read, &bl));
//   ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
//   ASSERT_EQ(0, stat_rval);
//   ASSERT_EQ(4, size);
//   ASSERT_NE(0, time.tv_nsec);
//   ASSERT_NE(0, time.tv_sec);
// }

// TEST_P(LibRadosSplitOpECPP, OMAPReads) {
//   SKIP_IF_CRIMSON();
//   bufferlist bl_write, omap_val_bl, omap_header_bl;
//   const std::string omap_key_1 = "omap_key_1_elephant";
//   const std::string omap_key_2 = "omap_key_2_fox";
//   const std::string omap_key_3 = "omap_key_3_squirrel";
//   const std::string omap_value = "omap_value_1_giraffe";
//   const std::string omap_header = "this is the omap header";
  
//   encode(omap_value, omap_val_bl);
//   encode(omap_header, omap_header_bl);
  
//   std::map<std::string, bufferlist> omap_map = {
//     {omap_key_1, omap_val_bl},
//     {omap_key_2, omap_val_bl},
//     {omap_key_3, omap_val_bl}
//   };

//   bl_write.append("ceph");
//   ObjectWriteOperation write1;
//   write1.write(0, bl_write);
//   write1.omap_set_header(omap_header_bl);
//   write1.omap_set(omap_map);
//   ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &write1));

//   int err = 0;
//   bufferlist bl_read;
//   ObjectReadOperation read;

  
//   read.read(0, bl_write.length(), &bl_read, nullptr);
//   std::map<std::string,bufferlist> vals{ {"_", {}}, {omap_key_1, {}}};
//   read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
//   ASSERT_TRUE(AssertOperateWithSplitOp(0, "omap_oid_axolotl", &read, nullptr));
//   ASSERT_EQ(0, err);
//   ASSERT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
//   ASSERT_EQ(3U, vals.size());
//   ASSERT_NE(vals.find(omap_key_1), vals.end());
//   bufferlist retrieved_val_bl = vals[omap_key_1];
//   std::string retrieved_value;
//   decode(retrieved_value, retrieved_val_bl);
//   ASSERT_EQ(omap_value, retrieved_value);

//   bufferlist omap_header_read_bl;
//   std::set<std::string> keys;
//   read.omap_get_keys2("", LONG_MAX, &keys, nullptr, &err);
//   read.omap_get_header(&omap_header_read_bl, &err);
//   ASSERT_TRUE(AssertOperateWithSplitOp(0, "omap_oid_axolotl", &read, nullptr));
//   ASSERT_EQ(0, err);
//   std::string omap_header_read;
//   decode(omap_header_read, omap_header_read_bl);
//   ASSERT_EQ(omap_header, omap_header_read);
//   ASSERT_EQ(3U, keys.size());
  
//   std::map<std::string,bufferlist> vals_by_keys{ {"_", {}} };
//   std::set<std::string> key_filter = {omap_key_1, omap_key_2};
//   read.omap_get_vals_by_keys(key_filter, &vals_by_keys, &err);
//   std::map<std::string, std::pair<bufferlist, int> > assertions;
//   assertions[omap_key_3] = make_pair(omap_val_bl, CEPH_OSD_CMPXATTR_OP_EQ);
//   read.omap_cmp(assertions, &err);
//   ASSERT_TRUE(AssertOperateWithSplitOp(0, "omap_oid_axolotl", &read, nullptr));
//   ASSERT_EQ(0, err);
//   ASSERT_EQ(2U, vals_by_keys.size());

//   std::set<std::string> keys_to_remove = {omap_key_2};
//   write1.omap_rm_keys(keys_to_remove);
//   ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &write1));
//   read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
//   ASSERT_TRUE(AssertOperateWithSplitOp(0, "omap_oid_axolotl", &read, nullptr));
//   ASSERT_EQ(0, err);
//   ASSERT_EQ(2U, vals.size());

//   write1.omap_clear();
//   ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_axolotl", &write1));
//   read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
//   ASSERT_TRUE(AssertOperateWithSplitOp(0, "omap_oid_axolotl", &read, nullptr));
//   ASSERT_EQ(0, err);
//   ASSERT_EQ(0U, vals.size());

//   // omap_rmkeyrange has not been tested
// }

TEST_P(LibRadosSplitOpECPP, OMAPErrorInject) {
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
  EXPECT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_inject", &write1));

  // 1b. Write data to xattrs
  write1.setxattr(xattr_key.c_str(), xattr_val_bl);
  EXPECT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_inject", &write1));


  // 2. Set osd_debug_reject_backfill_probability to 1.0
  CephContext* cct = static_cast<CephContext*>(cluster.cct());
  float prev_value = cct->_conf->osd_debug_reject_backfill_probability;
  cct->_conf->osd_debug_reject_backfill_probability = 1.0;


  // 3. Find primary osd for relevant pg
  int primary_osd = -1;
  bufferlist map_inbl, map_outbl;
  auto map_formatter = std::make_unique<JSONFormatter>(false);
  ceph::messaging::osd::OSDMapRequest osdMapRequest{pool_name, "omap_oid_inject", ""};
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


  // 4. Take primary osd out
  bufferlist out_inbl, out_outbl;
  std::ostringstream out_oss;
  out_oss << "{\"prefix\": \"osd out\", \"ids\": [\"" << primary_osd << "\"]}";
  rc = cluster.mon_command(out_oss.str(), out_inbl, &out_outbl, nullptr);
  EXPECT_TRUE(rc == 0);
  

  // 5a. Read from omap
  ObjectReadOperation read;
  int err = 0;

  bufferlist bl_read;
  read.read(0, bl_write.length(), &bl_read, nullptr);
  EXPECT_TRUE(AssertOperateWithSplitOp(0, "omap_oid_inject", &read, nullptr));

  std::map<std::string,bufferlist> vals{ {"_", {}} };
  read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
  EXPECT_TRUE(AssertOperateWithoutSplitOp(0, "omap_oid_inject", &read, nullptr));
  EXPECT_EQ(0, err);
  EXPECT_EQ(2U, vals.size());
  EXPECT_NE(vals.find(omap_key_1), vals.end());
  bufferlist retrieved_val_bl = vals[omap_key_1];
  std::string retrieved_value;
  decode(retrieved_value, retrieved_val_bl);
  EXPECT_EQ(omap_value, retrieved_value);

  // 5b. Read from xattr
  bufferlist attr_read_bl;
  read.getxattr(xattr_key.c_str(), &attr_read_bl, &err);
  std::map<string, bufferlist> pattrs{ {"_", {}}, {xattr_key, {}}};
  read.getxattrs(&pattrs, &err);
  read.cmpxattr(xattr_key.c_str(), CEPH_OSD_CMPXATTR_OP_EQ, xattr_val_bl);
  EXPECT_TRUE(AssertOperateWithSplitOp(1, "omap_oid_inject", &read, nullptr));
  EXPECT_EQ(0, err);
  EXPECT_EQ(1U, pattrs.size());


  // 6. Set osd_debug_reject_backfill_probability to 0.0
  cct->_conf->osd_debug_reject_backfill_probability = 0.0;


  // 7. Take all osds out
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


  // 8. Put all osds back in
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


  // 9. Set osd_debug_reject_backfill_probability back to previous value
  cct->_conf->osd_debug_reject_backfill_probability = prev_value;
}

INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpECPP);
