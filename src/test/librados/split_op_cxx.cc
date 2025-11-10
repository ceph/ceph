#include <common/perf_counters_collection.h>

#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/version/cls_version_ops.h"

#include <climits>

using namespace std;
using namespace librados;

typedef RadosTestPP LibRadosSplitOpPP;
typedef RadosTestECPP LibRadosSplitOpECPP;

TEST_P(LibRadosSplitOpECPP, ReadWithVersion) {
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
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);

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

TEST_P(LibRadosSplitOpECPP, XattrReads) {
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

  int getxattr_rval, getxattrs_rval;
  read.getxattr(attr_key.c_str(), &attr_read_bl, &getxattr_rval);
  std::map<string, bufferlist> pattrs{ {"_", {}}, {attr_key, {}}};
  read.getxattrs(&pattrs, &getxattrs_rval);
  read.cmpxattr(attr_key.c_str(), CEPH_OSD_CMPXATTR_OP_EQ, attr_bl);

  ASSERT_TRUE(AssertOperateWithSplitOp(1, "foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
  ASSERT_EQ(0, getxattr_rval);
  ASSERT_EQ(0, getxattrs_rval);
}

TEST_P(LibRadosSplitOpECPP, OMAPReads) {
  SKIP_IF_CRIMSON();
  bufferlist bl, omap_read_bl, omap_val_bl;
  std::string omap_key = "my_key";
  std::string omap_value = "my_value";
  encode(omap_value, omap_val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {omap_key, omap_val_bl}
  };

  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  write1.omap_set(omap_map);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);

  int err = 0;
  std::set<std::string> keys;
  read.omap_get_keys2("", LONG_MAX, &keys, nullptr, &err);
  ASSERT_EQ(0, err);

  std::map<std::string,bufferlist> vals;
  read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
  ASSERT_EQ(0, err);

  ASSERT_TRUE(AssertOperateWithSplitOp(1, "foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
}

INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpECPP);
