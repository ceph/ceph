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

TEST_P(LibRadosSplitOpECPP, XattrReads) {
  SKIP_IF_CRIMSON();
  bufferlist bl, attr_bl, attr_read_bl;
  std::string attr_key = "xattr_key";
  std::string attr_value = "xattr_value";

  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  encode(attr_value, attr_bl);
  write1.setxattr(attr_key.c_str(), attr_bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "xattr_oid_pumpkin", &write1));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);

  int getxattr_rval, getxattrs_rval;
  read.getxattr(attr_key.c_str(), &attr_read_bl, &getxattr_rval);
  std::map<string, bufferlist> pattrs{ {"_", {}}, {attr_key, {}}};
  read.getxattrs(&pattrs, &getxattrs_rval);
  read.cmpxattr(attr_key.c_str(), CEPH_OSD_CMPXATTR_OP_EQ, attr_bl);

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

  bufferlist bl_read;
  ObjectReadOperation read;
  
  read.read(0, bl_write.length(), &bl_read, nullptr);

  int err = 0;
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
}

INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpECPP);
