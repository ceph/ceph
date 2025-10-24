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
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);

  bufferlist exec_inbl, exec_outbl;
  int exec_rval;
  read.exec("version", "read", exec_inbl, &exec_outbl, &exec_rval);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, "foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
  ASSERT_EQ(0, exec_rval);
  cls_version_read_ret exec_version;
  auto iter = exec_outbl.cbegin();
  decode(exec_version, iter);
  ASSERT_EQ(0, exec_version.objv.ver);
  ASSERT_EQ("", exec_version.objv.tag);
}

TEST_P(LibRadosSplitOpECPP, ReadWithIllegalClsOp) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  bufferlist new_bl;
  new_bl.append("CEPH");
  ObjectWriteOperation write2;
  bufferlist exec_inbl, exec_outbl;
  int exec_rval;
  rados::cls::fifo::op::init_part op;
  encode(op, exec_inbl);
  write2.exec("fifo", "init_part", exec_inbl, &exec_outbl, &exec_rval);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(-EOPNOTSUPP, "foo", &write2));
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
