#include <iostream>
#include <errno.h>

#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include "cls/sdk/cls_sdk_ops.h"

using namespace librados;
using namespace cls::sdk;

TEST(ClsSDK, TestSDKCoverageWrite) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in;
  librados::ObjectWriteOperation op;
  op.exec(method::test_coverage_write, in);
  ASSERT_EQ(0, ioctx.operate("myobject", &op));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestSDKCoverageReplay) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in;
  librados::ObjectWriteOperation op;
  op.exec(method::test_coverage_write, in);
  ASSERT_EQ(0, ioctx.operate("myobject", &op));

  librados::ObjectWriteOperation op2;
  op2.exec(method::test_coverage_replay, in);
  ASSERT_EQ(0, ioctx.operate("myobject", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

