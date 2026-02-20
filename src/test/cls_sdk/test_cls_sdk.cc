#include <iostream>
#include <errno.h>

#include "test/librados/test_cxx.h"
#include "test/librados/test_pool_types.h"
#include "gtest/gtest.h"

using namespace librados;
using ceph::test::PoolType;
using ceph::test::pool_type_name;
using ceph::test::create_pool_by_type;
using ceph::test::destroy_pool_by_type;

class TestClsSDK : public ceph::test::ClsTestFixture {
  // Inherits: rados, ioctx, pool_name, pool_type, SetUp(), TearDown()
};

TEST_P(TestClsSDK, TestSDKCoverageWrite) {
  bufferlist in;
  librados::ObjectWriteOperation op;
  op.exec("sdk", "test_coverage_write", in);
  ASSERT_EQ(0, ioctx.operate("myobject", &op));
}

TEST_P(TestClsSDK, TestSDKCoverageReplay) {
  bufferlist in;
  librados::ObjectWriteOperation op;
  op.exec("sdk", "test_coverage_write", in);
  ASSERT_EQ(0, ioctx.operate("myobject", &op));

  librados::ObjectWriteOperation op2;
  op2.exec("sdk", "test_coverage_replay", in);
  ASSERT_EQ(0, ioctx.operate("myobject", &op2));
}

INSTANTIATE_TEST_SUITE_P(, TestClsSDK,
    ::testing::Values(PoolType::REPLICATED, PoolType::FAST_EC),
    [](const ::testing::TestParamInfo<PoolType>& info) {
      return pool_type_name(info.param);
    }
);
