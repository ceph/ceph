// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/UnlockRequest.h"

// template definitions
#include "librbd/object_map/UnlockRequest.cc"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;

class TestMockObjectMapUnlockRequest : public TestMockFixture {
public:
  typedef UnlockRequest<MockImageCtx> MockUnlockRequest;

  void expect_unlock(MockImageCtx &mock_image_ctx, int r) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, CEPH_NOSNAP));
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(oid, _, StrEq("lock"), StrEq("unlock"), _, _, _))
                  .WillOnce(Return(r));
  }
};

TEST_F(TestMockObjectMapUnlockRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockUnlockRequest *req = new MockUnlockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_unlock(mock_image_ctx, 0);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapUnlockRequest, UnlockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockUnlockRequest *req = new MockUnlockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_unlock(mock_image_ctx, -ENOENT);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

} // namespace object_map
} // namespace librbd
