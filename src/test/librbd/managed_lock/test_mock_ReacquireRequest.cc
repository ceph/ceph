// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/managed_lock/ReacquireRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>

// template definitions
#include "librbd/managed_lock/ReacquireRequest.cc"
template class librbd::managed_lock::ReacquireRequest<librbd::MockImageCtx>;

namespace {

MATCHER_P(IsLockType, exclusive, "") {
  cls_lock_set_cookie_op op;
  bufferlist bl;
  bl.share(arg);
  auto iter = bl.cbegin();
  decode(op, iter);
  return op.type == (exclusive ? ClsLockType::EXCLUSIVE : ClsLockType::SHARED);
}

} // anonymous namespace

namespace librbd {
namespace managed_lock {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;


class TestMockManagedLockReacquireRequest : public TestMockFixture {
public:
  typedef ReacquireRequest<MockImageCtx> MockReacquireRequest;

  void expect_set_cookie(MockImageCtx &mock_image_ctx, int r,
                         bool exclusive = true) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                     StrEq("set_cookie"), IsLockType(exclusive), _, _, _))
                  .WillOnce(Return(r));
  }
};

TEST_F(TestMockManagedLockReacquireRequest, SuccessExclusive) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_set_cookie(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockReacquireRequest *req = MockReacquireRequest::create(
      mock_image_ctx.md_ctx, mock_image_ctx.header_oid, "old_cookie",
      "new_cookie", true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockReacquireRequest, SuccessShared) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_set_cookie(mock_image_ctx, 0, false);

  C_SaferCond ctx;
  MockReacquireRequest *req = MockReacquireRequest::create(
      mock_image_ctx.md_ctx, mock_image_ctx.header_oid, "old_cookie",
      "new_cookie", false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockReacquireRequest, NotSupported) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_set_cookie(mock_image_ctx, -EOPNOTSUPP);

  C_SaferCond ctx;
  MockReacquireRequest *req = MockReacquireRequest::create(
      mock_image_ctx.md_ctx, mock_image_ctx.header_oid, "old_cookie",
      "new_cookie", true, &ctx);
  req->send();
  ASSERT_EQ(-EOPNOTSUPP, ctx.wait());
}

TEST_F(TestMockManagedLockReacquireRequest, Error) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_set_cookie(mock_image_ctx, -EBUSY);

  C_SaferCond ctx;
  MockReacquireRequest *req = MockReacquireRequest::create(
      mock_image_ctx.md_ctx, mock_image_ctx.header_oid, "old_cookie",
      "new_cookie", true, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

} // namespace managed_lock
} // namespace librbd
