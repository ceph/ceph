// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "librbd/managed_lock/ReleaseRequest.h"
#include "common/WorkQueue.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

namespace librbd {
namespace watcher {
template <>
struct Traits<MockImageCtx> {
  typedef librbd::MockImageWatcher Watcher;
};
}
}

// template definitions
#include "librbd/managed_lock/ReleaseRequest.cc"
template class librbd::managed_lock::ReleaseRequest<librbd::MockImageCtx>;

namespace librbd {
namespace managed_lock {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

static const std::string TEST_COOKIE("auto 123");

class TestMockManagedLockReleaseRequest : public TestMockFixture {
public:
  typedef ReleaseRequest<MockImageCtx> MockReleaseRequest;

  void expect_unlock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                     StrEq("unlock"), _, _, _, _))
                        .WillOnce(Return(r));
  }

};

TEST_F(TestMockManagedLockReleaseRequest, Success) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  expect_unlock(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockReleaseRequest *req = MockReleaseRequest::create(
      mock_image_ctx.md_ctx, mock_image_ctx.image_watcher, ictx->op_work_queue,
      mock_image_ctx.header_oid, TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockReleaseRequest, UnlockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  expect_unlock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockReleaseRequest *req = MockReleaseRequest::create(
      mock_image_ctx.md_ctx, mock_image_ctx.image_watcher, ictx->op_work_queue,
      mock_image_ctx.header_oid, TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

}

} // namespace managed_lock
} // namespace librbd
