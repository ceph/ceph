// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/exclusive_lock/PreAcquireRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

inline ImageCtx &get_image_ctx(MockTestImageCtx &image_ctx) {
  return *(image_ctx.image_ctx);
}

} // anonymous namespace
} // namespace librbd

// template definitions
#include "librbd/exclusive_lock/PreAcquireRequest.cc"
template class librbd::exclusive_lock::PreAcquireRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace exclusive_lock {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

static const std::string TEST_COOKIE("auto 123");

class TestMockExclusiveLockPreAcquireRequest : public TestMockFixture {
public:
  typedef PreAcquireRequest<MockTestImageCtx> MockPreAcquireRequest;

  void expect_flush_notifies(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, flush(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_prepare_lock(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, prepare_lock(_))
      .WillOnce(Invoke([](Context *on_ready) {
                  on_ready->complete(0);
                }));
  }

  void expect_handle_prepare_lock_complete(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, handle_prepare_lock_complete());
  }

};

TEST_F(TestMockExclusiveLockPreAcquireRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);

  C_SaferCond ctx;
  MockPreAcquireRequest *req = MockPreAcquireRequest::create(mock_image_ctx,
                                                             &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
 }

} // namespace exclusive_lock
} // namespace librbd
