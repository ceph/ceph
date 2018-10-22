// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librbd/mock/io/MockObjectDispatch.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/AsyncOpTracker.h"
#include "librbd/exclusive_lock/PreReleaseRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

// template definitions
#include "librbd/exclusive_lock/PreReleaseRequest.cc"
template class librbd::exclusive_lock::PreReleaseRequest<librbd::MockImageCtx>;

namespace librbd {

namespace exclusive_lock {

namespace {

struct MockContext : public Context {
  MOCK_METHOD1(complete, void(int));
  MOCK_METHOD1(finish, void(int));
};

} // anonymous namespace

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

static const std::string TEST_COOKIE("auto 123");

class TestMockExclusiveLockPreReleaseRequest : public TestMockFixture {
public:
  typedef PreReleaseRequest<MockImageCtx> MockPreReleaseRequest;

  void expect_complete_context(MockContext &mock_context, int r) {
    EXPECT_CALL(mock_context, complete(r));
  }

  void expect_test_features(MockImageCtx &mock_image_ctx, uint64_t features,
                            bool enabled) {
    EXPECT_CALL(mock_image_ctx, test_features(features))
                  .WillOnce(Return(enabled));
  }

  void expect_set_require_lock(MockImageCtx &mock_image_ctx,
                               librbd::io::Direction direction, bool enabled) {
    EXPECT_CALL(*mock_image_ctx.io_work_queue, set_require_lock(direction,
                                                                enabled));
  }

  void expect_block_writes(MockImageCtx &mock_image_ctx, int r) {
    expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                         ((mock_image_ctx.features & RBD_FEATURE_JOURNALING) != 0));
    if (mock_image_ctx.clone_copy_on_read ||
        (mock_image_ctx.features & RBD_FEATURE_JOURNALING) != 0) {
      expect_set_require_lock(mock_image_ctx, librbd::io::DIRECTION_BOTH, true);
    } else {
      expect_set_require_lock(mock_image_ctx, librbd::io::DIRECTION_WRITE,
                              true);
    }
    EXPECT_CALL(*mock_image_ctx.io_work_queue, block_writes(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_unblock_writes(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_work_queue, unblock_writes());
  }

  void expect_cancel_op_requests(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_image_ctx, cancel_async_requests(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_journal(MockImageCtx &mock_image_ctx,
                           MockJournal &mock_journal, int r) {
    EXPECT_CALL(mock_journal, close(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_object_map(MockImageCtx &mock_image_ctx,
                               MockObjectMap &mock_object_map) {
    EXPECT_CALL(mock_object_map, close(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_invalidate_cache(MockImageCtx &mock_image_ctx,
                               int r) {
    EXPECT_CALL(*mock_image_ctx.io_object_dispatcher, invalidate_cache(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_flush_notifies(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, flush(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_prepare_lock(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, prepare_lock(_))
      .WillOnce(Invoke([](Context *on_ready) {
                  on_ready->complete(0);
                }));
  }

  void expect_handle_prepare_lock_complete(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, handle_prepare_lock_complete());
  }

  AsyncOpTracker m_async_op_tracker;
};

TEST_F(TestMockExclusiveLockPreReleaseRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  expect_prepare_lock(mock_image_ctx);
  expect_cancel_op_requests(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, 0);
  expect_invalidate_cache(mock_image_ctx, 0);

  expect_flush_notifies(mock_image_ctx);

  MockJournal *mock_journal = new MockJournal();
  mock_image_ctx.journal = mock_journal;
  expect_close_journal(mock_image_ctx, *mock_journal, -EINVAL);

  MockObjectMap *mock_object_map = new MockObjectMap();
  mock_image_ctx.object_map = mock_object_map;
  expect_close_object_map(mock_image_ctx, *mock_object_map);

  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, false, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, SuccessJournalDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_block_writes(mock_image_ctx, 0);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_cancel_op_requests(mock_image_ctx, 0);
  expect_invalidate_cache(mock_image_ctx, 0);

  expect_flush_notifies(mock_image_ctx);

  MockObjectMap *mock_object_map = new MockObjectMap();
  mock_image_ctx.object_map = mock_object_map;
  expect_close_object_map(mock_image_ctx, *mock_object_map);

  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, false, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, SuccessObjectMapDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_block_writes(mock_image_ctx, 0);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_cancel_op_requests(mock_image_ctx, 0);
  expect_invalidate_cache(mock_image_ctx, 0);

  expect_flush_notifies(mock_image_ctx);

  C_SaferCond release_ctx;
  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, true, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, Blacklisted) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_cancel_op_requests(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, -EBLACKLISTED);
  expect_invalidate_cache(mock_image_ctx, -EBLACKLISTED);

  expect_flush_notifies(mock_image_ctx);

  MockJournal *mock_journal = new MockJournal();
  mock_image_ctx.journal = mock_journal;
  expect_close_journal(mock_image_ctx, *mock_journal, -EBLACKLISTED);

  MockObjectMap *mock_object_map = new MockObjectMap();
  mock_image_ctx.object_map = mock_object_map;
  expect_close_object_map(mock_image_ctx, *mock_object_map);

  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, false, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, BlockWritesError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_cancel_op_requests(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, true, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, UnlockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_cancel_op_requests(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, 0);
  expect_invalidate_cache(mock_image_ctx, 0);

  expect_flush_notifies(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, true, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace exclusive_lock
} // namespace librbd
