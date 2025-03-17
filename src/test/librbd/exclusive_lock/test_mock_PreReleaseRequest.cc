// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/cache/MockImageCache.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librbd/mock/io/MockObjectDispatch.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/AsyncOpTracker.h"
#include "librbd/exclusive_lock/ImageDispatch.h"
#include "librbd/exclusive_lock/PreReleaseRequest.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace exclusive_lock {

template <>
struct ImageDispatch<MockTestImageCtx> {
  MOCK_METHOD3(set_require_lock, void(bool init_shutdown, io::Direction,
                                      Context*));
  MOCK_METHOD1(unset_require_lock, void(io::Direction));
};

} // namespace exclusive_lock

namespace util {

inline ImageCtx* get_image_ctx(MockTestImageCtx* image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

// template definitions
#include "librbd/exclusive_lock/PreReleaseRequest.cc"

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

class TestMockExclusiveLockPreReleaseRequest : public TestMockFixture {
public:
  typedef ImageDispatch<MockTestImageCtx> MockImageDispatch;
  typedef PreReleaseRequest<MockTestImageCtx> MockPreReleaseRequest;

  void expect_complete_context(MockContext &mock_context, int r) {
    EXPECT_CALL(mock_context, complete(r));
  }

  void expect_test_features(MockTestImageCtx &mock_image_ctx, uint64_t features,
                            bool enabled) {
    EXPECT_CALL(mock_image_ctx, test_features(features))
                  .WillOnce(Return(enabled));
  }

  void expect_set_require_lock(MockImageDispatch &mock_image_dispatch,
                               bool init_shutdown,
                               librbd::io::Direction direction, int r) {
    EXPECT_CALL(mock_image_dispatch, set_require_lock(init_shutdown,
                                                      direction, _))
      .WillOnce(WithArg<2>(Invoke([r](Context* ctx) { ctx->complete(r); })));
  }

  void expect_set_require_lock(MockTestImageCtx &mock_image_ctx,
                               MockImageDispatch &mock_image_dispatch,
                               bool init_shutdown, int r) {
    expect_test_features(mock_image_ctx, RBD_FEATURE_EXCLUSIVE_LOCK, true);
    if (!mock_image_ctx.clone_copy_on_read) {
      expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                           ((mock_image_ctx.features & RBD_FEATURE_JOURNALING) != 0));
      if ((mock_image_ctx.features & RBD_FEATURE_JOURNALING) == 0) {
        expect_test_features(mock_image_ctx, RBD_FEATURE_DIRTY_CACHE,
                             ((mock_image_ctx.features & RBD_FEATURE_DIRTY_CACHE) != 0));
      }
    }
    if (mock_image_ctx.clone_copy_on_read ||
        (mock_image_ctx.features & RBD_FEATURE_JOURNALING) != 0 ||
        (mock_image_ctx.features & RBD_FEATURE_DIRTY_CACHE) != 0) {
      expect_set_require_lock(mock_image_dispatch, init_shutdown,
                              librbd::io::DIRECTION_BOTH, r);
    } else {
      expect_set_require_lock(mock_image_dispatch, init_shutdown,
                              librbd::io::DIRECTION_WRITE, r);
    }
  }

  void expect_unset_require_lock(MockImageDispatch &mock_image_dispatch) {
    EXPECT_CALL(mock_image_dispatch, unset_require_lock(
      io::DIRECTION_BOTH));
  }

  void expect_cancel_op_requests(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_image_ctx, cancel_async_requests(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_journal(MockTestImageCtx &mock_image_ctx,
                           MockJournal &mock_journal, int r) {
    EXPECT_CALL(mock_journal, close(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_object_map(MockTestImageCtx &mock_image_ctx,
                               MockObjectMap &mock_object_map) {
    EXPECT_CALL(mock_object_map, close(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_prerelease_exclusive_lock(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.plugin_registry, prerelease_exclusive_lock(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_invalidate_cache(MockTestImageCtx &mock_image_ctx,
                               int r) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, invalidate_cache(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

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

  void expect_flush_io(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, send(_))
      .WillOnce(Invoke([&mock_image_ctx, r](io::ImageDispatchSpec* spec) {
                  ASSERT_TRUE(std::get_if<io::ImageDispatchSpec::Flush>(
                    &spec->request) != nullptr);
                  spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                  auto aio_comp = spec->aio_comp;
                  auto ctx = new LambdaContext([aio_comp](int r) {
                    if (r < 0) {
                      aio_comp->fail(r);
                    } else {
                      aio_comp->set_request_count(1);
                      aio_comp->add_request();
                      aio_comp->complete_request(r);
                    }
                  });
                  mock_image_ctx.image_ctx->op_work_queue->queue(ctx, r);
                }));
  }

  AsyncOpTracker m_async_op_tracker;
};

TEST_F(TestMockExclusiveLockPreReleaseRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  expect_cancel_op_requests(mock_image_ctx, 0);
  MockImageDispatch mock_image_dispatch;
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch, false, 0);

  expect_prepare_lock(mock_image_ctx);

  expect_prerelease_exclusive_lock(mock_image_ctx, 0);

  expect_invalidate_cache(mock_image_ctx, 0);

  expect_flush_io(mock_image_ctx, 0);

  expect_flush_notifies(mock_image_ctx);

  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;
  expect_close_journal(mock_image_ctx, mock_journal, -EINVAL);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;
  expect_close_object_map(mock_image_ctx, mock_object_map);

  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, &mock_image_dispatch, false, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, SuccessJournalDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  MockImageDispatch mock_image_dispatch;
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch, false, 0);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_cancel_op_requests(mock_image_ctx, 0);
  expect_prepare_lock(mock_image_ctx);

  expect_prerelease_exclusive_lock(mock_image_ctx, 0);

  expect_invalidate_cache(mock_image_ctx, 0);

  expect_flush_io(mock_image_ctx, 0);

  expect_flush_notifies(mock_image_ctx);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;
  expect_close_object_map(mock_image_ctx, mock_object_map);

  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, &mock_image_dispatch, false, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, SuccessObjectMapDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  MockImageDispatch mock_image_dispatch;
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch, true, 0);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_cancel_op_requests(mock_image_ctx, 0);

  expect_prerelease_exclusive_lock(mock_image_ctx, 0);

  expect_invalidate_cache(mock_image_ctx, 0);

  expect_flush_io(mock_image_ctx, 0);

  expect_flush_notifies(mock_image_ctx);

  C_SaferCond release_ctx;
  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, &mock_image_dispatch, true, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, Blocklisted) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_cancel_op_requests(mock_image_ctx, 0);
  MockImageDispatch mock_image_dispatch;
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch, false,
                          -EBLOCKLISTED);
  expect_prepare_lock(mock_image_ctx);

  expect_prerelease_exclusive_lock(mock_image_ctx, 0);

  expect_invalidate_cache(mock_image_ctx, -EBLOCKLISTED);

  expect_flush_io(mock_image_ctx, -EBLOCKLISTED);

  expect_flush_notifies(mock_image_ctx);

  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;
  expect_close_journal(mock_image_ctx, mock_journal, -EBLOCKLISTED);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;
  expect_close_object_map(mock_image_ctx, mock_object_map);

  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, &mock_image_dispatch, false, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockPreReleaseRequest, Disabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  expect_cancel_op_requests(mock_image_ctx, 0);
  MockImageDispatch mock_image_dispatch;

  expect_test_features(mock_image_ctx, RBD_FEATURE_EXCLUSIVE_LOCK, false);

  expect_prepare_lock(mock_image_ctx);

  expect_prerelease_exclusive_lock(mock_image_ctx, 0);

  expect_invalidate_cache(mock_image_ctx, 0);

  expect_flush_io(mock_image_ctx, 0);

  expect_flush_notifies(mock_image_ctx);

  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;
  expect_close_journal(mock_image_ctx, mock_journal, -EINVAL);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;
  expect_close_object_map(mock_image_ctx, mock_object_map);

  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockPreReleaseRequest *req = MockPreReleaseRequest::create(
    mock_image_ctx, &mock_image_dispatch, false, m_async_op_tracker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace exclusive_lock
} // namespace librbd
