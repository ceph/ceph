// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rados/librados.hpp"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockExclusiveLock.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librados/AioCompletionImpl.h"
#include "librbd/image_watcher/RewatchRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

#include "librbd/image_watcher/RewatchRequest.cc"

namespace librbd {
namespace image_watcher {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

struct TestMockImageWatcherRewatchRequest : public TestMockFixture {
  typedef RewatchRequest<librbd::MockTestImageCtx> MockRewatchRequest;

  TestMockImageWatcherRewatchRequest()
    : m_watch_lock("watch_lock") {
  }

  void expect_aio_watch(MockImageCtx &mock_image_ctx, int r) {
    librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(
      mock_image_ctx.md_ctx));

    EXPECT_CALL(mock_io_ctx, aio_watch(mock_image_ctx.header_oid, _, _, _))
      .WillOnce(DoAll(WithArg<1>(Invoke([&mock_image_ctx, &mock_io_ctx, r](librados::AioCompletionImpl *c) {
                                   c->get();
                                   mock_image_ctx.image_ctx->op_work_queue->queue(new FunctionContext([&mock_io_ctx, c](int r) {
                                       mock_io_ctx.get_mock_rados_client()->finish_aio_completion(c, r);
                                     }), r);
                                   })),
                      Return(0)));
  }

  void expect_aio_unwatch(MockImageCtx &mock_image_ctx, int r) {
    librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(
      mock_image_ctx.md_ctx));

    EXPECT_CALL(mock_io_ctx, aio_unwatch(m_watch_handle, _))
      .WillOnce(DoAll(Invoke([&mock_image_ctx, &mock_io_ctx, r](uint64_t handle,
                                                                librados::AioCompletionImpl *c) {
                        c->get();
                        mock_image_ctx.image_ctx->op_work_queue->queue(new FunctionContext([&mock_io_ctx, c](int r) {
                            mock_io_ctx.get_mock_rados_client()->finish_aio_completion(c, r);
                          }), r);
                        }),
                      Return(0)));
  }

  void expect_reacquire_lock(MockExclusiveLock &mock_exclusive_lock) {
    EXPECT_CALL(mock_exclusive_lock, reacquire_lock());
  }

  struct WatchCtx : public librados::WatchCtx2 {
    virtual void handle_notify(uint64_t, uint64_t, uint64_t,
                               ceph::bufferlist&) {
      assert(false);
    }
    virtual void handle_error(uint64_t, int) {
      assert(false);
    }
  };

  RWLock m_watch_lock;
  WatchCtx m_watch_ctx;
  uint64_t m_watch_handle = 123;
};

TEST_F(TestMockImageWatcherRewatchRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, 0);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_reacquire_lock(mock_exclusive_lock);
  }

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx,
                                                       m_watch_lock,
                                                       &m_watch_ctx,
                                                       &m_watch_handle,
                                                       &ctx);
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    req->send();
  }
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageWatcherRewatchRequest, UnwatchError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, -EINVAL);
  expect_aio_watch(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx,
                                                       m_watch_lock,
                                                       &m_watch_ctx,
                                                       &m_watch_handle,
                                                       &ctx);
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    req->send();
  }
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageWatcherRewatchRequest, WatchBlacklist) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -EBLACKLISTED);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx,
                                                       m_watch_lock,
                                                       &m_watch_ctx,
                                                       &m_watch_handle,
                                                       &ctx);
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    req->send();
  }
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());
}

TEST_F(TestMockImageWatcherRewatchRequest, WatchDNE) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx,
                                                       m_watch_lock,
                                                       &m_watch_ctx,
                                                       &m_watch_handle,
                                                       &ctx);
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    req->send();
  }
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageWatcherRewatchRequest, WatchError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -EINVAL);
  expect_aio_watch(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx,
                                                       m_watch_lock,
                                                       &m_watch_ctx,
                                                       &m_watch_handle,
                                                       &ctx);
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    req->send();
  }
  ASSERT_EQ(0, ctx.wait());
}

} // namespace image_watcher
} // namespace librbd
