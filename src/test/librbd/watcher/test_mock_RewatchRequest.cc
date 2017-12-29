// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rados/librados.hpp"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librados/AioCompletionImpl.h"
#include "librbd/watcher/RewatchRequest.h"

namespace librbd {
namespace watcher {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

struct TestMockWatcherRewatchRequest : public TestMockFixture {
  typedef RewatchRequest MockRewatchRequest;

  TestMockWatcherRewatchRequest()
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

  struct WatchCtx : public librados::WatchCtx2 {
    void handle_notify(uint64_t, uint64_t, uint64_t,
                               ceph::bufferlist&) override {
      ceph_abort();
    }
    void handle_error(uint64_t, int) override {
      ceph_abort();
    }
  };

  RWLock m_watch_lock;
  WatchCtx m_watch_ctx;
  uint64_t m_watch_handle = 123;
};

TEST_F(TestMockWatcherRewatchRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx.md_ctx,
                                                       mock_image_ctx.header_oid,
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

TEST_F(TestMockWatcherRewatchRequest, UnwatchError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, -EINVAL);
  expect_aio_watch(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx.md_ctx,
                                                       mock_image_ctx.header_oid,
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

TEST_F(TestMockWatcherRewatchRequest, WatchBlacklist) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -EBLACKLISTED);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx.md_ctx,
                                                       mock_image_ctx.header_oid,
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

TEST_F(TestMockWatcherRewatchRequest, WatchDNE) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx.md_ctx,
                                                       mock_image_ctx.header_oid,
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

TEST_F(TestMockWatcherRewatchRequest, WatchError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -EINVAL);
  expect_aio_watch(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockRewatchRequest *req = MockRewatchRequest::create(mock_image_ctx.md_ctx,
                                                       mock_image_ctx.header_oid,
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

} // namespace watcher
} // namespace librbd
