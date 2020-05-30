// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "common/Cond.h"
#include "common/ceph_mutex.h"
#include "librados/AioCompletionImpl.h"
#include "librbd/Watcher.h"
#include "librbd/watcher/RewatchRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

namespace librbd {

namespace {

struct MockWatcher : public Watcher {
  std::string oid;

  MockWatcher(librados::IoCtx& ioctx, ContextWQ *work_queue,
              const std::string& oid)
    : Watcher(ioctx, work_queue, oid) {
  }

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                             uint64_t notifier_id, bufferlist &bl) {
  }
};

} // anonymous namespace
} // namespace librbd

namespace librbd {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::WithArg;
using ::testing::WithArgs;

class TestMockWatcher : public TestMockFixture {
public:
  TestMockWatcher() =  default;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    m_oid = get_temp_image_name();

    bufferlist bl;
    ASSERT_EQ(0, m_ioctx.write_full(m_oid, bl));
  }

  void expect_aio_watch(MockImageCtx &mock_image_ctx, int r,
                        const std::function<void()> &action = std::function<void()>()) {
    librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));
    librados::MockTestMemRadosClient *mock_rados_client(
      mock_io_ctx.get_mock_rados_client());

    EXPECT_CALL(mock_io_ctx, aio_watch(m_oid, _, _, _))
      .WillOnce(DoAll(WithArgs<1, 2, 3>(Invoke([this, &mock_image_ctx, mock_rados_client, r, action](
              librados::AioCompletionImpl *c, uint64_t *cookie,
              librados::WatchCtx2 *watch_ctx) {
          if (r == 0) {
            *cookie = 234U;
            m_watch_ctx = watch_ctx;
          }

          c->get();
          mock_image_ctx.image_ctx->op_work_queue->queue(new LambdaContext([mock_rados_client, action, c](int r) {
              if (action) {
                action();
              }

              mock_rados_client->finish_aio_completion(c, r);
            }), r);
          notify_watch();
        })), Return(0)));
  }

  void expect_aio_unwatch(MockImageCtx &mock_image_ctx, int r,
                          const std::function<void()> &action = std::function<void()>()) {
    librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));
    librados::MockTestMemRadosClient *mock_rados_client(
      mock_io_ctx.get_mock_rados_client());

    EXPECT_CALL(mock_io_ctx, aio_unwatch(_, _))
      .WillOnce(DoAll(Invoke([this, &mock_image_ctx, mock_rados_client, r, action](
              uint64_t handle, librados::AioCompletionImpl *c) {
          c->get();
          mock_image_ctx.image_ctx->op_work_queue->queue(new LambdaContext([mock_rados_client, action, c](int r) {
              if (action) {
                action();
              }

              mock_rados_client->finish_aio_completion(c, r);
            }), r);
          notify_watch();
        }), Return(0)));
  }

  std::string m_oid;
  librados::WatchCtx2 *m_watch_ctx = nullptr;

  void notify_watch() {
    std::lock_guard locker{m_lock};
    ++m_watch_count;
    m_cond.notify_all();
  }

  bool wait_for_watch(MockImageCtx &mock_image_ctx, size_t count) {
    std::unique_lock locker{m_lock};
    while (m_watch_count < count) {
      if (m_cond.wait_for(locker, 10s) == std::cv_status::timeout) {
        return false;
      }
    }
    m_watch_count -= count;
    return true;
  }

  ceph::mutex m_lock = ceph::make_mutex("TestMockWatcher::m_lock");
  ceph::condition_variable m_cond;
  size_t m_watch_count = 0;
};

TEST_F(TestMockWatcher, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockWatcher, RegisterError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, -EINVAL);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(-EINVAL, register_ctx.wait());
}

TEST_F(TestMockWatcher, UnregisterError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, -EINVAL);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(-EINVAL, unregister_ctx.wait());
}

TEST_F(TestMockWatcher, Reregister) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  ceph_assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  // wait for recovery unwatch/watch
  ASSERT_TRUE(wait_for_watch(mock_image_ctx, 3));

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockWatcher, ReregisterUnwatchError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, -EINVAL);
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  ceph_assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  // wait for recovery unwatch/watch
  ASSERT_TRUE(wait_for_watch(mock_image_ctx, 3));

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockWatcher, ReregisterWatchError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -EPERM);
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  ceph_assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  // wait for recovery unwatch/watch
  ASSERT_TRUE(wait_for_watch(mock_image_ctx, 4));

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockWatcher, ReregisterWatchBlacklist) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -EBLACKLISTED);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_TRUE(wait_for_watch(mock_image_ctx, 1));
  ASSERT_EQ(0, register_ctx.wait());

  ceph_assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -EBLACKLISTED);

  // wait for recovery unwatch/watch
  ASSERT_TRUE(wait_for_watch(mock_image_ctx, 2));
  ASSERT_TRUE(mock_image_watcher.is_blacklisted());

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockWatcher, ReregisterUnwatchPendingUnregister) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);

  // inject an unregister
  C_SaferCond unregister_ctx;
  expect_aio_unwatch(mock_image_ctx, -EBLACKLISTED,
                     [&mock_image_watcher, &unregister_ctx]() {
      mock_image_watcher.unregister_watch(&unregister_ctx);
    });

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  ceph_assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -EBLACKLISTED);

  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockWatcher, ReregisterWatchPendingUnregister) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  // inject an unregister
  C_SaferCond unregister_ctx;
  expect_aio_watch(mock_image_ctx, -ESHUTDOWN,
                   [&mock_image_watcher, &unregister_ctx]() {
      mock_image_watcher.unregister_watch(&unregister_ctx);
    });

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  ceph_assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockWatcher, ReregisterPendingUnregister) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockWatcher mock_image_watcher(m_ioctx, ictx->op_work_queue, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  // inject an unregister
  C_SaferCond unregister_ctx;
  expect_aio_watch(mock_image_ctx, 0,
                   [&mock_image_watcher, &unregister_ctx]() {
      mock_image_watcher.unregister_watch(&unregister_ctx);
    });

  expect_aio_unwatch(mock_image_ctx, 0);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  ceph_assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  ASSERT_EQ(0, unregister_ctx.wait());
}

} // namespace librbd
