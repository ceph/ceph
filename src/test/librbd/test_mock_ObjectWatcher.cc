// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "librados/AioCompletionImpl.h"
#include "librbd/ObjectWatcher.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

namespace librbd {

namespace {

struct MockObjectWatcher : public ObjectWatcher<MockImageCtx> {
  std::string oid;

  MockObjectWatcher(MockImageCtx &mock_image_ctx, const std::string &oid)
    : ObjectWatcher<MockImageCtx>(mock_image_ctx.md_ctx,
                                  mock_image_ctx.op_work_queue),
      oid(oid) {
  }

  virtual std::string get_oid() const override {
    return oid;
  }

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                             bufferlist &bl) {
  }
};

} // anonymous namespace

} // namespace librbd

// template definitions
#include "librbd/ObjectWatcher.cc"
template class librbd::ObjectWatcher<librbd::MockImageCtx>;

namespace librbd {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::WithArg;

class TestMockObjectWatcher : public TestMockFixture {
public:
  TestMockObjectWatcher() : m_lock("TestMockObjectWatcher::m_lock") {
  }

  virtual void SetUp() {
    TestMockFixture::SetUp();

    m_oid = get_temp_image_name();

    bufferlist bl;
    ASSERT_EQ(0, m_ioctx.write_full(m_oid, bl));
  }

  void expect_aio_watch(MockImageCtx &mock_image_ctx, int r,
                        const std::function<void()> &action = std::function<void()>()) {
    librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(
      mock_image_ctx.md_ctx));
    librados::MockTestMemRadosClient *mock_rados_client(
      mock_io_ctx.get_mock_rados_client());

    auto &expect = EXPECT_CALL(mock_io_ctx, aio_watch(m_oid, _, _, _));
    if (r < 0) {
      expect.WillOnce(DoAll(WithArg<1>(Invoke([this, mock_rados_client, r, action](librados::AioCompletionImpl *c) {
                                if (action) {
                                  action();
                                }

                                c->get();
                                mock_rados_client->finish_aio_completion(c, r);
                                notify_watch();
                              })),
                            Return(0)));
    } else {
      expect.WillOnce(DoAll(SaveArg<3>(&m_watch_ctx),
                            Invoke([this, &mock_io_ctx, action](const std::string& o,
                                                                librados::AioCompletionImpl *c,
                                                                uint64_t *handle,
                                                                librados::WatchCtx2 *ctx) {
                                if (action) {
                                  action();
                                }

                                mock_io_ctx.do_aio_watch(o, c, handle, ctx);
                                notify_watch();
                              }),
                            Return(0)));
    }
  }

  void expect_aio_unwatch(MockImageCtx &mock_image_ctx, int r,
                          const std::function<void()> &action = std::function<void()>()) {
    librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(
      mock_image_ctx.md_ctx));

    auto &expect = EXPECT_CALL(mock_io_ctx, aio_unwatch(_, _));
    if (r < 0) {
      expect.WillOnce(DoAll(Invoke([this, &mock_io_ctx, r, action](uint64_t handle,
                                                                   librados::AioCompletionImpl *c) {
                                if (action) {
                                  action();
                                }

                                librados::AioCompletionImpl *dummy_c = new librados::AioCompletionImpl();
                                mock_io_ctx.do_aio_unwatch(handle, dummy_c);
                                ASSERT_EQ(0, dummy_c->wait_for_complete());
                                dummy_c->release();

                                c->get();
                                mock_io_ctx.get_mock_rados_client()->finish_aio_completion(c, r);
                                notify_watch();
                              }),
                            Return(0)));
    } else {
      expect.WillOnce(DoAll(Invoke([this, &mock_io_ctx, action](uint64_t handle,
                                                                librados::AioCompletionImpl *c) {
                                if (action) {
                                  action();
                                }

                                mock_io_ctx.do_aio_unwatch(handle, c);
                                notify_watch();
                              }),
                            Return(0)));
    }
  }

  std::string m_oid;
  librados::WatchCtx2 *m_watch_ctx = nullptr;

  void notify_watch() {
    Mutex::Locker locker(m_lock);
    ++m_watch_count;
    m_cond.Signal();
  }

  bool wait_for_watch(MockImageCtx &mock_image_ctx, size_t count) {
    Mutex::Locker locker(m_lock);
    while (m_watch_count < count) {
      if (m_cond.WaitInterval(mock_image_ctx.cct, m_lock,
                              utime_t(10, 0)) != 0) {
        return false;
      }
    }
    return true;
  }

  Mutex m_lock;
  Cond m_cond;
  size_t m_watch_count = 0;
};

TEST_F(TestMockObjectWatcher, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

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

TEST_F(TestMockObjectWatcher, RegisterError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, -EINVAL);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(-EINVAL, register_ctx.wait());
}

TEST_F(TestMockObjectWatcher, UnregisterError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

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

TEST_F(TestMockObjectWatcher, Reregister) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  // wait for recovery unwatch/watch
  ASSERT_TRUE(wait_for_watch(mock_image_ctx, 3));

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockObjectWatcher, ReregisterUnwatchError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, -EINVAL);
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  // wait for recovery unwatch/watch
  ASSERT_TRUE(wait_for_watch(mock_image_ctx, 3));

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockObjectWatcher, ReregisterWatchError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);
  expect_aio_watch(mock_image_ctx, -ESHUTDOWN);
  expect_aio_watch(mock_image_ctx, 0);
  expect_aio_unwatch(mock_image_ctx, 0);

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  // wait for recovery unwatch/watch
  ASSERT_TRUE(wait_for_watch(mock_image_ctx, 4));

  C_SaferCond unregister_ctx;
  mock_image_watcher.unregister_watch(&unregister_ctx);
  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockObjectWatcher, ReregisterUnwatchPendingUnregister) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_aio_watch(mock_image_ctx, 0);

  // inject an unregister
  C_SaferCond unregister_ctx;
  expect_aio_unwatch(mock_image_ctx, 0, [&mock_image_watcher, &unregister_ctx]() {
      mock_image_watcher.unregister_watch(&unregister_ctx);
    });

  C_SaferCond register_ctx;
  mock_image_watcher.register_watch(&register_ctx);
  ASSERT_EQ(0, register_ctx.wait());

  assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockObjectWatcher, ReregisterWatchPendingUnregister) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

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

  assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  ASSERT_EQ(0, unregister_ctx.wait());
}

TEST_F(TestMockObjectWatcher, ReregisterPendingUnregister) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectWatcher mock_image_watcher(mock_image_ctx, m_oid);

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

  assert(m_watch_ctx != nullptr);
  m_watch_ctx->handle_error(0, -ESHUTDOWN);

  ASSERT_EQ(0, unregister_ctx.wait());
}

} // namespace librbd
