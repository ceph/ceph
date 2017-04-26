// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librados/AioCompletionImpl.h"
#include "librbd/ManagedLock.h"
#include "test/librados/test.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "tools/rbd_mirror/InstanceReplayer.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/Threads.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

template <>
struct ManagedLock<MockTestImageCtx> {
  static ManagedLock* s_instance;

  static ManagedLock *create(librados::IoCtx& ioctx, ContextWQ *work_queue,
                             const std::string& oid, librbd::Watcher *watcher,
                             managed_lock::Mode  mode,
                             bool blacklist_on_break_lock,
                             uint32_t blacklist_expire_seconds) {
    assert(s_instance != nullptr);
    return s_instance;
  }

  ManagedLock() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  ~ManagedLock() {
    assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD0(destroy, void());
  MOCK_METHOD1(shut_down, void(Context *));
  MOCK_METHOD1(acquire_lock, void(Context *));
  MOCK_METHOD2(get_locker, void(managed_lock::Locker *, Context *));
  MOCK_METHOD3(break_lock, void(const managed_lock::Locker &, bool, Context *));
};

ManagedLock<MockTestImageCtx> *ManagedLock<MockTestImageCtx>::s_instance = nullptr;

} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  Mutex &timer_lock;
  SafeTimer *timer;
  ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
};

template <>
struct InstanceReplayer<librbd::MockTestImageCtx> {
  MOCK_METHOD4(acquire_image, void(const std::string &, const std::string &,
                                   const std::string &, Context *));
  MOCK_METHOD5(release_image, void(const std::string &, const std::string &,
                                   const std::string &, bool, Context *));
};

} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/InstanceWatcher.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockInstanceWatcher : public TestMockFixture {
public:
  typedef librbd::ManagedLock<librbd::MockTestImageCtx> MockManagedLock;
  typedef InstanceReplayer<librbd::MockTestImageCtx> MockInstanceReplayer;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

  std::string m_instance_id;
  std::string m_oid;
  MockThreads *m_mock_threads;

  void SetUp() override {
    TestFixture::SetUp();
    m_local_io_ctx.remove(RBD_MIRROR_LEADER);
    EXPECT_EQ(0, m_local_io_ctx.create(RBD_MIRROR_LEADER, true));

    m_instance_id = stringify(m_local_io_ctx.get_instance_id());
    m_oid = RBD_MIRROR_INSTANCE_PREFIX + m_instance_id;

    m_mock_threads = new MockThreads(m_threads);
  }

  void TearDown() override {
    delete m_mock_threads;
    TestMockFixture::TearDown();
  }

  void expect_register_watch(librados::MockTestMemIoCtxImpl &mock_io_ctx) {
    EXPECT_CALL(mock_io_ctx, aio_watch(m_oid, _, _, _));
  }

  void expect_register_watch(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                             const std::string &instance_id) {
    std::string oid = RBD_MIRROR_INSTANCE_PREFIX + instance_id;
    EXPECT_CALL(mock_io_ctx, aio_watch(oid, _, _, _));
  }

  void expect_unregister_watch(librados::MockTestMemIoCtxImpl &mock_io_ctx) {
    EXPECT_CALL(mock_io_ctx, aio_unwatch(_, _));
  }

  void expect_register_instance(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                                int r) {
    EXPECT_CALL(mock_io_ctx, exec(RBD_MIRROR_LEADER, _, StrEq("rbd"),
                                  StrEq("mirror_instances_add"), _, _, _))
      .WillOnce(Return(r));
  }

  void expect_unregister_instance(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                                  int r) {
    EXPECT_CALL(mock_io_ctx, exec(RBD_MIRROR_LEADER, _, StrEq("rbd"),
                                  StrEq("mirror_instances_remove"), _, _, _))
      .WillOnce(Return(r));
  }

  void expect_acquire_lock(MockManagedLock &mock_managed_lock, int r) {
    EXPECT_CALL(mock_managed_lock, acquire_lock(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_release_lock(MockManagedLock &mock_managed_lock, int r) {
    EXPECT_CALL(mock_managed_lock, shut_down(_)).WillOnce(CompleteContext(r));
  }

  void expect_destroy_lock(MockManagedLock &mock_managed_lock,
                           Context *ctx = nullptr) {
    EXPECT_CALL(mock_managed_lock, destroy())
      .WillOnce(Invoke([ctx]() {
            if (ctx != nullptr) {
              ctx->complete(0);
            }
          }));
  }

  void expect_get_locker(MockManagedLock &mock_managed_lock,
                         const librbd::managed_lock::Locker &locker, int r) {
    EXPECT_CALL(mock_managed_lock, get_locker(_, _))
      .WillOnce(Invoke([r, locker](librbd::managed_lock::Locker *out,
                                   Context *ctx) {
                         if (r == 0) {
                           *out = locker;
                         }
                         ctx->complete(r);
                       }));
  }

  void expect_break_lock(MockManagedLock &mock_managed_lock,
                         const librbd::managed_lock::Locker &locker, int r) {
    EXPECT_CALL(mock_managed_lock, break_lock(locker, true, _))
      .WillOnce(WithArg<2>(CompleteContext(r)));
  }
};

TEST_F(TestMockInstanceWatcher, InitShutdown) {
  MockManagedLock mock_managed_lock;
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_local_io_ctx));

  auto instance_watcher = new MockInstanceWatcher(
    m_local_io_ctx, m_mock_threads->work_queue, nullptr, m_instance_id);
  InSequence seq;

  // Init
  expect_register_instance(mock_io_ctx, 0);
  expect_register_watch(mock_io_ctx);
  expect_acquire_lock(mock_managed_lock, 0);
  ASSERT_EQ(0, instance_watcher->init());

  // Shutdown
  expect_release_lock(mock_managed_lock, 0);
  expect_unregister_watch(mock_io_ctx);
  expect_unregister_instance(mock_io_ctx, 0);
  instance_watcher->shut_down();

  expect_destroy_lock(mock_managed_lock);
  delete instance_watcher;
}

TEST_F(TestMockInstanceWatcher, InitError) {
  MockManagedLock mock_managed_lock;
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_local_io_ctx));

  auto instance_watcher = new MockInstanceWatcher(
    m_local_io_ctx, m_mock_threads->work_queue, nullptr, m_instance_id);
  InSequence seq;

  expect_register_instance(mock_io_ctx, 0);
  expect_register_watch(mock_io_ctx);
  expect_acquire_lock(mock_managed_lock, -EINVAL);
  expect_unregister_watch(mock_io_ctx);
  expect_unregister_instance(mock_io_ctx, 0);

  ASSERT_EQ(-EINVAL, instance_watcher->init());

  expect_destroy_lock(mock_managed_lock);
  delete instance_watcher;
}

TEST_F(TestMockInstanceWatcher, ShutdownError) {
  MockManagedLock mock_managed_lock;
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_local_io_ctx));

  auto instance_watcher = new MockInstanceWatcher(
    m_local_io_ctx, m_mock_threads->work_queue, nullptr, m_instance_id);
  InSequence seq;

  // Init
  expect_register_instance(mock_io_ctx, 0);
  expect_register_watch(mock_io_ctx);
  expect_acquire_lock(mock_managed_lock, 0);
  ASSERT_EQ(0, instance_watcher->init());

  // Shutdown
  expect_release_lock(mock_managed_lock, -EINVAL);
  expect_unregister_watch(mock_io_ctx);
  expect_unregister_instance(mock_io_ctx, 0);
  instance_watcher->shut_down();

  expect_destroy_lock(mock_managed_lock);
  delete instance_watcher;
}


TEST_F(TestMockInstanceWatcher, Remove) {
  MockManagedLock mock_managed_lock;
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_local_io_ctx));
  librbd::managed_lock::Locker
    locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};

  InSequence seq;

  expect_get_locker(mock_managed_lock, locker, 0);
  expect_break_lock(mock_managed_lock, locker, 0);
  expect_unregister_instance(mock_io_ctx, 0);
  C_SaferCond on_destroy;
  expect_destroy_lock(mock_managed_lock, &on_destroy);

  C_SaferCond on_remove;
  MockInstanceWatcher::remove_instance(m_local_io_ctx,
                                       m_mock_threads->work_queue,
                                       "instance_id", &on_remove);
  ASSERT_EQ(0, on_remove.wait());
  ASSERT_EQ(0, on_destroy.wait());
}

TEST_F(TestMockInstanceWatcher, RemoveNoent) {
  MockManagedLock mock_managed_lock;
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_local_io_ctx));

  InSequence seq;

  expect_get_locker(mock_managed_lock, librbd::managed_lock::Locker(), -ENOENT);
  expect_unregister_instance(mock_io_ctx, 0);
  C_SaferCond on_destroy;
  expect_destroy_lock(mock_managed_lock, &on_destroy);

  C_SaferCond on_remove;
  MockInstanceWatcher::remove_instance(m_local_io_ctx,
                                       m_mock_threads->work_queue,
                                       "instance_id", &on_remove);
  ASSERT_EQ(0, on_remove.wait());
  ASSERT_EQ(0, on_destroy.wait());
}

TEST_F(TestMockInstanceWatcher, ImageAcquireRelease) {
  MockManagedLock mock_managed_lock;

  librados::IoCtx& io_ctx1 = m_local_io_ctx;
  std::string instance_id1 = m_instance_id;
  librados::MockTestMemIoCtxImpl &mock_io_ctx1(get_mock_io_ctx(io_ctx1));
  MockInstanceReplayer mock_instance_replayer1;
  auto instance_watcher1 = MockInstanceWatcher::create(
      io_ctx1, m_mock_threads->work_queue, &mock_instance_replayer1);

  librados::Rados cluster;
  librados::IoCtx io_ctx2;
  EXPECT_EQ("", connect_cluster_pp(cluster));
  EXPECT_EQ(0, cluster.ioctx_create(_local_pool_name.c_str(), io_ctx2));
  std::string instance_id2 = stringify(io_ctx2.get_instance_id());
  librados::MockTestMemIoCtxImpl &mock_io_ctx2(get_mock_io_ctx(io_ctx2));
  MockInstanceReplayer mock_instance_replayer2;
  auto instance_watcher2 = MockInstanceWatcher::create(
    io_ctx2, m_mock_threads->work_queue, &mock_instance_replayer2);

  InSequence seq;

  // Init instance watcher 1
  expect_register_instance(mock_io_ctx1, 0);
  expect_register_watch(mock_io_ctx1, instance_id1);
  expect_acquire_lock(mock_managed_lock, 0);
  ASSERT_EQ(0, instance_watcher1->init());

  // Init instance watcher 2
  expect_register_instance(mock_io_ctx2, 0);
  expect_register_watch(mock_io_ctx2, instance_id2);
  expect_acquire_lock(mock_managed_lock, 0);
  ASSERT_EQ(0, instance_watcher2->init());

  // Acquire Image on the the same instance
  EXPECT_CALL(mock_instance_replayer1, acquire_image("gid", "uuid", "id", _))
      .WillOnce(WithArg<3>(CompleteContext(0)));
  C_SaferCond on_acquire1;
  instance_watcher1->notify_image_acquire(instance_id1, "gid", "uuid", "id",
                                          &on_acquire1);
  ASSERT_EQ(0, on_acquire1.wait());

  // Acquire Image on the other instance
  EXPECT_CALL(mock_instance_replayer2, acquire_image("gid", "uuid", "id", _))
      .WillOnce(WithArg<3>(CompleteContext(0)));
  C_SaferCond on_acquire2;
  instance_watcher1->notify_image_acquire(instance_id2, "gid", "uuid", "id",
                                          &on_acquire2);
  ASSERT_EQ(0, on_acquire2.wait());

  // Release Image on the the same instance
  EXPECT_CALL(mock_instance_replayer1, release_image("gid", "uuid", "id", true,
                                                     _))
      .WillOnce(WithArg<4>(CompleteContext(0)));
  C_SaferCond on_release1;
  instance_watcher1->notify_image_release(instance_id1, "gid", "uuid", "id",
                                          true, &on_release1);
  ASSERT_EQ(0, on_release1.wait());

  // Release Image on the other instance
  EXPECT_CALL(mock_instance_replayer2, release_image("gid", "uuid", "id", true,
                                                     _))
      .WillOnce(WithArg<4>(CompleteContext(0)));
  C_SaferCond on_release2;
  instance_watcher1->notify_image_release(instance_id2, "gid", "uuid", "id",
                                          true, &on_release2);
  ASSERT_EQ(0, on_release2.wait());

  // Shutdown instance watcher 1
  expect_release_lock(mock_managed_lock, 0);
  expect_unregister_watch(mock_io_ctx1);
  expect_unregister_instance(mock_io_ctx1, 0);
  instance_watcher1->shut_down();

  expect_destroy_lock(mock_managed_lock);
  delete instance_watcher1;

  // Shutdown instance watcher 2
  expect_release_lock(mock_managed_lock, 0);
  expect_unregister_watch(mock_io_ctx2);
  expect_unregister_instance(mock_io_ctx2, 0);
  instance_watcher2->shut_down();

  expect_destroy_lock(mock_managed_lock);
  delete instance_watcher2;
}

TEST_F(TestMockInstanceWatcher, ImageAcquireReleaseCancel) {
  MockManagedLock mock_managed_lock;
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_local_io_ctx));

  auto instance_watcher = new MockInstanceWatcher(
    m_local_io_ctx, m_mock_threads->work_queue, nullptr, m_instance_id);
  InSequence seq;

  // Init
  expect_register_instance(mock_io_ctx, 0);
  expect_register_watch(mock_io_ctx);
  expect_acquire_lock(mock_managed_lock, 0);
  ASSERT_EQ(0, instance_watcher->init());

  // Send Acquire Image and cancel
  EXPECT_CALL(mock_io_ctx, aio_notify(_, _, _, _, _))
    .WillOnce(Invoke(
                  [this, instance_watcher, &mock_io_ctx](
                    const std::string& o, librados::AioCompletionImpl *c,
                    bufferlist& bl, uint64_t timeout_ms, bufferlist *pbl) {
                    c->get();
                    auto ctx = new FunctionContext(
                      [instance_watcher, &mock_io_ctx, c, pbl](int r) {
                        instance_watcher->cancel_notify_requests("other");
                        ::encode(librbd::watcher::NotifyResponse(), *pbl);
                        mock_io_ctx.get_mock_rados_client()->
                            finish_aio_completion(c, -ETIMEDOUT);
                      });
                    m_threads->work_queue->queue(ctx, 0);
                  }));

  C_SaferCond on_acquire;
  instance_watcher->notify_image_acquire("other", "gid", "uuid", "id",
                                         &on_acquire);
  ASSERT_EQ(-ECANCELED, on_acquire.wait());

  // Send Release Image and cancel
  EXPECT_CALL(mock_io_ctx, aio_notify(_, _, _, _, _))
    .WillOnce(Invoke(
                  [this, instance_watcher, &mock_io_ctx](
                    const std::string& o, librados::AioCompletionImpl *c,
                    bufferlist& bl, uint64_t timeout_ms, bufferlist *pbl) {
                    c->get();
                    auto ctx = new FunctionContext(
                      [instance_watcher, &mock_io_ctx, c, pbl](int r) {
                        instance_watcher->cancel_notify_requests("other");
                        ::encode(librbd::watcher::NotifyResponse(), *pbl);
                        mock_io_ctx.get_mock_rados_client()->
                            finish_aio_completion(c, -ETIMEDOUT);
                      });
                    m_threads->work_queue->queue(ctx, 0);
                  }));

  C_SaferCond on_release;
  instance_watcher->notify_image_release("other", "gid", "uuid", "id",
                                         true, &on_release);
  ASSERT_EQ(-ECANCELED, on_release.wait());

  // Shutdown
  expect_release_lock(mock_managed_lock, 0);
  expect_unregister_watch(mock_io_ctx);
  expect_unregister_instance(mock_io_ctx, 0);
  instance_watcher->shut_down();

  expect_destroy_lock(mock_managed_lock);
  delete instance_watcher;
}

} // namespace mirror
} // namespace rbd
