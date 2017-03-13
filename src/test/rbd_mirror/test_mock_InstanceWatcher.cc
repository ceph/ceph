// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ManagedLock.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/rbd_mirror/test_mock_fixture.h"
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
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;

  std::string m_instance_id;
  std::string m_oid;

  void SetUp() override {
    TestFixture::SetUp();
    m_local_io_ctx.remove(RBD_MIRROR_LEADER);
    EXPECT_EQ(0, m_local_io_ctx.create(RBD_MIRROR_LEADER, true));

    m_instance_id = stringify(m_local_io_ctx.get_instance_id());
    m_oid = RBD_MIRROR_INSTANCE_PREFIX + m_instance_id;
  }

  void expect_register_watch(librados::MockTestMemIoCtxImpl &mock_io_ctx) {
    EXPECT_CALL(mock_io_ctx, aio_watch(m_oid, _, _, _));
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

  void expect_destroy_lock(MockManagedLock &mock_managed_lock) {
    EXPECT_CALL(mock_managed_lock, destroy());
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

  auto instance_watcher = new MockInstanceWatcher(m_local_io_ctx,
                                                  m_threads->work_queue);
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

  auto instance_watcher = new MockInstanceWatcher(m_local_io_ctx,
                                                  m_threads->work_queue);
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

  auto instance_watcher = new MockInstanceWatcher(m_local_io_ctx,
                                                  m_threads->work_queue);
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
  expect_destroy_lock(mock_managed_lock);

  C_SaferCond on_remove;
  MockInstanceWatcher::remove_instance(m_local_io_ctx, m_threads->work_queue,
                                       "instance_id", &on_remove);
  ASSERT_EQ(0, on_remove.wait());
}

TEST_F(TestMockInstanceWatcher, RemoveNoent) {
  MockManagedLock mock_managed_lock;
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_local_io_ctx));

  InSequence seq;

  expect_get_locker(mock_managed_lock, librbd::managed_lock::Locker(), -ENOENT);
  expect_unregister_instance(mock_io_ctx, 0);
  expect_destroy_lock(mock_managed_lock);

  C_SaferCond on_remove;
  MockInstanceWatcher::remove_instance(m_local_io_ctx, m_threads->work_queue,
                                       "instance_id", &on_remove);
  ASSERT_EQ(0, on_remove.wait());
}

} // namespace mirror
} // namespace rbd
