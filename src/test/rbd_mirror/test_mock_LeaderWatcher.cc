// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Utils.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "tools/rbd_mirror/LeaderWatcher.h"
#include "tools/rbd_mirror/Threads.h"

using librbd::util::create_async_context_callback;

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

struct MockManagedLock {
  static MockManagedLock *s_instance;
  static MockManagedLock &get_instance() {
    ceph_assert(s_instance != nullptr);
    return *s_instance;
  }

  MockManagedLock() {
    s_instance = this;
  }

  bool m_release_lock_on_shutdown = false;
  Context *m_on_released = nullptr;

  MOCK_METHOD0(construct, void());
  MOCK_METHOD0(destroy, void());

  MOCK_CONST_METHOD0(is_lock_owner, bool());

  MOCK_METHOD1(shut_down, void(Context *));
  MOCK_METHOD1(try_acquire_lock, void(Context *));
  MOCK_METHOD1(release_lock, void(Context *));
  MOCK_METHOD0(reacquire_lock, void());
  MOCK_METHOD3(break_lock, void(const managed_lock::Locker &, bool, Context *));
  MOCK_METHOD2(get_locker, void(managed_lock::Locker *, Context *));

  MOCK_METHOD0(set_state_post_acquiring, void());

  MOCK_CONST_METHOD0(is_shutdown, bool());

  MOCK_CONST_METHOD0(is_state_post_acquiring, bool());
  MOCK_CONST_METHOD0(is_state_pre_releasing, bool());
  MOCK_CONST_METHOD0(is_state_locked, bool());
};

MockManagedLock *MockManagedLock::s_instance = nullptr;

template <>
struct ManagedLock<MockTestImageCtx> {
  ManagedLock(librados::IoCtx& ioctx, ContextWQ *work_queue,
              const std::string& oid, librbd::Watcher *watcher,
              managed_lock::Mode  mode, bool blacklist_on_break_lock,
              uint32_t blacklist_expire_seconds)
    : m_work_queue(work_queue), m_lock("ManagedLock::m_lock") {
    MockManagedLock::get_instance().construct();
  }

  virtual ~ManagedLock() {
    MockManagedLock::get_instance().destroy();
  }

  ContextWQ *m_work_queue;

  mutable Mutex m_lock;

  bool is_lock_owner() const {
    return MockManagedLock::get_instance().is_lock_owner();
  }

  void shut_down(Context *on_shutdown) {
    if (MockManagedLock::get_instance().m_release_lock_on_shutdown) {
      on_shutdown = new FunctionContext(
        [this, on_shutdown](int r) {
          MockManagedLock::get_instance().m_release_lock_on_shutdown = false;
          shut_down(on_shutdown);
        });
      release_lock(on_shutdown);
      return;
    }

    MockManagedLock::get_instance().shut_down(on_shutdown);
  }

  void try_acquire_lock(Context *on_acquired) {
    Context *post_acquire_ctx = create_async_context_callback(
      m_work_queue, new FunctionContext(
        [this, on_acquired](int r) {
          post_acquire_lock_handler(r, on_acquired);
        }));
    MockManagedLock::get_instance().try_acquire_lock(post_acquire_ctx);
  }

  void release_lock(Context *on_released) {
    ceph_assert(MockManagedLock::get_instance().m_on_released == nullptr);
    MockManagedLock::get_instance().m_on_released = on_released;

    Context *post_release_ctx = new FunctionContext(
      [this](int r) {
        ceph_assert(MockManagedLock::get_instance().m_on_released != nullptr);
        post_release_lock_handler(false, r,
                                  MockManagedLock::get_instance().m_on_released);
        MockManagedLock::get_instance().m_on_released = nullptr;
      });

    Context *release_ctx = new FunctionContext(
      [this, post_release_ctx](int r) {
        if (r < 0) {
          MockManagedLock::get_instance().m_on_released->complete(r);
        } else {
          MockManagedLock::get_instance().release_lock(post_release_ctx);
        }
      });

    Context *pre_release_ctx = new FunctionContext(
      [this, release_ctx](int r) {
        bool shutting_down =
          MockManagedLock::get_instance().m_release_lock_on_shutdown;
        pre_release_lock_handler(shutting_down, release_ctx);
      });

    m_work_queue->queue(pre_release_ctx, 0);
  }

  void reacquire_lock(Context* on_finish) {
    MockManagedLock::get_instance().reacquire_lock();
  }

  void get_locker(managed_lock::Locker *locker, Context *on_finish) {
    MockManagedLock::get_instance().get_locker(locker, on_finish);
  }

  void break_lock(const managed_lock::Locker &locker, bool force_break_lock,
                  Context *on_finish) {
    MockManagedLock::get_instance().break_lock(locker, force_break_lock,
                                               on_finish);
  }

  void set_state_post_acquiring() {
    MockManagedLock::get_instance().set_state_post_acquiring();
  }

  bool is_shutdown() const {
    return MockManagedLock::get_instance().is_shutdown();
  }

  bool is_state_post_acquiring() const {
    return MockManagedLock::get_instance().is_state_post_acquiring();
  }

  bool is_state_pre_releasing() const {
    return MockManagedLock::get_instance().is_state_pre_releasing();
  }

  bool is_state_locked() const {
    return MockManagedLock::get_instance().is_state_locked();
  }

  virtual void post_acquire_lock_handler(int r, Context *on_finish) = 0;
  virtual void pre_release_lock_handler(bool shutting_down,
                                        Context *on_finish) = 0;
  virtual void post_release_lock_handler(bool shutting_down, int r,
                                         Context *on_finish) = 0;
};

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
struct MirrorStatusWatcher<librbd::MockTestImageCtx> {
  static MirrorStatusWatcher* s_instance;

  static MirrorStatusWatcher *create(librados::IoCtx &io_ctx,
                                     ContextWQ *work_queue) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MirrorStatusWatcher() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  ~MirrorStatusWatcher() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD0(destroy, void());
  MOCK_METHOD1(init, void(Context *));
  MOCK_METHOD1(shut_down, void(Context *));
};

MirrorStatusWatcher<librbd::MockTestImageCtx> *MirrorStatusWatcher<librbd::MockTestImageCtx>::s_instance = nullptr;

template <>
struct Instances<librbd::MockTestImageCtx> {
  static Instances* s_instance;

  static Instances *create(Threads<librbd::MockTestImageCtx> *threads,
                           librados::IoCtx &ioctx,
                           const std::string& instance_id,
                           instances::Listener&) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  Instances() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  ~Instances() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD0(destroy, void());
  MOCK_METHOD1(init, void(Context *));
  MOCK_METHOD1(shut_down, void(Context *));
  MOCK_METHOD1(acked, void(const std::vector<std::string> &));
  MOCK_METHOD0(unblock_listener, void());
};

Instances<librbd::MockTestImageCtx> *Instances<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace mirror
} // namespace rbd


// template definitions
#include "tools/rbd_mirror/LeaderWatcher.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;

using librbd::MockManagedLock;

struct MockListener : public leader_watcher::Listener {
  static MockListener* s_instance;

  MockListener() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  ~MockListener() override {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD1(post_acquire_handler, void(Context *));
  MOCK_METHOD1(pre_release_handler, void(Context *));

  MOCK_METHOD1(update_leader_handler, void(const std::string &));
  MOCK_METHOD1(handle_instances_added, void(const InstanceIds&));
  MOCK_METHOD1(handle_instances_removed, void(const InstanceIds&));
};

MockListener *MockListener::s_instance = nullptr;

class TestMockLeaderWatcher : public TestMockFixture {
public:
  typedef MirrorStatusWatcher<librbd::MockTestImageCtx> MockMirrorStatusWatcher;
  typedef Instances<librbd::MockTestImageCtx> MockInstances;
  typedef LeaderWatcher<librbd::MockTestImageCtx> MockLeaderWatcher;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

  void SetUp() override {
    TestMockFixture::SetUp();
    m_mock_threads = new MockThreads(m_threads);
  }

  void TearDown() override {
    delete m_mock_threads;
    TestMockFixture::TearDown();
  }

  void expect_construct(MockManagedLock &mock_managed_lock) {
    EXPECT_CALL(mock_managed_lock, construct());
  }

  void expect_destroy(MockManagedLock &mock_managed_lock) {
    EXPECT_CALL(mock_managed_lock, destroy());
  }

  void expect_is_lock_owner(MockManagedLock &mock_managed_lock, bool owner) {
    EXPECT_CALL(mock_managed_lock, is_lock_owner())
      .WillOnce(Return(owner));
  }

  void expect_shut_down(MockManagedLock &mock_managed_lock,
                        bool release_lock_on_shutdown, int r) {
    mock_managed_lock.m_release_lock_on_shutdown = release_lock_on_shutdown;
    EXPECT_CALL(mock_managed_lock, shut_down(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_try_acquire_lock(MockManagedLock &mock_managed_lock, int r) {
    EXPECT_CALL(mock_managed_lock, try_acquire_lock(_))
      .WillOnce(CompleteContext(r));
    if (r == 0) {
      expect_set_state_post_acquiring(mock_managed_lock);
    }
  }

  void expect_release_lock(MockManagedLock &mock_managed_lock, int r,
                           Context *on_finish = nullptr) {
    EXPECT_CALL(mock_managed_lock, release_lock(_))
      .WillOnce(Invoke([on_finish, &mock_managed_lock, r](Context *ctx) {
                         if (on_finish != nullptr) {
                           auto on_released = mock_managed_lock.m_on_released;
                           ceph_assert(on_released != nullptr);
                           mock_managed_lock.m_on_released = new FunctionContext(
                             [on_released, on_finish](int r) {
                               on_released->complete(r);
                               on_finish->complete(r);
                             });
                         }
                         ctx->complete(r);
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
                         const librbd::managed_lock::Locker &locker, int r,
                         Context *on_finish) {
    EXPECT_CALL(mock_managed_lock, break_lock(locker, true, _))
      .WillOnce(Invoke([on_finish, r](const librbd::managed_lock::Locker &,
                                      bool, Context *ctx) {
                         ctx->complete(r);
                         on_finish->complete(0);
                       }));
  }

  void expect_set_state_post_acquiring(MockManagedLock &mock_managed_lock) {
    EXPECT_CALL(mock_managed_lock, set_state_post_acquiring());
  }

  void expect_is_shutdown(MockManagedLock &mock_managed_lock) {
    EXPECT_CALL(mock_managed_lock, is_shutdown())
      .Times(AtLeast(0)).WillRepeatedly(Return(false));
  }

  void expect_is_leader(MockManagedLock &mock_managed_lock, bool post_acquiring,
                        bool locked) {
    EXPECT_CALL(mock_managed_lock, is_state_post_acquiring())
      .WillOnce(Return(post_acquiring));
    if (!post_acquiring) {
      EXPECT_CALL(mock_managed_lock, is_state_locked())
        .WillOnce(Return(locked));
    }
  }

  void expect_is_leader(MockManagedLock &mock_managed_lock) {
    EXPECT_CALL(mock_managed_lock, is_state_post_acquiring())
      .Times(AtLeast(0)).WillRepeatedly(Return(false));
    EXPECT_CALL(mock_managed_lock, is_state_locked())
      .Times(AtLeast(0)).WillRepeatedly(Return(false));
    EXPECT_CALL(mock_managed_lock, is_state_pre_releasing())
      .Times(AtLeast(0)).WillRepeatedly(Return(false));
  }

  void expect_notify_heartbeat(MockManagedLock &mock_managed_lock,
                               Context *on_finish) {
    // is_leader in notify_heartbeat
    EXPECT_CALL(mock_managed_lock, is_state_post_acquiring())
      .WillOnce(Return(false));
    EXPECT_CALL(mock_managed_lock, is_state_locked())
      .WillOnce(Return(true));

    // is_leader in handle_notify_heartbeat
    EXPECT_CALL(mock_managed_lock, is_state_post_acquiring())
      .WillOnce(Return(false));
    EXPECT_CALL(mock_managed_lock, is_state_locked())
      .WillOnce(DoAll(Invoke([on_finish]() {
                        on_finish->complete(0);
                      }),
                      Return(true)));
  }

  void expect_destroy(MockMirrorStatusWatcher &mock_mirror_status_watcher) {
    EXPECT_CALL(mock_mirror_status_watcher, destroy());
  }

  void expect_init(MockMirrorStatusWatcher &mock_mirror_status_watcher, int r) {
    EXPECT_CALL(mock_mirror_status_watcher, init(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
  }

  void expect_shut_down(MockMirrorStatusWatcher &mock_mirror_status_watcher, int r) {
    EXPECT_CALL(mock_mirror_status_watcher, shut_down(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
    expect_destroy(mock_mirror_status_watcher);
  }

  void expect_destroy(MockInstances &mock_instances) {
    EXPECT_CALL(mock_instances, destroy());
  }

  void expect_init(MockInstances &mock_instances, int r) {
    EXPECT_CALL(mock_instances, init(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
  }

  void expect_shut_down(MockInstances &mock_instances, int r) {
    EXPECT_CALL(mock_instances, shut_down(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
    expect_destroy(mock_instances);
  }

  void expect_acquire_notify(MockManagedLock &mock_managed_lock,
                             MockListener &mock_listener, int r) {
    expect_is_leader(mock_managed_lock, true, false);
    EXPECT_CALL(mock_listener, post_acquire_handler(_))
      .WillOnce(CompleteContext(r));
    expect_is_leader(mock_managed_lock, true, false);
  }

  void expect_release_notify(MockManagedLock &mock_managed_lock,
                             MockListener &mock_listener, int r) {
    expect_is_leader(mock_managed_lock, false, false);
    EXPECT_CALL(mock_listener, pre_release_handler(_))
      .WillOnce(CompleteContext(r));
    expect_is_leader(mock_managed_lock, false, false);
  }

  void expect_unblock_listener(MockInstances& mock_instances) {
    EXPECT_CALL(mock_instances, unblock_listener());
  }

  void expect_instances_acked(MockInstances& mock_instances) {
    EXPECT_CALL(mock_instances, acked(_));
  }

  MockThreads *m_mock_threads;
};

TEST_F(TestMockLeaderWatcher, InitShutdown) {
  MockManagedLock mock_managed_lock;
  MockMirrorStatusWatcher mock_mirror_status_watcher;
  MockInstances mock_instances;
  MockListener listener;

  expect_is_shutdown(mock_managed_lock);
  expect_destroy(mock_managed_lock);

  InSequence seq;

  expect_construct(mock_managed_lock);
  MockLeaderWatcher leader_watcher(m_mock_threads, m_local_io_ctx, &listener);

  // Init
  expect_init(mock_mirror_status_watcher, 0);
  C_SaferCond on_heartbeat_finish;
  expect_is_leader(mock_managed_lock, false, false);
  expect_try_acquire_lock(mock_managed_lock, 0);
  expect_init(mock_instances, 0);
  expect_acquire_notify(mock_managed_lock, listener, 0);
  expect_unblock_listener(mock_instances);
  expect_notify_heartbeat(mock_managed_lock, &on_heartbeat_finish);
  expect_instances_acked(mock_instances);

  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_heartbeat_finish.wait());

  // Shutdown
  expect_release_notify(mock_managed_lock, listener, 0);
  expect_shut_down(mock_instances, 0);
  expect_release_lock(mock_managed_lock, 0);
  expect_shut_down(mock_managed_lock, true, 0);
  expect_shut_down(mock_mirror_status_watcher, 0);
  expect_is_leader(mock_managed_lock, false, false);

  leader_watcher.shut_down();
}

TEST_F(TestMockLeaderWatcher, InitReleaseShutdown) {
  MockManagedLock mock_managed_lock;
  MockMirrorStatusWatcher mock_mirror_status_watcher;
  MockInstances mock_instances;
  MockListener listener;

  expect_is_shutdown(mock_managed_lock);
  expect_destroy(mock_managed_lock);

  InSequence seq;

  expect_construct(mock_managed_lock);
  MockLeaderWatcher leader_watcher(m_mock_threads, m_local_io_ctx, &listener);

  // Init
  expect_init(mock_mirror_status_watcher, 0);
  C_SaferCond on_heartbeat_finish;
  expect_is_leader(mock_managed_lock, false, false);
  expect_try_acquire_lock(mock_managed_lock, 0);
  expect_init(mock_instances, 0);
  expect_acquire_notify(mock_managed_lock, listener, 0);
  expect_unblock_listener(mock_instances);
  expect_notify_heartbeat(mock_managed_lock, &on_heartbeat_finish);
  expect_instances_acked(mock_instances);

  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_heartbeat_finish.wait());

  // Release
  expect_is_leader(mock_managed_lock, false, true);
  expect_release_notify(mock_managed_lock, listener, 0);
  expect_shut_down(mock_instances, 0);
  C_SaferCond on_release;
  expect_release_lock(mock_managed_lock, 0, &on_release);

  leader_watcher.release_leader();
  ASSERT_EQ(0, on_release.wait());

  // Shutdown
  expect_shut_down(mock_managed_lock, false, 0);
  expect_shut_down(mock_mirror_status_watcher, 0);
  expect_is_leader(mock_managed_lock, false, false);

  leader_watcher.shut_down();
}

TEST_F(TestMockLeaderWatcher, InitStatusWatcherError) {
  MockManagedLock mock_managed_lock;
  MockMirrorStatusWatcher mock_mirror_status_watcher;
  MockInstances mock_instances;
  MockListener listener;

  expect_is_shutdown(mock_managed_lock);
  expect_is_leader(mock_managed_lock);
  expect_destroy(mock_managed_lock);

  InSequence seq;

  expect_construct(mock_managed_lock);
  MockLeaderWatcher leader_watcher(m_mock_threads, m_local_io_ctx, &listener);

  // Init
  expect_init(mock_mirror_status_watcher, -EINVAL);
  ASSERT_EQ(-EINVAL, leader_watcher.init());

  // Shutdown
  expect_shut_down(mock_managed_lock, false, 0);
  expect_shut_down(mock_mirror_status_watcher, 0);
  expect_is_leader(mock_managed_lock, false, false);

  leader_watcher.shut_down();
}

TEST_F(TestMockLeaderWatcher, AcquireError) {
  MockManagedLock mock_managed_lock;
  MockMirrorStatusWatcher mock_mirror_status_watcher;
  MockInstances mock_instances;
  MockListener listener;

  expect_is_shutdown(mock_managed_lock);
  expect_is_leader(mock_managed_lock);
  expect_destroy(mock_managed_lock);

  InSequence seq;

  expect_construct(mock_managed_lock);
  MockLeaderWatcher leader_watcher(m_mock_threads, m_local_io_ctx, &listener);

  // Init
  expect_init(mock_mirror_status_watcher, 0);
  C_SaferCond on_heartbeat_finish;
  expect_is_leader(mock_managed_lock, false, false);
  expect_try_acquire_lock(mock_managed_lock, -EAGAIN);
  expect_get_locker(mock_managed_lock, librbd::managed_lock::Locker(), -ENOENT);
  expect_try_acquire_lock(mock_managed_lock, 0);
  expect_init(mock_instances, 0);
  expect_acquire_notify(mock_managed_lock, listener, 0);
  expect_unblock_listener(mock_instances);
  expect_notify_heartbeat(mock_managed_lock, &on_heartbeat_finish);
  expect_instances_acked(mock_instances);

  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_heartbeat_finish.wait());

  // Shutdown
  expect_release_notify(mock_managed_lock, listener, 0);
  expect_shut_down(mock_instances, 0);
  expect_release_lock(mock_managed_lock, 0);
  expect_shut_down(mock_managed_lock, true, 0);
  expect_shut_down(mock_mirror_status_watcher, 0);
  expect_is_leader(mock_managed_lock, false, false);

  leader_watcher.shut_down();
}

TEST_F(TestMockLeaderWatcher, Break) {
  EXPECT_EQ(0, _rados->conf_set("rbd_mirror_leader_heartbeat_interval", "1"));
  EXPECT_EQ(0, _rados->conf_set("rbd_mirror_leader_max_missed_heartbeats",
                                "1"));
  CephContext *cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
  int max_acquire_attempts = cct->_conf.get_val<uint64_t>(
    "rbd_mirror_leader_max_acquire_attempts_before_break");

  MockManagedLock mock_managed_lock;
  MockMirrorStatusWatcher mock_mirror_status_watcher;
  MockInstances mock_instances;
  MockListener listener;
  librbd::managed_lock::Locker
    locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};

  expect_is_shutdown(mock_managed_lock);
  expect_is_leader(mock_managed_lock);
  expect_destroy(mock_managed_lock);
  EXPECT_CALL(listener, update_leader_handler(_));

  InSequence seq;

  expect_construct(mock_managed_lock);
  MockLeaderWatcher leader_watcher(m_mock_threads, m_local_io_ctx, &listener);

  // Init
  expect_init(mock_mirror_status_watcher, 0);
  expect_is_leader(mock_managed_lock, false, false);
  for (int i = 0; i < max_acquire_attempts; i++) {
    expect_try_acquire_lock(mock_managed_lock, -EAGAIN);
    expect_get_locker(mock_managed_lock, locker, 0);
  }
  C_SaferCond on_break;
  expect_break_lock(mock_managed_lock, locker, 0, &on_break);
  C_SaferCond on_heartbeat_finish;
  expect_try_acquire_lock(mock_managed_lock, 0);
  expect_init(mock_instances, 0);
  expect_acquire_notify(mock_managed_lock, listener, 0);
  expect_unblock_listener(mock_instances);
  expect_notify_heartbeat(mock_managed_lock, &on_heartbeat_finish);
  expect_instances_acked(mock_instances);

  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_heartbeat_finish.wait());

  // Shutdown
  expect_release_notify(mock_managed_lock, listener, 0);
  expect_shut_down(mock_instances, 0);
  expect_release_lock(mock_managed_lock, 0);
  expect_shut_down(mock_managed_lock, true, 0);
  expect_shut_down(mock_mirror_status_watcher, 0);
  expect_is_leader(mock_managed_lock, false, false);

  leader_watcher.shut_down();
}

} // namespace mirror
} // namespace rbd
