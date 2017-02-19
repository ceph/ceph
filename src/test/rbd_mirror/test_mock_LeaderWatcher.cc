// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "tools/rbd_mirror/LeaderWatcher.h"
#include "tools/rbd_mirror/Threads.h"

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
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockManagedLock() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_lock_owner, bool());

  MOCK_METHOD1(shut_down, void(Context *));
  MOCK_METHOD1(try_acquire_lock, void(Context *));
  MOCK_METHOD1(release_lock, void(Context *));
  MOCK_METHOD3(break_lock, void(const managed_lock::Locker &, bool, Context *));
  MOCK_METHOD2(get_locker, void(managed_lock::Locker *, Context *));

  MOCK_METHOD0(set_state_post_acquiring, void());

  MOCK_CONST_METHOD0(is_shutdown, bool());

  MOCK_CONST_METHOD0(is_state_post_acquiring, bool());
  MOCK_CONST_METHOD0(is_state_locked, bool());
};

MockManagedLock *MockManagedLock::s_instance = nullptr;

template <>
struct ManagedLock<MockTestImageCtx> {
  ManagedLock(librados::IoCtx& ioctx, ContextWQ *work_queue,
              const std::string& oid, librbd::Watcher *watcher,
              managed_lock::Mode  mode, bool blacklist_on_break_lock,
              uint32_t blacklist_expire_seconds)
    : m_lock("ManagedLock::m_lock") {
  }

  virtual ~ManagedLock() = default;

  mutable Mutex m_lock;

  bool is_lock_owner() const {
    return MockManagedLock::get_instance().is_lock_owner();
  }
  void shut_down(Context *on_shutdown) {
    MockManagedLock::get_instance().shut_down(on_shutdown);
  }
  void try_acquire_lock(Context *on_acquired) {
    MockManagedLock::get_instance().try_acquire_lock(on_acquired);
  }
  void release_lock(Context *on_released) {
    MockManagedLock::get_instance().release_lock(on_released);
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
  bool is_state_locked() const {
    return MockManagedLock::get_instance().is_state_locked();
  }
};

} // namespace librbd

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

class TestMockLeaderWatcher : public TestMockFixture {
public:
  typedef LeaderWatcher<librbd::MockTestImageCtx> MockLeaderWatcher;

  class MockListener : public MockLeaderWatcher::Listener {
  public:
    MOCK_METHOD1(post_acquire_handler, void(Context *));
    MOCK_METHOD1(pre_release_handler, void(Context *));
  };

  void expect_is_lock_owner(MockManagedLock &mock_managed_lock, bool owner) {
    EXPECT_CALL(mock_managed_lock, is_lock_owner())
      .WillOnce(Return(owner));
  }

  void expect_shut_down(MockManagedLock &mock_managed_lock, int r) {
    EXPECT_CALL(mock_managed_lock, shut_down(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_try_acquire_lock(MockManagedLock &mock_managed_lock, int r) {
    EXPECT_CALL(mock_managed_lock, try_acquire_lock(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_release_lock(MockManagedLock &mock_managed_lock, int r) {
    EXPECT_CALL(mock_managed_lock, release_lock(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_get_locker(MockManagedLock &mock_managed_lock,
                         const librbd::managed_lock::Locker &locker, int r,
                         Context *on_finish = nullptr) {
    EXPECT_CALL(mock_managed_lock, get_locker(_, _))
      .WillOnce(Invoke([on_finish, r, locker](librbd::managed_lock::Locker *out,
                                              Context *ctx) {
                         if (r == 0) {
                           *out = locker;
                         }
                         ctx->complete(r);
                         if (on_finish != nullptr) {
                           on_finish->complete(0);
                         }
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
  }

  void expect_notify_heartbeat(MockManagedLock &mock_managed_lock,
                               Context *on_finish) {
    EXPECT_CALL(mock_managed_lock, is_state_post_acquiring())
      .WillOnce(Return(false));
    EXPECT_CALL(mock_managed_lock, is_state_locked())
      .WillOnce(Return(true));
    EXPECT_CALL(mock_managed_lock, is_state_post_acquiring())
      .WillOnce(Return(false));
    EXPECT_CALL(mock_managed_lock, is_state_locked())
      .WillOnce(DoAll(Invoke([on_finish]() {
                        on_finish->complete(0);
                      }),
	              Return(true)));
  }
};

TEST_F(TestMockLeaderWatcher, InitShutdown) {
  MockManagedLock mock_managed_lock;
  MockListener listener;
  MockLeaderWatcher leader_watcher(m_threads, m_local_io_ctx, &listener);

  expect_is_shutdown(mock_managed_lock);

  InSequence seq;
  C_SaferCond on_heartbeat_finish;
  expect_try_acquire_lock(mock_managed_lock, 0);
  expect_notify_heartbeat(mock_managed_lock, &on_heartbeat_finish);

  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_heartbeat_finish.wait());

  expect_shut_down(mock_managed_lock, 0);

  leader_watcher.shut_down();
}

TEST_F(TestMockLeaderWatcher, InitReleaseShutdown) {
  MockManagedLock mock_managed_lock;
  MockListener listener;
  MockLeaderWatcher leader_watcher(m_threads, m_local_io_ctx, &listener);

  expect_is_shutdown(mock_managed_lock);

  InSequence seq;
  C_SaferCond on_heartbeat_finish;
  expect_try_acquire_lock(mock_managed_lock, 0);
  expect_notify_heartbeat(mock_managed_lock, &on_heartbeat_finish);

  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_heartbeat_finish.wait());

  expect_is_leader(mock_managed_lock, false, true);
  expect_release_lock(mock_managed_lock, 0);

  leader_watcher.release_leader();

  expect_shut_down(mock_managed_lock, 0);

  leader_watcher.shut_down();
}

TEST_F(TestMockLeaderWatcher, AcquireError) {
  MockManagedLock mock_managed_lock;
  MockListener listener;
  MockLeaderWatcher leader_watcher(m_threads, m_local_io_ctx, &listener);

  expect_is_shutdown(mock_managed_lock);
  expect_is_leader(mock_managed_lock);

  InSequence seq;
  C_SaferCond on_get_locker_finish;
  expect_try_acquire_lock(mock_managed_lock, -EAGAIN);
  expect_get_locker(mock_managed_lock, librbd::managed_lock::Locker(), 0,
                    &on_get_locker_finish);
  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_get_locker_finish.wait());

  expect_shut_down(mock_managed_lock, 0);

  leader_watcher.shut_down();
}

TEST_F(TestMockLeaderWatcher, ReleaseError) {
  MockManagedLock mock_managed_lock;
  MockListener listener;
  MockLeaderWatcher leader_watcher(m_threads, m_local_io_ctx, &listener);

  expect_is_shutdown(mock_managed_lock);

  InSequence seq;
  C_SaferCond on_heartbeat_finish;
  expect_try_acquire_lock(mock_managed_lock, 0);
  expect_notify_heartbeat(mock_managed_lock, &on_heartbeat_finish);

  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_heartbeat_finish.wait());

  expect_is_leader(mock_managed_lock, false, true);
  expect_release_lock(mock_managed_lock, -EINVAL);

  leader_watcher.release_leader();

  expect_shut_down(mock_managed_lock, 0);

  leader_watcher.shut_down();
}

TEST_F(TestMockLeaderWatcher, Break) {
  EXPECT_EQ(0, _rados->conf_set("rbd_mirror_leader_heartbeat_interval", "1"));
  EXPECT_EQ(0, _rados->conf_set("rbd_mirror_leader_max_missed_heartbeats",
				"1"));
  CephContext *cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
  int max_acquire_attempts =
    cct->_conf->rbd_mirror_leader_max_acquire_attempts_before_break;

  MockManagedLock mock_managed_lock;
  MockListener listener;
  MockLeaderWatcher leader_watcher(m_threads, m_local_io_ctx, &listener);
  librbd::managed_lock::Locker
    locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};

  expect_is_shutdown(mock_managed_lock);
  expect_is_leader(mock_managed_lock);

  InSequence seq;
  for (int i = 0; i <= max_acquire_attempts; i++) {
    expect_try_acquire_lock(mock_managed_lock, -EAGAIN);
    if (i < max_acquire_attempts) {
      expect_get_locker(mock_managed_lock, locker, 0);
    }
  }
  C_SaferCond on_break;
  expect_break_lock(mock_managed_lock, locker, 0, &on_break);
  C_SaferCond on_heartbeat_finish;
  expect_try_acquire_lock(mock_managed_lock, 0);
  expect_notify_heartbeat(mock_managed_lock, &on_heartbeat_finish);

  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_heartbeat_finish.wait());

  expect_shut_down(mock_managed_lock, 0);

  leader_watcher.shut_down();
}

} // namespace mirror
} // namespace rbd
