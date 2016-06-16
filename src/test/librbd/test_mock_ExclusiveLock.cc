// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/exclusive_lock/AcquireRequest.h"
#include "librbd/exclusive_lock/ReleaseRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

namespace librbd {

namespace {

struct MockExclusiveLockImageCtx : public MockImageCtx {
  MockExclusiveLockImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace exclusive_lock {

template<typename T>
struct BaseRequest {
  static std::list<T *> s_requests;
  Context *on_lock_unlock = nullptr;
  Context *on_finish = nullptr;

  static T* create(MockExclusiveLockImageCtx &image_ctx, const std::string &cookie,
                   Context *on_lock_unlock, Context *on_finish) {
    assert(!s_requests.empty());
    T* req = s_requests.front();
    req->on_lock_unlock = on_lock_unlock;
    req->on_finish = on_finish;
    s_requests.pop_front();
    return req;
  }

  BaseRequest() {
    s_requests.push_back(reinterpret_cast<T*>(this));
  }
};

template<typename T>
std::list<T *> BaseRequest<T>::s_requests;

template <>
struct AcquireRequest<MockExclusiveLockImageCtx> : public BaseRequest<AcquireRequest<MockExclusiveLockImageCtx> > {
  MOCK_METHOD0(send, void());
};

template <>
struct ReleaseRequest<MockExclusiveLockImageCtx> : public BaseRequest<ReleaseRequest<MockExclusiveLockImageCtx> > {
  MOCK_METHOD0(send, void());
};

} // namespace exclusive_lock
} // namespace librbd

// template definitions
#include "librbd/ExclusiveLock.cc"
template class librbd::ExclusiveLock<librbd::MockExclusiveLockImageCtx>;

ACTION_P(FinishLockUnlock, request) {
  if (request->on_lock_unlock != nullptr) {
    request->on_lock_unlock->complete(0);
  }
}

namespace librbd {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;

class TestMockExclusiveLock : public TestMockFixture {
public:
  typedef ExclusiveLock<MockExclusiveLockImageCtx> MockExclusiveLock;
  typedef exclusive_lock::AcquireRequest<MockExclusiveLockImageCtx> MockAcquireRequest;
  typedef exclusive_lock::ReleaseRequest<MockExclusiveLockImageCtx> MockReleaseRequest;

  void expect_get_watch_handle(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, get_watch_handle())
                  .WillRepeatedly(Return(1234567890));
  }

  void expect_set_require_lock_on_read(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, set_require_lock_on_read());
  }

  void expect_clear_require_lock_on_read(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, clear_require_lock_on_read());
  }

  void expect_block_writes(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, block_writes(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
    if ((mock_image_ctx.features & RBD_FEATURE_JOURNALING) != 0) {
      expect_set_require_lock_on_read(mock_image_ctx);
    }
  }

  void expect_unblock_writes(MockExclusiveLockImageCtx &mock_image_ctx) {
    expect_clear_require_lock_on_read(mock_image_ctx);
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, unblock_writes());
  }

  void expect_acquire_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                           MockAcquireRequest &acquire_request, int r) {
    expect_get_watch_handle(mock_image_ctx);
    EXPECT_CALL(acquire_request, send())
                  .WillOnce(DoAll(FinishLockUnlock(&acquire_request),
                                  FinishRequest(&acquire_request, r, &mock_image_ctx)));
    if (r == 0) {
      expect_notify_acquired_lock(mock_image_ctx);
      expect_unblock_writes(mock_image_ctx);
    }
  }

  void expect_release_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                           MockReleaseRequest &release_request, int r,
                           bool shutting_down = false) {
    EXPECT_CALL(release_request, send())
                  .WillOnce(DoAll(FinishLockUnlock(&release_request),
                                  FinishRequest(&release_request, r, &mock_image_ctx)));
    if (r == 0) {
      if (shutting_down) {
        expect_unblock_writes(mock_image_ctx);
      }
      expect_notify_released_lock(mock_image_ctx);
      expect_is_lock_request_needed(mock_image_ctx, false);
    }
  }

  void expect_notify_request_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                                  MockExclusiveLock &mock_exclusive_lock) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_request_lock())
                  .WillRepeatedly(Invoke(&mock_exclusive_lock,
                                         &MockExclusiveLock::handle_lock_released));
  }

  void expect_notify_acquired_lock(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_acquired_lock())
                  .Times(1);
  }

  void expect_notify_released_lock(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_released_lock())
                  .Times(1);
  }

  void expect_is_lock_request_needed(MockExclusiveLockImageCtx &mock_image_ctx, bool ret) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, is_lock_request_needed())
                  .WillRepeatedly(Return(ret));
  }

  void expect_flush_notifies(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, flush(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_append_lock_release_event(MockExclusiveLockImageCtx &mock_image_ctx,
                                            MockJournal &mock_journal, int r) {
        EXPECT_CALL(mock_journal, append_lock_release_event(_))
                      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  int when_init(MockExclusiveLockImageCtx &mock_image_ctx,
                MockExclusiveLock &exclusive_lock) {
    C_SaferCond ctx;
    {
      RWLock::WLocker owner_locker(mock_image_ctx.owner_lock);
      exclusive_lock.init(mock_image_ctx.features, &ctx);
    }
    return ctx.wait();
  }

  int when_try_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                     MockExclusiveLock &exclusive_lock) {
    C_SaferCond ctx;
    {
      RWLock::WLocker owner_locker(mock_image_ctx.owner_lock);
      exclusive_lock.try_lock(&ctx);
    }
    return ctx.wait();
  }
  int when_request_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                     MockExclusiveLock &exclusive_lock) {
    C_SaferCond ctx;
    {
      RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
      exclusive_lock.request_lock(&ctx);
    }
    return ctx.wait();
  }
  int when_release_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                     MockExclusiveLock &exclusive_lock) {
    C_SaferCond ctx;
    {
      RWLock::WLocker owner_locker(mock_image_ctx.owner_lock);
      exclusive_lock.release_lock(&ctx);
    }
    return ctx.wait();
  }
  int when_shut_down(MockExclusiveLockImageCtx &mock_image_ctx,
                     MockExclusiveLock &exclusive_lock) {
    C_SaferCond ctx;
    {
      RWLock::WLocker owner_locker(mock_image_ctx.owner_lock);
      exclusive_lock.shut_down(&ctx);
    }
    return ctx.wait();
  }

  bool is_lock_owner(MockExclusiveLockImageCtx &mock_image_ctx,
                     MockExclusiveLock &exclusive_lock) {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    return exclusive_lock.is_lock_owner();
  }
};

TEST_F(TestMockExclusiveLock, StateTransitions) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_acquire, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockReleaseRequest request_release;
  expect_release_lock(mock_image_ctx, request_release, 0);
  ASSERT_EQ(0, when_release_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockAcquireRequest request_lock_acquire;
  expect_acquire_lock(mock_image_ctx, request_lock_acquire, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_image_ctx, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, TryLockLockedState) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_acquire, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_image_ctx, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, TryLockAlreadyLocked) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  MockJournal *mock_journal = new MockJournal();
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_acquire, -EAGAIN);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_append_lock_release_event(mock_image_ctx, *mock_journal, 0);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, TryLockBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  MockJournal *mock_journal = new MockJournal();
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_acquire, -EBUSY);
  ASSERT_EQ(-EBUSY, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_append_lock_release_event(mock_image_ctx, *mock_journal, 0);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, TryLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  MockJournal *mock_journal = new MockJournal();
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_acquire, -EINVAL);

  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));
  ASSERT_EQ(-EINVAL, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_append_lock_release_event(mock_image_ctx, *mock_journal, 0);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, RequestLockLockedState) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_acquire, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_image_ctx, shutdown_release, 0, true);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));

  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, RequestLockBlacklist) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  MockJournal *mock_journal = new MockJournal();
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // will abort after seeing blacklist error (avoid infinite request loop)
  MockAcquireRequest request_lock_acquire;
  expect_acquire_lock(mock_image_ctx, request_lock_acquire, -EBLACKLISTED);
  expect_notify_request_lock(mock_image_ctx, exclusive_lock);
  ASSERT_EQ(-EBLACKLISTED, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_append_lock_release_event(mock_image_ctx, *mock_journal, 0);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, RequestLockBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // will repeat until successfully acquires the lock
  MockAcquireRequest request_lock_acquire1;
  expect_acquire_lock(mock_image_ctx, request_lock_acquire1, -EBUSY);
  expect_notify_request_lock(mock_image_ctx, exclusive_lock);

  MockAcquireRequest request_lock_acquire2;
  expect_acquire_lock(mock_image_ctx, request_lock_acquire2, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_image_ctx, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, RequestLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // will repeat until successfully acquires the lock
  MockAcquireRequest request_lock_acquire1;
  expect_acquire_lock(mock_image_ctx, request_lock_acquire1, -EINVAL);
  expect_notify_request_lock(mock_image_ctx, exclusive_lock);

  MockAcquireRequest request_lock_acquire2;
  expect_acquire_lock(mock_image_ctx, request_lock_acquire2, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_image_ctx, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, ReleaseLockUnlockedState) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  MockJournal *mock_journal = new MockJournal();
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  ASSERT_EQ(0, when_release_lock(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_append_lock_release_event(mock_image_ctx, *mock_journal, 0);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, ReleaseLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_acquire, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));

  MockReleaseRequest release;
  expect_release_lock(mock_image_ctx, release, -EINVAL);

  ASSERT_EQ(-EINVAL, when_release_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_image_ctx, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, ConcurrentRequests) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  MockJournal *mock_journal = new MockJournal();
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockAcquireRequest try_lock_acquire;
  C_SaferCond wait_for_send_ctx1;
  expect_get_watch_handle(mock_image_ctx);
  EXPECT_CALL(try_lock_acquire, send())
                .WillOnce(Notify(&wait_for_send_ctx1));

  MockAcquireRequest request_acquire;
  expect_acquire_lock(mock_image_ctx, request_acquire, 0);

  MockReleaseRequest release;
  C_SaferCond wait_for_send_ctx2;
  EXPECT_CALL(release, send())
                .WillOnce(Notify(&wait_for_send_ctx2));
  expect_notify_released_lock(mock_image_ctx);
  expect_is_lock_request_needed(mock_image_ctx, false);

  C_SaferCond try_request_ctx1;
  {
    RWLock::WLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.try_lock(&try_request_ctx1);
  }

  C_SaferCond request_lock_ctx1;
  C_SaferCond request_lock_ctx2;
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.request_lock(&request_lock_ctx1);
    exclusive_lock.request_lock(&request_lock_ctx2);
  }

  C_SaferCond release_lock_ctx1;
  {
    RWLock::WLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.release_lock(&release_lock_ctx1);
  }

  C_SaferCond request_lock_ctx3;
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.request_lock(&request_lock_ctx3);
  }

  // fail the try_lock
  ASSERT_EQ(0, wait_for_send_ctx1.wait());
  try_lock_acquire.on_lock_unlock->complete(0);
  try_lock_acquire.on_finish->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, try_request_ctx1.wait());

  // all three pending request locks should complete
  ASSERT_EQ(0, request_lock_ctx1.wait());
  ASSERT_EQ(0, request_lock_ctx2.wait());
  ASSERT_EQ(0, request_lock_ctx3.wait());

  // proceed with the release
  ASSERT_EQ(0, wait_for_send_ctx2.wait());
  release.on_lock_unlock->complete(0);
  release.on_finish->complete(0);
  ASSERT_EQ(0, release_lock_ctx1.wait());

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_append_lock_release_event(mock_image_ctx, *mock_journal, 0);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

} // namespace librbd

