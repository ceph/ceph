// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/managed_lock/test_mock_LockWatcher.h"
#include "librbd/Lock.h"
#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/managed_lock/ReacquireRequest.h"
#include "librbd/managed_lock/ReleaseRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

namespace librbd {
namespace managed_lock {

namespace {

struct MockWorkQueue {
  ThreadPool thread_pool;
  ContextWQ work_queue;

  MockWorkQueue(CephContext *cct) :
    thread_pool(cct, "pool_name", "thread_name", 1, ""),
    work_queue("work_queue", 0, &thread_pool) {
      thread_pool.start();
    }

  ~MockWorkQueue() {
    thread_pool.stop();
  }

  void queue(Context *ctx, int r=0) {
    work_queue.queue(ctx, r);
  }
};

struct MockManagedLockWatcher : public librbd::managed_lock::MockLockWatcher {
  MockWorkQueue *work_queue;

  MockManagedLockWatcher(Lock<MockManagedLockWatcher> *lock) {
    MockLockWatcher::s_instance = this;
    work_queue = new MockWorkQueue(reinterpret_cast<CephContext *>(
                                   lock->io_ctx().cct()));
  }

  virtual ~MockManagedLockWatcher() {
    MockLockWatcher::s_instance = nullptr;
    delete work_queue;
  }
};

} // anonymous namespace


template<typename T>
struct BaseRequest {
  static std::list<T *> s_requests;
  Context *on_finish = nullptr;

  static T* create(librados::IoCtx &ioctx, MockManagedLockWatcher *watcher,
                   const std::string& oid, const std::string& cookie,
                   Context *on_finish, bool shut_down = false) {
    assert(!s_requests.empty());
    T* req = s_requests.front();
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
struct AcquireRequest<MockManagedLockWatcher> :
          public BaseRequest<AcquireRequest<MockManagedLockWatcher> > {
  static AcquireRequest* create(librados::IoCtx &ioctx,
                                MockManagedLockWatcher *watcher,
                                const std::string& oid,
                                const std::string& cookie,
                                Context *on_finish) {
    return BaseRequest::create(ioctx, watcher, oid, cookie, on_finish);
  }

  MOCK_METHOD0(send, void());
};

template <>
struct ReacquireRequest<MockManagedLockWatcher> :
          public BaseRequest<ReacquireRequest<MockManagedLockWatcher> > {
  static ReacquireRequest* create(librados::IoCtx &ioctx,
                                const std::string& oid,
                                const std::string& old_cookie,
                                const std::string& new_cookie,
                                Context *on_finish) {
    return BaseRequest::create(ioctx, nullptr, oid, old_cookie, on_finish);
  }

  MOCK_METHOD0(send, void());
};

template <>
struct ReleaseRequest<MockManagedLockWatcher> :
          public BaseRequest<ReleaseRequest<MockManagedLockWatcher> > {
  MOCK_METHOD0(send, void());
};

} // namespace managed_lock
} // namespace librbd

// template definitions
#include "librbd/Lock.cc"
template class librbd::Lock<librbd::managed_lock::MockManagedLockWatcher>;

#include "librbd/managed_lock/AutomaticPolicy.cc"
template class librbd::managed_lock::AutomaticPolicy<librbd::managed_lock::MockManagedLockWatcher>;


ACTION_P3(QueueRequest, request, r, wq) {
  if (request->on_finish != nullptr) {
    if (wq != nullptr) {
      wq->queue(request->on_finish, r);
    } else {
      request->on_finish->complete(r);
    }
  }
}

ACTION_P2(QueueContext, r, wq) {
  wq->queue(arg0, r);
}

namespace librbd {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;

class TestMockManagedLock : public TestMockFixture {
public:
  typedef Lock<MockManagedLockWatcher> MockManagedLock;
  typedef managed_lock::AcquireRequest<MockManagedLockWatcher> MockAcquireRequest;
  typedef managed_lock::ReacquireRequest<MockManagedLockWatcher> MockReacquireRequest;
  typedef managed_lock::ReleaseRequest<MockManagedLockWatcher> MockReleaseRequest;

  void expect_is_registered(MockManagedLockWatcher *mock_watcher) {
    EXPECT_CALL(*mock_watcher, is_registered())
      .WillOnce(Return(false))
      .WillRepeatedly(Return(true));
  }

  void expect_register_watch(MockManagedLockWatcher *mock_watcher) {
    EXPECT_CALL(*mock_watcher, register_watch(_))
      .WillOnce(CompleteContext(0, (ContextWQ *)nullptr));
  }

  void expect_unregister_watch(MockManagedLockWatcher *mock_watcher) {
    EXPECT_CALL(*mock_watcher, unregister_watch(_))
      .WillOnce(CompleteContext(0, (ContextWQ *)nullptr));
  }

  void expect_encode_lock_cookie(MockManagedLockWatcher *mock_watcher,
                                 uint64_t watch_handle = 1234567890) {
    std::stringstream ss;
    ss << "auto " << watch_handle;
    EXPECT_CALL(*mock_watcher, encode_lock_cookie())
      .WillOnce(Return(ss.str()));
  }

  void expect_acquire_lock(MockManagedLockWatcher *mock_watcher,
                           MockAcquireRequest &acquire_request, int r,
                           bool register_watch = true) {
    if (register_watch) {
      expect_register_watch(mock_watcher);
    }
    expect_encode_lock_cookie(mock_watcher);
    EXPECT_CALL(acquire_request, send())
                  .WillOnce(QueueRequest(&acquire_request, r,
                                         mock_watcher->work_queue));
    if (r == 0) {
      expect_notify_acquired_lock(mock_watcher);
    }
  }

  void expect_release_lock(MockManagedLockWatcher *mock_watcher,
                           MockReleaseRequest &release_request, int r,
                           bool shutting_down = false) {
    EXPECT_CALL(release_request, send())
                  .WillOnce(QueueRequest(&release_request, r,
                                         mock_watcher->work_queue));
    if (r == 0) {
      expect_notify_released_lock(mock_watcher);
    }
    if (shutting_down) {
      expect_unregister_watch(mock_watcher);
    }
  }

  void expect_reacquire_lock(MockManagedLockWatcher *mock_watcher,
                             MockReacquireRequest &mock_reacquire_request,
                             int r) {
    expect_encode_lock_cookie(mock_watcher, 98765);
    EXPECT_CALL(mock_reacquire_request, send())
                  .WillOnce(QueueRequest(&mock_reacquire_request, r,
                                         mock_watcher->work_queue));
  }

  void expect_notify_request_lock(MockManagedLockWatcher *mock_watcher,
                                  MockManagedLock &mock_managed_lock) {
    EXPECT_CALL(*mock_watcher, notify_request_lock())
                  .WillRepeatedly(Invoke(&mock_managed_lock,
                                         &MockManagedLock::handle_peer_notification));
  }

  void expect_notify_acquired_lock(MockManagedLockWatcher *mock_watcher) {
    EXPECT_CALL(*mock_watcher, notify_acquired_lock()).Times(1);
  }

  void expect_notify_released_lock(MockManagedLockWatcher *mock_watcher) {
    EXPECT_CALL(*mock_watcher, notify_released_lock()).Times(1);
  }

  void expect_flush_notifies(MockManagedLockWatcher *mock_watcher) {
    EXPECT_CALL(*mock_watcher, flush(_))
                  .WillOnce(CompleteContext(0, (ContextWQ *)nullptr));
  }

  int when_try_lock(MockManagedLock &managed_lock) {
    C_SaferCond ctx;
    {
      managed_lock.try_lock(&ctx);
    }
    return ctx.wait();
  }
  int when_request_lock(MockManagedLock &managed_lock) {
    C_SaferCond ctx;
    {
      managed_lock.request_lock(&ctx);
    }
    return ctx.wait();
  }
  int when_release_lock(MockManagedLock &managed_lock) {
    C_SaferCond ctx;
    {
      managed_lock.release_lock(&ctx);
    }
    return ctx.wait();
  }
  int when_shut_down(MockManagedLock &managed_lock) {
    C_SaferCond ctx;
    {
      managed_lock.shut_down(&ctx);
    }
    return ctx.wait();
  }

  bool is_lock_owner(MockManagedLock &managed_lock) {
    return managed_lock.is_lock_owner();
  }
};

TEST_F(TestMockManagedLock, StateTransitions) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_watcher, try_lock_acquire, 0);
  ASSERT_EQ(0, when_try_lock(managed_lock));
  ASSERT_TRUE(is_lock_owner(managed_lock));

  MockReleaseRequest request_release;
  expect_release_lock(mock_watcher, request_release, 0);
  ASSERT_EQ(0, when_release_lock(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));

  MockAcquireRequest request_lock_acquire;
  expect_acquire_lock(mock_watcher, request_lock_acquire, 0, false);
  ASSERT_EQ(0, when_request_lock(managed_lock));
  ASSERT_TRUE(is_lock_owner(managed_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_watcher, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));
}

TEST_F(TestMockManagedLock, TryLockLockedState) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_watcher, try_lock_acquire, 0);
  ASSERT_EQ(0, when_try_lock(managed_lock));
  ASSERT_EQ(0, when_try_lock(managed_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_watcher, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, TryLockAlreadyLocked) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_watcher, try_lock_acquire, -EAGAIN);
  ASSERT_EQ(0, when_try_lock(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));

  expect_unregister_watch(mock_watcher);
  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, TryLockBusy) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_watcher, try_lock_acquire, -EBUSY);
  ASSERT_EQ(-EBUSY, when_try_lock(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));

  expect_unregister_watch(mock_watcher);
  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, TryLockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_watcher, try_lock_acquire, -EINVAL);

  ASSERT_EQ(-EINVAL, when_try_lock(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));

  expect_unregister_watch(mock_watcher);
  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, RequestLockLockedState) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_watcher, try_lock_acquire, 0);
  ASSERT_EQ(0, when_try_lock(managed_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_watcher, shutdown_release, 0, true);
  ASSERT_EQ(0, when_request_lock(managed_lock));

  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, RequestLockBlacklist) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  // will abort after seeing blacklist error (avoid infinite request loop)
  MockAcquireRequest request_lock_acquire;
  expect_acquire_lock(mock_watcher, request_lock_acquire, -EBLACKLISTED);
  expect_notify_request_lock(mock_watcher, managed_lock);
  ASSERT_EQ(-EBLACKLISTED, when_request_lock(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));

  expect_unregister_watch(mock_watcher);
  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, RequestLockBusy) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  // will repeat until successfully acquires the lock
  MockAcquireRequest request_lock_acquire1;
  expect_acquire_lock(mock_watcher, request_lock_acquire1, -EBUSY);
  expect_notify_request_lock(mock_watcher, managed_lock);

  MockAcquireRequest request_lock_acquire2;
  expect_acquire_lock(mock_watcher, request_lock_acquire2, 0, false);
  ASSERT_EQ(0, when_request_lock(managed_lock));
  ASSERT_TRUE(is_lock_owner(managed_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_watcher, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, RequestLockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  // will repeat until successfully acquires the lock
  MockAcquireRequest request_lock_acquire1;
  expect_acquire_lock(mock_watcher, request_lock_acquire1, -EINVAL);
  expect_notify_request_lock(mock_watcher, managed_lock);

  MockAcquireRequest request_lock_acquire2;
  expect_acquire_lock(mock_watcher, request_lock_acquire2, 0, false);
  ASSERT_EQ(0, when_request_lock(managed_lock));
  ASSERT_TRUE(is_lock_owner(managed_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_watcher, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, ReleaseLockUnlockedState) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  ASSERT_EQ(0, when_release_lock(managed_lock));

  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, ReleaseLockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest try_lock_acquire;
  expect_acquire_lock(mock_watcher, try_lock_acquire, 0);
  ASSERT_EQ(0, when_try_lock(managed_lock));

  MockReleaseRequest release;
  expect_release_lock(mock_watcher, release, -EINVAL);

  ASSERT_EQ(-EINVAL, when_release_lock(managed_lock));
  ASSERT_TRUE(is_lock_owner(managed_lock));

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_watcher, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));
}

TEST_F(TestMockManagedLock, ConcurrentRequests) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  expect_register_watch(mock_watcher);
  expect_encode_lock_cookie(mock_watcher);

  MockAcquireRequest try_lock_acquire;
  C_SaferCond wait_for_send_ctx1;
  EXPECT_CALL(try_lock_acquire, send())
                .WillOnce(Notify(&wait_for_send_ctx1));

  MockAcquireRequest request_acquire;
  expect_acquire_lock(mock_watcher, request_acquire, 0, false);

  MockReleaseRequest release;
  C_SaferCond wait_for_send_ctx2;
  EXPECT_CALL(release, send())
                .WillOnce(Notify(&wait_for_send_ctx2));
  expect_notify_released_lock(mock_watcher);

  C_SaferCond try_request_ctx1;
  managed_lock.try_lock(&try_request_ctx1);

  C_SaferCond request_lock_ctx1;
  C_SaferCond request_lock_ctx2;
  managed_lock.request_lock(&request_lock_ctx1);
  managed_lock.request_lock(&request_lock_ctx2);

  C_SaferCond release_lock_ctx1;
  managed_lock.release_lock(&release_lock_ctx1);

  C_SaferCond request_lock_ctx3;
  managed_lock.request_lock(&request_lock_ctx3);

  // fail the try_lock
  ASSERT_EQ(0, wait_for_send_ctx1.wait());
  try_lock_acquire.on_finish->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, try_request_ctx1.wait());

  // all three pending request locks should complete
  ASSERT_EQ(0, request_lock_ctx1.wait());
  ASSERT_EQ(0, request_lock_ctx2.wait());
  ASSERT_EQ(0, request_lock_ctx3.wait());

  // proceed with the release
  ASSERT_EQ(0, wait_for_send_ctx2.wait());
  release.on_finish->complete(0);
  ASSERT_EQ(0, release_lock_ctx1.wait());

  expect_unregister_watch(mock_watcher);
  ASSERT_EQ(0, when_shut_down(managed_lock));
}

TEST_F(TestMockManagedLock, RequestLockWatchRegisterError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);

  InSequence seq;

  EXPECT_CALL(*mock_watcher, is_registered())
    .WillOnce(Return(false));

  EXPECT_CALL(*mock_watcher, register_watch(_))
    .WillOnce(QueueContext(-EINVAL, mock_watcher->work_queue));

  EXPECT_CALL(*mock_watcher, is_registered())
    .WillOnce(Return(false));

  ASSERT_EQ(-EINVAL, when_request_lock(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));

  ASSERT_EQ(0, when_shut_down(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));
}

TEST_F(TestMockManagedLock, ReacquireLock) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest request_lock_acquire;
  expect_acquire_lock(mock_watcher, request_lock_acquire, 0);
  ASSERT_EQ(0, when_request_lock(managed_lock));
  ASSERT_TRUE(is_lock_owner(managed_lock));

  MockReacquireRequest mock_reacquire_request;
  C_SaferCond reacquire_ctx;
  expect_reacquire_lock(mock_watcher, mock_reacquire_request, 0);
  managed_lock.reacquire_lock(&reacquire_ctx);
  ASSERT_EQ(0, reacquire_ctx.wait());

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_watcher, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));
}

TEST_F(TestMockManagedLock, ReacquireLockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockManagedLock managed_lock(ictx->md_ctx, ictx->header_oid);
  MockManagedLockWatcher *mock_watcher =
          static_cast<MockManagedLockWatcher *>(MockLockWatcher::s_instance);
  expect_is_registered(mock_watcher);

  InSequence seq;

  MockAcquireRequest request_lock_acquire;
  expect_acquire_lock(mock_watcher, request_lock_acquire, 0);
  ASSERT_EQ(0, when_request_lock(managed_lock));
  ASSERT_TRUE(is_lock_owner(managed_lock));

  MockReacquireRequest mock_reacquire_request;
  C_SaferCond reacquire_ctx;
  expect_reacquire_lock(mock_watcher, mock_reacquire_request, -EOPNOTSUPP);

  MockReleaseRequest reacquire_lock_release;
  expect_release_lock(mock_watcher, reacquire_lock_release, 0, false);

  MockAcquireRequest reacquire_lock_acquire;
  expect_acquire_lock(mock_watcher, reacquire_lock_acquire, 0, false);

  managed_lock.reacquire_lock(&reacquire_ctx);
  ASSERT_EQ(-EOPNOTSUPP, reacquire_ctx.wait());

  MockReleaseRequest shutdown_release;
  expect_release_lock(mock_watcher, shutdown_release, 0, true);
  ASSERT_EQ(0, when_shut_down(managed_lock));
  ASSERT_FALSE(is_lock_owner(managed_lock));
}

} // namespace librbd
