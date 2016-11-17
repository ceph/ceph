// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ManagedLock.h"
#include "librbd/exclusive_lock/PreAcquireRequest.h"
#include "librbd/exclusive_lock/PostAcquireRequest.h"
#include "librbd/exclusive_lock/PreReleaseRequest.h"
#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/managed_lock/ReacquireRequest.h"
#include "librbd/managed_lock/ReleaseRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

namespace librbd {

namespace {

struct MockExclusiveLockImageCtx : public MockImageCtx {
  ContextWQ *op_work_queue;

  MockExclusiveLockImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
    op_work_queue = image_ctx.op_work_queue;
  }
};

} // anonymous namespace

namespace watcher {
template <>
struct Traits<MockExclusiveLockImageCtx> {
  typedef librbd::MockImageWatcher Watcher;
};
}

namespace exclusive_lock {

using librbd::ImageWatcher;

template<typename T>
struct BaseRequest {
  static std::list<T *> s_requests;
  Context *on_lock_unlock = nullptr;
  Context *on_finish = nullptr;

  static T* create(MockExclusiveLockImageCtx &image_ctx,
                   Context *on_lock_unlock, Context *on_finish,
                   bool shutting_down = false) {
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
struct PreAcquireRequest<MockExclusiveLockImageCtx> : public BaseRequest<PreAcquireRequest<MockExclusiveLockImageCtx> > {
  static PreAcquireRequest<MockExclusiveLockImageCtx> *create(
      MockExclusiveLockImageCtx &image_ctx, Context *on_finish) {
    return BaseRequest::create(image_ctx, nullptr, on_finish);
  }
  MOCK_METHOD0(send, void());
};

template <>
struct PostAcquireRequest<MockExclusiveLockImageCtx> : public BaseRequest<PostAcquireRequest<MockExclusiveLockImageCtx> > {
  MOCK_METHOD0(send, void());
};

template <>
struct PreReleaseRequest<MockExclusiveLockImageCtx> : public BaseRequest<PreReleaseRequest<MockExclusiveLockImageCtx> > {
  MOCK_METHOD0(send, void());
};

} // namespace exclusive_lock

namespace managed_lock {

template<typename T>
struct BaseRequest {
  static std::list<T *> s_requests;
  Context *on_finish = nullptr;

  static T* create(librados::IoCtx& ioctx, MockImageWatcher *watcher,
                   ContextWQ *work_queue, const std::string& oid,
                   const std::string& cookie, bool exclusive,
                   Context *on_finish) {
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
struct AcquireRequest<MockExclusiveLockImageCtx> : public BaseRequest<AcquireRequest<MockExclusiveLockImageCtx> > {
  MOCK_METHOD0(send, void());
};

template <>
struct ReacquireRequest<MockExclusiveLockImageCtx> : public BaseRequest<ReacquireRequest<MockExclusiveLockImageCtx> > {
  static ReacquireRequest* create(librados::IoCtx &ioctx, const std::string& oid,
                                const string& old_cookie, const std::string& new_cookie,
                                bool exclusive, Context *on_finish) {
    return BaseRequest::create(ioctx, nullptr, nullptr, oid, new_cookie,
                               exclusive, on_finish);
  }

  MOCK_METHOD0(send, void());
};

template <>
struct ReleaseRequest<MockExclusiveLockImageCtx> : public BaseRequest<ReleaseRequest<MockExclusiveLockImageCtx> > {
  static ReleaseRequest* create(librados::IoCtx& ioctx, MockImageWatcher *watcher,
                                ContextWQ *work_queue, const std::string& oid,
                                const std::string& cookie, Context *on_finish) {
    return BaseRequest::create(ioctx, watcher, work_queue, oid, cookie, true,
                               on_finish);
  }

  MOCK_METHOD0(send, void());
};


} // namespace managed_lock
} // namespace librbd

// template definitions
#include "librbd/ExclusiveLock.cc"
template class librbd::ExclusiveLock<librbd::MockExclusiveLockImageCtx>;

#include "librbd/ManagedLock.cc"
template class librbd::ManagedLock<librbd::MockExclusiveLockImageCtx>;

ACTION_P(FinishLockUnlock, request) {
  if (request->on_lock_unlock != nullptr) {
    request->on_lock_unlock->complete(0);
  }
}

ACTION_P2(CompleteRequest, request, ret) {
  request->on_finish->complete(ret);
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
  typedef exclusive_lock::PreAcquireRequest<MockExclusiveLockImageCtx> MockPreAcquireRequest;
  typedef exclusive_lock::PostAcquireRequest<MockExclusiveLockImageCtx> MockPostAcquireRequest;
  typedef exclusive_lock::PreReleaseRequest<MockExclusiveLockImageCtx> MockPreReleaseRequest;
  typedef managed_lock::AcquireRequest<MockExclusiveLockImageCtx> MockManagedAcquireRequest;
  typedef managed_lock::ReacquireRequest<MockExclusiveLockImageCtx> MockManagedReacquireRequest;
  typedef managed_lock::ReleaseRequest<MockExclusiveLockImageCtx> MockManagedReleaseRequest;

  void expect_get_watch_handle(MockExclusiveLockImageCtx &mock_image_ctx,
                               uint64_t watch_handle = 1234567890) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, get_watch_handle())
                  .WillOnce(Return(watch_handle));
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
                           MockPreAcquireRequest &pre_acquire_request,
                           MockManagedAcquireRequest &managed_acquire_request,
                           MockPostAcquireRequest *post_acquire_request,
                           int pre_r, int managed_r, int post_r) {

    expect_get_watch_handle(mock_image_ctx);

    EXPECT_CALL(pre_acquire_request, send())
                .WillOnce(CompleteRequest(&pre_acquire_request, pre_r));
    EXPECT_CALL(managed_acquire_request, send())
                .WillOnce(CompleteRequest(&managed_acquire_request, managed_r));
    if (managed_r == 0) {
      assert(post_acquire_request != nullptr);
      EXPECT_CALL(*post_acquire_request, send())
          .WillOnce(DoAll(FinishLockUnlock(post_acquire_request),
                          CompleteRequest(post_acquire_request, post_r)));
    }

    if (pre_r == 0 && managed_r == 0 && post_r == 0) {
      expect_notify_acquired_lock(mock_image_ctx);
      expect_unblock_writes(mock_image_ctx);
    }
  }

  void expect_release_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                           MockPreReleaseRequest &pre_release_request,
                           MockManagedReleaseRequest &managed_release_request,
                           int pre_r, int managed_r, bool shutting_down = false) {
    EXPECT_CALL(pre_release_request, send())
        .WillOnce(DoAll(FinishLockUnlock(&pre_release_request),
                        CompleteRequest(&pre_release_request, pre_r)));
    EXPECT_CALL(managed_release_request, send())
        .WillOnce(CompleteRequest(&managed_release_request, managed_r));

    if (pre_r == 0 && managed_r == 0) {
      if (shutting_down) {
        expect_unblock_writes(mock_image_ctx);
      }
      expect_notify_released_lock(mock_image_ctx);
      expect_is_lock_request_needed(mock_image_ctx, false);
    }
  }

  void expect_reacquire_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                             MockManagedReacquireRequest &mock_reacquire_request,
                             int r) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, get_watch_handle())
                  .WillOnce(Return(98765));
    EXPECT_CALL(mock_reacquire_request, send())
                  .WillOnce(CompleteRequest(&mock_reacquire_request, r));
  }

  void expect_notify_request_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                                  MockExclusiveLock &mock_exclusive_lock) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_request_lock())
                  .WillRepeatedly(Invoke(&mock_exclusive_lock,
                                         &MockExclusiveLock::handle_peer_notification));
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
      exclusive_lock.try_acquire_lock(&ctx);
    }
    return ctx.wait();
  }
  int when_request_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                        MockExclusiveLock &exclusive_lock) {
    C_SaferCond ctx;
    {
      RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
      exclusive_lock.acquire_lock(&ctx);
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

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  MockPostAcquireRequest try_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_pre_acquire,
                      try_lock_managed_acquire, &try_lock_post_acquire, 0, 0, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest pre_request_release;
  MockManagedReleaseRequest managed_request_release;
  expect_release_lock(mock_image_ctx, pre_request_release,
                      managed_request_release, 0, 0);
  ASSERT_EQ(0, when_release_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockPreAcquireRequest request_lock_pre_acquire;
  MockManagedAcquireRequest request_lock_managed_acquire;
  MockPostAcquireRequest request_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire,
                      request_lock_managed_acquire, &request_lock_post_acquire,
                      0, 0, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
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

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  MockPostAcquireRequest try_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_pre_acquire,
                      try_lock_managed_acquire, &try_lock_post_acquire, 0, 0, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, TryLockAlreadyLocked) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_pre_acquire,
                      try_lock_managed_acquire, nullptr, 0, -EAGAIN, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, TryLockBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_pre_acquire,
                      try_lock_managed_acquire, nullptr, 0, -EBUSY, 0);
  ASSERT_EQ(-EBUSY, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, TryLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_pre_acquire,
                      try_lock_managed_acquire, nullptr, 0, -EINVAL, 0);
  ASSERT_EQ(-EINVAL, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
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

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  MockPostAcquireRequest try_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_pre_acquire,
                      try_lock_managed_acquire, &try_lock_post_acquire, 0, 0, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, RequestLockBlacklist) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // will abort after seeing blacklist error (avoid infinite request loop)
  MockPreAcquireRequest request_pre_acquire;
  MockManagedAcquireRequest request_managed_acquire;
  expect_acquire_lock(mock_image_ctx, request_pre_acquire,
                      request_managed_acquire, nullptr, 0, -EBLACKLISTED, 0);
  ASSERT_EQ(-EBLACKLISTED, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
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
  MockPreAcquireRequest request_lock_pre_acquire1;
  MockManagedAcquireRequest request_lock_managed_acquire1;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire1,
                      request_lock_managed_acquire1, nullptr, 0, -EBUSY, 0);
  expect_notify_request_lock(mock_image_ctx, exclusive_lock);

  MockPreAcquireRequest request_lock_pre_acquire2;
  MockManagedAcquireRequest request_lock_managed_acquire2;
  MockPostAcquireRequest request_lock_post_acquire2;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire2,
                      request_lock_managed_acquire2,
                      &request_lock_post_acquire2, 0, 0, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
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
  MockPreAcquireRequest request_lock_pre_acquire1;
  MockManagedAcquireRequest request_lock_managed_acquire1;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire1,
                      request_lock_managed_acquire1, nullptr, 0, -EINVAL, 0);
  expect_notify_request_lock(mock_image_ctx, exclusive_lock);

  MockPreAcquireRequest request_lock_pre_acquire2;
  MockManagedAcquireRequest request_lock_managed_acquire2;
  MockPostAcquireRequest request_lock_post_acquire2;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire2,
                      request_lock_managed_acquire2,
                      &request_lock_post_acquire2, 0, 0, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, RequestLockJournalEPERM) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // will abort after seeing perm error (avoid infinite request loop)
  MockPreAcquireRequest request_lock_pre_acquire;
  MockManagedAcquireRequest request_lock_managed_acquire;
  MockPostAcquireRequest request_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire,
                      request_lock_managed_acquire,
                      &request_lock_post_acquire, 0, 0, -EPERM);
  MockManagedReleaseRequest release_on_error;
  EXPECT_CALL(release_on_error, send())
    .WillOnce(CompleteRequest(&release_on_error, 0));
  ASSERT_EQ(-EPERM, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, ReleaseLockUnlockedState) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  ASSERT_EQ(0, when_release_lock(mock_image_ctx, exclusive_lock));

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
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

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  MockPostAcquireRequest try_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_pre_acquire,
                      try_lock_managed_acquire, &try_lock_post_acquire, 0, 0, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest request_pre_release;
  MockManagedReleaseRequest request_managed_release;
  expect_release_lock(mock_image_ctx, request_pre_release,
                      request_managed_release, 0, -EINVAL, false);
  ASSERT_EQ(-EINVAL, when_release_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, ConcurrentRequests) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  MockPostAcquireRequest try_lock_post_acquire;
  C_SaferCond wait_for_send_ctx1;
  expect_get_watch_handle(mock_image_ctx);
  EXPECT_CALL(try_lock_pre_acquire, send())
                .WillOnce(CompleteRequest(&try_lock_pre_acquire, 0));
  EXPECT_CALL(try_lock_managed_acquire, send())
                .WillOnce(CompleteRequest(&try_lock_managed_acquire, 0));
  EXPECT_CALL(try_lock_post_acquire, send())
                .WillOnce(Notify(&wait_for_send_ctx1));

  MockManagedReleaseRequest managed_release_on_error;
  EXPECT_CALL(managed_release_on_error, send())
                .WillOnce(CompleteRequest(&managed_release_on_error, 0));

  MockPreAcquireRequest request_pre_acquire;
  MockManagedAcquireRequest request_managed_acquire;
  MockPostAcquireRequest request_post_acquire;
  expect_acquire_lock(mock_image_ctx, request_pre_acquire,
                      request_managed_acquire, &request_post_acquire, 0, 0, 0);

  MockPreReleaseRequest pre_release;
  MockManagedReleaseRequest managed_release;
  C_SaferCond wait_for_send_ctx2;
  EXPECT_CALL(pre_release, send()).WillOnce(Notify(&wait_for_send_ctx2));
  EXPECT_CALL(managed_release, send())
        .WillOnce(CompleteRequest(&managed_release, 0));
  expect_notify_released_lock(mock_image_ctx);
  expect_is_lock_request_needed(mock_image_ctx, false);

  C_SaferCond try_request_ctx1;
  {
    RWLock::WLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.try_acquire_lock(&try_request_ctx1);
  }

  C_SaferCond request_lock_ctx1;
  C_SaferCond request_lock_ctx2;
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.acquire_lock(&request_lock_ctx1);
    exclusive_lock.acquire_lock(&request_lock_ctx2);
  }

  C_SaferCond release_lock_ctx1;
  {
    RWLock::WLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.release_lock(&release_lock_ctx1);
  }

  C_SaferCond request_lock_ctx3;
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.acquire_lock(&request_lock_ctx3);
  }

  // fail the try_lock
  ASSERT_EQ(0, wait_for_send_ctx1.wait());
  try_lock_post_acquire.on_lock_unlock->complete(0);
  try_lock_post_acquire.on_finish->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, try_request_ctx1.wait());

  // all three pending request locks should complete
  ASSERT_EQ(0, request_lock_ctx1.wait());
  ASSERT_EQ(0, request_lock_ctx2.wait());
  ASSERT_EQ(0, request_lock_ctx3.wait());

  // proceed with the release
  ASSERT_EQ(0, wait_for_send_ctx2.wait());
  pre_release.on_lock_unlock->complete(0);
  pre_release.on_finish->complete(0);
  ASSERT_EQ(0, release_lock_ctx1.wait());

  expect_unblock_writes(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, BlockRequests) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockPreAcquireRequest try_lock_pre_acquire;
  MockManagedAcquireRequest try_lock_managed_acquire;
  MockPostAcquireRequest try_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, try_lock_pre_acquire,
                      try_lock_managed_acquire, &try_lock_post_acquire, 0, 0, 0);
  ASSERT_EQ(0, when_try_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  int ret_val;
  ASSERT_TRUE(exclusive_lock.accept_requests(&ret_val));
  ASSERT_EQ(0, ret_val);

  exclusive_lock.block_requests(-EROFS);
  ASSERT_FALSE(exclusive_lock.accept_requests(&ret_val));
  ASSERT_EQ(-EROFS, ret_val);

  exclusive_lock.unblock_requests();
  ASSERT_TRUE(exclusive_lock.accept_requests(&ret_val));
  ASSERT_EQ(0, ret_val);

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, RequestLockWatchNotRegistered) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  EXPECT_CALL(*mock_image_ctx.image_watcher, get_watch_handle())
    .WillOnce(DoAll(Invoke([&mock_image_ctx, &exclusive_lock]() {
                      mock_image_ctx.image_ctx->op_work_queue->queue(
                        new FunctionContext([&mock_image_ctx, &exclusive_lock](int r) {
                          RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
                          exclusive_lock.reacquire_lock();
                        }));
                    }),
                    Return(0)));

  MockPreAcquireRequest request_lock_pre_acquire;
  MockManagedAcquireRequest request_lock_managed_acquire;
  MockPostAcquireRequest request_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire,
                      request_lock_managed_acquire,
                      &request_lock_post_acquire, 0, 0, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, ReacquireLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockPreAcquireRequest request_lock_pre_acquire;
  MockManagedAcquireRequest request_lock_managed_acquire;
  MockPostAcquireRequest request_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire,
                      request_lock_managed_acquire,
                      &request_lock_post_acquire, 0, 0, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  EXPECT_CALL(*mock_image_ctx.image_watcher, get_watch_handle())
                 .WillOnce(Return(1234567890));

  C_SaferCond reacquire_ctx;
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.reacquire_lock(&reacquire_ctx);
  }
  ASSERT_EQ(0, reacquire_ctx.wait());

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));
}

TEST_F(TestMockExclusiveLock, ReacquireLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock exclusive_lock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_block_writes(mock_image_ctx);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  MockPreAcquireRequest request_lock_pre_acquire;
  MockManagedAcquireRequest request_lock_managed_acquire;
  MockPostAcquireRequest request_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, request_lock_pre_acquire,
                      request_lock_managed_acquire,
                      &request_lock_post_acquire, 0, 0, 0);
  ASSERT_EQ(0, when_request_lock(mock_image_ctx, exclusive_lock));
  ASSERT_TRUE(is_lock_owner(mock_image_ctx, exclusive_lock));

  MockManagedReacquireRequest mock_reacquire_request;
  C_SaferCond reacquire_ctx;
  expect_reacquire_lock(mock_image_ctx, mock_reacquire_request, -EOPNOTSUPP);

  MockPreReleaseRequest reacquire_lock_pre_release;
  MockManagedReleaseRequest reacquire_lock_managed_release;
  expect_release_lock(mock_image_ctx, reacquire_lock_pre_release,
                      reacquire_lock_managed_release, 0, 0, false);

  MockPreAcquireRequest reacquire_lock_pre_acquire;
  MockManagedAcquireRequest reacquire_lock_managed_acquire;
  MockPostAcquireRequest reacquire_lock_post_acquire;
  expect_acquire_lock(mock_image_ctx, reacquire_lock_pre_acquire,
                      reacquire_lock_managed_acquire,
                      &reacquire_lock_post_acquire, 0, 0, 0);

  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    exclusive_lock.reacquire_lock(&reacquire_ctx);
  }
  ASSERT_EQ(-EOPNOTSUPP, reacquire_ctx.wait());

  MockPreReleaseRequest shutdown_pre_release;
  MockManagedReleaseRequest shutdown_managed_release;
  expect_release_lock(mock_image_ctx, shutdown_pre_release,
                      shutdown_managed_release, 0, 0, true);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));
  ASSERT_FALSE(is_lock_owner(mock_image_ctx, exclusive_lock));
}

} // namespace librbd

