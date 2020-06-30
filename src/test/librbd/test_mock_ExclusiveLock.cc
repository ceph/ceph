// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/exclusive_lock/MockPolicy.h"
#include "test/librbd/mock/io/MockImageDispatch.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ManagedLock.h"
#include "librbd/exclusive_lock/ImageDispatch.h"
#include "librbd/exclusive_lock/PreAcquireRequest.h"
#include "librbd/exclusive_lock/PostAcquireRequest.h"
#include "librbd/exclusive_lock/PreReleaseRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>
#include <boost/scope_exit.hpp>

namespace librbd {

namespace {

struct MockExclusiveLockImageCtx : public MockImageCtx {
  asio::ContextWQ *op_work_queue;

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

template <>
struct ManagedLock<MockExclusiveLockImageCtx> {
  ManagedLock(librados::IoCtx& ioctx, asio::ContextWQ *work_queue,
              const std::string& oid, librbd::MockImageWatcher *watcher,
              managed_lock::Mode  mode, bool blacklist_on_break_lock,
              uint32_t blacklist_expire_seconds)
  {}

  virtual ~ManagedLock() = default;

  mutable ceph::mutex m_lock = ceph::make_mutex("ManagedLock::m_lock");

  virtual void shutdown_handler(int r, Context *) = 0;
  virtual void pre_acquire_lock_handler(Context *) = 0;
  virtual void post_acquire_lock_handler(int, Context *) = 0;
  virtual void pre_release_lock_handler(bool, Context *) = 0;
  virtual void post_release_lock_handler(bool, int, Context *) = 0;
  virtual void post_reacquire_lock_handler(int, Context *) = 0;

  MOCK_CONST_METHOD0(is_lock_owner, bool());

  MOCK_METHOD1(shut_down, void(Context*));
  MOCK_METHOD1(acquire_lock, void(Context*));

  void set_state_uninitialized() {
  }

  MOCK_METHOD0(set_state_initializing, void());
  MOCK_METHOD0(set_state_unlocked, void());
  MOCK_METHOD0(set_state_waiting_for_lock, void());
  MOCK_METHOD0(set_state_post_acquiring, void());

  MOCK_CONST_METHOD0(is_state_shutdown, bool());
  MOCK_CONST_METHOD0(is_state_acquiring, bool());
  MOCK_CONST_METHOD0(is_state_post_acquiring, bool());
  MOCK_CONST_METHOD0(is_state_releasing, bool());
  MOCK_CONST_METHOD0(is_state_pre_releasing, bool());
  MOCK_CONST_METHOD0(is_state_locked, bool());
  MOCK_CONST_METHOD0(is_state_waiting_for_lock, bool());

  MOCK_CONST_METHOD0(is_action_acquire_lock, bool());
  MOCK_METHOD0(execute_next_action, void());

};

namespace exclusive_lock {

using librbd::ImageWatcher;

template<typename T>
struct BaseRequest {
  static std::list<T *> s_requests;
  Context *on_lock_unlock = nullptr;
  Context *on_finish = nullptr;

  static T* create(MockExclusiveLockImageCtx &image_ctx,
                   Context *on_lock_unlock, Context *on_finish) {
    ceph_assert(!s_requests.empty());
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

template<>
struct ImageDispatch<MockExclusiveLockImageCtx>
  : public librbd::io::MockImageDispatch {
  static ImageDispatch* s_instance;
  static ImageDispatch* create(MockExclusiveLockImageCtx*) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  void destroy() {
  }

  ImageDispatch() {
    s_instance = this;
  }

  io::ImageDispatchLayer get_dispatch_layer() const override {
    return io::IMAGE_DISPATCH_LAYER_EXCLUSIVE_LOCK;
  }

  MOCK_METHOD2(set_require_lock, void(io::Direction, Context*));
  MOCK_METHOD1(unset_require_lock, void(io::Direction));

};

ImageDispatch<MockExclusiveLockImageCtx>* ImageDispatch<MockExclusiveLockImageCtx>::s_instance = nullptr;

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
  static PreReleaseRequest<MockExclusiveLockImageCtx> *create(
      MockExclusiveLockImageCtx &image_ctx,
      ImageDispatch<MockExclusiveLockImageCtx>* ImageDispatch,
      bool shutting_down, AsyncOpTracker &async_op_tracker,
      Context *on_finish) {
    return BaseRequest::create(image_ctx, nullptr, on_finish);
  }
  MOCK_METHOD0(send, void());
};

} // namespace exclusive_lock
} // namespace librbd

// template definitions
#include "librbd/ExclusiveLock.cc"

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
using ::testing::WithArg;

class TestMockExclusiveLock : public TestMockFixture {
public:
  typedef ManagedLock<MockExclusiveLockImageCtx> MockManagedLock;
  typedef ExclusiveLock<MockExclusiveLockImageCtx> MockExclusiveLock;
  typedef exclusive_lock::ImageDispatch<MockExclusiveLockImageCtx> MockImageDispatch;
  typedef exclusive_lock::PreAcquireRequest<MockExclusiveLockImageCtx> MockPreAcquireRequest;
  typedef exclusive_lock::PostAcquireRequest<MockExclusiveLockImageCtx> MockPostAcquireRequest;
  typedef exclusive_lock::PreReleaseRequest<MockExclusiveLockImageCtx> MockPreReleaseRequest;


  void expect_set_state_initializing(MockManagedLock *managed_lock) {
    EXPECT_CALL(*managed_lock, set_state_initializing());
  }

  void expect_set_state_unlocked(MockManagedLock *managed_lock) {
    EXPECT_CALL(*managed_lock, set_state_unlocked());
  }

  void expect_set_state_waiting_for_lock(MockManagedLock *managed_lock) {
    EXPECT_CALL(*managed_lock, set_state_waiting_for_lock());
  }

  void expect_set_state_post_acquiring(MockManagedLock *managed_lock) {
    EXPECT_CALL(*managed_lock, set_state_post_acquiring());
  }

  void expect_is_state_acquiring(MockManagedLock *managed_lock, bool ret_val) {
    EXPECT_CALL(*managed_lock, is_state_acquiring())
      .WillOnce(Return(ret_val));
  }

  void expect_is_state_waiting_for_lock(MockManagedLock *managed_lock,
                                        bool ret_val) {
    EXPECT_CALL(*managed_lock, is_state_waiting_for_lock())
      .WillOnce(Return(ret_val));
  }

  void expect_is_state_pre_releasing(MockManagedLock *managed_lock,
                                     bool ret_val) {
    EXPECT_CALL(*managed_lock, is_state_pre_releasing())
      .WillOnce(Return(ret_val));
  }

  void expect_is_state_releasing(MockManagedLock *managed_lock, bool ret_val) {
    EXPECT_CALL(*managed_lock, is_state_releasing())
      .WillOnce(Return(ret_val));
  }

  void expect_is_state_locked(MockManagedLock *managed_lock, bool ret_val) {
    EXPECT_CALL(*managed_lock, is_state_locked())
      .WillOnce(Return(ret_val));
  }

  void expect_is_state_shutdown(MockManagedLock *managed_lock, bool ret_val) {
    EXPECT_CALL(*managed_lock, is_state_shutdown())
      .WillOnce(Return(ret_val));
  }

  void expect_is_action_acquire_lock(MockManagedLock *managed_lock,
                                     bool ret_val) {
    EXPECT_CALL(*managed_lock, is_action_acquire_lock())
      .WillOnce(Return(ret_val));
  }

  void expect_set_require_lock(MockImageDispatch &mock_image_dispatch,
                               io::Direction direction) {
    EXPECT_CALL(mock_image_dispatch, set_require_lock(direction, _))
      .WillOnce(WithArg<1>(Invoke([](Context* ctx) { ctx->complete(0); })));
  }

  void expect_set_require_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                               MockImageDispatch &mock_image_dispatch) {
    if (mock_image_ctx.clone_copy_on_read ||
        (mock_image_ctx.features & RBD_FEATURE_JOURNALING) != 0) {
      expect_set_require_lock(mock_image_dispatch, io::DIRECTION_BOTH);
    } else {
      expect_set_require_lock(mock_image_dispatch, io::DIRECTION_WRITE);
    }
  }

  void expect_unset_require_lock(MockImageDispatch &mock_image_dispatch) {
    EXPECT_CALL(mock_image_dispatch, unset_require_lock(io::DIRECTION_BOTH));
  }

  void expect_block_writes(MockExclusiveLockImageCtx &mock_image_ctx,
                           MockImageDispatch& mock_image_dispatch) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, block_writes(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_unblock_writes(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, unblock_writes());
  }

  void expect_register_dispatch(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, register_dispatch(_));
  }

  void expect_shut_down_dispatch(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, shut_down_dispatch(_, _))
      .WillOnce(WithArg<1>(Invoke([](Context* ctx) {
        ctx->complete(0);
      })));
  }

  void expect_prepare_lock_complete(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, handle_prepare_lock_complete());
  }

  void expect_pre_acquire_request(MockPreAcquireRequest &pre_acquire_request,
                                  int r) {
    EXPECT_CALL(pre_acquire_request, send())
      .WillOnce(CompleteRequest(&pre_acquire_request, r));
  }

  void expect_post_acquire_request(MockExclusiveLock *mock_exclusive_lock,
                                   MockPostAcquireRequest &post_acquire_request,
                                   int r) {
    EXPECT_CALL(post_acquire_request, send())
      .WillOnce(DoAll(FinishLockUnlock(&post_acquire_request),
                      CompleteRequest(&post_acquire_request, r)));
    expect_set_state_post_acquiring(mock_exclusive_lock);
  }

  void expect_pre_release_request(MockPreReleaseRequest &pre_release_request,
                                  int r) {
    EXPECT_CALL(pre_release_request, send())
      .WillOnce(CompleteRequest(&pre_release_request, r));
  }

  void expect_notify_request_lock(MockExclusiveLockImageCtx &mock_image_ctx,
                                  MockExclusiveLock *mock_exclusive_lock) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_request_lock());
  }

  void expect_notify_acquired_lock(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_acquired_lock())
      .Times(1);
  }

  void expect_notify_released_lock(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_released_lock())
      .Times(1);
  }

  void expect_flush_notifies(MockExclusiveLockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, flush(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_shut_down(MockManagedLock *managed_lock) {
    EXPECT_CALL(*managed_lock, shut_down(_))
      .WillOnce(CompleteContext(0, static_cast<asio::ContextWQ*>(nullptr)));
  }

  void expect_accept_blocked_request(
      MockExclusiveLockImageCtx &mock_image_ctx,
      exclusive_lock::MockPolicy &policy,
      exclusive_lock::OperationRequestType request_type, bool value) {
    EXPECT_CALL(mock_image_ctx, get_exclusive_lock_policy())
      .WillOnce(Return(&policy));
    EXPECT_CALL(policy, accept_blocked_request(request_type))
      .WillOnce(Return(value));
  }

  int when_init(MockExclusiveLockImageCtx &mock_image_ctx,
                MockExclusiveLock *exclusive_lock) {
    C_SaferCond ctx;
    {
      std::unique_lock owner_locker{mock_image_ctx.owner_lock};
      exclusive_lock->init(mock_image_ctx.features, &ctx);
    }
    return ctx.wait();
  }

  int when_pre_acquire_lock_handler(MockManagedLock *managed_lock) {
    C_SaferCond ctx;
    managed_lock->pre_acquire_lock_handler(&ctx);
    return ctx.wait();
  }

  int when_post_acquire_lock_handler(MockManagedLock *managed_lock, int r) {
    C_SaferCond ctx;
    managed_lock->post_acquire_lock_handler(r, &ctx);
    return ctx.wait();
  }

  int when_pre_release_lock_handler(MockManagedLock *managed_lock,
                                    bool shutting_down) {
    C_SaferCond ctx;
    managed_lock->pre_release_lock_handler(shutting_down, &ctx);
    return ctx.wait();
  }

  int when_post_release_lock_handler(MockManagedLock *managed_lock,
                                     bool shutting_down, int r) {
    C_SaferCond ctx;
    managed_lock->post_release_lock_handler(shutting_down, r, &ctx);
    return ctx.wait();
  }

  int when_post_reacquire_lock_handler(MockManagedLock *managed_lock, int r) {
    C_SaferCond ctx;
    managed_lock->post_reacquire_lock_handler(r, &ctx);
    return ctx.wait();
  }

  int when_shut_down(MockExclusiveLockImageCtx &mock_image_ctx,
                     MockExclusiveLock *exclusive_lock) {
    C_SaferCond ctx;
    {
      std::unique_lock owner_locker{mock_image_ctx.owner_lock};
      exclusive_lock->shut_down(&ctx);
    }
    return ctx.wait();
  }

  bool is_lock_owner(MockExclusiveLockImageCtx &mock_image_ctx,
                     MockExclusiveLock *exclusive_lock) {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    return exclusive_lock->is_lock_owner();
  }
};

TEST_F(TestMockExclusiveLock, StateTransitions) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // (try) acquire lock
  MockPreAcquireRequest try_lock_pre_acquire;
  expect_pre_acquire_request(try_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  MockPostAcquireRequest try_lock_post_acquire;
  expect_post_acquire_request(exclusive_lock, try_lock_post_acquire, 0);
  expect_is_state_acquiring(exclusive_lock, true);
  expect_notify_acquired_lock(mock_image_ctx);
  expect_unset_require_lock(mock_image_dispatch);
  ASSERT_EQ(0, when_post_acquire_lock_handler(exclusive_lock, 0));

  // release lock
  MockPreReleaseRequest pre_request_release;
  expect_pre_release_request(pre_request_release, 0);
  ASSERT_EQ(0, when_pre_release_lock_handler(exclusive_lock, false));

  expect_is_state_pre_releasing(exclusive_lock, false);
  expect_is_state_releasing(exclusive_lock, true);
  expect_notify_released_lock(mock_image_ctx);
  ASSERT_EQ(0, when_post_release_lock_handler(exclusive_lock, false, 0));

  // (try) acquire lock
  MockPreAcquireRequest request_lock_pre_acquire;
  expect_pre_acquire_request(request_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  MockPostAcquireRequest request_lock_post_acquire;
  expect_post_acquire_request(exclusive_lock, request_lock_post_acquire, 0);
  expect_is_state_acquiring(exclusive_lock, true);
  expect_notify_acquired_lock(mock_image_ctx);
  expect_unset_require_lock(mock_image_dispatch);
  ASSERT_EQ(0, when_post_acquire_lock_handler(exclusive_lock, 0));

  // shut down (and release)
  expect_shut_down(exclusive_lock);
  expect_is_state_waiting_for_lock(exclusive_lock, false);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  expect_pre_release_request(shutdown_pre_release, 0);
  ASSERT_EQ(0, when_pre_release_lock_handler(exclusive_lock, true));

  expect_shut_down_dispatch(mock_image_ctx);
  expect_notify_released_lock(mock_image_ctx);
  ASSERT_EQ(0, when_post_release_lock_handler(exclusive_lock, true, 0));
}

TEST_F(TestMockExclusiveLock, TryLockAlreadyLocked) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // try acquire lock
  MockPreAcquireRequest try_lock_pre_acquire;
  expect_pre_acquire_request(try_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  expect_is_state_acquiring(exclusive_lock, true);
  expect_prepare_lock_complete(mock_image_ctx);
  expect_is_action_acquire_lock(exclusive_lock, false);
  ASSERT_EQ(0, when_post_acquire_lock_handler(exclusive_lock, -EAGAIN));
}

TEST_F(TestMockExclusiveLock, TryLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // try acquire lock
  MockPreAcquireRequest try_lock_pre_acquire;
  expect_pre_acquire_request(try_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  expect_is_state_acquiring(exclusive_lock, true);
  expect_prepare_lock_complete(mock_image_ctx);
  expect_is_action_acquire_lock(exclusive_lock, false);
  ASSERT_EQ(-EBUSY, when_post_acquire_lock_handler(exclusive_lock, -EBUSY));
}

TEST_F(TestMockExclusiveLock, AcquireLockAlreadyLocked) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // acquire lock
  MockPreAcquireRequest try_lock_pre_acquire;
  expect_pre_acquire_request(try_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  expect_is_state_acquiring(exclusive_lock, true);
  expect_prepare_lock_complete(mock_image_ctx);
  expect_is_action_acquire_lock(exclusive_lock, true);
  expect_set_state_waiting_for_lock(exclusive_lock);
  expect_notify_request_lock(mock_image_ctx, exclusive_lock);
  ASSERT_EQ(-ECANCELED, when_post_acquire_lock_handler(exclusive_lock,
                                                       -EAGAIN));
}

TEST_F(TestMockExclusiveLock, AcquireLockBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // acquire lock
  MockPreAcquireRequest try_lock_pre_acquire;
  expect_pre_acquire_request(try_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  expect_is_state_acquiring(exclusive_lock, true);
  expect_prepare_lock_complete(mock_image_ctx);
  expect_is_action_acquire_lock(exclusive_lock, true);
  expect_set_state_waiting_for_lock(exclusive_lock);
  expect_notify_request_lock(mock_image_ctx, exclusive_lock);
  ASSERT_EQ(-ECANCELED, when_post_acquire_lock_handler(exclusive_lock,
                                                       -EBUSY));
}

TEST_F(TestMockExclusiveLock, AcquireLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // acquire lock
  MockPreAcquireRequest try_lock_pre_acquire;
  expect_pre_acquire_request(try_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  expect_is_state_acquiring(exclusive_lock, true);
  expect_prepare_lock_complete(mock_image_ctx);
  expect_is_action_acquire_lock(exclusive_lock, true);
  ASSERT_EQ(-EBLACKLISTED, when_post_acquire_lock_handler(exclusive_lock,
                                                          -EBLACKLISTED));
}

TEST_F(TestMockExclusiveLock, PostAcquireLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // (try) acquire lock
  MockPreAcquireRequest request_lock_pre_acquire;
  expect_pre_acquire_request(request_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  MockPostAcquireRequest request_lock_post_acquire;
  expect_post_acquire_request(exclusive_lock, request_lock_post_acquire,
                              -EPERM);
  expect_is_state_acquiring(exclusive_lock, true);
  ASSERT_EQ(-EPERM, when_post_acquire_lock_handler(exclusive_lock, 0));
}

TEST_F(TestMockExclusiveLock, PreReleaseLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // release lock
  MockPreReleaseRequest pre_request_release;
  expect_pre_release_request(pre_request_release, -EINVAL);
  ASSERT_EQ(-EINVAL, when_pre_release_lock_handler(exclusive_lock, false));

  expect_is_state_pre_releasing(exclusive_lock, true);
  ASSERT_EQ(-EINVAL, when_post_release_lock_handler(exclusive_lock, false,
                                                    -EINVAL));
}

TEST_F(TestMockExclusiveLock, ReacquireLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  // (try) acquire lock
  MockPreAcquireRequest try_lock_pre_acquire;
  expect_pre_acquire_request(try_lock_pre_acquire, 0);
  ASSERT_EQ(0, when_pre_acquire_lock_handler(exclusive_lock));

  MockPostAcquireRequest try_lock_post_acquire;
  expect_post_acquire_request(exclusive_lock, try_lock_post_acquire, 0);
  expect_is_state_acquiring(exclusive_lock, true);
  expect_notify_acquired_lock(mock_image_ctx);
  expect_unset_require_lock(mock_image_dispatch);
  ASSERT_EQ(0, when_post_acquire_lock_handler(exclusive_lock, 0));

  // reacquire lock
  expect_notify_acquired_lock(mock_image_ctx);
  ASSERT_EQ(0, when_post_reacquire_lock_handler(exclusive_lock, 0));

  // shut down (and release)
  expect_shut_down(exclusive_lock);
  expect_is_state_waiting_for_lock(exclusive_lock, false);
  ASSERT_EQ(0, when_shut_down(mock_image_ctx, exclusive_lock));

  MockPreReleaseRequest shutdown_pre_release;
  expect_pre_release_request(shutdown_pre_release, 0);
  ASSERT_EQ(0, when_pre_release_lock_handler(exclusive_lock, true));

  expect_shut_down_dispatch(mock_image_ctx);
  expect_notify_released_lock(mock_image_ctx);
  ASSERT_EQ(0, when_post_release_lock_handler(exclusive_lock, true, 0));
}

TEST_F(TestMockExclusiveLock, BlockRequests) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockExclusiveLockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock *exclusive_lock = new MockExclusiveLock(mock_image_ctx);
  exclusive_lock::MockPolicy mock_exclusive_lock_policy;

  expect_op_work_queue(mock_image_ctx);

  BOOST_SCOPE_EXIT(&exclusive_lock) {
    exclusive_lock->put();
  } BOOST_SCOPE_EXIT_END

  InSequence seq;
  expect_set_state_initializing(exclusive_lock);
  MockImageDispatch mock_image_dispatch;
  expect_block_writes(mock_image_ctx, mock_image_dispatch);
  expect_register_dispatch(mock_image_ctx);
  expect_set_require_lock(mock_image_ctx, mock_image_dispatch);
  expect_unblock_writes(mock_image_ctx);
  expect_set_state_unlocked(exclusive_lock);
  ASSERT_EQ(0, when_init(mock_image_ctx, exclusive_lock));

  int ret_val;
  expect_is_state_shutdown(exclusive_lock, false);
  expect_is_state_locked(exclusive_lock, true);
  ASSERT_TRUE(exclusive_lock->accept_request(
                exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL, &ret_val));
  ASSERT_EQ(0, ret_val);

  exclusive_lock->block_requests(-EROFS);
  expect_is_state_shutdown(exclusive_lock, false);
  expect_is_state_locked(exclusive_lock, true);
  expect_accept_blocked_request(mock_image_ctx, mock_exclusive_lock_policy,
                                exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                                false);
  ASSERT_FALSE(exclusive_lock->accept_request(
                 exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL, &ret_val));
  ASSERT_EQ(-EROFS, ret_val);

  expect_is_state_shutdown(exclusive_lock, false);
  expect_is_state_locked(exclusive_lock, true);
  expect_accept_blocked_request(
    mock_image_ctx, mock_exclusive_lock_policy,
    exclusive_lock::OPERATION_REQUEST_TYPE_TRASH_SNAP_REMOVE, true);
  ASSERT_TRUE(exclusive_lock->accept_request(
                exclusive_lock::OPERATION_REQUEST_TYPE_TRASH_SNAP_REMOVE,
                &ret_val));
  ASSERT_EQ(0, ret_val);

  exclusive_lock->unblock_requests();
  expect_is_state_shutdown(exclusive_lock, false);
  expect_is_state_locked(exclusive_lock, true);
  ASSERT_TRUE(exclusive_lock->accept_request(
                exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL, &ret_val));
  ASSERT_EQ(0, ret_val);
}

} // namespace librbd

