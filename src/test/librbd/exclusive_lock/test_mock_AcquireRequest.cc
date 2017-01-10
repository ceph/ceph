// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockImageState.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockJournalPolicy.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/exclusive_lock/AcquireRequest.h"
#include "librbd/exclusive_lock/BreakRequest.h"
#include "librbd/exclusive_lock/GetLockerRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace exclusive_lock {

template<>
struct BreakRequest<librbd::MockTestImageCtx> {
  Context *on_finish = nullptr;
  static BreakRequest *s_instance;
  static BreakRequest *create(librbd::MockTestImageCtx &image_ctx,
                              const Locker &locker, bool blacklist_locker,
                              bool force_break_lock, Context *on_finish) {
    EXPECT_EQ(image_ctx.blacklist_on_break_lock, blacklist_locker);
    EXPECT_FALSE(force_break_lock);
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  BreakRequest() {
    s_instance = this;
  }
  MOCK_METHOD0(send, void());
};

template <>
struct GetLockerRequest<librbd::MockTestImageCtx> {
  Locker *locker;
  Context *on_finish;

  static GetLockerRequest *s_instance;
  static GetLockerRequest *create(librbd::MockTestImageCtx &image_ctx,
                                  Locker *locker, Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->locker = locker;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetLockerRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

BreakRequest<librbd::MockTestImageCtx> *BreakRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
GetLockerRequest<librbd::MockTestImageCtx> *GetLockerRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace exclusive_lock

namespace image {

template<>
struct RefreshRequest<librbd::MockTestImageCtx> {
  static RefreshRequest *s_instance;
  Context *on_finish;

  static RefreshRequest *create(librbd::MockTestImageCtx &image_ctx,
                                bool acquire_lock_refresh,
                                bool skip_open_parent, Context *on_finish) {
    EXPECT_TRUE(acquire_lock_refresh);
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  RefreshRequest() {
    s_instance = this;
  }
  MOCK_METHOD0(send, void());
};

RefreshRequest<librbd::MockTestImageCtx> *RefreshRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image
} // namespace librbd

// template definitions
#include "librbd/Journal.cc"
#include "librbd/exclusive_lock/AcquireRequest.cc"
template class librbd::exclusive_lock::AcquireRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace exclusive_lock {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

static const std::string TEST_COOKIE("auto 123");

class TestMockExclusiveLockAcquireRequest : public TestMockFixture {
public:
  typedef AcquireRequest<MockTestImageCtx> MockAcquireRequest;
  typedef BreakRequest<MockTestImageCtx> MockBreakRequest;
  typedef GetLockerRequest<MockTestImageCtx> MockGetLockerRequest;
  typedef ExclusiveLock<MockTestImageCtx> MockExclusiveLock;
  typedef librbd::image::RefreshRequest<MockTestImageCtx> MockRefreshRequest;

  void expect_test_features(MockTestImageCtx &mock_image_ctx, uint64_t features,
                            bool enabled) {
    EXPECT_CALL(mock_image_ctx, test_features(features))
                  .WillOnce(Return(enabled));
  }

  void expect_test_features(MockTestImageCtx &mock_image_ctx, uint64_t features,
                            RWLock &lock, bool enabled) {
    EXPECT_CALL(mock_image_ctx, test_features(features, _))
                  .WillOnce(Return(enabled));
  }

  void expect_lock(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("lock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_unlock(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("unlock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_is_refresh_required(MockTestImageCtx &mock_image_ctx, bool required) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
      .WillOnce(Return(required));
  }

  void expect_refresh(MockTestImageCtx &mock_image_ctx,
                      MockRefreshRequest &mock_refresh_request, int r) {
    EXPECT_CALL(mock_refresh_request, send())
                  .WillOnce(FinishRequest(&mock_refresh_request, r,
                                          &mock_image_ctx));
  }

  void expect_create_object_map(MockTestImageCtx &mock_image_ctx,
                                MockObjectMap *mock_object_map) {
    EXPECT_CALL(mock_image_ctx, create_object_map(_))
                  .WillOnce(Return(mock_object_map));
  }

  void expect_open_object_map(MockTestImageCtx &mock_image_ctx,
                              MockObjectMap &mock_object_map, int r) {
    EXPECT_CALL(mock_object_map, open(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_object_map(MockTestImageCtx &mock_image_ctx,
                              MockObjectMap &mock_object_map) {
    EXPECT_CALL(mock_object_map, close(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_create_journal(MockTestImageCtx &mock_image_ctx,
                             MockJournal *mock_journal) {
    EXPECT_CALL(mock_image_ctx, create_journal())
                  .WillOnce(Return(mock_journal));
  }

  void expect_open_journal(MockTestImageCtx &mock_image_ctx,
                           MockJournal &mock_journal, int r) {
    EXPECT_CALL(mock_journal, open(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_journal(MockTestImageCtx &mock_image_ctx,
                            MockJournal &mock_journal) {
    EXPECT_CALL(mock_journal, close(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_get_journal_policy(MockTestImageCtx &mock_image_ctx,
                                 MockJournalPolicy &mock_journal_policy) {
    EXPECT_CALL(mock_image_ctx, get_journal_policy())
                  .WillOnce(Return(&mock_journal_policy));
  }

  void expect_journal_disabled(MockJournalPolicy &mock_journal_policy,
                               bool disabled) {
    EXPECT_CALL(mock_journal_policy, journal_disabled())
      .WillOnce(Return(disabled));
  }

  void expect_allocate_journal_tag(MockTestImageCtx &mock_image_ctx,
                                   MockJournalPolicy &mock_journal_policy,
                                   int r) {
    EXPECT_CALL(mock_journal_policy, allocate_tag_on_lock(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_get_locker(MockTestImageCtx &mock_image_ctx,
                         MockGetLockerRequest &mock_get_locker_request,
                         const Locker &locker, int r) {
    EXPECT_CALL(mock_get_locker_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_get_locker_request, locker, r]() {
          *mock_get_locker_request.locker = locker;
          mock_image_ctx.image_ctx->op_work_queue->queue(
            mock_get_locker_request.on_finish, r);
        }));
  }

  void expect_break_lock(MockTestImageCtx &mock_image_ctx,
                         MockBreakRequest &mock_break_request, int r) {
    EXPECT_CALL(mock_break_request, send())
                  .WillOnce(FinishRequest(&mock_break_request, r,
                                          &mock_image_ctx));
  }

  void expect_flush_notifies(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, flush(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_prepare_lock(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, prepare_lock(_))
      .WillOnce(Invoke([](Context *on_ready) {
                  on_ready->complete(0);
                }));
  }

  void expect_handle_prepare_lock_complete(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, handle_prepare_lock_complete());
  }

};

TEST_F(TestMockExclusiveLockAcquireRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap mock_object_map;
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, &mock_object_map);
  expect_open_object_map(mock_image_ctx, mock_object_map, 0);

  MockJournal mock_journal;
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                       mock_image_ctx.snap_lock, true);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_journal_disabled(mock_journal_policy, false);
  expect_create_journal(mock_image_ctx, &mock_journal);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_open_journal(mock_image_ctx, mock_journal, 0);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_allocate_journal_tag(mock_image_ctx, mock_journal_policy, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, SuccessRefresh) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  MockRefreshRequest mock_refresh_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh(mock_image_ctx, mock_refresh_request, 0);

  MockObjectMap mock_object_map;
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, false);
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                       mock_image_ctx.snap_lock, false);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, SuccessJournalDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap mock_object_map;
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, &mock_object_map);
  expect_open_object_map(mock_image_ctx, mock_object_map, 0);

  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                       mock_image_ctx.snap_lock, false);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, SuccessObjectMapDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  MockJournal mock_journal;
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                       mock_image_ctx.snap_lock, true);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_journal_disabled(mock_journal_policy, false);
  expect_create_journal(mock_image_ctx, &mock_journal);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_open_journal(mock_image_ctx, mock_journal, 0);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_allocate_journal_tag(mock_image_ctx, mock_journal_policy, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, RefreshError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  MockRefreshRequest mock_refresh_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh(mock_image_ctx, mock_refresh_request, -EINVAL);
  expect_unlock(mock_image_ctx, 0);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond *acquire_ctx = new C_SaferCond();
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, RefreshLockDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  MockRefreshRequest mock_refresh_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh(mock_image_ctx, mock_refresh_request, -ERESTART);

  MockObjectMap mock_object_map;
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, false);
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                       mock_image_ctx.snap_lock, false);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, JournalError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap *mock_object_map = new MockObjectMap();
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, mock_object_map);
  expect_open_object_map(mock_image_ctx, *mock_object_map, 0);

  MockJournal *mock_journal = new MockJournal();
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                       mock_image_ctx.snap_lock, true);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_journal_disabled(mock_journal_policy, false);
  expect_create_journal(mock_image_ctx, mock_journal);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_open_journal(mock_image_ctx, *mock_journal, -EINVAL);
  expect_close_journal(mock_image_ctx, *mock_journal);
  expect_close_object_map(mock_image_ctx, *mock_object_map);
  expect_unlock(mock_image_ctx, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, AllocateJournalTagError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap *mock_object_map = new MockObjectMap();
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, mock_object_map);
  expect_open_object_map(mock_image_ctx, *mock_object_map, 0);

  MockJournal *mock_journal = new MockJournal();
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                       mock_image_ctx.snap_lock, true);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_journal_disabled(mock_journal_policy, false);
  expect_create_journal(mock_image_ctx, mock_journal);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_open_journal(mock_image_ctx, *mock_journal, 0);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_allocate_journal_tag(mock_image_ctx, mock_journal_policy, -EPERM);
  expect_close_journal(mock_image_ctx, *mock_journal);
  expect_close_object_map(mock_image_ctx, *mock_object_map);
  expect_unlock(mock_image_ctx, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, LockBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  MockBreakRequest mock_break_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_break_lock(mock_image_ctx, mock_break_request, 0);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, -ENOENT);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, GetLockInfoError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, -EINVAL);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, GetLockInfoEmpty) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, -ENOENT);
  expect_lock(mock_image_ctx, -EINVAL);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, BreakLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  MockBreakRequest mock_break_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_break_lock(mock_image_ctx, mock_break_request, -EINVAL);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, OpenObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_flush_notifies(mock_image_ctx);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap *mock_object_map = new MockObjectMap();
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, mock_object_map);
  expect_open_object_map(mock_image_ctx, *mock_object_map, -EFBIG);

  MockJournal mock_journal;
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING,
                       mock_image_ctx.snap_lock, true);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_journal_disabled(mock_journal_policy, false);
  expect_create_journal(mock_image_ctx, &mock_journal);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_open_journal(mock_image_ctx, mock_journal, 0);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_allocate_journal_tag(mock_image_ctx, mock_journal_policy, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(nullptr, mock_image_ctx.object_map);
}

} // namespace exclusive_lock
} // namespace librbd
