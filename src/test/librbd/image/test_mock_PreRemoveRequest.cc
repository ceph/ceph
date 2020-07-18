// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/image/ListWatchersRequest.h"
#include "librbd/image/PreRemoveRequest.h"
#include "librbd/image/RefreshParentRequest.h"
#include "librbd/operation/SnapshotRemoveRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>
#include <boost/scope_exit.hpp>

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace operation {

template <>
class SnapshotRemoveRequest<MockTestImageCtx> {
public:
  static SnapshotRemoveRequest *s_instance;
  static SnapshotRemoveRequest *create(MockTestImageCtx &image_ctx,
                                       cls::rbd::SnapshotNamespace sn,
                                       std::string name,
                                       uint64_t id, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  SnapshotRemoveRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

SnapshotRemoveRequest<MockTestImageCtx> *SnapshotRemoveRequest<MockTestImageCtx>::s_instance;

} // namespace operation

namespace image {

template<>
class ListWatchersRequest<MockTestImageCtx> {
public:
  static ListWatchersRequest *s_instance;
  Context *on_finish = nullptr;

  static ListWatchersRequest *create(MockTestImageCtx &image_ctx, int flags,
                                     std::list<obj_watch_t> *watchers,
                                     Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  ListWatchersRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

ListWatchersRequest<MockTestImageCtx> *ListWatchersRequest<MockTestImageCtx>::s_instance;

} // namespace image
} // namespace librbd

// template definitions
#include "librbd/image/PreRemoveRequest.cc"

ACTION_P(TestFeatures, image_ctx) {
  return ((image_ctx->features & arg0) != 0);
}

ACTION_P(ShutDownExclusiveLock, image_ctx) {
  // shutting down exclusive lock will close object map and journal
  image_ctx->exclusive_lock = nullptr;
  image_ctx->object_map = nullptr;
  image_ctx->journal = nullptr;
}

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

class TestMockImagePreRemoveRequest : public TestMockFixture {
public:
  typedef PreRemoveRequest<MockTestImageCtx> MockPreRemoveRequest;
  typedef ListWatchersRequest<MockTestImageCtx> MockListWatchersRequest;
  typedef librbd::operation::SnapshotRemoveRequest<MockTestImageCtx> MockSnapshotRemoveRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_test_imctx));
    m_mock_imctx = new MockTestImageCtx(*m_test_imctx);
  }

  void TearDown() override {
    delete m_mock_imctx;
    TestMockFixture::TearDown();
  }

  void expect_test_features(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, test_features(_))
      .WillRepeatedly(TestFeatures(&mock_image_ctx));
  }

  void expect_set_journal_policy(MockTestImageCtx &mock_image_ctx) {
    if (m_test_imctx->test_features(RBD_FEATURE_JOURNALING)) {
      EXPECT_CALL(mock_image_ctx, set_journal_policy(_))
        .WillOnce(Invoke([](journal::Policy* policy) {
                    ASSERT_TRUE(policy->journal_disabled());
                    delete policy;
                  }));
    }
  }

  void expect_acquire_exclusive_lock(MockTestImageCtx &mock_image_ctx,
                                     MockExclusiveLock &mock_exclusive_lock,
                                     int r) {
    if (m_mock_imctx->exclusive_lock != nullptr) {
      EXPECT_CALL(mock_exclusive_lock, acquire_lock(_))
        .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
    }
  }

  void expect_shut_down_exclusive_lock(MockTestImageCtx &mock_image_ctx,
                                       MockExclusiveLock &mock_exclusive_lock,
                                       int r) {
    if (m_mock_imctx->exclusive_lock != nullptr) {
      EXPECT_CALL(mock_exclusive_lock, shut_down(_))
        .WillOnce(DoAll(ShutDownExclusiveLock(&mock_image_ctx),
                        CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_is_exclusive_lock_owner(MockTestImageCtx &mock_image_ctx,
                                      MockExclusiveLock &mock_exclusive_lock,
                                      bool is_owner) {
    if (m_mock_imctx->exclusive_lock != nullptr) {
      EXPECT_CALL(mock_exclusive_lock, is_lock_owner()).WillOnce(Return(is_owner));
    }
  }

  void expect_list_image_watchers(
    MockTestImageCtx &mock_image_ctx,
    MockListWatchersRequest &mock_list_watchers_request, int r) {
    EXPECT_CALL(mock_list_watchers_request, send())
      .WillOnce(FinishRequest(&mock_list_watchers_request, r, &mock_image_ctx));
  }

  void expect_get_group(MockTestImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.old_format) {
      return;
    }

    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("image_group_get"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_remove_snap(MockTestImageCtx &mock_image_ctx,
                          MockSnapshotRemoveRequest& mock_snap_remove_request,
                          int r) {
    EXPECT_CALL(mock_snap_remove_request, send())
      .WillOnce(FinishRequest(&mock_snap_remove_request, r, &mock_image_ctx));
  }

  librbd::ImageCtx *m_test_imctx = nullptr;
  MockTestImageCtx *m_mock_imctx = nullptr;
};

TEST_F(TestMockImagePreRemoveRequest, Success) {
  MockExclusiveLock mock_exclusive_lock;
  if (m_test_imctx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    m_mock_imctx->exclusive_lock = &mock_exclusive_lock;
  }

  expect_op_work_queue(*m_mock_imctx);
  expect_test_features(*m_mock_imctx);

  InSequence seq;
  expect_set_journal_policy(*m_mock_imctx);
  expect_acquire_exclusive_lock(*m_mock_imctx, mock_exclusive_lock, 0);
  expect_is_exclusive_lock_owner(*m_mock_imctx, mock_exclusive_lock, true);

  MockListWatchersRequest mock_list_watchers_request;
  expect_list_image_watchers(*m_mock_imctx, mock_list_watchers_request, 0);

  expect_get_group(*m_mock_imctx, 0);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, OperationsDisabled) {
  REQUIRE_FORMAT_V2();

  m_mock_imctx->operations_disabled = true;

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(-EROFS, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, ExclusiveLockTryAcquireFailed) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  MockExclusiveLock mock_exclusive_lock;
  m_mock_imctx->exclusive_lock = &mock_exclusive_lock;

  expect_op_work_queue(*m_mock_imctx);
  expect_test_features(*m_mock_imctx);

  InSequence seq;
  expect_set_journal_policy(*m_mock_imctx);
  expect_acquire_exclusive_lock(*m_mock_imctx, mock_exclusive_lock,
                                    -EINVAL);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, ExclusiveLockTryAcquireNotLockOwner) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  MockExclusiveLock mock_exclusive_lock;
  m_mock_imctx->exclusive_lock = &mock_exclusive_lock;

  expect_op_work_queue(*m_mock_imctx);
  expect_test_features(*m_mock_imctx);

  InSequence seq;
  expect_set_journal_policy(*m_mock_imctx);
  expect_acquire_exclusive_lock(*m_mock_imctx, mock_exclusive_lock, 0);
  expect_is_exclusive_lock_owner(*m_mock_imctx, mock_exclusive_lock, false);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, Force) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  MockExclusiveLock mock_exclusive_lock;
  m_mock_imctx->exclusive_lock = &mock_exclusive_lock;

  expect_op_work_queue(*m_mock_imctx);
  expect_test_features(*m_mock_imctx);

  InSequence seq;
  expect_set_journal_policy(*m_mock_imctx);
  expect_acquire_exclusive_lock(*m_mock_imctx, mock_exclusive_lock,
                                    -EINVAL);
  expect_shut_down_exclusive_lock(*m_mock_imctx, mock_exclusive_lock, 0);

  MockListWatchersRequest mock_list_watchers_request;
  expect_list_image_watchers(*m_mock_imctx, mock_list_watchers_request, 0);

  expect_get_group(*m_mock_imctx, 0);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, true, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, ExclusiveLockShutDownFailed) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  MockExclusiveLock mock_exclusive_lock;
  m_mock_imctx->exclusive_lock = &mock_exclusive_lock;

  expect_op_work_queue(*m_mock_imctx);
  expect_test_features(*m_mock_imctx);

  InSequence seq;
  expect_set_journal_policy(*m_mock_imctx);
  expect_acquire_exclusive_lock(*m_mock_imctx, mock_exclusive_lock, -EINVAL);
  expect_shut_down_exclusive_lock(*m_mock_imctx, mock_exclusive_lock, -EINVAL);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, true, &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, Migration) {
  m_mock_imctx->features |= RBD_FEATURE_MIGRATING;

  expect_test_features(*m_mock_imctx);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, Snapshots) {
  m_mock_imctx->snap_info = {
    {123, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}}};

  expect_test_features(*m_mock_imctx);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(-ENOTEMPTY, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, Watchers) {
  MockExclusiveLock mock_exclusive_lock;
  if (m_test_imctx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    m_mock_imctx->exclusive_lock = &mock_exclusive_lock;
  }

  expect_op_work_queue(*m_mock_imctx);
  expect_test_features(*m_mock_imctx);

  InSequence seq;
  expect_set_journal_policy(*m_mock_imctx);
  expect_acquire_exclusive_lock(*m_mock_imctx, mock_exclusive_lock, 0);
  expect_is_exclusive_lock_owner(*m_mock_imctx, mock_exclusive_lock, true);

  MockListWatchersRequest mock_list_watchers_request;
  expect_list_image_watchers(*m_mock_imctx, mock_list_watchers_request,
                             -EINVAL);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, GroupError) {
  REQUIRE_FORMAT_V2();

  MockExclusiveLock mock_exclusive_lock;
  if (m_test_imctx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    m_mock_imctx->exclusive_lock = &mock_exclusive_lock;
  }

  expect_op_work_queue(*m_mock_imctx);
  expect_test_features(*m_mock_imctx);

  InSequence seq;
  expect_set_journal_policy(*m_mock_imctx);
  expect_acquire_exclusive_lock(*m_mock_imctx, mock_exclusive_lock, 0);
  expect_is_exclusive_lock_owner(*m_mock_imctx, mock_exclusive_lock, true);

  MockListWatchersRequest mock_list_watchers_request;
  expect_list_image_watchers(*m_mock_imctx, mock_list_watchers_request, 0);

  expect_get_group(*m_mock_imctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImagePreRemoveRequest, AutoDeleteSnapshots) {
  REQUIRE_FORMAT_V2();

  MockExclusiveLock mock_exclusive_lock;
  if (m_test_imctx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    m_mock_imctx->exclusive_lock = &mock_exclusive_lock;
  }

  expect_op_work_queue(*m_mock_imctx);
  expect_test_features(*m_mock_imctx);

  m_mock_imctx->snap_info = {
    {123, {"snap1", {cls::rbd::TrashSnapshotNamespace{}}, {}, {}, {}, {}, {}}}};

  InSequence seq;
  expect_set_journal_policy(*m_mock_imctx);
  expect_acquire_exclusive_lock(*m_mock_imctx, mock_exclusive_lock, 0);
  expect_is_exclusive_lock_owner(*m_mock_imctx, mock_exclusive_lock, true);

  MockListWatchersRequest mock_list_watchers_request;
  expect_list_image_watchers(*m_mock_imctx, mock_list_watchers_request, 0);

  expect_get_group(*m_mock_imctx, 0);

  MockSnapshotRemoveRequest mock_snap_remove_request;
  expect_remove_snap(*m_mock_imctx, mock_snap_remove_request, 0);

  C_SaferCond ctx;
  auto req = MockPreRemoveRequest::create(m_mock_imctx, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

} // namespace image
} // namespace librbd
