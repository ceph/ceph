// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/mirror/snapshot/SetImageStateRequest.h"
#include "librbd/operation/SnapshotCreateRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace mirror {
namespace snapshot {

template<>
class SetImageStateRequest<MockImageCtx> {
public:
  static SetImageStateRequest *s_instance;
  Context *on_finish = nullptr;

  static SetImageStateRequest *create(MockImageCtx *image_ctx, uint64_t snap_id,
                                      Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SetImageStateRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

SetImageStateRequest<MockImageCtx> *SetImageStateRequest<MockImageCtx>::s_instance;

} // namespace snapshot
} // namespace mirror
} // namespace librbd

// template definitions
#include "librbd/operation/SnapshotCreateRequest.cc"

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockOperationSnapshotCreateRequest : public TestMockFixture {
public:
  typedef SnapshotCreateRequest<MockImageCtx> MockSnapshotCreateRequest;
  typedef mirror::snapshot::SetImageStateRequest<MockImageCtx> MockSetImageStateRequest;

  void expect_notify_quiesce(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_quiesce(_, _))
      .WillOnce(DoAll(WithArg<1>(CompleteContext(
                                   r, mock_image_ctx.image_ctx->op_work_queue)),
                      Return(0)));
  }

  void expect_block_writes(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, block_writes(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_verify_lock_ownership(MockImageCtx &mock_image_ctx) {
    if (mock_image_ctx.exclusive_lock != nullptr) {
      EXPECT_CALL(*mock_image_ctx.exclusive_lock, is_lock_owner())
                    .WillRepeatedly(Return(true));
    }
  }

  void expect_allocate_snap_id(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               selfmanaged_snap_create(_));
    if (r < 0 && r != -ESTALE) {
      expect.WillOnce(Return(r));
    } else {
      expect.Times(r < 0 ? 2 : 1).WillRepeatedly(DoDefault());
    }
  }

  void expect_release_snap_id(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               selfmanaged_snap_remove(_));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_snap_create(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                               StrEq(mock_image_ctx.old_format ? "snap_add" :
                                                                 "snapshot_add"),
                               _, _, _));
    if (r == -ESTALE) {
      expect.WillOnce(Return(r)).WillOnce(DoDefault());
    } else if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_object_map_snap_create(MockImageCtx &mock_image_ctx) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(*mock_image_ctx.object_map, snapshot_add(_, _))
                    .WillOnce(WithArg<1>(CompleteContext(
                      0, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_set_image_state(
      MockImageCtx &mock_image_ctx,
      MockSetImageStateRequest &mock_set_image_state_request, int r) {
    EXPECT_CALL(mock_set_image_state_request, send())
      .WillOnce(FinishRequest(&mock_set_image_state_request, r,
                              &mock_image_ctx));
  }

  void expect_update_snap_context(MockImageCtx &mock_image_ctx) {
    // state machine checks to ensure a refresh hasn't already added the snap
    EXPECT_CALL(mock_image_ctx, get_snap_info(_))
                  .WillOnce(Return(static_cast<const librbd::SnapInfo*>(NULL)));
    EXPECT_CALL(mock_image_ctx, add_snap(_, "snap1", _, _, _, _, _, _));
  }

  void expect_unblock_writes(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, unblock_writes())
                  .Times(1);
  }

  void expect_notify_unquiesce(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, notify_unquiesce(_, _))
      .WillOnce(WithArg<1>(
                  CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }
};

TEST_F(TestMockOperationSnapshotCreateRequest, Success) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_notify_quiesce(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, 0);
  expect_snap_create(mock_image_ctx, 0);
  if (!mock_image_ctx.old_format) {
    expect_object_map_snap_create(mock_image_ctx);
    expect_update_snap_context(mock_image_ctx);
  }
  expect_unblock_writes(mock_image_ctx);
  expect_notify_unquiesce(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext prog_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(),
    "snap1", 0, 0, prog_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, NotifyQuiesceError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_notify_quiesce(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext prog_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(),
    "snap1", 0, 0, prog_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, AllocateSnapIdError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_notify_quiesce(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  expect_notify_unquiesce(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext prog_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(),
    "snap1", 0, 0, prog_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, CreateSnapStale) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  expect_notify_quiesce(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, -ESTALE);
  expect_snap_create(mock_image_ctx, -ESTALE);
  if (!mock_image_ctx.old_format) {
    expect_object_map_snap_create(mock_image_ctx);
    expect_update_snap_context(mock_image_ctx);
  }
  expect_unblock_writes(mock_image_ctx);
  expect_notify_unquiesce(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext prog_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(),
    "snap1", 0, 0, prog_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, CreateSnapError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  expect_notify_quiesce(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, 0);
  expect_snap_create(mock_image_ctx, -EINVAL);
  expect_release_snap_id(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);
  expect_notify_unquiesce(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext prog_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(),
    "snap1", 0, 0, prog_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, ReleaseSnapIdError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  expect_notify_quiesce(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, 0);
  expect_snap_create(mock_image_ctx, -EINVAL);
  expect_release_snap_id(mock_image_ctx, -ESTALE);
  expect_unblock_writes(mock_image_ctx);
  expect_notify_unquiesce(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext prog_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(),
    "snap1", 0, 0, prog_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, SkipObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_notify_quiesce(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, 0);
  expect_snap_create(mock_image_ctx, 0);
  expect_update_snap_context(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_notify_unquiesce(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext prog_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(),
    "snap1", 0, SNAP_CREATE_FLAG_SKIP_OBJECT_MAP, prog_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, SetImageState) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_notify_quiesce(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, 0);
  expect_snap_create(mock_image_ctx, 0);
  expect_object_map_snap_create(mock_image_ctx);
  MockSetImageStateRequest mock_set_image_state_request;
  expect_set_image_state(mock_image_ctx, mock_set_image_state_request, 0);
  expect_update_snap_context(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_notify_unquiesce(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext prog_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx,
    cls::rbd::MirrorSnapshotNamespace{
      cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {}, "", CEPH_NOSNAP},
    "snap1", 0, 0, prog_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
