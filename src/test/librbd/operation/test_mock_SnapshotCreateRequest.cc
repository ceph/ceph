// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/internal.h"
#include "librbd/operation/SnapshotCreateRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

// template definitions
#include "librbd/operation/SnapshotCreateRequest.cc"

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::WithArg;

class TestMockOperationSnapshotCreateRequest : public TestMockFixture {
public:
  typedef SnapshotCreateRequest<MockImageCtx> MockSnapshotCreateRequest;

  void expect_block_writes(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, block_writes(_))
                  .WillRepeatedly(CompleteContext(0, NULL));
  }

  void expect_verify_lock_ownership(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, is_lock_supported(_))
                  .WillRepeatedly(Return(true));
    EXPECT_CALL(*mock_image_ctx.image_watcher, is_lock_owner())
                  .WillRepeatedly(Return(true));
  }

  void expect_allocate_snap_id(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               selfmanaged_snap_create(_));
    if (r < 0 && r != -ESTALE) {
      expect.WillOnce(Return(r));
    } else {
      expect.Times(r < 0 ? 2 : 1).WillRepeatedly(DoDefault());
    }
  }

  void expect_release_snap_id(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               selfmanaged_snap_remove(_));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_snap_create(MockImageCtx &mock_image_ctx, int r) {
    if (!mock_image_ctx.old_format) {
      EXPECT_CALL(*mock_image_ctx.image_watcher, assert_header_locked(_))
                    .Times(r == -ESTALE ? 2 : 1);
    }

    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, "rbd",
                               mock_image_ctx.old_format ? "snap_add" :
                                                           "snapshot_add",
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
    bool enabled = mock_image_ctx.image_ctx->test_features(RBD_FEATURE_OBJECT_MAP);
    EXPECT_CALL(mock_image_ctx.object_map, enabled(_))
                  .WillOnce(Return(enabled));
    if (enabled) {
      EXPECT_CALL(mock_image_ctx.object_map, snapshot_add(_, _))
                    .WillOnce(WithArg<1>(CompleteContext(
                      0, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_update_snap_context(MockImageCtx &mock_image_ctx) {
    // state machine checks to ensure a refresh hasn't already added the snap
    EXPECT_CALL(mock_image_ctx, get_snap_info(_))
                  .WillOnce(Return(reinterpret_cast<const librbd::SnapInfo*>(NULL)));
    EXPECT_CALL(mock_image_ctx, add_snap("snap1", _, _, _, _, _));
  }

  void expect_unblock_writes(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, unblock_writes())
                  .Times(1);
  }

};

TEST_F(TestMockOperationSnapshotCreateRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, 0);
  expect_snap_create(mock_image_ctx, 0);
  expect_update_snap_context(mock_image_ctx);
  expect_object_map_snap_create(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, AllocateSnapIdError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, CreateSnapStale) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, -ESTALE);
  expect_snap_create(mock_image_ctx, -ESTALE);
  expect_update_snap_context(mock_image_ctx);
  expect_object_map_snap_create(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, CreateSnapError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, 0);
  expect_snap_create(mock_image_ctx, -EINVAL);
  expect_release_snap_id(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotCreateRequest, ReleaseSnapIdError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_verify_lock_ownership(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  expect_block_writes(mock_image_ctx);
  expect_allocate_snap_id(mock_image_ctx, 0);
  expect_snap_create(mock_image_ctx, -EINVAL);
  expect_release_snap_id(mock_image_ctx, -ESTALE);
  expect_unblock_writes(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotCreateRequest *req = new MockSnapshotCreateRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
