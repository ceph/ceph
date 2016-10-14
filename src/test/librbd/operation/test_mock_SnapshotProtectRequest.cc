// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/operation/SnapshotProtectRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

// template definitions
#include "librbd/operation/SnapshotProtectRequest.cc"

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockOperationSnapshotProtectRequest : public TestMockFixture {
public:
  typedef SnapshotProtectRequest<MockImageCtx> MockSnapshotProtectRequest;

  void expect_get_snap_id(MockImageCtx &mock_image_ctx, uint64_t snap_id) {
    EXPECT_CALL(mock_image_ctx, get_snap_id(_))
                  .WillOnce(Return(snap_id));
  }

  void expect_is_snap_protected(MockImageCtx &mock_image_ctx, bool is_protected,
                                int r) {
    auto &expect = EXPECT_CALL(mock_image_ctx, is_snap_protected(_, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoAll(SetArgPointee<1>(is_protected), Return(0)));
    }
  }

  void expect_set_protection_status(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("set_protection_status"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }
};

TEST_F(TestMockOperationSnapshotProtectRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_get_snap_id(mock_image_ctx, ictx->snap_info.rbegin()->first);
  expect_is_snap_protected(mock_image_ctx, false, 0);
  expect_set_protection_status(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  MockSnapshotProtectRequest *req = new MockSnapshotProtectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotProtectRequest, GetSnapIdMissing) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_get_snap_id(mock_image_ctx, CEPH_NOSNAP);

  C_SaferCond cond_ctx;
  MockSnapshotProtectRequest *req = new MockSnapshotProtectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-ENOENT, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotProtectRequest, IsSnapProtectedError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_get_snap_id(mock_image_ctx, ictx->snap_info.rbegin()->first);
  expect_is_snap_protected(mock_image_ctx, false, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotProtectRequest *req = new MockSnapshotProtectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotProtectRequest, SnapAlreadyProtected) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_get_snap_id(mock_image_ctx, ictx->snap_info.rbegin()->first);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  C_SaferCond cond_ctx;
  MockSnapshotProtectRequest *req = new MockSnapshotProtectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EBUSY, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotProtectRequest, SetProtectionStateError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_get_snap_id(mock_image_ctx, ictx->snap_info.rbegin()->first);
  expect_is_snap_protected(mock_image_ctx, false, 0);
  expect_set_protection_status(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotProtectRequest *req = new MockSnapshotProtectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
