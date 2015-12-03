// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "include/rados/librados.hpp"
#include "common/bit_vector.hpp"
#include "librbd/internal.h"
#include "librbd/operation/SnapshotUnprotectRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

// template definitions
#include "librbd/operation/SnapshotUnprotectRequest.cc"

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::SetArgReferee;
using ::testing::SetArgPointee;
using ::testing::WithArg;

class TestMockOperationSnapshotUnprotectRequest : public TestMockFixture {
public:
  typedef SnapshotUnprotectRequest<MockImageCtx> MockSnapshotUnprotectRequest;

  void expect_get_snap_id(MockImageCtx &mock_image_ctx, uint64_t snap_id) {
    EXPECT_CALL(mock_image_ctx, get_snap_id(_))
                  .WillOnce(Return(snap_id));
  }

  void expect_is_snap_unprotected(MockImageCtx &mock_image_ctx,
                                  bool is_unprotected, int r) {
    auto &expect = EXPECT_CALL(mock_image_ctx, is_snap_unprotected(_, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoAll(SetArgPointee<1>(is_unprotected), Return(0)));
    }
  }

  void expect_set_protection_status(MockImageCtx &mock_image_ctx,
                                    uint64_t snap_id, uint8_t status, int r) {
    bufferlist bl;
    ::encode(snap_id, bl);
    ::encode(status, bl);

    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, "rbd",
                                    "set_protection_status", ContentsEqual(bl),
                                    _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  size_t expect_create_pool_io_contexts(MockImageCtx &mock_image_ctx) {
    librados::MockTestMemIoCtxImpl &io_ctx_impl =
      get_mock_io_ctx(mock_image_ctx.md_ctx);
    librados::MockTestMemRadosClient *rados_client =
      io_ctx_impl.get_mock_rados_client();

    std::list<std::pair<int64_t, std::string> > pools;
    int r = rados_client->pool_list(pools);
    if (r < 0) {
      ADD_FAILURE() << "failed to list pools";
      return 0;
    }

    EXPECT_CALL(*rados_client, create_ioctx(_, _))
                  .Times(pools.size()).WillRepeatedly(DoAll(
                    GetReference(&io_ctx_impl), Return(&io_ctx_impl)));
    return pools.size();
  }

  void expect_get_children(MockImageCtx &mock_image_ctx, size_t pools, int r) {
    bufferlist bl;
    std::set<std::string> children;
    ::encode(children, bl);

    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(RBD_CHILDREN, _, "rbd", "get_children", _,
                               _, _));
    if (r < 0) {
      expect.WillRepeatedly(Return(r));
    } else {
      expect.Times(pools).WillRepeatedly(DoAll(
        SetArgPointee<5>(bl), Return(0)));
    }
  }
};

TEST_F(TestMockOperationSnapshotUnprotectRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_get_snap_id(mock_image_ctx, snap_id);
  expect_is_snap_unprotected(mock_image_ctx, false, 0);
  expect_set_protection_status(mock_image_ctx, snap_id,
                               RBD_PROTECTION_STATUS_UNPROTECTING, 0);
  size_t pools = expect_create_pool_io_contexts(mock_image_ctx);
  expect_get_children(mock_image_ctx, pools, -ENOENT);
  expect_set_protection_status(mock_image_ctx, snap_id,
                               RBD_PROTECTION_STATUS_UNPROTECTED, 0);

  C_SaferCond cond_ctx;
  MockSnapshotUnprotectRequest *req = new MockSnapshotUnprotectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotUnprotectRequest, GetSnapIdMissing) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_get_snap_id(mock_image_ctx, CEPH_NOSNAP);

  C_SaferCond cond_ctx;
  MockSnapshotUnprotectRequest *req = new MockSnapshotUnprotectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-ENOENT, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotUnprotectRequest, IsSnapUnprotectedError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_get_snap_id(mock_image_ctx, ictx->snap_info.rbegin()->first);
  expect_is_snap_unprotected(mock_image_ctx, false, -EBADMSG);

  C_SaferCond cond_ctx;
  MockSnapshotUnprotectRequest *req = new MockSnapshotUnprotectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EBADMSG, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotUnprotectRequest, SnapAlreadyUnprotected) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_get_snap_id(mock_image_ctx, ictx->snap_info.rbegin()->first);
  expect_is_snap_unprotected(mock_image_ctx, true, 0);

  C_SaferCond cond_ctx;
  MockSnapshotUnprotectRequest *req = new MockSnapshotUnprotectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotUnprotectRequest, SetProtectionStatusError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_get_snap_id(mock_image_ctx, snap_id);
  expect_is_snap_unprotected(mock_image_ctx, false, 0);
  expect_set_protection_status(mock_image_ctx, snap_id,
                               RBD_PROTECTION_STATUS_UNPROTECTING, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotUnprotectRequest *req = new MockSnapshotUnprotectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotUnprotectRequest, ChildrenExist) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_get_snap_id(mock_image_ctx, snap_id);
  expect_is_snap_unprotected(mock_image_ctx, false, 0);
  expect_set_protection_status(mock_image_ctx, snap_id,
                               RBD_PROTECTION_STATUS_UNPROTECTING, 0);
  size_t pools = expect_create_pool_io_contexts(mock_image_ctx);
  expect_get_children(mock_image_ctx, pools, 0);
  expect_set_protection_status(mock_image_ctx, snap_id,
                               RBD_PROTECTION_STATUS_PROTECTED, 0);

  C_SaferCond cond_ctx;
  MockSnapshotUnprotectRequest *req = new MockSnapshotUnprotectRequest(
    mock_image_ctx, &cond_ctx, "snap1");
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EBUSY, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
