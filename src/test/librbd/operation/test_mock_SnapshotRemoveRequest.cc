// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/internal.h"
#include "librbd/operation/SnapshotRemoveRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

// template definitions
#include "librbd/operation/SnapshotRemoveRequest.cc"

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::WithArg;

class TestMockOperationSnapshotRemoveRequest : public TestMockFixture {
public:
  typedef SnapshotRemoveRequest<MockImageCtx> MockSnapshotRemoveRequest;

  int create_snapshot(const char *snap_name) {
    librbd::ImageCtx *ictx;
    int r = open_image(m_image_name, &ictx);
    if (r < 0) {
      return r;
    }

    r = librbd::snap_create(ictx, snap_name);
    if (r < 0) {
      return r;
    }

    r = librbd::snap_protect(ictx, snap_name);
     if (r < 0) {
      return r;
    }
    close_image(ictx);
    return 0;
  }

  void expect_object_map_snap_remove(MockImageCtx &mock_image_ctx, int r) {
    bool enabled = mock_image_ctx.image_ctx->test_features(RBD_FEATURE_OBJECT_MAP);
    EXPECT_CALL(mock_image_ctx.object_map, enabled(_))
                  .WillOnce(Return(enabled));
    if (enabled) {
      EXPECT_CALL(mock_image_ctx.object_map, snapshot_remove(_, _))
                    .WillOnce(WithArg<1>(CompleteContext(
                      r, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_get_parent_spec(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(mock_image_ctx, get_parent_spec(_, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      parent_spec &parent_spec = mock_image_ctx.snap_info.rbegin()->second.parent.spec;
      expect.WillOnce(DoAll(SetArgPointee<1>(parent_spec),
                            Return(0)));
    }
  }

  void expect_remove_child(MockImageCtx &mock_image_ctx, int r) {
    bool deep_flatten = mock_image_ctx.image_ctx->test_features(RBD_FEATURE_DEEP_FLATTEN);
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(RBD_CHILDREN, _, "rbd", "remove_child",_,
                                    _, _));
    if (deep_flatten) {
      expect.Times(0);
    } else {
      expect.WillOnce(Return(r));
    }
  }

  void expect_verify_lock_ownership(MockImageCtx &mock_image_ctx) {
    if (mock_image_ctx.old_format) {
      return;
    }

    EXPECT_CALL(*mock_image_ctx.image_watcher, is_lock_owner())
                  .WillRepeatedly(Return(false));
  }

  void expect_snap_remove(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, "rbd",
                               mock_image_ctx.old_format ? "snap_remove" :
                                                           "snapshot_remove",
                                _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_rm_snap(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, rm_snap(_, _)).Times(1);
  }

  void expect_release_snap_id(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                                selfmanaged_snap_remove(_))
                                  .WillOnce(DoDefault());
  }

};

TEST_F(TestMockOperationSnapshotRemoveRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_get_parent_spec(mock_image_ctx, 0);
  expect_verify_lock_ownership(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, 0);
  expect_rm_snap(mock_image_ctx);
  expect_release_snap_id(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, "snap1", snap_id);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, FlattenedCloneRemovesChild) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snapshot("snap1"));

  int order = 22;
  uint64_t features;
  ASSERT_TRUE(::get_features(&features));
  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));

  librbd::NoOpProgressContext prog_ctx;
  ASSERT_EQ(0, librbd::flatten(ictx, prog_ctx));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_get_parent_spec(mock_image_ctx, 0);
  expect_remove_child(mock_image_ctx, -ENOENT);
  expect_verify_lock_ownership(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, 0);
  expect_rm_snap(mock_image_ctx);
  expect_release_snap_id(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, "snap1", snap_id);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, ObjectMapSnapRemoveError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_object_map_snap_remove(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, "snap1", snap_id);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, RemoveChildParentError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_get_parent_spec(mock_image_ctx, -ENOENT);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, "snap1", snap_id);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-ENOENT, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, RemoveChildError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snapshot("snap1"));

  int order = 22;
  uint64_t features;
  ASSERT_TRUE(::get_features(&features));
  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  if (ictx->test_features(RBD_FEATURE_DEEP_FLATTEN)) {
    std::cout << "SKIPPING" << std::endl;
    return SUCCEED();
  }

  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));

  librbd::NoOpProgressContext prog_ctx;
  ASSERT_EQ(0, librbd::flatten(ictx, prog_ctx));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_get_parent_spec(mock_image_ctx, 0);
  expect_remove_child(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, "snap1", snap_id);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, RemoveSnapError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::ictx_check(ictx));

  MockImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_get_parent_spec(mock_image_ctx, 0);
  expect_verify_lock_ownership(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, -ENOENT);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, "snap1", snap_id);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    req->send();
  }
  ASSERT_EQ(-ENOENT, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
