// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/SnapshotRollbackRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::StrEq;

class TestMockObjectMapSnapshotRollbackRequest : public TestMockFixture {
public:
  void expect_read_map(librbd::ImageCtx *ictx, uint64_t snap_id, int r) {
    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  read(ObjectMap<>::object_map_name(ictx->id, snap_id),
                       0, 0, _, _)).WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  read(ObjectMap<>::object_map_name(ictx->id, snap_id),
                       0, 0, _, _)).WillOnce(DoDefault());
    }
  }

  void expect_write_map(librbd::ImageCtx *ictx, int r) {
    EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                exec(ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP), _,
		     StrEq("lock"), StrEq("assert_locked"), _, _, _, _))
                  .WillOnce(DoDefault());
    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  write_full(
                    ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP), _, _))
                  .WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  write_full(
                    ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP), _, _))
                  .WillOnce(DoDefault());
    }
  }

  void expect_invalidate(librbd::ImageCtx *ictx, uint32_t times) {
    EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                exec(ictx->header_oid, _, StrEq("rbd"), StrEq("set_flags"), _,
                     _, _, _))
                  .Times(times)
                  .WillRepeatedly(DoDefault());
  }
};

TEST_F(TestMockObjectMapSnapshotRollbackRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_read_map(ictx, snap_id, 0);
  expect_write_map(ictx, 0);

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRollbackRequest(
    *ictx, snap_id, &cond_ctx);
  request->send();
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRollbackRequest, ReadMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_read_map(ictx, snap_id, -ENOENT);
  expect_invalidate(ictx, 2);

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRollbackRequest(
    *ictx, snap_id, &cond_ctx);
  request->send();
  ASSERT_EQ(0, cond_ctx.wait());

  {
    std::shared_lock image_locker{ictx->image_lock};
    uint64_t flags;
    ASSERT_EQ(0, ictx->get_flags(snap_id, &flags));
    ASSERT_NE(0U, flags & RBD_FLAG_OBJECT_MAP_INVALID);
  }
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP,
                                RBD_FLAG_OBJECT_MAP_INVALID, &flags_set));
  ASSERT_TRUE(flags_set);
  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRollbackRequest, WriteMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_read_map(ictx, snap_id, 0);
  expect_write_map(ictx, -EINVAL);
  expect_invalidate(ictx, 1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRollbackRequest(
    *ictx, snap_id, &cond_ctx);
  request->send();
  ASSERT_EQ(0, cond_ctx.wait());

  {
    std::shared_lock image_locker{ictx->image_lock};
    uint64_t flags;
    ASSERT_EQ(0, ictx->get_flags(snap_id, &flags));
    ASSERT_EQ(0U, flags & RBD_FLAG_OBJECT_MAP_INVALID);
  }
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP,
                                RBD_FLAG_OBJECT_MAP_INVALID, &flags_set));
  ASSERT_TRUE(flags_set);
  expect_unlock_exclusive_lock(*ictx);
}

} // namespace object_map
} // namespace librbd
