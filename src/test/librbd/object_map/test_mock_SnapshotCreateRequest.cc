// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/SnapshotCreateRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::StrEq;

class TestMockObjectMapSnapshotCreateRequest : public TestMockFixture {
public:
  void inject_snap_info(librbd::ImageCtx *ictx, uint64_t snap_id) {
    std::unique_lock image_locker{ictx->image_lock};
    ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "snap name", snap_id,
		   ictx->size, ictx->parent_md,
                   RBD_PROTECTION_STATUS_UNPROTECTED, 0, utime_t());
  }

  void expect_read_map(librbd::ImageCtx *ictx, int r) {
    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  read(ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP),
                       0, 0, _, _)).WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  read(ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP),
                       0, 0, _, _)).WillOnce(DoDefault());
    }
  }

  void expect_write_map(librbd::ImageCtx *ictx, uint64_t snap_id, int r) {
    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  write_full(
                    ObjectMap<>::object_map_name(ictx->id, snap_id), _, _))
                  .WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  write_full(
                    ObjectMap<>::object_map_name(ictx->id, snap_id), _, _))
                  .WillOnce(DoDefault());
    }
  }

  void expect_add_snapshot(librbd::ImageCtx *ictx, int r) {
    std::string oid(ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP));
    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, StrEq("lock"), StrEq("assert_locked"), _, _, _,
                       _))
                    .WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, StrEq("lock"), StrEq("assert_locked"), _, _, _,
                       _))
                    .WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, StrEq("rbd"), StrEq("object_map_snap_add"), _, _,
                       _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_invalidate(librbd::ImageCtx *ictx) {
    EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                exec(ictx->header_oid, _, StrEq("rbd"), StrEq("set_flags"), _,
                     _, _, _))
                  .WillOnce(DoDefault());
  }
};

TEST_F(TestMockObjectMapSnapshotCreateRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;

  uint64_t snap_id = 1;
  inject_snap_info(ictx, snap_id);
  expect_read_map(ictx, 0);
  expect_write_map(ictx, snap_id, 0);
  if (ictx->test_features(RBD_FEATURE_FAST_DIFF)) {
    expect_add_snapshot(ictx, 0);
  }

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotCreateRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotCreateRequest, ReadMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;

  uint64_t snap_id = 1;
  inject_snap_info(ictx, snap_id);
  expect_read_map(ictx, -ENOENT);
  expect_invalidate(ictx);

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotCreateRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotCreateRequest, WriteMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;

  uint64_t snap_id = 1;
  inject_snap_info(ictx, snap_id);
  expect_read_map(ictx, 0);
  expect_write_map(ictx, snap_id, -EINVAL);
  expect_invalidate(ictx);

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotCreateRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotCreateRequest, AddSnapshotError) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;

  uint64_t snap_id = 1;
  inject_snap_info(ictx, snap_id);
  expect_read_map(ictx, 0);
  expect_write_map(ictx, snap_id, 0);
  expect_add_snapshot(ictx, -EINVAL);
  expect_invalidate(ictx);

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotCreateRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotCreateRequest, FlagCleanObjects) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(1024);
  for (uint64_t i = 0; i < object_map.size(); ++i) {
    object_map[i] = i % 2 == 0 ? OBJECT_EXISTS : OBJECT_NONEXISTENT;
  }

  uint64_t snap_id = 1;
  inject_snap_info(ictx, snap_id);

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotCreateRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  for (uint64_t i = 0; i < object_map.size(); ++i) {
    ASSERT_EQ(i % 2 == 0 ? OBJECT_EXISTS_CLEAN : OBJECT_NONEXISTENT,
              object_map[i]);
  }
}

} // namespace object_map
} // namespace librbd
