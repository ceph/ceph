// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/SnapshotRemoveRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::StrEq;

class TestMockObjectMapSnapshotRemoveRequest : public TestMockFixture {
public:
  void expect_load_map(librbd::ImageCtx *ictx, uint64_t snap_id, int r) {
    std::string snap_oid(ObjectMap<>::object_map_name(ictx->id, snap_id));
    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(snap_oid, _, StrEq("rbd"), StrEq("object_map_load"), _,
                       _, _, _))
                    .WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(snap_oid, _, StrEq("rbd"), StrEq("object_map_load"), _,
                       _, _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_remove_snapshot(librbd::ImageCtx *ictx, int r) {
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
                  exec(oid, _, StrEq("rbd"), StrEq("object_map_snap_remove"), _,
                       _, _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_remove_map(librbd::ImageCtx *ictx, uint64_t snap_id, int r) {
    std::string snap_oid(ObjectMap<>::object_map_name(ictx->id, snap_id));
    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx), remove(snap_oid, _))
                    .WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx), remove(snap_oid, _))
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

TEST_F(TestMockObjectMapSnapshotRemoveRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  if (ictx->test_features(RBD_FEATURE_FAST_DIFF)) {
    expect_load_map(ictx, snap_id, 0);
    expect_remove_snapshot(ictx, 0);
  }
  expect_remove_map(ictx, snap_id, 0);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRemoveRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRemoveRequest, LoadMapMissing) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  auto snap_it = ictx->snap_info.find(snap_id);
  ASSERT_NE(ictx->snap_info.end(), snap_it);
  snap_it->second.flags |= RBD_FLAG_OBJECT_MAP_INVALID;

  expect_load_map(ictx, snap_id, -ENOENT);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRemoveRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  {
    // shouldn't invalidate the HEAD revision when we fail to load
    // the already deleted snapshot
    std::shared_lock image_locker{ictx->image_lock};
    uint64_t flags;
    ASSERT_EQ(0, ictx->get_flags(CEPH_NOSNAP, &flags));
    ASSERT_EQ(0U, flags & RBD_FLAG_OBJECT_MAP_INVALID);
  }

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRemoveRequest, LoadMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_load_map(ictx, snap_id, -EINVAL);
  expect_invalidate(ictx);
  expect_remove_map(ictx, snap_id, 0);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRemoveRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRemoveRequest, RemoveSnapshotMissing) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_load_map(ictx, snap_id, 0);
  expect_remove_snapshot(ictx, -ENOENT);
  expect_remove_map(ictx, snap_id, 0);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRemoveRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRemoveRequest, RemoveSnapshotError) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_load_map(ictx, snap_id, 0);
  expect_remove_snapshot(ictx, -EINVAL);
  expect_invalidate(ictx);
  expect_remove_map(ictx, snap_id, 0);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRemoveRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRemoveRequest, RemoveMapMissing) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  if (ictx->test_features(RBD_FEATURE_FAST_DIFF)) {
    expect_load_map(ictx, snap_id, 0);
    expect_remove_snapshot(ictx, 0);
  }
  expect_remove_map(ictx, snap_id, -ENOENT);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRemoveRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRemoveRequest, RemoveMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  if (ictx->test_features(RBD_FEATURE_FAST_DIFF)) {
    expect_load_map(ictx, snap_id, 0);
    expect_remove_snapshot(ictx, 0);
  }
  expect_remove_map(ictx, snap_id, -EINVAL);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new SnapshotRemoveRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx);
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapSnapshotRemoveRequest, ScrubCleanObjects) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  librbd::NoOpProgressContext prog_ctx;
  uint64_t size = 4294967296; // 4GB = 1024 * 4MB
  ASSERT_EQ(0, resize(ictx, size));

  // update image objectmap for snap inherit
  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(1024);
  for (uint64_t i = 512; i < object_map.size(); ++i) {
    object_map[i] = i % 2 == 0 ? OBJECT_EXISTS : OBJECT_NONEXISTENT;
  }

  C_SaferCond cond_ctx1;
  {
    librbd::ObjectMap<> *om = new librbd::ObjectMap<>(*ictx, ictx->snap_id);
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    om->set_object_map(object_map);
    om->aio_save(&cond_ctx1);
    om->put();
  }
  ASSERT_EQ(0, cond_ctx1.wait());
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  // simutate the image objectmap state after creating snap
  for (uint64_t i = 512; i < object_map.size(); ++i) {
    object_map[i] = i % 2 == 0 ? OBJECT_EXISTS_CLEAN : OBJECT_NONEXISTENT;
  }

  C_SaferCond cond_ctx2;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  AsyncRequest<> *request = new SnapshotRemoveRequest(
    *ictx, &object_map_lock, &object_map, snap_id, &cond_ctx2);
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    std::unique_lock image_locker{ictx->image_lock};
    request->send();
  }
  ASSERT_EQ(0, cond_ctx2.wait());

  for (uint64_t i = 512; i < object_map.size(); ++i) {
    ASSERT_EQ(i % 2 == 0 ? OBJECT_EXISTS : OBJECT_NONEXISTENT,
              object_map[i]);
  }
}

} // namespace object_map
} // namespace librbd
