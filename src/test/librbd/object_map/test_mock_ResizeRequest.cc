// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/internal.h"
#include "librbd/object_map/ResizeRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Return;

class TestMockObjectMapResizeRequest : public TestMockFixture {
public:
  void expect_resize(librbd::ImageCtx *ictx, uint64_t snap_id, int r) {
    std::string oid(ObjectMap::object_map_name(ictx->id, snap_id));
    if (snap_id == CEPH_NOSNAP) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, "lock", "assert_locked", _, _, _))
                    .WillOnce(DoDefault());
    }

    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, "rbd", "object_map_resize", _, _, _))
                    .WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, "rbd", "object_map_resize", _, _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_invalidate(librbd::ImageCtx *ictx) {
    EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                exec(ictx->header_oid, _, "lock", "assert_locked", _, _, _)).Times(0);
    EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                exec(ictx->header_oid, _, "rbd", "set_flags", _, _, _))
                  .WillOnce(DoDefault());
  }
};

TEST_F(TestMockObjectMapResizeRequest, UpdateInMemory) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new ResizeRequest(
    *ictx, &object_map, CEPH_NOSNAP, object_map.size(), OBJECT_EXISTS,
    &cond_ctx);
  req->send();
  ASSERT_EQ(0, cond_ctx.wait());

  for (uint64_t i = 0; i < object_map.size(); ++i) {
    ASSERT_EQ(i == 0 ? OBJECT_NONEXISTENT : OBJECT_EXISTS,
              object_map[i]);
  }
}

TEST_F(TestMockObjectMapResizeRequest, UpdateHeadOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  expect_resize(ictx, CEPH_NOSNAP, 0);

  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new ResizeRequest(
    *ictx, &object_map, CEPH_NOSNAP, object_map.size(), OBJECT_EXISTS,
    &cond_ctx);
  req->send();
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapResizeRequest, UpdateSnapOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::snap_set(ictx, "snap1"));

  uint64_t snap_id = ictx->snap_id;
  expect_resize(ictx, snap_id, 0);

  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new ResizeRequest(
    *ictx, &object_map, snap_id, object_map.size(), OBJECT_EXISTS,
    &cond_ctx);
  req->send();
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapResizeRequest, UpdateOnDiskError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  expect_resize(ictx, CEPH_NOSNAP, -EINVAL);
  expect_invalidate(ictx);

  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new ResizeRequest(
    *ictx, &object_map, CEPH_NOSNAP, object_map.size(), OBJECT_EXISTS,
    &cond_ctx);
  req->send();
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

} // namespace object_map
} // namespace librbd
