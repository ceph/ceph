// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/api/Image.h"
#include "librbd/object_map/UpdateRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;

class TestMockObjectMapUpdateRequest : public TestMockFixture {
public:
  void expect_update(librbd::ImageCtx *ictx, uint64_t snap_id,
                     uint64_t start_object_no, uint64_t end_object_no,
                     uint8_t new_state,
                     const boost::optional<uint8_t>& current_state, int r) {
    bufferlist bl;
    encode(start_object_no, bl);
    encode(end_object_no, bl);
    encode(new_state, bl);
    encode(current_state, bl);

    std::string oid(ObjectMap<>::object_map_name(ictx->id, snap_id));
    if (snap_id == CEPH_NOSNAP) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, StrEq("lock"), StrEq("assert_locked"), _, _, _,
                       _))
                    .WillOnce(DoDefault());
    }

    if (r < 0) {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, StrEq("rbd"), StrEq("object_map_update"),
                       ContentsEqual(bl), _, _, _))
                    .WillOnce(Return(r));
    } else {
      EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
                  exec(oid, _, StrEq("rbd"), StrEq("object_map_update"),
                       ContentsEqual(bl), _, _, _))
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

TEST_F(TestMockObjectMapUpdateRequest, UpdateInMemory) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_progress;
  ASSERT_EQ(0, ictx->operations->resize(4 << ictx->order, true, no_progress));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(4);
  for (uint64_t i = 0; i < object_map.size(); ++i) {
    object_map[i] = i % 4;
  }

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new UpdateRequest<>(
    *ictx, &object_map_lock, &object_map, CEPH_NOSNAP, 0, object_map.size(),
    OBJECT_NONEXISTENT, OBJECT_EXISTS, {}, false, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    std::unique_lock object_map_locker{object_map_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  for (uint64_t i = 0; i < object_map.size(); ++i) {
    if (i % 4 == OBJECT_EXISTS || i % 4 == OBJECT_EXISTS_CLEAN) {
      ASSERT_EQ(OBJECT_NONEXISTENT, object_map[i]);
    } else {
      ASSERT_EQ(i % 4, object_map[i]);
    }
  }
}

TEST_F(TestMockObjectMapUpdateRequest, UpdateHeadOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  expect_update(ictx, CEPH_NOSNAP, 0, 1, OBJECT_NONEXISTENT, OBJECT_EXISTS, 0);

  ceph::shared_mutex object_map_lock =
    ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new UpdateRequest<>(
    *ictx, &object_map_lock, &object_map, CEPH_NOSNAP, 0, object_map.size(),
    OBJECT_NONEXISTENT, OBJECT_EXISTS, {}, false, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    std::unique_lock object_map_locker{object_map_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapUpdateRequest, UpdateSnapOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, librbd::api::Image<>::snap_set(ictx,
				              cls::rbd::UserSnapshotNamespace(),
				              "snap1"));

  uint64_t snap_id = ictx->snap_id;
  expect_update(ictx, snap_id, 0, 1, OBJECT_NONEXISTENT, OBJECT_EXISTS, 0);

  ceph::shared_mutex object_map_lock =
    ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new UpdateRequest<>(
    *ictx, &object_map_lock, &object_map, snap_id, 0, object_map.size(),
    OBJECT_NONEXISTENT, OBJECT_EXISTS, {}, false, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    std::unique_lock object_map_locker{object_map_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapUpdateRequest, UpdateOnDiskError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  expect_update(ictx, CEPH_NOSNAP, 0, 1, OBJECT_NONEXISTENT, OBJECT_EXISTS,
                -EINVAL);
  expect_invalidate(ictx);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new UpdateRequest<>(
    *ictx, &object_map_lock, &object_map, CEPH_NOSNAP, 0, object_map.size(),
    OBJECT_NONEXISTENT, OBJECT_EXISTS, {}, false, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    std::unique_lock object_map_locker{object_map_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapUpdateRequest, RebuildSnapOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());
  ASSERT_EQ(CEPH_NOSNAP, ictx->snap_id);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_update(ictx, snap_id, 0, 1, OBJECT_EXISTS_CLEAN,
                boost::optional<uint8_t>(), 0);
  expect_unlock_exclusive_lock(*ictx);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new UpdateRequest<>(
    *ictx, &object_map_lock, &object_map, snap_id, 0, object_map.size(),
    OBJECT_EXISTS_CLEAN, boost::optional<uint8_t>(), {}, false, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    std::unique_lock object_map_locker{object_map_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  // do not update the in-memory map if rebuilding a snapshot
  ASSERT_NE(OBJECT_EXISTS_CLEAN, object_map[0]);
}

TEST_F(TestMockObjectMapUpdateRequest, BatchUpdate) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_progress;
  ASSERT_EQ(0, ictx->operations->resize(712312 * ictx->get_object_size(), false,
                                        no_progress));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  InSequence seq;
  expect_update(ictx, CEPH_NOSNAP, 0, 262144, OBJECT_NONEXISTENT, OBJECT_EXISTS,
                0);
  expect_update(ictx, CEPH_NOSNAP, 262144, 524288, OBJECT_NONEXISTENT,
                OBJECT_EXISTS, 0);
  expect_update(ictx, CEPH_NOSNAP, 524288, 712312, OBJECT_NONEXISTENT,
                OBJECT_EXISTS, 0);
  expect_unlock_exclusive_lock(*ictx);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(712312);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new UpdateRequest<>(
    *ictx, &object_map_lock, &object_map, CEPH_NOSNAP, 0, object_map.size(),
    OBJECT_NONEXISTENT, OBJECT_EXISTS, {}, false, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    std::unique_lock object_map_locker{object_map_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockObjectMapUpdateRequest, IgnoreMissingObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  expect_update(ictx, CEPH_NOSNAP, 0, 1, OBJECT_NONEXISTENT, OBJECT_EXISTS,
                -ENOENT);

  ceph::shared_mutex object_map_lock = ceph::make_shared_mutex("lock");
  ceph::BitVector<2> object_map;
  object_map.resize(1);

  C_SaferCond cond_ctx;
  AsyncRequest<> *req = new UpdateRequest<>(
    *ictx, &object_map_lock, &object_map, CEPH_NOSNAP, 0, object_map.size(),
    OBJECT_NONEXISTENT, OBJECT_EXISTS, {}, true, &cond_ctx);
  {
    std::shared_lock image_locker{ictx->image_lock};
    std::unique_lock object_map_locker{object_map_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

} // namespace object_map
} // namespace librbd
