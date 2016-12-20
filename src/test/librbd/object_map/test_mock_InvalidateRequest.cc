// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "librbd/internal.h"
#include "librbd/object_map/InvalidateRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::StrEq;

class TestMockObjectMapInvalidateRequest : public TestMockFixture {
public:
};

TEST_F(TestMockObjectMapInvalidateRequest, UpdatesInMemoryFlag) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new InvalidateRequest<>(*ictx, CEPH_NOSNAP, false, &cond_ctx);

  EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
              exec(ictx->header_oid, _, StrEq("rbd"), StrEq("set_flags"), _, _, _))
                .Times(0);

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));
}

TEST_F(TestMockObjectMapInvalidateRequest, UpdatesHeadOnDiskFlag) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new InvalidateRequest<>(*ictx, CEPH_NOSNAP, false, &cond_ctx);

  EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
              exec(ictx->header_oid, _, StrEq("rbd"), StrEq("set_flags"), _, _, _))
                .WillOnce(DoDefault());

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapInvalidateRequest, UpdatesSnapOnDiskFlag) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, librbd::snap_set(ictx, "snap1"));

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new InvalidateRequest<>(*ictx, ictx->snap_id, false,
                                                &cond_ctx);

  EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
              exec(ictx->header_oid, _, StrEq("rbd"), StrEq("set_flags"), _, _, _))
                .WillOnce(DoDefault());

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockObjectMapInvalidateRequest, SkipOnDiskUpdateWithoutLock) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new InvalidateRequest<>(*ictx, CEPH_NOSNAP, false, &cond_ctx);

  EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
              exec(ictx->header_oid, _, StrEq("rbd"), StrEq("set_flags"), _, _, _))
                .Times(0);

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

TEST_F(TestMockObjectMapInvalidateRequest, IgnoresOnDiskUpdateFailure) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, acquire_exclusive_lock(*ictx));

  C_SaferCond cond_ctx;
  AsyncRequest<> *request = new InvalidateRequest<>(*ictx, CEPH_NOSNAP, false, &cond_ctx);

  EXPECT_CALL(get_mock_io_ctx(ictx->md_ctx),
              exec(ictx->header_oid, _, StrEq("rbd"), StrEq("set_flags"), _, _, _))
                .WillOnce(Return(-EINVAL));

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    request->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());

  expect_unlock_exclusive_lock(*ictx);
}

} // namespace object_map
} // namespace librbd
