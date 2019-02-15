// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "common/Cond.h"
#include "cls/rbd/cls_rbd_client.h"
#include <list>

void register_test_object_map() {
}

class TestObjectMap : public TestFixture {
public:

  int when_open_object_map(librbd::ImageCtx *ictx) {
    C_SaferCond ctx;
    librbd::ObjectMap<> object_map(*ictx, ictx->snap_id);
    object_map.open(&ctx);
    return ctx.wait();
  }
};

TEST_F(TestObjectMap, RefreshInvalidatesWhenCorrupt) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  C_SaferCond lock_ctx;
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  std::string oid = librbd::ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist bl;
  bl.append("corrupt");
  ASSERT_EQ(0, ictx->md_ctx.write_full(oid, bl));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);
}

TEST_F(TestObjectMap, RefreshInvalidatesWhenTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  C_SaferCond lock_ctx;
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  librados::ObjectWriteOperation op;
  librbd::cls_client::object_map_resize(&op, 0, OBJECT_NONEXISTENT);

  std::string oid = librbd::ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP);
  ASSERT_EQ(0, ictx->md_ctx.operate(oid, &op));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);
}

TEST_F(TestObjectMap, InvalidateFlagOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  C_SaferCond lock_ctx;
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  std::string oid = librbd::ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist bl;
  bl.append("corrupt");
  ASSERT_EQ(0, ictx->md_ctx.write_full(oid, bl));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);
}

TEST_F(TestObjectMap, AcquireLockInvalidatesWhenTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  librados::ObjectWriteOperation op;
  librbd::cls_client::object_map_resize(&op, 0, OBJECT_NONEXISTENT);

  std::string oid = librbd::ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP);
  ASSERT_EQ(0, ictx->md_ctx.operate(oid, &op));

  C_SaferCond lock_ctx;
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);

  // Test the flag is stored on disk
  ASSERT_EQ(0, ictx->state->refresh());
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);
}
