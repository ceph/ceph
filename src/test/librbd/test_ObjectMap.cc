// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "cls/rbd/cls_rbd_client.h"
#include <list>

void register_test_object_map() {
}

class TestObjectMap : public TestFixture {
public:

  int when_open_object_map(librbd::ImageCtx *ictx) {
    C_SaferCond ctx;
    librbd::ObjectMap object_map(*ictx, ictx->snap_id);
    object_map.open(&ctx);
    return ctx.wait();
  }
};

TEST_F(TestObjectMap, RefreshInvalidatesWhenCorrupt) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  C_SaferCond lock_ctx;
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->exclusive_lock->try_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  std::string oid = librbd::ObjectMap::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist bl;
  bl.append("corrupt");
  ASSERT_EQ(0, ictx->data_ctx.write_full(oid, bl));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));
}

TEST_F(TestObjectMap, RefreshInvalidatesWhenTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  C_SaferCond lock_ctx;
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->exclusive_lock->try_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  librados::ObjectWriteOperation op;
  librbd::cls_client::object_map_resize(&op, 0, OBJECT_NONEXISTENT);

  std::string oid = librbd::ObjectMap::object_map_name(ictx->id, CEPH_NOSNAP);
  ASSERT_EQ(0, ictx->data_ctx.operate(oid, &op));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));
}

TEST_F(TestObjectMap, InvalidateFlagOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  C_SaferCond lock_ctx;
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->exclusive_lock->try_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  std::string oid = librbd::ObjectMap::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist bl;
  bl.append("corrupt");
  ASSERT_EQ(0, ictx->data_ctx.write_full(oid, bl));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));
}

TEST_F(TestObjectMap, InvalidateFlagInMemoryOnly) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  std::string oid = librbd::ObjectMap::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist valid_bl;
  ASSERT_LT(0, ictx->data_ctx.read(oid, valid_bl, 0, 0));

  bufferlist corrupt_bl;
  corrupt_bl.append("corrupt");
  ASSERT_EQ(0, ictx->data_ctx.write_full(oid, corrupt_bl));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  ASSERT_EQ(0, ictx->data_ctx.write_full(oid, valid_bl));
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));
}

