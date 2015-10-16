// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
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
};

TEST_F(TestObjectMap, RefreshInvalidatesWhenCorrupt) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
  }

  std::string oid = librbd::ObjectMap::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist bl;
  bl.append("corrupt");
  ASSERT_EQ(0, ictx->data_ctx.write_full(oid, bl));

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    ictx->object_map.refresh(CEPH_NOSNAP);
  }
  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));
}

TEST_F(TestObjectMap, RefreshInvalidatesWhenTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
  }

  librados::ObjectWriteOperation op;
  librbd::cls_client::object_map_resize(&op, 0, OBJECT_NONEXISTENT);

  std::string oid = librbd::ObjectMap::object_map_name(ictx->id, CEPH_NOSNAP);
  ASSERT_EQ(0, ictx->data_ctx.operate(oid, &op));

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    ictx->object_map.refresh(CEPH_NOSNAP);
  }
  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));
}

TEST_F(TestObjectMap, InvalidateFlagOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
  }

  std::string oid = librbd::ObjectMap::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist bl;
  bl.append("corrupt");
  ASSERT_EQ(0, ictx->data_ctx.write_full(oid, bl));

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    ictx->object_map.refresh(CEPH_NOSNAP);
  }
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

  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    RWLock::WLocker snap_locker(ictx->snap_lock);
    ictx->object_map.refresh(CEPH_NOSNAP);
  }
  ASSERT_TRUE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));

  ASSERT_EQ(0, ictx->data_ctx.write_full(oid, valid_bl));
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->test_flags(RBD_FLAG_OBJECT_MAP_INVALID));
}

