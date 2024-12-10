// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd/librbd.h"
#include "include/rbd/librbd.hpp"
#include "librbd/api/Group.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"

#include <boost/scope_exit.hpp>
#include <chrono>
#include <vector>
#include <set>
#include <algorithm>

void register_test_groups() {
}

class TestGroup : public TestFixture {

};

TEST_F(TestGroup, group_create)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, _pool_name.c_str(), &ioctx);
  BOOST_SCOPE_EXIT(ioctx) {
    rados_ioctx_destroy(ioctx);
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, rbd_group_create(ioctx, "mygroup"));

  size_t size = 0;
  ASSERT_EQ(-ERANGE, rbd_group_list(ioctx, NULL, &size));
  ASSERT_EQ(strlen("mygroup") + 1, size);

  char groups[80];
  ASSERT_EQ(static_cast<int>(strlen("mygroup") + 1),
	    rbd_group_list(ioctx, groups, &size));
  ASSERT_STREQ("mygroup", groups);

  ASSERT_EQ(0, rbd_group_remove(ioctx, "mygroup"));

  ASSERT_EQ(0, rbd_group_list(ioctx, groups, &size));
}

TEST_F(TestGroup, group_createPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, "mygroupPP"));

  std::vector<std::string> groups;
  ASSERT_EQ(0, rbd.group_list(ioctx, &groups));
  ASSERT_EQ(1U, groups.size());
  ASSERT_EQ("mygroupPP", groups[0]);

  groups.clear();
  ASSERT_EQ(0, rbd.group_rename(ioctx, "mygroupPP", "newgroupPP"));
  ASSERT_EQ(0, rbd.group_list(ioctx, &groups));
  ASSERT_EQ(1U, groups.size());
  ASSERT_EQ("newgroupPP", groups[0]);

  ASSERT_EQ(0, rbd.group_remove(ioctx, "newgroupPP"));

  groups.clear();
  ASSERT_EQ(0, rbd.group_list(ioctx, &groups));
  ASSERT_EQ(0U, groups.size());
}

TEST_F(TestGroup, group_get_id)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, _pool_name.c_str(), &ioctx);
  BOOST_SCOPE_EXIT(ioctx) {
    rados_ioctx_destroy(ioctx);
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, rbd_group_create(ioctx, "group_get_id"));
  
  size_t size = 0;
  ASSERT_EQ(-ERANGE, rbd_group_get_id(ioctx, "group_get_id", NULL, &size));
  ASSERT_GT(size, 0);

  char group_id[32];
  ASSERT_EQ(0, rbd_group_get_id(ioctx, "group_get_id", group_id, &size));
  ASSERT_EQ(strlen(group_id) + 1, size);

  ASSERT_EQ(0, rbd_group_remove(ioctx, "group_get_id"));
}

TEST_F(TestGroup, group_get_idPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, "group_get_idPP"));

  std::string group_id;
  ASSERT_EQ(0, rbd.group_get_id(ioctx, "group_get_idPP", &group_id));
  ASSERT_FALSE(group_id.empty());

  ASSERT_EQ(0, rbd.group_remove(ioctx, "group_get_idPP"));
}

TEST_F(TestGroup, add_image)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, _pool_name.c_str(), &ioctx);
  BOOST_SCOPE_EXIT(ioctx) {
    rados_ioctx_destroy(ioctx);
  } BOOST_SCOPE_EXIT_END;

  const char *group_name = "mycg";
  ASSERT_EQ(0, rbd_group_create(ioctx, group_name));

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, m_image_name.c_str(), &image, NULL));
  BOOST_SCOPE_EXIT(image) {
    EXPECT_EQ(0, rbd_close(image));
  } BOOST_SCOPE_EXIT_END;

  uint64_t features;
  ASSERT_EQ(0, rbd_get_features(image, &features));
  ASSERT_TRUE((features & RBD_FEATURE_OPERATIONS) == 0ULL);

  uint64_t op_features;
  ASSERT_EQ(0, rbd_get_op_features(image, &op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_GROUP) == 0ULL);

  rbd_group_info_t group_info;
  ASSERT_EQ(0, rbd_get_group(image, &group_info, sizeof(group_info)));
  ASSERT_EQ(0, strcmp("", group_info.name));
  ASSERT_EQ(RBD_GROUP_INVALID_POOL, group_info.pool);
  rbd_group_info_cleanup(&group_info, sizeof(group_info));

  ASSERT_EQ(0, rbd_group_image_add(ioctx, group_name, ioctx,
                                   m_image_name.c_str()));

  ASSERT_EQ(-ERANGE, rbd_get_group(image, &group_info, 0));
  ASSERT_EQ(0, rbd_get_group(image, &group_info, sizeof(group_info)));
  ASSERT_EQ(0, strcmp(group_name, group_info.name));
  ASSERT_EQ(rados_ioctx_get_id(ioctx), group_info.pool);
  rbd_group_info_cleanup(&group_info, sizeof(group_info));

  ASSERT_EQ(0, rbd_get_features(image, &features));
  ASSERT_TRUE((features & RBD_FEATURE_OPERATIONS) ==
                RBD_FEATURE_OPERATIONS);
  ASSERT_EQ(0, rbd_get_op_features(image, &op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_GROUP) ==
                RBD_OPERATION_FEATURE_GROUP);

  size_t num_images = 0;
  ASSERT_EQ(-ERANGE, rbd_group_image_list(ioctx, group_name, NULL,
                                          sizeof(rbd_group_image_info_t),
                                          &num_images));
  ASSERT_EQ(1U, num_images);

  rbd_group_image_info_t images[1];
  ASSERT_EQ(1, rbd_group_image_list(ioctx, group_name, images,
                                    sizeof(rbd_group_image_info_t),
                                    &num_images));

  ASSERT_EQ(m_image_name, images[0].name);
  ASSERT_EQ(rados_ioctx_get_id(ioctx), images[0].pool);

  ASSERT_EQ(0, rbd_group_image_list_cleanup(images,
                                            sizeof(rbd_group_image_info_t),
                                            num_images));
  ASSERT_EQ(0, rbd_group_image_remove(ioctx, group_name, ioctx,
                                      m_image_name.c_str()));

  ASSERT_EQ(0, rbd_get_features(image, &features));
  ASSERT_TRUE((features & RBD_FEATURE_OPERATIONS) == 0ULL);
  ASSERT_EQ(0, rbd_get_op_features(image, &op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_GROUP) == 0ULL);

  ASSERT_EQ(0, rbd_group_image_list(ioctx, group_name, images,
                                    sizeof(rbd_group_image_info_t),
                                    &num_images));
  ASSERT_EQ(0U, num_images);

  ASSERT_EQ(0, rbd_group_remove(ioctx, group_name));
}

TEST_F(TestGroup, add_imagePP)
{
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const char *group_name = "mycgPP";
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, group_name));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, m_image_name.c_str(), NULL));

  uint64_t features;
  ASSERT_EQ(0, image.features(&features));
  ASSERT_TRUE((features & RBD_FEATURE_OPERATIONS) == 0ULL);

  uint64_t op_features;
  ASSERT_EQ(0, image.get_op_features(&op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_GROUP) == 0ULL);

  librbd::group_info_t group_info;
  ASSERT_EQ(0, image.get_group(&group_info, sizeof(group_info)));
  ASSERT_EQ(std::string(""), group_info.name);
  ASSERT_EQ(RBD_GROUP_INVALID_POOL, group_info.pool);

  ASSERT_EQ(0, rbd.group_image_add(ioctx, group_name, ioctx,
                                   m_image_name.c_str()));

  ASSERT_EQ(-ERANGE, image.get_group(&group_info, 0));
  ASSERT_EQ(0, image.get_group(&group_info, sizeof(group_info)));
  ASSERT_EQ(std::string(group_name), group_info.name);
  ASSERT_EQ(ioctx.get_id(), group_info.pool);

  ASSERT_EQ(0, image.features(&features));
  ASSERT_TRUE((features & RBD_FEATURE_OPERATIONS) ==
                RBD_FEATURE_OPERATIONS);
  ASSERT_EQ(0, image.get_op_features(&op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_GROUP) ==
                RBD_OPERATION_FEATURE_GROUP);

  std::vector<librbd::group_image_info_t> images;
  ASSERT_EQ(0, rbd.group_image_list(ioctx, group_name, &images,
                                    sizeof(librbd::group_image_info_t)));
  ASSERT_EQ(1U, images.size());
  ASSERT_EQ(m_image_name, images[0].name);
  ASSERT_EQ(ioctx.get_id(), images[0].pool);

  ASSERT_EQ(0, rbd.group_image_remove(ioctx, group_name, ioctx,
                                      m_image_name.c_str()));

  ASSERT_EQ(0, image.features(&features));
  ASSERT_TRUE((features & RBD_FEATURE_OPERATIONS) == 0ULL);
  ASSERT_EQ(0, image.get_op_features(&op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_GROUP) == 0ULL);

  images.clear();
  ASSERT_EQ(0, rbd.group_image_list(ioctx, group_name, &images,
                                    sizeof(librbd::group_image_info_t)));
  ASSERT_EQ(0U, images.size());

  ASSERT_EQ(0, rbd.group_remove(ioctx, group_name));
}

TEST_F(TestGroup, add_snapshot)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, _pool_name.c_str(), &ioctx);
  BOOST_SCOPE_EXIT(ioctx) {
    rados_ioctx_destroy(ioctx);
  } BOOST_SCOPE_EXIT_END;

  const char *group_name = "snap_group";
  const char *snap_name = "snap_snapshot";

  const char orig_data[] = "orig data";
  const char test_data[] = "test data";
  char read_data[10];

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, m_image_name.c_str(), &image, NULL));
  BOOST_SCOPE_EXIT(image) {
    EXPECT_EQ(0, rbd_close(image));
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(10, rbd_write2(image, 0, 10, orig_data,
                           LIBRADOS_OP_FLAG_FADVISE_FUA));
  ASSERT_EQ(10, rbd_read(image, 0, 10, read_data));
  ASSERT_EQ(0, memcmp(orig_data, read_data, 10));

  ASSERT_EQ(0, rbd_group_create(ioctx, group_name));

  ASSERT_EQ(0, rbd_group_image_add(ioctx, group_name, ioctx,
                                   m_image_name.c_str()));

  struct Watcher {
    static void quiesce_cb(void *arg) {
      Watcher *watcher = static_cast<Watcher *>(arg);
      watcher->handle_quiesce();
    }
    static void unquiesce_cb(void *arg) {
      Watcher *watcher = static_cast<Watcher *>(arg);
      watcher->handle_unquiesce();
    }

    rbd_image_t &image;
    uint64_t handle = 0;
    size_t quiesce_count = 0;
    size_t unquiesce_count = 0;
    int r = 0;

    ceph::mutex lock = ceph::make_mutex("lock");
    ceph::condition_variable cv;

    Watcher(rbd_image_t &image) : image(image) {
    }

    void handle_quiesce() {
      ASSERT_EQ(quiesce_count, unquiesce_count);
      quiesce_count++;
      rbd_quiesce_complete(image, handle, r);
    }
    void handle_unquiesce() {
      std::unique_lock locker(lock);
      unquiesce_count++;
      cv.notify_one();
    }
    bool wait_for_unquiesce(size_t c) {
      std::unique_lock locker(lock);
      return cv.wait_for(locker, std::chrono::seconds(60),
                         [this, c]() { return unquiesce_count >= c; });
    }
  } watcher(image);

  ASSERT_EQ(0, rbd_quiesce_watch(image, Watcher::quiesce_cb,
                                 Watcher::unquiesce_cb, &watcher,
                                 &watcher.handle));

  ASSERT_EQ(0, rbd_group_snap_create(ioctx, group_name, snap_name));
  ASSERT_TRUE(watcher.wait_for_unquiesce(1U));
  ASSERT_EQ(1U, watcher.quiesce_count);

  size_t num_snaps = 0;
  ASSERT_EQ(-ERANGE, rbd_group_snap_list(ioctx, group_name, NULL,
                                         sizeof(rbd_group_snap_info_t),
                                         &num_snaps));
  ASSERT_EQ(1U, num_snaps);

  rbd_group_snap_info_t snaps[1];
  ASSERT_EQ(1, rbd_group_snap_list(ioctx, group_name, snaps,
                                   sizeof(rbd_group_snap_info_t),
                                   &num_snaps));

  ASSERT_STREQ(snap_name, snaps[0].name);

  ASSERT_EQ(10, rbd_write2(image, 9, 10, test_data,
                           LIBRADOS_OP_FLAG_FADVISE_FUA));
  ASSERT_EQ(10, rbd_read(image, 9, 10, read_data));
  ASSERT_EQ(0, memcmp(test_data, read_data, 10));

  ASSERT_EQ(10, rbd_read(image, 0, 10, read_data));
  ASSERT_NE(0, memcmp(orig_data, read_data, 10));
  ASSERT_EQ(0, rbd_group_snap_rollback(ioctx, group_name, snap_name));
  if (!is_feature_enabled(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    ASSERT_EQ(0, rbd_invalidate_cache(image));
  }
  ASSERT_EQ(10, rbd_read(image, 0, 10, read_data));
  ASSERT_EQ(0, memcmp(orig_data, read_data, 10));

  ASSERT_EQ(0, rbd_group_snap_list_cleanup(snaps, sizeof(rbd_group_snap_info_t),
                                           num_snaps));
  ASSERT_EQ(0, rbd_group_snap_remove(ioctx, group_name, snap_name));

  ASSERT_EQ(0, rbd_group_snap_list(ioctx, group_name, snaps,
                                   sizeof(rbd_group_snap_info_t),
                                   &num_snaps));
  ASSERT_EQ(0U, num_snaps);

  ASSERT_EQ(-EINVAL, rbd_group_snap_create2(ioctx, group_name, snap_name,
                                            RBD_SNAP_CREATE_SKIP_QUIESCE |
                                            RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR));
  watcher.r = -EINVAL;
  ASSERT_EQ(-EINVAL, rbd_group_snap_create2(ioctx, group_name, snap_name, 0));

  num_snaps = 1;
  ASSERT_EQ(0, rbd_group_snap_list(ioctx, group_name, snaps,
                                   sizeof(rbd_group_snap_info_t),
                                   &num_snaps));

  watcher.quiesce_count = 0;
  watcher.unquiesce_count = 0;
  ASSERT_EQ(0, rbd_group_snap_create2(ioctx, group_name, snap_name,
                                      RBD_SNAP_CREATE_SKIP_QUIESCE));
  ASSERT_EQ(0U, watcher.quiesce_count);
  num_snaps = 1;
  ASSERT_EQ(1, rbd_group_snap_list(ioctx, group_name, snaps,
                                   sizeof(rbd_group_snap_info_t),
                                   &num_snaps));
  ASSERT_EQ(0, rbd_group_snap_list_cleanup(snaps, sizeof(rbd_group_snap_info_t),
                                           num_snaps));
  ASSERT_EQ(0, rbd_group_snap_remove(ioctx, group_name, snap_name));

  ASSERT_EQ(0, rbd_group_snap_create2(ioctx, group_name, snap_name,
                                      RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR));
  ASSERT_EQ(1, rbd_group_snap_list(ioctx, group_name, snaps,
                                   sizeof(rbd_group_snap_info_t),
                                   &num_snaps));
  ASSERT_EQ(0, rbd_group_snap_list_cleanup(snaps, sizeof(rbd_group_snap_info_t),
                                           num_snaps));
  ASSERT_EQ(0, rbd_group_snap_remove(ioctx, group_name, snap_name));

  ASSERT_EQ(0, rbd_quiesce_unwatch(image, watcher.handle));
  ASSERT_EQ(0, rbd_group_remove(ioctx, group_name));
}

TEST_F(TestGroup, add_snapshotPP)
{
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const char *group_name = "snap_groupPP";
  const char *snap_name = "snap_snapshot";

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, group_name));

  ASSERT_EQ(0, rbd.group_image_add(ioctx, group_name, ioctx,
                                   m_image_name.c_str()));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, m_image_name.c_str(), NULL));
  bufferlist expect_bl;
  bufferlist read_bl;
  expect_bl.append(std::string(512, '1'));
  ASSERT_EQ(512, image.write2(0, expect_bl.length(), expect_bl,
                              LIBRADOS_OP_FLAG_FADVISE_FUA));
  ASSERT_EQ(512, image.read(0, 512, read_bl));
  ASSERT_TRUE(expect_bl.contents_equal(read_bl));

  ASSERT_EQ(0, rbd.group_snap_create(ioctx, group_name, snap_name));

  std::vector<librbd::group_snap_info_t> snaps;
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps,
                                   sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(1U, snaps.size());

  ASSERT_EQ(snap_name, snaps[0].name);

  bufferlist write_bl;
  write_bl.append(std::string(1024, '2'));
  ASSERT_EQ(1024, image.write2(256, write_bl.length(), write_bl,
                               LIBRADOS_OP_FLAG_FADVISE_FUA));
  ASSERT_EQ(1024, image.read(256, 1024, read_bl));
  ASSERT_TRUE(write_bl.contents_equal(read_bl));

  ASSERT_EQ(512, image.read(0, 512, read_bl));
  ASSERT_FALSE(expect_bl.contents_equal(read_bl));
  ASSERT_EQ(0, rbd.group_snap_rollback(ioctx, group_name, snap_name));
  if (!is_feature_enabled(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    ASSERT_EQ(0, image.invalidate_cache());
  }
  ASSERT_EQ(512, image.read(0, 512, read_bl));
  ASSERT_TRUE(expect_bl.contents_equal(read_bl));

  ASSERT_EQ(0, image.close());

  ASSERT_EQ(0, rbd.group_snap_remove(ioctx, group_name, snap_name));

  snaps.clear();
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps,
                                   sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(0U, snaps.size());

  ASSERT_EQ(0, rbd.group_snap_create(ioctx, group_name, snap_name));
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps,
                                   sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(1U, snaps.size());
  ASSERT_EQ(0, rbd.group_snap_remove(ioctx, group_name, snap_name));

  ASSERT_EQ(-EINVAL, rbd.group_snap_create2(ioctx, group_name, snap_name,
                                            RBD_SNAP_CREATE_SKIP_QUIESCE |
                                            RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR));
  snaps.clear();
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps,
                                   sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(0U, snaps.size());

  ASSERT_EQ(0, rbd.group_snap_create2(ioctx, group_name, snap_name,
                                      RBD_SNAP_CREATE_SKIP_QUIESCE));
  snaps.clear();
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps,
                                   sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(1U, snaps.size());

  ASSERT_EQ(0, rbd.group_snap_remove(ioctx, group_name, snap_name));
  ASSERT_EQ(0, rbd.group_remove(ioctx, group_name));
}

TEST_F(TestGroup, snap_get_info)
{
  REQUIRE_FORMAT_V2();

  std::string pool_name2 = get_temp_pool_name("test-librbd-");
  ASSERT_EQ(0, rados_pool_create(_cluster, pool_name2.c_str()));

  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, _pool_name.c_str(), &ioctx));

  rados_ioctx_t ioctx2;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, pool_name2.c_str(), &ioctx2));

  const char *gp_name = "gp_snapgetinfo";
  ASSERT_EQ(0, rbd_group_create(ioctx2, gp_name));

  const char *gp_snap_name = "snap_snapshot";
  ASSERT_EQ(0, rbd_group_snap_create(ioctx2, gp_name, gp_snap_name));

  rbd_group_snap_info2_t gp_snap_info;
  ASSERT_EQ(-ENOENT, rbd_group_snap_get_info(ioctx2, "absent", gp_snap_name,
                                             &gp_snap_info));
  ASSERT_EQ(-ENOENT, rbd_group_snap_get_info(ioctx2, gp_name, "absent",
                                             &gp_snap_info));

  ASSERT_EQ(0, rbd_group_snap_get_info(ioctx2, gp_name, gp_snap_name,
                                       &gp_snap_info));
  ASSERT_STREQ(gp_snap_name, gp_snap_info.name);
  ASSERT_EQ(RBD_GROUP_SNAP_STATE_COMPLETE, gp_snap_info.state);
  ASSERT_EQ(RBD_GROUP_SNAP_NAMESPACE_TYPE_USER, gp_snap_info.namespace_type);
  ASSERT_STREQ("", gp_snap_info.image_snap_name);
  ASSERT_EQ(0U, gp_snap_info.image_snaps_count);

  rbd_group_snap_get_info_cleanup(&gp_snap_info);
  ASSERT_EQ(0, rbd_group_snap_remove(ioctx2, gp_name, gp_snap_name));

  ASSERT_EQ(0, rbd_group_image_add(ioctx2, gp_name, ioctx,
                                   m_image_name.c_str()));
  ASSERT_EQ(0, rbd_group_snap_create(ioctx2, gp_name, gp_snap_name));

  ASSERT_EQ(0, rbd_group_snap_get_info(ioctx2, gp_name, gp_snap_name,
                                       &gp_snap_info));
  ASSERT_STREQ(gp_snap_name, gp_snap_info.name);
  ASSERT_EQ(RBD_GROUP_SNAP_STATE_COMPLETE, gp_snap_info.state);
  ASSERT_EQ(RBD_GROUP_SNAP_NAMESPACE_TYPE_USER, gp_snap_info.namespace_type);
  ASSERT_EQ(1U, gp_snap_info.image_snaps_count);
  ASSERT_EQ(m_image_name, gp_snap_info.image_snaps[0].image_name);
  ASSERT_EQ(rados_ioctx_get_id(ioctx), gp_snap_info.image_snaps[0].pool_id);

  rbd_group_snap_get_info_cleanup(&gp_snap_info);
  ASSERT_EQ(0, rbd_group_snap_remove(ioctx2, gp_name, gp_snap_name));
  ASSERT_EQ(0, rbd_group_remove(ioctx2, gp_name));
  rados_ioctx_destroy(ioctx2);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, rados_pool_delete(_cluster, pool_name2.c_str()));
}

TEST_F(TestGroup, snap_get_infoPP)
{
  REQUIRE_FORMAT_V2();

  std::string pool_name2 = get_temp_pool_name("test-librbd-");
  ASSERT_EQ(0, _rados.pool_create(pool_name2.c_str()));

  librados::IoCtx ioctx2;
  ASSERT_EQ(0, _rados.ioctx_create(pool_name2.c_str(), ioctx2));

  const char *gp_name = "gp_snapgetinfoPP";
  ASSERT_EQ(0, m_rbd.group_create(ioctx2, gp_name));

  const char *gp_snap_name = "snap_snapshot";
  ASSERT_EQ(0, m_rbd.group_snap_create(ioctx2, gp_name, gp_snap_name));

  librbd::group_snap_info2_t gp_snap_info;
  ASSERT_EQ(-ENOENT, m_rbd.group_snap_get_info(ioctx2, "absent", gp_snap_name,
                                               &gp_snap_info));
  ASSERT_EQ(-ENOENT, m_rbd.group_snap_get_info(ioctx2, gp_name, "absent",
                                               &gp_snap_info));

  ASSERT_EQ(0, m_rbd.group_snap_get_info(ioctx2, gp_name, gp_snap_name,
                                         &gp_snap_info));
  ASSERT_EQ(gp_snap_name, gp_snap_info.name);
  ASSERT_EQ(RBD_GROUP_SNAP_STATE_COMPLETE, gp_snap_info.state);
  ASSERT_EQ(RBD_GROUP_SNAP_NAMESPACE_TYPE_USER, gp_snap_info.namespace_type);
  ASSERT_EQ("", gp_snap_info.image_snap_name);
  ASSERT_EQ(0U, gp_snap_info.image_snaps.size());

  ASSERT_EQ(0, m_rbd.group_snap_remove(ioctx2, gp_name, gp_snap_name));

  ASSERT_EQ(0, m_rbd.group_image_add(ioctx2, gp_name, m_ioctx,
                                     m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_snap_create(ioctx2, gp_name, gp_snap_name));

  ASSERT_EQ(0, m_rbd.group_snap_get_info(ioctx2, gp_name, gp_snap_name,
                                         &gp_snap_info));
  ASSERT_EQ(gp_snap_name, gp_snap_info.name);
  ASSERT_EQ(RBD_GROUP_SNAP_STATE_COMPLETE, gp_snap_info.state);
  ASSERT_EQ(RBD_GROUP_SNAP_NAMESPACE_TYPE_USER, gp_snap_info.namespace_type);
  ASSERT_EQ(1U, gp_snap_info.image_snaps.size());
  ASSERT_EQ(m_image_name, gp_snap_info.image_snaps[0].image_name);
  ASSERT_EQ(m_ioctx.get_id(), gp_snap_info.image_snaps[0].pool_id);

  ASSERT_EQ(0, m_rbd.group_snap_remove(ioctx2, gp_name, gp_snap_name));
  ASSERT_EQ(0, m_rbd.group_remove(ioctx2, gp_name));
  ASSERT_EQ(0, _rados.pool_delete(pool_name2.c_str()));
}

TEST_F(TestGroup, snap_list2)
{
  REQUIRE_FORMAT_V2();

  std::string pool_name2 = get_temp_pool_name("test-librbd-");
  ASSERT_EQ(0, rados_pool_create(_cluster, pool_name2.c_str()));

  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, _pool_name.c_str(), &ioctx));

  rados_ioctx_t ioctx2;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, pool_name2.c_str(), &ioctx2));

  std::string image_name2 = get_temp_image_name();
  uint64_t features;
  int order = 0;
  ASSERT_TRUE(get_features(&features));
  ASSERT_EQ(0, rbd_create2(ioctx2, image_name2.c_str(), m_image_size, features,
                           &order));

  const char *gp_name = "gp_snaplist2";
  ASSERT_EQ(0, rbd_group_create(ioctx, gp_name));

  size_t num_snaps = 10U;
  auto gp_snaps = static_cast<rbd_group_snap_info2_t*>(calloc(
    num_snaps, sizeof(rbd_group_snap_info2_t)));
  ASSERT_EQ(-ENOENT, rbd_group_snap_list2(ioctx, "absent", gp_snaps,
                                          &num_snaps));
  ASSERT_EQ(0, rbd_group_snap_list2(ioctx, gp_name, gp_snaps, &num_snaps));
  ASSERT_EQ(0U, num_snaps);

  const char* const gp_snap_names[] = {
    "snap_snapshot0", "snap_snapshot1", "snap_snapshot2", "snap_snapshot3"};
  ASSERT_EQ(0, rbd_group_snap_create(ioctx, gp_name, gp_snap_names[0]));

  ASSERT_EQ(0, rbd_group_image_add(ioctx, gp_name, ioctx,
                                   m_image_name.c_str()));
  ASSERT_EQ(0, rbd_group_snap_create(ioctx, gp_name, gp_snap_names[1]));

  ASSERT_EQ(0, rbd_group_image_add(ioctx, gp_name, ioctx2,
                                   image_name2.c_str()));
  ASSERT_EQ(0, rbd_group_snap_create(ioctx, gp_name, gp_snap_names[2]));

  ASSERT_EQ(0, rbd_group_image_remove(ioctx, gp_name, ioctx,
                                      m_image_name.c_str()));
  ASSERT_EQ(0, rbd_group_snap_create(ioctx, gp_name, gp_snap_names[3]));

  num_snaps = 3U;
  ASSERT_EQ(-ERANGE, rbd_group_snap_list2(ioctx, gp_name, gp_snaps,
                                          &num_snaps));
  ASSERT_EQ(4U, num_snaps);
  ASSERT_EQ(0, rbd_group_snap_list2(ioctx, gp_name, gp_snaps, &num_snaps));
  ASSERT_EQ(4U, num_snaps);

  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(RBD_GROUP_SNAP_STATE_COMPLETE, gp_snaps[i].state);
    ASSERT_EQ(RBD_GROUP_SNAP_NAMESPACE_TYPE_USER, gp_snaps[i].namespace_type);
    if (!strcmp(gp_snaps[i].name, gp_snap_names[0])) {
      ASSERT_EQ(0U, gp_snaps[i].image_snaps_count);
    } else if (!strcmp(gp_snaps[i].name, gp_snap_names[1])) {
      ASSERT_EQ(1U, gp_snaps[i].image_snaps_count);
      ASSERT_EQ(m_image_name, gp_snaps[i].image_snaps[0].image_name);
      ASSERT_EQ(rados_ioctx_get_id(ioctx), gp_snaps[i].image_snaps[0].pool_id);
    } else if (!strcmp(gp_snaps[i].name, gp_snap_names[2])) {
      ASSERT_EQ(2U, gp_snaps[i].image_snaps_count);
      for (int j = 0; j < 2; j++) {
	if (m_image_name == gp_snaps[i].image_snaps[j].image_name) {
	  ASSERT_EQ(rados_ioctx_get_id(ioctx),
                    gp_snaps[i].image_snaps[j].pool_id);
	} else if (image_name2 == gp_snaps[i].image_snaps[j].image_name) {
	  ASSERT_EQ(rados_ioctx_get_id(ioctx2),
                    gp_snaps[i].image_snaps[j].pool_id);
	} else {
          FAIL() << "Unexpected image in group snap: "
                 << gp_snaps[i].image_snaps[j].image_name;
	}
      }
    } else if (!strcmp(gp_snaps[i].name, gp_snap_names[3])) {
      ASSERT_EQ(1U, gp_snaps[i].image_snaps_count);
      ASSERT_EQ(image_name2, gp_snaps[i].image_snaps[0].image_name);
      ASSERT_EQ(rados_ioctx_get_id(ioctx2),
                gp_snaps[i].image_snaps[0].pool_id);
    } else {
      FAIL() << "Unexpected group snap: " << gp_snaps[i].name;
    }
  }

  for (const auto& gp_snap_name : gp_snap_names) {
    ASSERT_EQ(0, rbd_group_snap_remove(ioctx, gp_name, gp_snap_name));
  }
  rbd_group_snap_list2_cleanup(gp_snaps, num_snaps);
  free(gp_snaps);
  ASSERT_EQ(0, rbd_group_snap_list2(ioctx, gp_name, NULL, &num_snaps));
  ASSERT_EQ(0U, num_snaps);
  ASSERT_EQ(0, rbd_group_remove(ioctx, gp_name));
  rados_ioctx_destroy(ioctx2);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, rados_pool_delete(_cluster, pool_name2.c_str()));
}

TEST_F(TestGroup, snap_list2PP)
{
  REQUIRE_FORMAT_V2();

  std::string pool_name2 = get_temp_pool_name("test-librbd-");
  ASSERT_EQ(0, _rados.pool_create(pool_name2.c_str()));

  librados::IoCtx ioctx2;
  ASSERT_EQ(0, _rados.ioctx_create(pool_name2.c_str(), ioctx2));

  std::string image_name2 = get_temp_image_name();
  ASSERT_EQ(0, create_image_pp(m_rbd, ioctx2, image_name2.c_str(),
                               m_image_size));

  const char *gp_name = "gp_snaplist2PP";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, gp_name));

  std::vector<librbd::group_snap_info2_t> gp_snaps;
  ASSERT_EQ(-ENOENT, m_rbd.group_snap_list2(m_ioctx, "absent", &gp_snaps));
  ASSERT_EQ(0, m_rbd.group_snap_list2(m_ioctx, gp_name, &gp_snaps));
  ASSERT_EQ(0U, gp_snaps.size());

  const char* const gp_snap_names[] = {
    "snap_snapshot0", "snap_snapshot1", "snap_snapshot2", "snap_snapshot3"};

  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, gp_name, gp_snap_names[0]));

  ASSERT_EQ(0, m_rbd.group_image_add(m_ioctx, gp_name, m_ioctx,
                                     m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, gp_name, gp_snap_names[1]));

  ASSERT_EQ(0, m_rbd.group_image_add(m_ioctx, gp_name, ioctx2,
                                     image_name2.c_str()));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, gp_name, gp_snap_names[2]));

  ASSERT_EQ(0, m_rbd.group_image_remove(m_ioctx, gp_name,
                                        m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, gp_name, gp_snap_names[3]));

  ASSERT_EQ(0, m_rbd.group_snap_list2(m_ioctx, gp_name, &gp_snaps));
  ASSERT_EQ(4U, gp_snaps.size());

  for (const auto& gp_snap : gp_snaps) {
    ASSERT_EQ(RBD_GROUP_SNAP_STATE_COMPLETE, gp_snap.state);
    ASSERT_EQ(RBD_GROUP_SNAP_NAMESPACE_TYPE_USER, gp_snap.namespace_type);
    if (gp_snap.name == gp_snap_names[0]) {
      ASSERT_EQ(0U, gp_snap.image_snaps.size());
    } else if (gp_snap.name == gp_snap_names[1]) {
      ASSERT_EQ(1U, gp_snap.image_snaps.size());
      ASSERT_EQ(m_image_name, gp_snap.image_snaps[0].image_name);
      ASSERT_EQ(m_ioctx.get_id(), gp_snap.image_snaps[0].pool_id);
    } else if (gp_snap.name == gp_snap_names[2]) {
      ASSERT_EQ(2U, gp_snap.image_snaps.size());
      for (const auto& image_snap : gp_snap.image_snaps) {
	if (image_snap.image_name == m_image_name) {
	  ASSERT_EQ(m_ioctx.get_id(), image_snap.pool_id);
	} else if (image_snap.image_name == image_name2) {
	  ASSERT_EQ(ioctx2.get_id(), image_snap.pool_id);
	} else {
          FAIL() << "Unexpected image in group snap: "
                 << image_snap.image_name;
	}
      }
    } else if (gp_snap.name == gp_snap_names[3]) {
      ASSERT_EQ(1U, gp_snap.image_snaps.size());
      ASSERT_EQ(image_name2, gp_snap.image_snaps[0].image_name);
      ASSERT_EQ(ioctx2.get_id(), gp_snap.image_snaps[0].pool_id);
    } else {
      FAIL() << "Unexpected group snap: " << gp_snap.name;
    }
  }

  for (const auto& gp_snap_name : gp_snap_names) {
    ASSERT_EQ(0, m_rbd.group_snap_remove(m_ioctx, gp_name, gp_snap_name));
  }
  std::vector<librbd::group_snap_info2_t> gp_snaps2;
  ASSERT_EQ(0, m_rbd.group_snap_list2(m_ioctx, gp_name, &gp_snaps2));
  ASSERT_EQ(0U, gp_snaps2.size());
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, gp_name));
  ASSERT_EQ(0, _rados.pool_delete(pool_name2.c_str()));
}

TEST_F(TestGroup, snap_list_internal)
{
  REQUIRE_FORMAT_V2();

  // Check that the listing works with different
  // values for try_to_sort and fail_if_not_sorted

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const char *group_name = "gp_snaplist_internalPP";

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, group_name));

  std::vector<librbd::group_snap_info2_t> gp_snaps;

  // No snaps present
  ASSERT_EQ(0, librbd::api::Group<>::snap_list(ioctx, group_name, true, true,
                                               &gp_snaps));
  ASSERT_EQ(0U, gp_snaps.size());

  ASSERT_EQ(0, librbd::api::Group<>::snap_list(ioctx, group_name, false, false,
                                               &gp_snaps));
  ASSERT_EQ(0U, gp_snaps.size());

  // Create a stale snap_order key by deleting the snapshot_ key
  ASSERT_EQ(0, librbd::api::Group<>::snap_create(ioctx, group_name,
                                                 "test-snap", 0));
  ASSERT_EQ(0, librbd::api::Group<>::snap_list(ioctx, group_name, false, false,
                                               &gp_snaps));
  ASSERT_EQ(1U, gp_snaps.size());

  std::string group_id;
  ASSERT_EQ(0, librbd::api::Group<>::get_id(ioctx, group_name, &group_id));

  std::string group_header = RBD_GROUP_HEADER_PREFIX + group_id;
  std::set<std::string> keys = {"snapshot_" + gp_snaps[0].id};
  ASSERT_EQ(0, ioctx.omap_rm_keys(group_header, keys));

  for (int i = 0; i < 20; i++) {
    std::string name = "snap" + stringify(i);
    ASSERT_EQ(0, librbd::api::Group<>::snap_create(ioctx, group_name,
                                                   name.c_str(), 0));
  }

  ASSERT_EQ(0, librbd::api::Group<>::snap_list(ioctx, group_name, true, true,
                                               &gp_snaps));
  ASSERT_EQ(20U, gp_snaps.size());

  // Verify that the sorted list is correct
  for (size_t i = 0; i < gp_snaps.size(); i++){
    std::string name = "snap" + stringify(i);
    ASSERT_EQ(name, gp_snaps[i].name);
  }

  // Sort on group snap ids to simulate the unsorted list.
  std::vector<librbd::group_snap_info2_t> snaps_sorted_by_id = gp_snaps;
  std::sort(snaps_sorted_by_id.begin(), snaps_sorted_by_id.end(),
            [](const librbd::group_snap_info2_t &a,
	       const librbd::group_snap_info2_t &b) {
	      return a.id < b.id;
	    });

  // Check that the vectors actually differ
  bool differ = false;
  for (size_t i = 0; i < gp_snaps.size(); i++) {
    if (gp_snaps[i].id != snaps_sorted_by_id[i].id) {
      differ = true;
      break;
    }
  }
  ASSERT_TRUE(differ);

  // Remove the snap_order key for one of the snaps.
  keys = {"snap_order_" + gp_snaps[1].id};
  ASSERT_EQ(0, ioctx.omap_rm_keys(group_header, keys));

  //This should fail.
  ASSERT_EQ(-EINVAL, librbd::api::Group<>::snap_list(ioctx, group_name, true,
                                                     true, &gp_snaps));

  // Should work if fail_if_not_sorted is false
  ASSERT_EQ(0, librbd::api::Group<>::snap_list(ioctx, group_name, true, false,
                                               &gp_snaps));
  ASSERT_EQ(20U, gp_snaps.size());

  ASSERT_EQ(0, librbd::api::Group<>::snap_list(ioctx, group_name, false, false,
                                               &gp_snaps));
  ASSERT_EQ(20U, gp_snaps.size());

  //Compare unsorted listing
  for (size_t i = 0; i < gp_snaps.size(); i++){
    ASSERT_EQ(snaps_sorted_by_id[i].id, gp_snaps[i].id);
  }

  ASSERT_EQ(0, rbd.group_remove(ioctx, group_name));
}
