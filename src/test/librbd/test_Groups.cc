// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd/librbd.h"
#include "include/rbd/librbd.hpp"
#include "test/librados/test.h"
#include "gtest/gtest.h"

#include <boost/scope_exit.hpp>
#include <chrono>
#include <vector>

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

  librbd::RBD rbd;
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
  ASSERT_EQ(0, rbd.group_create(ioctx, "mygroup"));

  std::vector<std::string> groups;
  ASSERT_EQ(0, rbd.group_list(ioctx, &groups));
  ASSERT_EQ(1U, groups.size());
  ASSERT_EQ("mygroup", groups[0]);

  groups.clear();
  ASSERT_EQ(0, rbd.group_rename(ioctx, "mygroup", "newgroup"));
  ASSERT_EQ(0, rbd.group_list(ioctx, &groups));
  ASSERT_EQ(1U, groups.size());
  ASSERT_EQ("newgroup", groups[0]);

  ASSERT_EQ(0, rbd.group_remove(ioctx, "newgroup"));

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

  const char *group_name = "mycg";
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

  ASSERT_EQ(10, rbd_write(image, 0, 10, orig_data));
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

  ASSERT_EQ(10, rbd_write(image, 9, 10, test_data));
  ASSERT_EQ(10, rbd_read(image, 9, 10, read_data));
  ASSERT_EQ(0, memcmp(test_data, read_data, 10));

  ASSERT_EQ(10, rbd_read(image, 0, 10, read_data));
  ASSERT_NE(0, memcmp(orig_data, read_data, 10));
  ASSERT_EQ(0, rbd_group_snap_rollback(ioctx, group_name, snap_name));
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

  const char *group_name = "snap_group";
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
  ASSERT_EQ((ssize_t)expect_bl.length(), image.write(0, expect_bl.length(), expect_bl));
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
  ASSERT_EQ(1024, image.write(256, write_bl.length(), write_bl));
  ASSERT_EQ(1024, image.read(256, 1024, read_bl));
  ASSERT_TRUE(write_bl.contents_equal(read_bl));

  ASSERT_EQ(512, image.read(0, 512, read_bl));
  ASSERT_FALSE(expect_bl.contents_equal(read_bl));
  ASSERT_EQ(0, rbd.group_snap_rollback(ioctx, group_name, snap_name));
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
