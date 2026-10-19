// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd/librbd.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include "librbd/api/Group.h"
#include "librbd/group/AddImageRequest.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/group/MutationLock.h"
#include "librbd/group/RemoveImageRequest.h"
#include "librbd/mirror/GroupPromoteRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
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
protected:
  void check_force_promote_members(bool overlap);

  int list_group_snapshots(const std::string& group_id,
                            std::vector<cls::rbd::GroupSnapshot>* snaps) {
    C_SaferCond cond;
    auto req = librbd::group::ListSnapshotsRequest<>::create(
      m_ioctx, group_id, false, false, snaps, &cond);
    req->send();
    return cond.wait();
  }
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
  ASSERT_EQ(-ENOENT, rbd_group_image_list(ioctx, "group_does_not_exist",
                                          images,
                                          sizeof(rbd_group_image_info_t),
                                          &num_images));
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
  ASSERT_EQ(-ENOENT, rbd.group_image_list(ioctx, "group_does_not_exist",
                                          &images,
                                          sizeof(librbd::group_image_info_t)));
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
  ASSERT_EQ(-ENOENT, rbd_group_snap_list(ioctx, "group_does_not_exist", snaps,
                                         sizeof(rbd_group_snap_info_t),
                                         &num_snaps));
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
  ASSERT_EQ(-ENOENT, rbd.group_snap_list(ioctx, "group_does_not_exist", &snaps,
                                         sizeof(librbd::group_snap_info_t)));
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
  ASSERT_EQ(RBD_GROUP_SNAP_STATE_CREATED, gp_snap_info.state);
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
  ASSERT_EQ(RBD_GROUP_SNAP_STATE_CREATED, gp_snap_info.state);
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
  ASSERT_EQ(RBD_GROUP_SNAP_STATE_CREATED, gp_snap_info.state);
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
  ASSERT_EQ(RBD_GROUP_SNAP_STATE_CREATED, gp_snap_info.state);
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
    ASSERT_EQ(RBD_GROUP_SNAP_STATE_CREATED, gp_snaps[i].state);
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

  ASSERT_EQ(0, m_rbd.group_image_remove(m_ioctx, gp_name, m_ioctx,
                                        m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, gp_name, gp_snap_names[3]));

  ASSERT_EQ(0, m_rbd.group_snap_list2(m_ioctx, gp_name, &gp_snaps));
  ASSERT_EQ(4U, gp_snaps.size());

  for (const auto& gp_snap : gp_snaps) {
    ASSERT_EQ(RBD_GROUP_SNAP_STATE_CREATED, gp_snap.state);
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

// Test sequence:
// 1. Create a non-mirrored group and a standalone mirrored image.
// 2. Try adding the image to the group.
// Expected: The add fails and the group and image mirror states are preserved.
TEST_F(TestGroup, mirrorAddEnabledImageToDisabledGroupPP) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  const char *group_name = "mirror_disabled_group";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, group_name));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));
  ASSERT_EQ(0,
    image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));

  librbd::mirror_image_info_t mirror_info;
  ASSERT_EQ(0, image.mirror_image_get_info(&mirror_info, sizeof(mirror_info)));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, mirror_info.state);

  ASSERT_EQ(-EINVAL, m_rbd.group_image_add(
    m_ioctx, group_name, m_ioctx, m_image_name.c_str()));

  std::vector<librbd::group_image_info_t> images;
  ASSERT_EQ(0, m_rbd.group_image_list(m_ioctx, group_name, &images,
                                      sizeof(librbd::group_image_info_t)));
  ASSERT_TRUE(images.empty());

  ASSERT_EQ(0, image.mirror_image_disable(false));
  image.close();
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, group_name));
}

// Test sequence:
// 1. Enable group mirroring and leave the image mirroring disabled.
// 2. Try adding that image to the mirror group.
// Expected: The add fails without enabling image mirroring or changing
//   membership.
TEST_F(TestGroup, mirrorAddDisabledImageToEnabledGroupPP) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(
    m_ioctx, &peer_uuid, RBD_MIRROR_PEER_DIRECTION_RX_TX, "cluster",
    "client"));

  const char *group_name = "mirror_enabled_group";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.mirror_group_enable(
    m_ioctx, group_name, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));

  ASSERT_EQ(-EINVAL, m_rbd.group_image_add(
    m_ioctx, group_name, m_ioctx, m_image_name.c_str()));

  std::vector<librbd::group_image_info_t> images;
  ASSERT_EQ(0, m_rbd.group_image_list(m_ioctx, group_name, &images,
                                      sizeof(librbd::group_image_info_t)));
  ASSERT_TRUE(images.empty());

  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, group_name, false));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
}

// Test sequence:
// 1. Create a primary mirror group and demote a standalone mirrored image.
// 2. Try attaching the secondary image to the primary group.
// Expected: Read-only enforcement rejects the add and the image is not a group
//   member.
TEST_F(TestGroup, mirrorAddSecondaryImageToPrimaryGroupPP) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(
    m_ioctx, &peer_uuid, RBD_MIRROR_PEER_DIRECTION_RX_TX, "cluster",
    "client"));

  const char *group_name = "mirror_primary_group";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.mirror_group_enable(
    m_ioctx, group_name, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));
  ASSERT_EQ(0,
    image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  ASSERT_EQ(0, image.mirror_image_demote());

  ASSERT_EQ(-EINVAL, m_rbd.group_image_add(
    m_ioctx, group_name, m_ioctx, m_image_name.c_str()));

  std::vector<librbd::group_image_info_t> images;
  ASSERT_EQ(0, m_rbd.group_image_list(m_ioctx, group_name, &images,
                                      sizeof(librbd::group_image_info_t)));
  ASSERT_TRUE(images.empty());

  ASSERT_EQ(0, image.mirror_image_disable(true));
  image.close();
  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, group_name, false));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
}

void TestGroup::check_force_promote_members(bool overlap) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(
    m_ioctx, &peer_uuid, RBD_MIRROR_PEER_DIRECTION_RX_TX, "cluster", "client"));

  const char* group_name = "force_promote_members";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, group_name, m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_group_enable(
    m_ioctx, group_name, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  ASSERT_EQ(0, m_rbd.mirror_group_demote(m_ioctx, group_name));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));
  std::string image_id;
  ASSERT_EQ(0, image.get_id(&image_id));
  cls::rbd::MirrorImage mirror_image;
  ASSERT_EQ(0, librbd::cls_client::mirror_image_get(
    &m_ioctx, image_id, &mirror_image));
  const auto global_image_id = mirror_image.global_image_id;
  // An image replayed standalone can join the secondary's live membership
  // before its mirror type changes to GROUP.
  mirror_image.type = cls::rbd::MIRROR_IMAGE_TYPE_STANDALONE;
  ASSERT_EQ(0, librbd::cls_client::mirror_image_set(
    &m_ioctx, image_id, mirror_image));

  std::string extra_image_id;
  C_SaferCond promote_done;
  C_SaferCond mutation_done;
  Context* finish_mutation = nullptr;
  const auto extra_image_name = m_image_name + "_extra";
  if (overlap) {
    ASSERT_EQ(0, create_image_pp(
      m_rbd, m_ioctx, extra_image_name.c_str(), m_image_size));
    librbd::Image extra_image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, extra_image,
                            extra_image_name.c_str()));
    ASSERT_EQ(0, extra_image.mirror_image_enable2(
      RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
    ASSERT_EQ(0, extra_image.mirror_image_demote());
    ASSERT_EQ(0, extra_image.get_id(&extra_image_id));
    std::string group_id;
    ASSERT_EQ(0, m_rbd.group_get_id(m_ioctx, group_name, &group_id));
    C_SaferCond locked;
    librbd::group::execute_mutation(m_ioctx, group_id, true, false,
      [&](int r, Context* on_finish) {
        finish_mutation = on_finish;
        locked.complete(r);
      }, &mutation_done);
    int r = locked.wait();
    if (r < 0) {
      finish_mutation->complete(r);
      mutation_done.wait();
    }
    ASSERT_EQ(0, r);

    auto promote = librbd::mirror::GroupPromoteRequest<>::create(
      m_ioctx, group_id, group_name, true, &promote_done);
    promote->send();
    EXPECT_EQ(ETIMEDOUT, promote_done.wait_for(0.05));
    // Bootstrap updates live membership before saving the next snapshot.
    C_SaferCond cond;
    auto req = librbd::group::AddImageRequest<>::create(
      m_ioctx, group_id, m_ioctx, extra_image_id, &cond);
    req->send();
    r = cond.wait();
    finish_mutation->complete(r);
    ASSERT_EQ(0, mutation_done.wait());
    ASSERT_EQ(0, promote_done.wait_for(30));
    ASSERT_EQ(0, r);
  }

  if (!overlap) {
    ASSERT_EQ(0, m_rbd.mirror_group_promote(m_ioctx, group_name, true));
  } else {
    std::string group_id;
    ASSERT_EQ(0, m_rbd.group_get_id(m_ioctx, group_name, &group_id));
    C_SaferCond add_done;
    auto add = librbd::group::AddImageRequest<>::create(
      m_ioctx, group_id, m_ioctx, extra_image_id, &add_done, true);
    add->send();
    ASSERT_EQ(-EAGAIN, add_done.wait());
    C_SaferCond remove_done;
    auto remove = librbd::group::RemoveImageRequest<>::create(
      m_ioctx, group_id, m_ioctx, image_id, &remove_done, true);
    remove->send();
    ASSERT_EQ(-EAGAIN, remove_done.wait());
  }
  std::vector<librbd::group_image_info_t> members;
  ASSERT_EQ(0, m_rbd.group_image_list(m_ioctx, group_name, &members,
                                      sizeof(members[0])));
  ASSERT_EQ(1U, members.size());
  ASSERT_EQ(m_image_name, members[0].name);
  ASSERT_EQ(0, librbd::cls_client::mirror_image_get(
    &m_ioctx, image_id, &mirror_image));
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_TYPE_GROUP, mirror_image.type);
  ASSERT_EQ(global_image_id, mirror_image.global_image_id);
  if (overlap) {
    ASSERT_EQ(0, librbd::cls_client::mirror_image_get(
      &m_ioctx, extra_image_id, &mirror_image));
    ASSERT_EQ(cls::rbd::MIRROR_IMAGE_TYPE_STANDALONE, mirror_image.type);
  }

  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, group_name, false));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, group_name));
  if (overlap) {
    librbd::Image extra_image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, extra_image,
                            extra_image_name.c_str()));
    ASSERT_EQ(0, extra_image.mirror_image_disable(true));
    ASSERT_EQ(0, extra_image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, extra_image_name.c_str()));
  }
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
}

// Test sequence:
// 1. Demote a mirrored group and leave its member with standalone mirror type.
// 2. Force-promote it without any incomplete group snapshot.
// Expected: The saved member regains GROUP type and keeps its global image ID.
TEST_F(TestGroup, mirrorForcePromoteRepairsMemberTypePP) {
  check_force_promote_members(false);
}

// Test sequence:
// 1. Hold an active replay mutation and start force promotion.
// 2. Finish its member add, then release the mutation for promotion to continue.
// 3. Try queued replay adds and removals after the group becomes primary.
// Expected: Promotion waits, repairs member types and saved membership, and
//   rejects late writes.
TEST_F(TestGroup, mirrorForcePromoteWaitsForMembershipPP) {
  check_force_promote_members(true);
}

// Test sequence:
// 1. Create a non-mirrored group with a saved user snapshot.
// 2. Remove its image by name and ID while mirror group metadata is absent.
// Expected: Removal succeeds and the saved snapshot still references the image
//   snapshot.
TEST_F(TestGroup, removeImageWithUserGroupSnapshotsPP) {
  REQUIRE_FORMAT_V2();

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));
  std::string image_id;
  ASSERT_EQ(0, image.get_id(&image_id));

  for (bool by_id : {false, true}) {
    SCOPED_TRACE(::testing::Message() << "by_id=" << by_id);
    const char* group_name = "snapshot_dependency_group";
    ASSERT_EQ(0, m_rbd.group_create(m_ioctx, group_name));
    ASSERT_EQ(0, m_rbd.group_image_add(
      m_ioctx, group_name, m_ioctx, m_image_name.c_str()));
    ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, group_name, "saved"));

    std::string group_id;
    ASSERT_EQ(0, m_rbd.group_get_id(m_ioctx, group_name, &group_id));

    if (by_id) {
      ASSERT_EQ(0, m_rbd.group_image_remove_by_id(
        m_ioctx, group_name, m_ioctx, image_id.c_str()));
    } else {
      ASSERT_EQ(0, m_rbd.group_image_remove(
        m_ioctx, group_name, m_ioctx, m_image_name.c_str()));
    }

    std::vector<librbd::group_image_info_t> members;
    ASSERT_EQ(0, m_rbd.group_image_list(
      m_ioctx, group_name, &members, sizeof(librbd::group_image_info_t)));
    ASSERT_TRUE(members.empty());
    std::vector<cls::rbd::GroupSnapshot> snaps;
    ASSERT_EQ(0, list_group_snapshots(group_id, &snaps));
    ASSERT_EQ(1U, snaps.size());
    ASSERT_EQ(1U, snaps[0].snaps.size());
    ASSERT_EQ(image_id, snaps[0].snaps[0].image_id);
    cls::rbd::SnapshotInfo image_snap;
    ASSERT_EQ(0, librbd::cls_client::snapshot_get(
      &m_ioctx, librbd::util::header_name(image_id),
      snaps[0].snaps[0].snap_id, &image_snap));
    ASSERT_TRUE(std::holds_alternative<
      cls::rbd::ImageSnapshotNamespaceGroup>(image_snap.snapshot_namespace));

    ASSERT_EQ(0, m_rbd.group_snap_remove(m_ioctx, group_name, "saved"));
    ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, group_name));
  }
}

// Test sequence:
// 1. Detach an image that has mirror and user group snapshots.
// 2. Try attach before acknowledgement and after acknowledgement, then remove
//   the mirror snapshots and retry.
// 3. Leave the attached image GROUP with an incomplete membership snapshot,
//   as if attach stopped before saving completion, and retry the add twice.
// Expected: User snapshots alone allow attach, the global ID is kept, and
//   retry saves a complete snapshot and leaves exactly one member. Mirror
//   snapshot references block attach.
TEST_F(TestGroup, mirrorAttachWaitsForMirrorGroupSnapshotCleanupPP) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(
    m_ioctx, &peer_uuid, RBD_MIRROR_PEER_DIRECTION_RX_TX, "cluster", "client"));
  const char* source = "source_group";
  const char* target = "target_group";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, source));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, source, m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_group_enable(
    m_ioctx, source, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, source, "saved"));
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, target));
  ASSERT_EQ(0, m_rbd.mirror_group_enable(
    m_ioctx, target, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));
  std::string image_id;
  ASSERT_EQ(0, image.get_id(&image_id));
  librbd::mirror_image_info_t before;
  ASSERT_EQ(0, image.mirror_image_get_info(&before, sizeof(before)));
  ASSERT_EQ(0, m_rbd.group_image_remove(
    m_ioctx, source, m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(-EBUSY, m_rbd.group_image_add(
    m_ioctx, source, m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(-EBUSY, m_rbd.group_image_add(
    m_ioctx, target, m_ioctx, m_image_name.c_str()));

  std::string group_id;
  ASSERT_EQ(0, m_rbd.group_get_id(m_ioctx, source, &group_id));
  std::vector<cls::rbd::GroupSnapshot> snaps;
  ASSERT_EQ(0, list_group_snapshots(group_id, &snaps));
  size_t removed = 0;
  for (auto& snap : snaps) {
    if (!std::holds_alternative<cls::rbd::GroupSnapshotNamespaceMirror>(
          snap.snapshot_namespace) ||
        std::none_of(snap.snaps.begin(), snap.snaps.end(),
          [&image_id](const auto& spec) { return spec.image_id == image_id; })) {
      continue;
    }
    auto& ns = std::get<cls::rbd::GroupSnapshotNamespaceMirror>(
      snap.snapshot_namespace);
    ns.mirror_peer_uuids.clear();
    ASSERT_EQ(0, librbd::cls_client::group_snap_set(
      &m_ioctx, librbd::util::group_header_name(group_id), snap));
    // Peer acknowledgement permits disable, but attach still needs cleanup.
    ASSERT_EQ(-EBUSY, m_rbd.group_image_add(
      m_ioctx, target, m_ioctx, m_image_name.c_str()));
    std::vector<librbd::ImageCtx*> image_ctxs;
    C_SaferCond cond;
    auto req = librbd::mirror::snapshot::RemoveGroupSnapshotRequest<>::create(
      m_ioctx, group_id, &snap, &image_ctxs, &cond);
    req->send();
    ASSERT_EQ(0, cond.wait());
    ++removed;
  }
  ASSERT_GT(removed, 0U);
  auto saved = std::find_if(snaps.begin(), snaps.end(), [](const auto& snap) {
    return std::holds_alternative<cls::rbd::GroupSnapshotNamespaceUser>(
      snap.snapshot_namespace);
  });
  ASSERT_NE(snaps.end(), saved);
  ASSERT_EQ(1U, saved->snaps.size());
  cls::rbd::SnapshotInfo user_snap;
  ASSERT_EQ(0, librbd::cls_client::snapshot_get(
    &m_ioctx, librbd::util::header_name(image_id), saved->snaps[0].snap_id,
    &user_snap));
  ASSERT_TRUE(std::holds_alternative<cls::rbd::ImageSnapshotNamespaceGroup>(
    user_snap.snapshot_namespace));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, target, m_ioctx, m_image_name.c_str()));
  librbd::mirror_image_info_t after;
  ASSERT_EQ(0, image.mirror_image_get_info(&after, sizeof(after)));
  ASSERT_EQ(before.global_id, after.global_id);

  // Recreate the on-disk state after the type changed but before completion.
  std::string target_id;
  ASSERT_EQ(0, m_rbd.group_get_id(m_ioctx, target, &target_id));
  std::vector<cls::rbd::GroupSnapshot> target_snaps;
  ASSERT_EQ(0, list_group_snapshots(target_id, &target_snaps));
  auto attached = std::find_if(target_snaps.begin(), target_snaps.end(),
    [&image_id](const auto& snap) {
      return snap.snaps.size() == 1 && snap.snaps[0].image_id == image_id;
    });
  ASSERT_NE(target_snaps.end(), attached);
  const auto incomplete_id = attached->id;
  const auto header_oid = librbd::util::group_header_name(target_id);
  ASSERT_EQ(0, librbd::cls_client::group_snap_remove(
    &m_ioctx, header_oid, attached->id));
  attached->state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;
  attached->snaps[0].snap_id = CEPH_NOSNAP;
  std::get<cls::rbd::GroupSnapshotNamespaceMirror>(
    attached->snapshot_namespace).complete =
      cls::rbd::MIRROR_GROUP_SNAPSHOT_INCOMPLETE;
  ASSERT_EQ(0, librbd::cls_client::group_snap_set(
    &m_ioctx, header_oid, *attached));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, target, m_ioctx, m_image_name.c_str()));
  target_snaps.clear();
  ASSERT_EQ(0, list_group_snapshots(target_id, &target_snaps));
  ASSERT_TRUE(std::any_of(target_snaps.begin(), target_snaps.end(),
    [&image_id, &incomplete_id](const auto& snap) {
      auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &snap.snapshot_namespace);
      return ns != nullptr && snap.id != incomplete_id &&
        is_mirror_group_snapshot_complete(snap.state, ns->complete) &&
        snap.snaps.size() == 1 && snap.snaps[0].image_id == image_id &&
        snap.snaps[0].snap_id != CEPH_NOSNAP;
    }));
  // A retry must accept the membership and snapshots created by this attach.
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, target, m_ioctx, m_image_name.c_str()));
  std::vector<librbd::group_image_info_t> members;
  ASSERT_EQ(0, m_rbd.group_image_list(
    m_ioctx, target, &members, sizeof(members[0])));
  ASSERT_EQ(1U, members.size());
  ASSERT_EQ(m_image_name, members[0].name);

  // The old user group snapshot remains valid after attachment elsewhere.
  ASSERT_EQ(0, m_rbd.group_snap_remove(m_ioctx, source, "saved"));
  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, target, false));
  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, source, false));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, target));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, source));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
}

// Test sequence:
// 1. Detach an image with a retained user group snapshot.
// 2. Try enabling standalone mirroring while that snapshot remains.
// 3. Remove the group snapshot and retry enabling mirroring.
// Expected: Enable returns -EINVAL until the group snapshot is removed.
TEST_F(TestGroup, mirrorEnableRejectsUserGroupSnapshotsPP) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(
    m_ioctx, &peer_uuid, RBD_MIRROR_PEER_DIRECTION_RX_TX, "cluster", "client"));
  const char* group_name = "source_group";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, group_name, m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, group_name, "saved"));
  ASSERT_EQ(0, m_rbd.group_image_remove(
    m_ioctx, group_name, m_ioctx, m_image_name.c_str()));
  std::string group_id;
  ASSERT_EQ(0, m_rbd.group_get_id(m_ioctx, group_name, &group_id));
  std::vector<cls::rbd::GroupSnapshot> saved;
  ASSERT_EQ(0, list_group_snapshots(group_id, &saved));
  ASSERT_EQ(1U, saved.size());
  ASSERT_EQ(1U, saved[0].snaps.size());

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));
  ASSERT_EQ(-EINVAL, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  cls::rbd::SnapshotInfo user_snap;
  ASSERT_EQ(0, librbd::cls_client::snapshot_get(
    &m_ioctx, librbd::util::header_name(saved[0].snaps[0].image_id),
    saved[0].snaps[0].snap_id, &user_snap));
  ASSERT_TRUE(std::holds_alternative<cls::rbd::ImageSnapshotNamespaceGroup>(
    user_snap.snapshot_namespace));
  librbd::mirror_image_info_t info;
  ASSERT_EQ(0, image.mirror_image_get_info(&info, sizeof(info)));
  ASSERT_EQ(RBD_MIRROR_IMAGE_DISABLED, info.state);

  ASSERT_EQ(0, m_rbd.group_snap_remove(m_ioctx, group_name, "saved"));
  ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  ASSERT_EQ(0, image.mirror_image_disable(false));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
}

// Test sequence:
// 1. Detach an image with user and mirror group snapshots and two pending
//   peers.
// 2. Try normal and forced disable before acknowledging both group peers.
// 3. Retry with image peers still pending, then acknowledge those peers too.
// 4. Check a pending group peer with no image peers, then a missing group
//   snapshot member whose image snapshot still exists.
// Expected: Disable returns -EBUSY while either kind of peer remains. It then
//   waits for missing image references to be repaired, removes the mirror group
//   snapshots and preserves the user group snapshot.
TEST_F(TestGroup, mirrorDisableWaitsForGroupSnapshotPeersPP) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer1;
  std::string peer2;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(
    m_ioctx, &peer1, RBD_MIRROR_PEER_DIRECTION_RX_TX, "cluster1", "client"));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(
    m_ioctx, &peer2, RBD_MIRROR_PEER_DIRECTION_RX_TX, "cluster2", "client"));
  const char* group_name = "source_group";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, group_name, m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_group_enable(
    m_ioctx, group_name, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, group_name, "saved"));
  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));
  std::string image_id;
  std::string group_id;
  ASSERT_EQ(0, image.get_id(&image_id));
  ASSERT_EQ(0, m_rbd.group_get_id(m_ioctx, group_name, &group_id));
  ASSERT_EQ(0, m_rbd.group_image_remove(
    m_ioctx, group_name, m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(-EBUSY, image.mirror_image_disable(false));
  ASSERT_EQ(-EBUSY, image.mirror_image_disable(true));

  std::vector<cls::rbd::GroupSnapshot> snaps;
  ASSERT_EQ(0, list_group_snapshots(group_id, &snaps));
  size_t acknowledged = 0;
  for (const auto& peer : {peer1, peer2}) {
    for (auto& snap : snaps) {
      auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &snap.snapshot_namespace);
      if (ns == nullptr || std::none_of(snap.snaps.begin(), snap.snaps.end(),
            [&image_id](const auto& spec) {
              return spec.image_id == image_id;
            })) {
        continue;
      }
      ASSERT_EQ(1U, ns->mirror_peer_uuids.erase(peer));
      // Simulate acknowledgements without removing the group snapshot.
      ASSERT_EQ(0, librbd::cls_client::group_snap_set(
        &m_ioctx, librbd::util::group_header_name(group_id), snap));
      ++acknowledged;
    }
    ASSERT_GT(acknowledged, 0U);
    if (peer == peer1) {
      ASSERT_EQ(-EBUSY, image.mirror_image_disable(false));
      ASSERT_EQ(-EBUSY, image.mirror_image_disable(true));
    }
  }

  // The group is still enabled and its saved snapshots still exist.
  cls::rbd::MirrorGroup mirror_group;
  ASSERT_EQ(0, librbd::cls_client::mirror_group_get(
    &m_ioctx, group_id, &mirror_group));
  ASSERT_EQ(cls::rbd::MIRROR_GROUP_STATE_ENABLED, mirror_group.state);
  ASSERT_EQ(-EBUSY, image.mirror_image_disable(false));
  ASSERT_EQ(-EBUSY, image.mirror_image_disable(true));
  size_t image_snap_count = 0;
  for (const auto& snap : snaps) {
    if (!std::holds_alternative<cls::rbd::GroupSnapshotNamespaceMirror>(
          snap.snapshot_namespace)) {
      continue;
    }
    for (const auto& spec : snap.snaps) {
      if (spec.image_id != image_id || spec.snap_id == CEPH_NOSNAP) {
        continue;
      }
      for (const auto& peer : {peer1, peer2}) {
        ASSERT_EQ(0, librbd::cls_client::mirror_image_snapshot_unlink_peer(
          &m_ioctx, librbd::util::header_name(image_id), spec.snap_id, peer));
      }
      ++image_snap_count;
    }
  }
  ASSERT_GT(image_snap_count, 0U);
  // The direct CLS updates above do not send image update notifications.
  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));

  auto pending = std::find_if(snaps.begin(), snaps.end(),
    [&image_id](const auto& snap) {
      return std::holds_alternative<cls::rbd::GroupSnapshotNamespaceMirror>(
               snap.snapshot_namespace) &&
        std::any_of(snap.snaps.begin(), snap.snaps.end(),
          [&image_id](const auto& spec) { return spec.image_id == image_id; });
    });
  ASSERT_NE(snaps.end(), pending);
  auto& pending_ns = std::get<cls::rbd::GroupSnapshotNamespaceMirror>(
    pending->snapshot_namespace);
  pending_ns.mirror_peer_uuids.insert(peer1);
  ASSERT_EQ(0, librbd::cls_client::group_snap_set(
    &m_ioctx, librbd::util::group_header_name(group_id), *pending));
  ASSERT_EQ(-EBUSY, image.mirror_image_disable(false));
  ASSERT_EQ(-EBUSY, image.mirror_image_disable(true));
  pending_ns.mirror_peer_uuids.clear();

  auto missing_member = *pending;
  missing_member.snaps.clear();
  ASSERT_EQ(0, librbd::cls_client::group_snap_set(
    &m_ioctx, librbd::util::group_header_name(group_id), missing_member));
  ASSERT_EQ(-EBUSY, image.mirror_image_disable(false));
  cls::rbd::GroupSnapshot remaining;
  ASSERT_EQ(0, librbd::cls_client::group_snap_get_by_id(
    &m_ioctx, librbd::util::group_header_name(group_id), pending->id, &remaining));
  ASSERT_TRUE(remaining.snaps.empty());
  ASSERT_EQ(0, librbd::cls_client::group_snap_set(
    &m_ioctx, librbd::util::group_header_name(group_id), *pending));
  ASSERT_EQ(0, image.mirror_image_disable(false));
  librbd::mirror_image_info_t info;
  ASSERT_EQ(0, image.mirror_image_get_info(&info, sizeof(info)));
  ASSERT_EQ(RBD_MIRROR_IMAGE_DISABLED, info.state);
  snaps.clear();
  ASSERT_EQ(0, list_group_snapshots(group_id, &snaps));
  for (const auto& snap : snaps) {
    if (std::holds_alternative<cls::rbd::GroupSnapshotNamespaceMirror>(
          snap.snapshot_namespace)) {
      ASSERT_TRUE(std::none_of(snap.snaps.begin(), snap.snaps.end(),
        [&image_id](const auto& spec) { return spec.image_id == image_id; }));
    }
  }
  std::vector<librbd::snap_info_t> image_snaps;
  ASSERT_EQ(0, image.snap_list(image_snaps));
  ASSERT_EQ(1U, image_snaps.size());
  librbd::snap_namespace_type_t ns_type;
  ASSERT_EQ(0, image.snap_get_namespace_type(image_snaps[0].id, &ns_type));
  ASSERT_EQ(RBD_SNAP_NAMESPACE_TYPE_GROUP, ns_type);

  ASSERT_EQ(0, m_rbd.group_snap_remove(m_ioctx, group_name, "saved"));
  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, group_name, false));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer1));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer2));
}

// Test sequence:
// 1. Create grp1 with two images and a user group snapshot.
// 2. Move one image to non-mirrored grp2 and create grp2's own user snapshot.
// 3. Try enabling mirroring on both groups while grp1's snapshot remains.
// 4. Remove grp1's snapshot and retry enabling mirroring on both groups.
// Expected: The move succeeds. Both enables return -EINVAL until grp1's
//   snapshot is removed. A group's own user snapshots do not block enable.
TEST_F(TestGroup, mirrorGroupEnableRejectsForeignUserGroupSnapshotsPP) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(
    m_ioctx, &peer_uuid, RBD_MIRROR_PEER_DIRECTION_RX_TX, "cluster", "client"));

  const char* source = "grp1";
  const char* target = "grp2";
  const auto moved_image_name = m_image_name + "_moved";
  ASSERT_EQ(0, create_image_pp(
    m_rbd, m_ioctx, moved_image_name.c_str(), m_image_size));
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, source));
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, target));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, source, m_ioctx, m_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, source, m_ioctx, moved_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, source, "snap1"));
  ASSERT_EQ(0, m_rbd.group_image_remove(
    m_ioctx, source, m_ioctx, moved_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_image_add(
    m_ioctx, target, m_ioctx, moved_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.group_snap_create(m_ioctx, target, "snap2"));

  ASSERT_EQ(-EINVAL, m_rbd.mirror_group_enable(
    m_ioctx, source, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  ASSERT_EQ(-EINVAL, m_rbd.mirror_group_enable(
    m_ioctx, target, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));

  ASSERT_EQ(0, m_rbd.group_snap_remove(m_ioctx, source, "snap1"));
  ASSERT_EQ(0, m_rbd.mirror_group_enable(
    m_ioctx, source, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  ASSERT_EQ(0, m_rbd.mirror_group_enable(
    m_ioctx, target, RBD_MIRROR_IMAGE_MODE_SNAPSHOT));

  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, target, false));
  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, source, false));
  ASSERT_EQ(0, m_rbd.group_snap_remove(m_ioctx, target, "snap2"));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, target));
  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, source));
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, moved_image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
}

TEST_F(TestGroup, mirrorPP)
{
  REQUIRE_FORMAT_V2();

  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &peer_uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "cluster", "client"));
  const char *group_name = "snap_group1";
  ASSERT_EQ(0, m_rbd.group_create(m_ioctx, group_name));
  ASSERT_EQ(0, m_rbd.group_image_add(m_ioctx, group_name, m_ioctx,
                                     m_image_name.c_str()));

  std::vector<librbd::group_snap_info_t> group_snaps;
  ASSERT_EQ(0, m_rbd.group_snap_list(m_ioctx, group_name, &group_snaps,
                                     sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(0U, group_snaps.size());

  std::string snap_id;

  ASSERT_EQ(-EINVAL, m_rbd.mirror_group_create_snapshot(m_ioctx, group_name, 0,
                                                        &snap_id));
  ASSERT_EQ(0, m_rbd.group_snap_list(m_ioctx, group_name, &group_snaps,
                                     sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(0U, group_snaps.size());

  ASSERT_EQ(0, m_rbd.mirror_group_enable(m_ioctx, group_name,
                                         RBD_MIRROR_IMAGE_MODE_SNAPSHOT));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));

  librbd::mirror_image_mode_t mode;
  ASSERT_EQ(0, image.mirror_image_get_mode(&mode));
  ASSERT_EQ(RBD_MIRROR_IMAGE_MODE_SNAPSHOT, mode);

  std::vector<librbd::snap_info_t> snaps;
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_EQ(1U, snaps.size());

  ASSERT_EQ(0, m_rbd.mirror_group_create_snapshot(m_ioctx, group_name, 0,
                                                  &snap_id));
  snaps.clear();
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_EQ(2U, snaps.size());
  librbd::snap_namespace_type_t snap_ns_type;
  ASSERT_EQ(0, image.snap_get_namespace_type(snaps[1].id, &snap_ns_type));
  ASSERT_EQ(RBD_SNAP_NAMESPACE_TYPE_MIRROR, snap_ns_type);
  librbd::snap_mirror_namespace_t mirror_snap;
  ASSERT_EQ(0, image.snap_get_mirror_namespace(snaps[1].id, &mirror_snap,
                                               sizeof(mirror_snap)));

  ASSERT_EQ(0, m_rbd.group_snap_list(m_ioctx, group_name, &group_snaps,
                                     sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(2U, group_snaps.size());

  ASSERT_EQ(0, m_rbd.mirror_group_demote(m_ioctx, group_name));

  librbd::mirror_image_info_t mirror_info;
  ASSERT_EQ(0, image.mirror_image_get_info(&mirror_info, sizeof(mirror_info)));
  ASSERT_FALSE(mirror_info.primary);

  ASSERT_EQ(0, m_rbd.mirror_group_resync(m_ioctx, group_name));

  ASSERT_EQ(0, m_rbd.mirror_group_promote(m_ioctx, group_name, false));
  ASSERT_EQ(0, image.mirror_image_get_info(&mirror_info, sizeof(mirror_info)));
  ASSERT_TRUE(mirror_info.primary);

  ASSERT_EQ(0, m_rbd.mirror_group_disable(m_ioctx, group_name, false));

  ASSERT_EQ(0, m_rbd.group_remove(m_ioctx, group_name));
}
