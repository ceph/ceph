// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd/librbd.h"
#include "include/rbd/librbd.hpp"
#include "test/librados/test.h"
#include "gtest/gtest.h"

#include <boost/scope_exit.hpp>
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
  ASSERT_EQ(strlen("mygroup") + 1, rbd_group_list(ioctx, groups, &size));
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

  vector<string> groups;
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

  ASSERT_EQ(0, rbd_group_image_add(ioctx, group_name, ioctx,
                                   m_image_name.c_str()));

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
  ASSERT_EQ(1, num_images);

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
  ASSERT_EQ(0, num_images);

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

  ASSERT_EQ(0, rbd.group_image_add(ioctx, group_name, ioctx,
                                   m_image_name.c_str()));

  ASSERT_EQ(0, image.features(&features));
  ASSERT_TRUE((features & RBD_FEATURE_OPERATIONS) ==
                RBD_FEATURE_OPERATIONS);
  ASSERT_EQ(0, image.get_op_features(&op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_GROUP) ==
                RBD_OPERATION_FEATURE_GROUP);

  vector<librbd::group_image_info_t> images;
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

  ASSERT_EQ(0, rbd_group_create(ioctx, group_name));

  ASSERT_EQ(0, rbd_group_image_add(ioctx, group_name, ioctx,
                                   m_image_name.c_str()));

  ASSERT_EQ(0, rbd_group_snap_create(ioctx, group_name, snap_name));

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

  ASSERT_EQ(0, rbd_group_snap_list_cleanup(snaps, sizeof(rbd_group_snap_info_t),
                                           num_snaps));
  ASSERT_EQ(0, rbd_group_snap_remove(ioctx, group_name, snap_name));

  ASSERT_EQ(0, rbd_group_snap_list(ioctx, group_name, snaps,
                                   sizeof(rbd_group_snap_info_t),
                                   &num_snaps));
  ASSERT_EQ(0U, num_snaps);

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

  ASSERT_EQ(0, rbd.group_snap_create(ioctx, group_name, snap_name));

  std::vector<librbd::group_snap_info_t> snaps;
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps,
                                   sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(1U, snaps.size());

  ASSERT_EQ(snap_name, snaps[0].name);

  ASSERT_EQ(0, rbd.group_snap_remove(ioctx, group_name, snap_name));

  snaps.clear();
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps,
                                   sizeof(librbd::group_snap_info_t)));
  ASSERT_EQ(0U, snaps.size());

  ASSERT_EQ(0, rbd.group_remove(ioctx, group_name));
}
