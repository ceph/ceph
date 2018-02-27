// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/int_types.h"
#include "include/stringify.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/WatchNotifyTypes.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequestWQ.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <boost/assign/std/set.hpp>
#include <boost/assign/std/map.hpp>
#include <boost/bind.hpp>
#include <boost/scope_exit.hpp>
#include <boost/thread/thread.hpp>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <vector>

using namespace ceph;
using namespace boost::assign;
using namespace librbd::watch_notify;

void register_test_groups() {
}

class TestGroup : public TestFixture {

};

TEST_F(TestGroup, group_create)
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
}

TEST_F(TestGroup, add_snapshot)
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
}
