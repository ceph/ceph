// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/WatchNotifyTypes.h"
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

void register_test_consistency_groups() {
}

class TestLibCG : public TestFixture {

};

TEST_F(TestLibCG, group_create)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, "mygroup"));

  vector<string> groups;
  ASSERT_EQ(0, rbd.group_list(ioctx, groups));
  ASSERT_EQ(1U, groups.size());
  ASSERT_EQ("mygroup", groups[0]);

  ASSERT_EQ(0, rbd.group_remove(ioctx, "mygroup"));

  groups.clear();
  ASSERT_EQ(0, rbd.group_list(ioctx, groups));
  ASSERT_EQ(0U, groups.size());
}
