// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "include/util.h"
#include "gtest/gtest.h"
#include "common/ceph_mutex.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/lockdep.h"
#include "include/coredumpctl.h"

TEST(lockdep, abba)
{
  ASSERT_TRUE(g_lockdep);

  ceph::mutex a(ceph::make_mutex("a")), b(ceph::make_mutex("b"));
  a.lock();
  ASSERT_TRUE(ceph_mutex_is_locked(a));
  ASSERT_TRUE(ceph_mutex_is_locked_by_me(a));
  b.lock();
  ASSERT_TRUE(ceph_mutex_is_locked(b));
  ASSERT_TRUE(ceph_mutex_is_locked_by_me(b));
  a.unlock();
  b.unlock();

  b.lock();
  PrCtl unset_dumpable;
  EXPECT_DEATH(a.lock(), "");
  b.unlock();
}

TEST(lockdep, recursive)
{
  ASSERT_TRUE(g_lockdep);

  ceph::mutex a(ceph::make_mutex("a"));
  a.lock();
  PrCtl unset_dumpable;
  EXPECT_DEATH(a.lock(), "");
  a.unlock();

  ceph::recursive_mutex b(ceph::make_recursive_mutex("b"));
  b.lock();
  ASSERT_TRUE(ceph_mutex_is_locked(b));
  ASSERT_TRUE(ceph_mutex_is_locked_by_me(b));
  b.lock();
  b.unlock();
  b.unlock();
}

int main(int argc, char **argv) {
#ifdef NDEBUG
  cout << "NDEBUG is defined" << std::endl;
#else
  cout << "NDEBUG is NOT defined" << std::endl;
#endif
#ifndef CEPH_DEBUG_MUTEX
  cerr << "WARNING: CEPH_DEBUG_MUTEX is not defined, lockdep will not work"
       << std::endl;
  exit(0);
#endif
  std::vector<const char*> args(argv, argv + argc);
  args.push_back("--lockdep");
  auto cct = global_init(NULL, args,
			 CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
