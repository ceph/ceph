// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <signal.h>
#include "common/Thread.h"
#include "global/global_init.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "gtest/gtest.h"

class ErasureCodePluginRegistryTest : public ::testing::Test {
protected:

  class Thread_factory : public Thread {
  public:
    static void cleanup(void *arg) {
      ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
      if (instance.lock.is_locked())
        instance.lock.Unlock();
    }

    virtual void *entry() {
      ErasureCodeProfile profile;
      ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
      ErasureCodeInterfaceRef erasure_code;
      pthread_cleanup_push(cleanup, NULL);
      instance.factory("hangs",
		       g_conf->erasure_code_dir,
		       profile, &erasure_code, &cerr);
      pthread_cleanup_pop(0);
      return NULL;
    }
  };

};

TEST_F(ErasureCodePluginRegistryTest, factory_mutex) {
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();

  EXPECT_TRUE(instance.lock.TryLock());
  instance.lock.Unlock();

  // 
  // Test that the loading of a plugin is protected by a mutex.
  //
  useconds_t delay = 0;
  const useconds_t DELAY_MAX = 20 * 1000 * 1000;
  Thread_factory sleep_forever;
  sleep_forever.create("sleep_forever");
  do {
    cout << "Trying (1) with delay " << delay << "us\n";
    if (delay > 0)
      usleep(delay);
    if (!instance.loading)
      delay = ( delay + 1 ) * 2;
  } while(!instance.loading && delay < DELAY_MAX);
  ASSERT_TRUE(delay < DELAY_MAX);

  EXPECT_FALSE(instance.lock.TryLock());

  EXPECT_EQ(0, pthread_cancel(sleep_forever.get_thread_id()));
  EXPECT_EQ(0, sleep_forever.join());
}

TEST_F(ErasureCodePluginRegistryTest, all)
{
  ErasureCodeProfile profile;
  string directory(".libs");
  ErasureCodeInterfaceRef erasure_code;
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-EIO, instance.factory("invalid",
				   g_conf->erasure_code_dir,
				   profile, &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-EXDEV, instance.factory("missing_version",
				     g_conf->erasure_code_dir,
				     profile,
				     &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-ENOENT, instance.factory("missing_entry_point",
				      g_conf->erasure_code_dir,
				      profile,
				      &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-ESRCH, instance.factory("fail_to_initialize",
				     g_conf->erasure_code_dir,
				     profile,
				     &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-EBADF, instance.factory("fail_to_register",
				     g_conf->erasure_code_dir,
				     profile,
				     &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(0, instance.factory("example",
				g_conf->erasure_code_dir,
				profile, &erasure_code, &cerr));
  EXPECT_TRUE(erasure_code.get());
  ErasureCodePlugin *plugin = 0;
  {
    Mutex::Locker l(instance.lock);
    EXPECT_EQ(-EEXIST, instance.load("example", directory, &plugin, &cerr));
    EXPECT_EQ(-ENOENT, instance.remove("does not exist"));
    EXPECT_EQ(0, instance.remove("example"));
    EXPECT_EQ(0, instance.load("example", directory, &plugin, &cerr));
  }
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  g_conf->set_val("erasure_code_dir", ".libs", false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 && 
 *   make unittest_erasure_code_plugin && 
 *   valgrind --tool=memcheck \
 *      ./unittest_erasure_code_plugin \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
