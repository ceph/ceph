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
#include <stdlib.h>
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
      PluginRegistry *instance = g_ceph_context->get_plugin_registry();
      if (instance->lock.is_locked())
        instance->lock.Unlock();
    }

    virtual void *entry() {
      ErasureCodeProfile profile;
      PluginRegistry *instance = g_ceph_context->get_plugin_registry();
      ErasureCodeInterfaceRef erasure_code;
      pthread_cleanup_push(cleanup, NULL);
      ErasureCodePlugin* ecp = dynamic_cast<ErasureCodePlugin*>(instance->get_with_load("erasure-code", "hangs"));
      pthread_cleanup_pop(0);
      return NULL;
    }
  };

};

TEST_F(ErasureCodePluginRegistryTest, factory_mutex) {
  PluginRegistry *instance = g_ceph_context->get_plugin_registry();

  EXPECT_TRUE(instance->lock.TryLock());
  instance->lock.Unlock();

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
    if (!instance->loading)
      delay = ( delay + 1 ) * 2;
  } while(!instance->loading && delay < DELAY_MAX);
  ASSERT_TRUE(delay < DELAY_MAX);

  EXPECT_FALSE(instance->lock.TryLock());

  EXPECT_EQ(0, pthread_cancel(sleep_forever.get_thread_id()));
  EXPECT_EQ(0, sleep_forever.join());
}

TEST_F(ErasureCodePluginRegistryTest, all)
{
  ErasureCodeProfile profile;
  ErasureCodeInterfaceRef erasure_code;
  PluginRegistry *instance = g_ceph_context->get_plugin_registry();
  {
    Mutex::Locker l(instance->lock);
    EXPECT_EQ(-EIO, instance->load("erasure-code", "invalid"));
    EXPECT_EQ(-EXDEV, instance->load("erasure-code", "missing_version"));
    EXPECT_EQ(-ENOENT, instance->load("erasure-code", "missing_entry_point"));
    EXPECT_EQ(-ESRCH, instance->load("erasure-code", "fail_to_initialize"));
    EXPECT_EQ(-EBADF, instance->load("erasure-code", "fail_to_register"));
    EXPECT_EQ(0, instance->load("erasure-code", "erasurecode_example"));
  }
  ErasureCodePlugin* ecp = dynamic_cast<ErasureCodePlugin*>(instance->get_with_load("erasure-code", "erasurecode_example"));
  EXPECT_TRUE(ecp);
  EXPECT_EQ(0, ecp->factory(profile, &erasure_code, &cerr));
  EXPECT_TRUE(erasure_code.get());
  {
    Mutex::Locker l(instance->lock);
    ErasureCodePlugin *plugin = 0;
    EXPECT_EQ(-EEXIST, instance->load("erasure-code", "erasurecode_example"));
    EXPECT_EQ(-ENOENT, instance->remove("erasure-code", "does not exist"));
    EXPECT_EQ(0, instance->remove("erasure-code", "erasurecode_example"));
    EXPECT_EQ(0, instance->load("erasure-code", "erasurecode_example"));
  }
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  string directory(env ? env : ".libs");

  g_conf->set_val("plugin_dir", directory, false, false);

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
