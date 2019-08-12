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
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "common/config_proxy.h"
#include "gtest/gtest.h"


class ErasureCodePluginRegistryTest : public ::testing::Test {};

TEST_F(ErasureCodePluginRegistryTest, factory_mutex) {
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();

  {
    unique_lock l{instance.lock, std::try_to_lock};
    EXPECT_TRUE(l.owns_lock());
  }
  // 
  // Test that the loading of a plugin is protected by a mutex.

  std::thread sleep_for_10_secs([] {
    ErasureCodeProfile profile;
    ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
    ErasureCodeInterfaceRef erasure_code;
    instance.factory("hangs",
		     g_conf().get_val<std::string>("erasure_code_dir"),
		     profile, &erasure_code, &cerr);
  });
  auto wait_until = [&instance](bool loading, unsigned max_secs) {
    auto delay = 0ms;
    const auto DELAY_MAX = std::chrono::seconds(max_secs);
    for (; delay < DELAY_MAX; delay = (delay + 1ms) * 2) {
      cout << "Trying (1) with delay " << delay << "us\n";
      if (delay.count() > 0) {
	std::this_thread::sleep_for(delay);
      }
      if (instance.loading == loading) {
	return true;
      }
    }
    return false;
  };
  // should be loading in 5 seconds
  ASSERT_TRUE(wait_until(true, 5));
  {
    unique_lock l{instance.lock, std::try_to_lock};
    EXPECT_TRUE(!l.owns_lock());
  }
  // should finish loading in 15 seconds
  ASSERT_TRUE(wait_until(false, 15));
  {
    unique_lock l{instance.lock, std::try_to_lock};
    EXPECT_TRUE(l.owns_lock());
  }
  sleep_for_10_secs.join();
}

TEST_F(ErasureCodePluginRegistryTest, all)
{
  ErasureCodeProfile profile;
  string directory = g_conf().get_val<std::string>("erasure_code_dir");
  ErasureCodeInterfaceRef erasure_code;
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-EIO, instance.factory("invalid",
				   g_conf().get_val<std::string>("erasure_code_dir"),
				   profile, &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-EXDEV, instance.factory("missing_version",
				     g_conf().get_val<std::string>("erasure_code_dir"),
				     profile,
				     &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-ENOENT, instance.factory("missing_entry_point",
				      g_conf().get_val<std::string>("erasure_code_dir"),
				      profile,
				      &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-ESRCH, instance.factory("fail_to_initialize",
				     g_conf().get_val<std::string>("erasure_code_dir"),
				     profile,
				     &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(-EBADF, instance.factory("fail_to_register",
				     g_conf().get_val<std::string>("erasure_code_dir"),
				     profile,
				     &erasure_code, &cerr));
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(0, instance.factory("example",
				g_conf().get_val<std::string>("erasure_code_dir"),
				profile, &erasure_code, &cerr));
  EXPECT_TRUE(erasure_code.get());
  ErasureCodePlugin *plugin = 0;
  {
    std::lock_guard l{instance.lock};
    EXPECT_EQ(-EEXIST, instance.load("example", directory, &plugin, &cerr));
    EXPECT_EQ(-ENOENT, instance.remove("does not exist"));
    EXPECT_EQ(0, instance.remove("example"));
    EXPECT_EQ(0, instance.load("example", directory, &plugin, &cerr));
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../../../build ; make -j4 &&
 *   make unittest_erasure_code_plugin && 
 *   valgrind --tool=memcheck \
 *      ./bin/unittest_erasure_code_plugin \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
