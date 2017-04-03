// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 FUJITSU LIMITED
 *
 * Author: Shotaro Kawaguchi <kawaguchi.s@jp.fujitsu.com>
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "common/config.h"

TEST(ErasureCodePlugin, factory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  map<std::string,std::string> profile;
  {
    ErasureCodeInterfaceRef erasure_code;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("shec",
				  g_conf->get_val<std::string>("erasure_code_dir"),
				  profile,
				  &erasure_code, &cerr));
    EXPECT_TRUE(erasure_code.get());
  }
  const char *techniques[] = {
    "single",
    "multiple",
    0
  };
  for(const char **technique = techniques; *technique; technique++) {
    ErasureCodeInterfaceRef erasure_code;
    profile["technique"] = *technique;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("shec",
				  g_conf->get_val<std::string>("erasure_code_dir"),
				  profile,
                                  &erasure_code, &cerr));
    EXPECT_TRUE(erasure_code.get());
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_erasure_code_plugin_shec &&
 *   valgrind --tool=memcheck ./unittest_erasure_code_plugin_shec \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
