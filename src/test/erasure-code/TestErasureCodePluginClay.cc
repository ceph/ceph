// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2018 Indian Institute of Science <office.ece@iisc.ac.in>
 *
 * Author: Myna Vajha <mynaramana@gmail.com>
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
#include "log/Log.h"
#include "global/global_context.h"
#include "common/config_proxy.h"
#include "gtest/gtest.h"

TEST(ErasureCodePlugin, factory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeProfile profile;
  {
    ErasureCodeInterfaceRef erasure_code;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("clay",
                                  g_conf().get_val<std::string>("erasure_code_dir"),
				  profile,
                                  &erasure_code, &cerr));
    EXPECT_TRUE(erasure_code);
  }
  //check clay plugin with scalar_mds=jerasure
  {
    const char *techniques[] = {
      "reed_sol_van",
      "reed_sol_r6_op",
      "cauchy_orig",
      "cauchy_good",
      "liber8tion",
      0
    };
    for(const char **technique = techniques; *technique; technique++) {
      ErasureCodeInterfaceRef erasure_code;
      ErasureCodeProfile profile;
      profile["scalar_mds"] = "jerasure";
      profile["technique"] = *technique;
      EXPECT_FALSE(erasure_code);
      EXPECT_EQ(0, instance.factory("clay",
                                    g_conf().get_val<std::string>("erasure_code_dir"),
				    profile,
                                    &erasure_code, &cerr));
      EXPECT_TRUE(erasure_code.get());
    }
  }
#ifdef HAVE_BETTER_YASM_ELF64
  //check clay plugin with scalar_mds=isa
  {
    const char *techniques[] = {
      "reed_sol_van",
      "cauchy",
      0
    };
    for(const char **technique = techniques; *technique; technique++) {
      ErasureCodeInterfaceRef erasure_code;
      ErasureCodeProfile profile;
      profile["scalar_mds"] = "isa";
      profile["technique"] = *technique;
      EXPECT_FALSE(erasure_code);
      EXPECT_EQ(0, instance.factory("clay",
                                    g_conf().get_val<std::string>("erasure_code_dir"),
				    profile,
                                    &erasure_code, &cerr));
      EXPECT_TRUE(erasure_code.get());
    }
  }
#endif
  //check clay plugin with scalar_mds=shec
  {
    const char *techniques[] = {
      "single",
      "multiple",
      0
    };
    for(const char **technique = techniques; *technique; technique++) {
      ErasureCodeInterfaceRef erasure_code;
      ErasureCodeProfile profile;
      profile["scalar_mds"] = "shec";
      profile["technique"] = *technique;
      EXPECT_FALSE(erasure_code);
      EXPECT_EQ(0, instance.factory("clay",
                                    g_conf().get_val<std::string>("erasure_code_dir"),
				    profile,
                                    &erasure_code, &cerr));
      EXPECT_TRUE(erasure_code.get());
    }
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_erasure_code_plugin_clay &&
 *   valgrind --tool=memcheck ./unittest_erasure_code_plugin_clay \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
