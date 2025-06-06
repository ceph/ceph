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
#include <stdlib.h>

#include "erasure-code/ErasureCodePlugin.h"
#include "log/Log.h"
#include "global/global_context.h"
#include "common/config_proxy.h"
#include "gtest/gtest.h"

using namespace std;

TEST(ErasureCodePlugin, factory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  {
    ErasureCodeProfile profile;
    profile["technique"] = "doesnotexist";
    ErasureCodeInterfaceRef erasure_code;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(-ENOENT, instance.factory("jerasure",
					g_conf().get_val<std::string>("erasure_code_dir"),
					profile,
                                        &erasure_code, &cerr));
    EXPECT_FALSE(erasure_code);
  }
  const char *techniques[] = {
    "reed_sol_van",
    "reed_sol_r6_op",
    "cauchy_orig",
    "cauchy_good",
    "liberation",
    "blaum_roth",
    "liber8tion",
    0
  };
  for(const char **technique = techniques; *technique; technique++) {
    ErasureCodeInterfaceRef erasure_code;
    ErasureCodeProfile profile;
    profile["technique"] = *technique;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("jerasure",
				  g_conf().get_val<std::string>("erasure_code_dir"),
				  profile,
                                  &erasure_code, &cerr));
    EXPECT_TRUE(erasure_code.get());
  }
}

bufferptr create_bufferptr(uint64_t value) {
  bufferlist bl;
  bl.append_zero(4096);
  memcpy(bl.c_str(), &value, sizeof(value));
  return bl.begin().get_current_ptr();
}

TEST(ErasureCodePlugin, parity_delta_write) {
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeInterfaceRef erasure_code;
  ErasureCodeProfile profile;
  profile["technique"] = "reed_sol_van";
  profile["k"] = "5";
  int k=5;
  profile["m"] = "3";
  int m=3;
  EXPECT_EQ(0, instance.factory("jerasure",
                              g_conf().get_val<std::string>("erasure_code_dir"),
                              profile,
                              &erasure_code, &cerr));
  shard_id_map<bufferptr> data(8);
  shard_id_map<bufferptr> coding(8);
  shard_id_map<bufferptr> coding2(8);
  shard_id_map<bufferptr> decode_in(8);
  shard_id_map<bufferptr> decode_out(8);

  uint32_t seeds[] = {100, 101, 102, 103, 104};
  uint32_t overwrite3 = 1032;

  for (shard_id_t s; s < k; ++s) {
    data[s] = create_bufferptr(seeds[int(s)]);
  }
  for (shard_id_t s(k); s < k + m; ++s) {
    coding[s] = create_bufferptr(-1);
    coding2[s] = create_bufferptr(-1);
  }

  // Do a normal encode.
  erasure_code->encode_chunks(data, coding);

  shard_id_map<bufferptr> delta(8);
  delta[shard_id_t(3)] = create_bufferptr(-1);

  bufferptr overwrite_bp = create_bufferptr(overwrite3);

  erasure_code->encode_delta(data[shard_id_t(3)], overwrite_bp, &delta[shard_id_t(3)]);
  erasure_code->apply_delta(delta, coding);
  data[shard_id_t(3)] = overwrite_bp;

  erasure_code->encode_chunks(data, coding2);

  for (shard_id_t s(k); s < k + m; ++s) {
    ASSERT_EQ(*(uint32_t*)coding[s].c_str(), *(uint32_t*)coding2[s].c_str());
  }

  data.erase(shard_id_t(4));
  data.emplace(shard_id_t(4), (char*)malloc(4096), 4096);
  shard_id_set want;
  want.insert_range(shard_id_t(0), 5);
  decode_in[shard_id_t(0)] = data[shard_id_t(0)];
  decode_in[shard_id_t(1)] = data[shard_id_t(1)];
  decode_in[shard_id_t(2)] = data[shard_id_t(2)];
  decode_in[shard_id_t(3)] = data[shard_id_t(3)];
  decode_out[shard_id_t(4)] = data[shard_id_t(4)];
  decode_in[shard_id_t(6)] = coding[shard_id_t(6)];

  ASSERT_EQ(0, erasure_code->decode_chunks(want, decode_in, decode_out));

  seeds[3] = overwrite3;
  for (shard_id_t s(0); s < k; ++s) {
    ASSERT_EQ(seeds[int(s)], *(uint32_t*)data[s].c_str());
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_erasure_code_plugin_jerasure &&
 *   valgrind --tool=memcheck ./unittest_erasure_code_plugin_jerasure \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
