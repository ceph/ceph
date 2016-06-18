// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include "rgw/rgw_rados.h"
#include "rgw/rgw_http_client.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include <curl/curl.h>
#include <gtest/gtest.h>

TEST(HTTPManager, SignalThread)
{
  auto cct = g_ceph_context;
  RGWHTTPManager http(cct);

  ASSERT_EQ(0, http.set_threaded());

  // default pipe buffer size according to man pipe
  constexpr size_t max_pipe_buffer_size = 65536;
  // each signal writes 4 bytes to the pipe
  constexpr size_t max_pipe_signals = max_pipe_buffer_size / sizeof(uint32_t);
  // add_request and unregister_request
  constexpr size_t pipe_signals_per_request = 2;
  // number of http requests to fill the pipe buffer
  constexpr size_t max_requests = max_pipe_signals / pipe_signals_per_request;

  // send one extra request to test that we don't deadlock
  constexpr size_t num_requests = max_requests + 1;

  for (int i = 0; i < (int)num_requests; i++) {
    RGWHTTPClient client{cct};
    http.add_request(&client, "PUT", "http://127.0.0.1:80");
  }
}

int main(int argc, char** argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  curl_global_init(CURL_GLOBAL_ALL);
  ::testing::InitGoogleTest(&argc, argv);
  int r = RUN_ALL_TESTS();
  curl_global_cleanup();
  return r;
}
