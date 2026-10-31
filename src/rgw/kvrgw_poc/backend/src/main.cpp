// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "kvrgw_runtime.hpp"
#include "perf_driver.hpp"

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>

namespace {

kvrgw::KvRgwRuntime *g_runtime = nullptr;

void handle_signal(int)
{
  if (g_runtime != nullptr) {
    g_runtime->request_stop();
  }
}

} // namespace

int main(int argc, char **argv)
{
  std::string socket_path = "/tmp/kvrgw.sock";
  std::string data_root = "../data";
  bool perf_mode = false;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--perf") {
      perf_mode = true;
    }
    else if (socket_path == "/tmp/kvrgw.sock" && arg[0] != '-') {
      socket_path = arg;
    }
    else if (data_root == "../data" && arg[0] != '-') {
      data_root = arg;
    }
  }
  if (const char *env_data = std::getenv("KVRGW_DATA")) {
    data_root = env_data;
  }
  if (const char *env_socket = std::getenv("KVRGW_SOCKET")) {
    socket_path = env_socket;
  }
  (void)socket_path;

  if (!perf_mode) {
    std::cerr << "kv-rgw-backend is --perf only; S3 is kv-rgw-frontend"
              << std::endl;
    return 2;
  }

  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);

  kvrgw::KvRgwRuntime runtime;
  g_runtime = &runtime;

  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = true;
  opts.data_root = data_root;
  if (!runtime.start(opts)) {
    g_runtime = nullptr;
    return 1;
  }

  const int rc = kvrgw::run_perf_driver(runtime);
  runtime.stop();
  g_runtime = nullptr;
  return rc;
}
