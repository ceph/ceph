// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdlib>
#include <iostream>

#include "include/ceph_assert.h"
#include "gtest_seastar.h"

#include "common/ceph_argparse.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/perf_counters_collection.h"

SeastarRunner seastar_test_suite_t::seastar_env;

int main(int argc, char **argv)
{
  // preprocess args
  std::vector<const char*> args;
  bool global_log_level_is_set = false;
  const char* prefix_log_level = "--default-log-level";
  for (int i = 0; i < argc; ++i) {
    if (std::strncmp(argv[i], prefix_log_level,
                     std::strlen(prefix_log_level)) == 0) {
      global_log_level_is_set = true;
    }
    args.push_back(argv[i]);
  }
  // HACK: differentiate between the `make check` bot and human user
  // for the sake of log flooding
  if (!global_log_level_is_set && !std::getenv("FOR_MAKE_CHECK")) {
    std::cout << "WARNING: set default seastar log level to debug" << std::endl;
    ++argc;
    args.push_back("--default-log-level=debug");
  }

  auto app_argv = const_cast<char**>(args.data());
  auto app_argc = static_cast<int>(args.size());
  ::testing::InitGoogleTest(&app_argc, app_argv);

  int ret = seastar_test_suite_t::seastar_env.init(app_argc, app_argv);
  if (ret != 0) {
    seastar_test_suite_t::seastar_env.stop();
    return ret;
  }

  seastar_test_suite_t::seastar_env.run([] {
    return crimson::common::sharded_conf().start(
      EntityName{}, std::string_view{"ceph"}
    ).then([] {
      return crimson::common::sharded_perf_coll().start();
    });
  });

  ret = RUN_ALL_TESTS();

  seastar_test_suite_t::seastar_env.run([] {
    return crimson::common::sharded_perf_coll().stop().then([] {
      return crimson::common::sharded_conf().stop();
    });
  });

  seastar_test_suite_t::seastar_env.stop();
  return ret;
}
