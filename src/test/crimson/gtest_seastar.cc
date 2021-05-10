// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_assert.h"
#include "gtest_seastar.h"

#include "common/ceph_argparse.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/perf_counters_collection.h"

SeastarRunner seastar_test_suite_t::seastar_env;

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  seastar_test_suite_t::seastar_env.init(argc, argv);

  seastar::global_logger_registry().set_all_loggers_level(
    seastar::log_level::debug
  );

  seastar_test_suite_t::seastar_env.run([] {
    return crimson::common::sharded_conf().start(
      EntityName{}, string_view{"ceph"}
    ).then([] {
      return crimson::common::sharded_perf_coll().start();
    });
  });

  int ret = RUN_ALL_TESTS();

  seastar_test_suite_t::seastar_env.run([] {
    return crimson::common::sharded_perf_coll().stop().then([] {
      return crimson::common::sharded_conf().stop();
    });
  });

  seastar_test_suite_t::seastar_env.stop();
  return ret;
}
