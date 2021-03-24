// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_assert.h"
#include "gtest_seastar.h"

SeastarRunner seastar_test_suite_t::seastar_env;

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  seastar_test_suite_t::seastar_env.init(argc, argv);

  seastar::global_logger_registry().set_all_loggers_level(
    seastar::log_level::debug
  );

  int ret = RUN_ALL_TESTS();

  seastar_test_suite_t::seastar_env.stop();
  return ret;
}
