// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "gtest/gtest.h"

#include "seastar_runner.h"

struct seastar_test_suite_t : public ::testing::Test {
  static SeastarRunner seastar_env;

  template <typename Func>
  void run(Func &&func) {
    return seastar_env.run(std::forward<Func>(func));
  }

  template <typename Func>
  void run_async(Func &&func) {
    run(
      [func=std::forward<Func>(func)]() mutable {
	return seastar::async(std::forward<Func>(func));
      });
  }

  virtual seastar::future<> set_up_fut() { return seastar::now(); }
  void SetUp() final {
    return run([this] { return set_up_fut(); });
  }

  virtual seastar::future<> tear_down_fut() { return seastar::now(); }
  void TearDown() final {
    return run([this] { return tear_down_fut(); });
  }
};
