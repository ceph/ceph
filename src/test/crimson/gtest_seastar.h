// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <stdio.h>
#include <signal.h>
#include <thread>

#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/alien.hh>
#include <seastar/core/thread.hh>

#include "gtest/gtest.h"

struct seastar_gtest_env_t {
  seastar::app_template app;
  seastar::file_desc begin_fd;
  std::unique_ptr<seastar::readable_eventfd> on_end;

  int argc = 0;
  char **argv = nullptr;
  std::thread thread;

  seastar_gtest_env_t();
  ~seastar_gtest_env_t();

  void init(int argc, char **argv);
  void stop();
  void reactor();

  template <typename Func>
  void run(Func &&func) {
    auto fut = seastar::alien::submit_to(0, std::forward<Func>(func));
    fut.get();
  }
};

struct seastar_test_suite_t : public ::testing::Test {
  static seastar_gtest_env_t seastar_env;

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
