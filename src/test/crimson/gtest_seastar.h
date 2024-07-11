// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/errorator.h"
#include "crimson/common/log.h"
#include "gtest/gtest.h"

#include "seastar_runner.h"

struct seastar_test_suite_t : public ::testing::Test {
  static SeastarRunner seastar_env;

  template <typename Func>
  void do_run(Func &&func, const char *name) {
    seastar_env.run([func=std::forward<Func>(func), name]() mutable {
      crimson::get_logger(ceph_subsys_test).info(
        "{} started...", name);
      return std::invoke(std::move(func)
      ).finally([name] {
        crimson::get_logger(ceph_subsys_test).info(
          "{} finished", name);
      });
    });
  }

  template <typename Func>
  void run(Func &&func) {
    do_run(std::forward<Func>(func), "run");
  }

  template <typename Func>
  void run_ertr(Func &&func) {
    do_run(
      [func=std::forward<Func>(func)]() mutable {
	return std::invoke(std::move(func)).handle_error(
	  crimson::ct_error::assert_all("error"));
      }, "run_ertr");
  }

  template <typename Func>
  void run_async(Func &&func) {
    do_run(
      [func=std::forward<Func>(func)]() mutable {
	return seastar::async(std::forward<Func>(func));
      }, "run_async");
  }

  template <typename F>
  auto scl(F &&f) {
    return [fptr = std::make_unique<F>(std::forward<F>(f))]() mutable {
      return std::invoke(*fptr).finally([fptr=std::move(fptr)] {});
    };
  }

  void run_scl(auto &&f) {
    do_run([this, f=std::forward<decltype(f)>(f)]() mutable {
      return std::invoke(scl(std::move(f)));
    }, "run_scl");
  }

  void run_ertr_scl(auto &&f) {
    run_ertr(scl(std::forward<decltype(f)>(f)));
  }

  virtual seastar::future<> set_up_fut() { return seastar::now(); }
  void SetUp() final {
    do_run([this] { return set_up_fut(); }, "setup");
  }

  virtual seastar::future<> tear_down_fut() { return seastar::now(); }
  void TearDown() final {
    do_run([this] { return tear_down_fut(); }, "teardown");
  }
};
