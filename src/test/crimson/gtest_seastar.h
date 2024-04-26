// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/errorator.h"
#include "gtest/gtest.h"

#include "seastar_runner.h"

struct seastar_test_suite_t : public ::testing::Test {
  static SeastarRunner seastar_env;

  template <typename Func>
  void run(Func &&func) {
    seastar_env.run(std::forward<Func>(func));
  }

  template <typename Func>
  void run_ertr(Func &&func) {
    run(
      [func=std::forward<Func>(func)]() mutable {
	return std::invoke(std::move(func)).handle_error(
	  crimson::ct_error::assert_all("error"));
      });
  }

  template <typename Func>
  void run_async(Func &&func) {
    run(
      [func=std::forward<Func>(func)]() mutable {
	return seastar::async(std::forward<Func>(func));
      });
  }

  template <typename F>
  auto scl(F &&f) {
    return [fptr = std::make_unique<F>(std::forward<F>(f))]() mutable {
      return std::invoke(*fptr).finally([fptr=std::move(fptr)] {});
    };
  }

  void run_scl(auto &&f) {
    run([this, f=std::forward<decltype(f)>(f)]() mutable {
      return std::invoke(scl(std::move(f)));
    });
  }

  void run_ertr_scl(auto &&f) {
    run_ertr(scl(std::forward<decltype(f)>(f)));
  }

  virtual seastar::future<> set_up_fut() { return seastar::now(); }
  void SetUp() final {
    run([this] { return set_up_fut(); });
  }

  virtual seastar::future<> tear_down_fut() { return seastar::now(); }
  void TearDown() final {
    run([this] { return tear_down_fut(); });
  }
};
