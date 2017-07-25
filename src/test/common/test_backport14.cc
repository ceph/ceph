// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * Author: Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/backport14.h" // include first: tests that header is standalone
#include <gtest/gtest.h>

int int_func() { return 1; }
bool bool_func0() { return true; }
bool bool_func1(int a) { return true; }
bool bool_func2(const std::string& a, int b) { return true; }

// given a callable and argument list, test that the result of ceph::not_fn
// evaluates to false as both an lvalue and rvalue
template <typename F, typename ...Args>
void test_not(F&& fn, Args&&... args)
{
  auto res = ceph::not_fn(std::forward<F>(fn));
  // test res as lvalue
  EXPECT_FALSE(res(std::forward<Args>(args)...));
  // test res as rvalue
  // note: this forwards args twice, but it's okay if none are rvalues
  EXPECT_FALSE(std::move(res)(std::forward<Args>(args)...));
}

TEST(Backport14, not_fn)
{
  // function pointers
  test_not(int_func);
  test_not(&int_func);
  test_not(bool_func0);
  test_not(&bool_func0);
  test_not(bool_func1, 5);
  test_not(bool_func2, "foo", 5);

  // lambdas
  auto int_lambda = [] { return 1; };
  auto bool_lambda0 = [] { return true; };
  auto bool_lambda1 = [] (int a) { return true; };
  auto bool_lambda2 = [] (const std::string& a, int b) { return true; };

  test_not(int_lambda);
  test_not(bool_lambda0);
  test_not(bool_lambda1, 5);
  test_not(bool_lambda2, "foo", 5);

  // functors
  struct int_functor {
    int operator()() { return 1; }
  };
  test_not(int_functor{});

  struct bool_functor {
    bool operator()() { return true; }
    bool operator()(int a) { return true; }
    bool operator()(const std::string& a, int b) { return true; }
  };

  test_not(bool_functor{});
  test_not(bool_functor{}, 5);
  test_not(bool_functor{}, "foo", 5);

  // lvalue-only overload
  struct lvalue_only_functor {
    bool operator()() & { return true; } // no overload for rvalue
  };
  auto lvalue_result = ceph::not_fn(lvalue_only_functor{});
  EXPECT_FALSE(lvalue_result());
  // should not compile:
  //   EXPECT_FALSE(std::move(lvalue_result)());

  // rvalue-only overload
  struct rvalue_only_functor {
    bool operator()() && { return true; } // no overload for lvalue
  };
  EXPECT_FALSE(ceph::not_fn(rvalue_only_functor{})());
  auto lvalue_functor = rvalue_only_functor{};
  EXPECT_FALSE(ceph::not_fn(lvalue_functor)()); // lvalue functor, rvalue result
  // should not compile:
  //   auto lvalue_result2 = ceph::not_fn(rvalue_only_functor{});
  //   EXPECT_FALSE(lvalue_result2());
}
