// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include <sstream>

#include "include/random.h"

#include "gtest/gtest.h"

// Helper to see if calls compile with various types:
template <typename T>
T type_check_ok(const T min, const T max)
{
  return ceph::util::generate_random_number(min, max);
}

/* Help wrangle "unused variable" warnings: */
template <typename X>
void swallow_values(const X x)
{
  static_cast<void>(x);
}

template <typename X, typename ...XS>
void swallow_values(const X x, const XS... xs)
{
  swallow_values(x), swallow_values(xs...);
}

// Mini-examples showing canonical usage:
TEST(util, test_random_canonical)
{
  // Seed random number generation:
  ceph::util::randomize_rng();
 
  // Get a random int between 0 and max int:
  auto a = ceph::util::generate_random_number();
 
  // Get a random int between 0 and 20:
  auto b = ceph::util::generate_random_number(20);
 
  // Get a random int between 1 and 20:
  auto c = ceph::util::generate_random_number(1, 20);
 
  // Get a random float between 0.0 and 20.0:
  auto d = ceph::util::generate_random_number(20.0);
 
  // Get a random float between 0.001 and 0.991:
  auto e = ceph::util::generate_random_number(0.001, 0.991);
 
  // Make a function object RNG suitable for putting on its own thread:
  auto gen_fn = ceph::util::random_number_generator<int>();
  auto z = gen_fn();
  gen_fn.seed(42);   // re-seed

  // Placate the compiler: 
  swallow_values(a, b, c, d, e, z);
}

TEST(util, test_random)
{
  /* The intent of this test is not to formally test random number generation, 
  but rather to casually check that "it works" and catch regressions: */
 
  // The default overload should compile:
  ceph::util::randomize_rng();
 
  {
    int a = ceph::util::generate_random_number();
    int b = ceph::util::generate_random_number();
 
    /* Technically, this can still collide and cause a false negative, but let's 
    be optimistic: */
    if (std::numeric_limits<int>::max() > 32767) {
       ASSERT_NE(a, b);
     }
  }
 
  {
  auto a = ceph::util::generate_random_number(1, std::numeric_limits<int>::max());
  auto b = ceph::util::generate_random_number(1, std::numeric_limits<int>::max());
 
  if (std::numeric_limits<int>::max() > 32767) {
     ASSERT_GT(a, 0);
     ASSERT_GT(b, 0);
 
     ASSERT_NE(a, b);
   }
  }
 
  for (auto n = 100000; n; --n) {
     constexpr int min = 0, max = 6;
     int a = ceph::util::generate_random_number(min, max);
     ASSERT_GT(a, -1);
     ASSERT_LT(a, 7);
   }
 
  // Multiple types (integral):
  {
    int min = 0, max = 1;
    type_check_ok(min, max);
  }
 
  {
    long min = 0, max = 1l;
    type_check_ok(min, max);
  }
 
  // Multiple types (floating point):
  {
    double min = 0.0, max = 1.0;
    type_check_ok(min, max);
  }
 
  {
    float min = 0.0, max = 1.0;
    type_check_ok(min, max);
  }
 
  // min > max should not explode:
  {
    float min = 1.0, max = 0.0;
    type_check_ok(min, max);
  }
}

TEST(util, test_random_class_interface)
{
  ceph::util::random_number_generator<int> rng_i;
  ceph::util::random_number_generator<float> rng_f;
 
  // Other ctors:
  {
    ceph::util::random_number_generator<int> rng(1234);   // seed
  }
 
  {
    int a = rng_i();
    int b = rng_i();
 
    // Technically can fail, but should "almost never" happen:
    ASSERT_NE(a, b);
  }
 
  {
    int a = rng_i(10);
    ASSERT_LE(a, 10);
    ASSERT_GE(a, 0);
  }
 
  {
    float a = rng_f(10.0);
    ASSERT_LE(a, 10.0);
    ASSERT_GE(a, 0.0);
  }
 
  {
    int a = rng_i(10, 20);
    ASSERT_LE(a, 20);
    ASSERT_GE(a, 10);
  }
 
  {
    float a = rng_f(10.0, 20.0);
    ASSERT_LE(a, 20.0);
    ASSERT_GE(a, 10.0);
  }
}

