// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/strtol.h"
#include <string>

#include "gtest/gtest.h"

static void test_strict_strtoll(const char *str, long long expected)
{
  std::string err;
  long long val = strict_strtoll(str, 10, &err);
  if (!err.empty()) {
    ASSERT_EQ(err, "");
  }
  else {
    ASSERT_EQ(val, expected);
  }
}

static void test_strict_strtol(const char *str, long expected)
{
  std::string err;
  long val = strict_strtol(str, 10, &err);
  if (!err.empty()) {
    ASSERT_EQ(err, "");
  }
  else {
    ASSERT_EQ(val, expected);
  }
}

static void test_strict_strtod(const char *str, double expected)
{
  std::string err;
  double val = strict_strtod(str, &err);
  if (!err.empty()) {
    ASSERT_EQ(err, "");
  }
  else {
    // when comparing floats, use a margin of error
    if ((expected - 0.001 > val) || (expected + 0.001 < val)) {
      ASSERT_EQ(val, expected);
    }
  }
}

static void test_strict_strtof(const char *str, float expected)
{
  std::string err;
  float val = strict_strtof(str, &err);
  if (!err.empty()) {
    ASSERT_EQ(err, "");
  }
  else {
    // when comparing floats, use a margin of error
    if ((expected - 0.001 > val) || (expected + 0.001 < val)) {
      ASSERT_EQ(val, expected);
    }
  }
}

TEST(StrToL, Simple1) {
  test_strict_strtoll("123", 123);
  test_strict_strtoll("0", 0);
  test_strict_strtoll("-123", -123);
  test_strict_strtoll("8796093022208", 8796093022208LL);
  test_strict_strtoll("-8796093022208", -8796093022208LL);

  test_strict_strtol("208", 208);
  test_strict_strtol("-4", -4);
  test_strict_strtol("0", 0);
  test_strict_strtol("2147483646", 2147483646);

  test_strict_strtof("0.05", 0.05);
  test_strict_strtof("0", 0.0);
  test_strict_strtof("-0", 0.0);
  test_strict_strtof("10000000.5", 10000000.5);

  test_strict_strtod("-0.2", -0.2);
  test_strict_strtod("0.1", 0.1);
  test_strict_strtod("0", 0.0);
}

static void test_strict_strtoll_err(const char *str)
{
  std::string err;
  strict_strtoll(str, 10, &err);
  ASSERT_NE(err, "");
}

static void test_strict_strtol_err(const char *str)
{
  std::string err;
  strict_strtol(str, 10, &err);
  ASSERT_NE(err, "");
}

static void test_strict_strtod_err(const char *str)
{
  std::string err;
  strict_strtod(str, &err);
  ASSERT_NE(err, "");
}

static void test_strict_strtof_err(const char *str)
{
  std::string err;
  strict_strtof(str, &err);
  ASSERT_NE(err, "");
}

TEST(StrToL, Error1) {
  test_strict_strtoll_err("604462909807314587353088"); // overflow
  test_strict_strtoll_err("aw shucks"); // invalid
  test_strict_strtoll_err("343245 aw shucks"); // invalid chars at end

  test_strict_strtol_err("35 aw shucks"); // invalid chars at end
  test_strict_strtol_err("--0");

  test_strict_strtod_err("345345.0-");
  test_strict_strtod_err("34.0 garbo");

  test_strict_strtof_err("0.05.0");
}
