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
#include <map>

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


static void test_strict_sistrtoll(const char *str)
{
  std::string err;
  strict_sistrtoll(str, &err);
  ASSERT_EQ(err, "");
}

static void test_strict_sistrtoll_units(const std::string& foo,
                                      char u, const int m)
{
  std::string s(foo);
  s.push_back(u);
  const char *str = s.c_str();
  std::string err;
  uint64_t r = strict_sistrtoll(str, &err);
  ASSERT_EQ(err, "");

  str = foo.c_str();
  std::string err2;
  long long tmp = strict_strtoll(str, 10, &err2);
  ASSERT_EQ(err2, "");
  tmp = (tmp << m);
  ASSERT_EQ(tmp, (long long)r);
}

TEST(SIStrToLL, WithUnits) {
  std::map<char,int> units;
  units['B'] = 0;
  units['K'] = 10;
  units['M'] = 20;
  units['G'] = 30;
  units['T'] = 40;
  units['P'] = 50;
  units['E'] = 60;

  for (std::map<char,int>::iterator p = units.begin();
       p != units.end(); ++p) {
    // the upper bound of uint64_t is 2^64 = 4E
    test_strict_sistrtoll_units("4", p->first, p->second);
    test_strict_sistrtoll_units("1", p->first, p->second);
    test_strict_sistrtoll_units("0", p->first, p->second);
  }
}

TEST(SIStrToLL, WithoutUnits) {
  test_strict_sistrtoll("1024");
  test_strict_sistrtoll("1152921504606846976");
  test_strict_sistrtoll("0");
}

static void test_strict_sistrtoll_err(const char *str)
{
  std::string err;
  strict_sistrtoll(str, &err);
  ASSERT_NE(err, "");
}

TEST(SIStrToLL, Error) {
  test_strict_sistrtoll_err("1024F");
  test_strict_sistrtoll_err("QDDSA");
  test_strict_sistrtoll_err("1b");
  test_strict_sistrtoll_err("100k");
  test_strict_sistrtoll_err("1000m");
  test_strict_sistrtoll_err("1g");
  test_strict_sistrtoll_err("20t");
  test_strict_sistrtoll_err("100p");
  test_strict_sistrtoll_err("1000e");
  test_strict_sistrtoll_err("B");
  test_strict_sistrtoll_err("M");
  test_strict_sistrtoll_err("BM");
  test_strict_sistrtoll_err("B0wef");
  test_strict_sistrtoll_err("0m");
  test_strict_sistrtoll_err("-1"); // it returns uint64_t
  test_strict_sistrtoll_err("-1K");
  // the upper bound of uint64_t is 2^64 = 4E, so 1024E overflows
  test_strict_sistrtoll_err("1024E"); // overflows after adding the suffix
}

// since strict_sistrtoll is an alias of strict_si_cast<uint64_t>(), quite a few
// of cases are covered by existing test cases of strict_sistrtoll already.
TEST(StrictSICast, Error) {
  {
    std::string err;
    // the SI prefix is way too large for `int`.
    (void)strict_si_cast<int>("2E", &err);
    ASSERT_NE(err, "");
  }
  {
    std::string err;
    (void)strict_si_cast<int>("-2E", &err);
    ASSERT_NE(err, "");
  }
  {
    std::string err;
    (void)strict_si_cast<int>("1T", &err);
    ASSERT_NE(err, "");
  }
}

/*
 * Local Variables:
 * compile-command: "cd .. ; make unittest_strtol && ./unittest_strtol"
 * End:
 */
