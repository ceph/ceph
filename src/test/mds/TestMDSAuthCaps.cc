// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>

#include "include/stringify.h"
#include "mds/MDSAuthCaps.h"

#include "gtest/gtest.h"

using std::string;
using std::cout;

const char *parse_good[] = {
  "allow * path=\"/foo\"",
  "allow * path=/foo",
  "allow * path=\"/foo bar/baz\"",
  "allow * uid=1",
  "allow * path=\"/foo\" uid=1",
  "allow *",
  "allow r",
  "allow rw",
  0
};

TEST(MDSAuthCaps, ParseGood) {
  for (int i=0; parse_good[i]; i++) {
    string str = parse_good[i];
    MDSAuthCaps cap;
    std::cout << "Testing good input: '" << str << "'" << std::endl;
    ASSERT_TRUE(cap.parse(str, &cout));
  }
}

const char *parse_bad[] = {
  "allow r poolfoo",
  "allow r w",
  "ALLOW r",
  "allow w",
  "allow rwx,",
  "allow rwx x",
  "allow r pool foo r",
  "allow wwx pool taco",
  "allow wwx pool taco^funny&chars",
  "allow rwx pool 'weird name''",
  "allow rwx object_prefix \"beforepool\" pool weird",
  "allow rwx auid 123 pool asdf",
  "allow xrwx pool foo,, allow r pool bar",
  ";allow rwx pool foo rwx ; allow r pool bar",
  "allow rwx pool foo ;allow r pool bar gibberish",
  "allow rwx auid 123 pool asdf namespace=foo",
  "allow rwx auid 123 namespace",
  "allow rwx namespace",
  "allow namespace",
  "allow namespace=foo",
  "allow rwx auid 123 namespace asdf",
  "allow wwx pool ''",
  0
};

TEST(MDSAuthCaps, ParseBad) {
  for (int i=0; parse_bad[i]; i++) {
    string str = parse_bad[i];
    MDSAuthCaps cap;
    std::cout << "Testing bad input: '" << str << "'" << std::endl;
    ASSERT_FALSE(cap.parse(str, &cout));
  }
}

TEST(MDSAuthCaps, AllowAll) {
  MDSAuthCaps cap;
  ASSERT_FALSE(cap.allow_all());

  ASSERT_TRUE(cap.parse("allow r", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap = MDSAuthCaps();

  ASSERT_TRUE(cap.parse("allow rw", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap = MDSAuthCaps();

  ASSERT_TRUE(cap.parse("allow", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap = MDSAuthCaps();

  ASSERT_TRUE(cap.parse("allow *", NULL));
  ASSERT_TRUE(cap.allow_all());
  ASSERT_TRUE(cap.is_capable("/foo/bar", 0, true, true));
}

TEST(MDSAuthCaps, AllowUid) {
  MDSAuthCaps cap;
  ASSERT_TRUE(cap.parse("allow * uid=10", NULL));
  ASSERT_FALSE(cap.allow_all());
  ASSERT_TRUE(cap.is_capable("/foo", 10, true, true));
  ASSERT_FALSE(cap.is_capable("/foo", -1, true, true));
  ASSERT_FALSE(cap.is_capable("/foo", 0, true, true));
}

TEST(MDSAuthCaps, AllowPath) {
  MDSAuthCaps cap;
  ASSERT_TRUE(cap.parse("allow * path=/sandbox", NULL));
  ASSERT_FALSE(cap.allow_all());
  ASSERT_TRUE(cap.is_capable("/sandbox/foo", 0, true, true));
  ASSERT_TRUE(cap.is_capable("/sandbox", 0, true, true));
  ASSERT_FALSE(cap.is_capable("/foo", 0, true, true));
}

TEST(MDSAuthCaps, OutputParsed) {
  struct CapsTest {
    const char *input;
    const char *output;
  };
  CapsTest test_values[] = {
    {"allow",
     "MDSAuthCaps[allow rw]"},
    {"allow *",
     "MDSAuthCaps[allow *]"},
    {"allow r",
     "MDSAuthCaps[allow r]"},
    {"allow rw",
     "MDSAuthCaps[allow rw]"},
    {"allow * uid=1",
     "MDSAuthCaps[allow * uid=1]"},
    {"allow * path=/foo",
     "MDSAuthCaps[allow * path=\"/foo\"]"},
    {"allow * path=\"/foo\"",
     "MDSAuthCaps[allow * path=\"/foo\"]"},
    {"allow * path=\"/foo\" uid=1",
     "MDSAuthCaps[allow * path=\"/foo\" uid=1]"},
  };
  size_t num_tests = sizeof(test_values) / sizeof(*test_values);
  for (size_t i = 0; i < num_tests; ++i) {
    MDSAuthCaps cap;
    std::cout << "Testing input '" << test_values[i].input << "'" << std::endl;
    ASSERT_TRUE(cap.parse(test_values[i].input, &cout));
    ASSERT_EQ(test_values[i].output, stringify(cap));
  }
}

