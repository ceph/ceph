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

using namespace std;

entity_addr_t addr;

const char *parse_good[] = {
  "allow rw uid=1 gids=1",
  "allow * path=\"/foo\"",
  "allow * path=/foo",
  "allow * path=/foo-bar_baz",
  "allow * path=\"/foo bar/baz\"",
  "allow * uid=1",
  "allow * path=\"/foo\" uid=1",
  "allow *",
  "allow r",
  "allow rw",
  "allow rw uid=1 gids=1,2,3",
  "allow rw path=/foo uid=1 gids=1,2,3",
  "allow r, allow rw path=/foo",
  "allow r, allow * uid=1",
  "allow r ,allow * uid=1",
  "allow r ;allow * uid=1",
  "allow r ; allow * uid=1",
  "allow r ; allow * uid=1",
  "allow r uid=1 gids=1,2,3, allow * uid=2",
  "allow r network 1.2.3.4/8",
  "allow rw path=/foo uid=1 gids=1,2,3 network 2.3.4.5/16",
  "allow r root_squash",
  "allow rw path=/foo root_squash",
  "allow rw fsname=a root_squash",
  "allow rw fsname=a path=/foo root_squash",
  "allow rw fsname=a root_squash, allow rwp fsname=a path=/volumes",
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
  "allow rw gids=1",
  "allow rw gids=1,2,3",
  "allow rw uid=123 gids=asdf",
  "allow rw uid=123 gids=1,2,asdf",
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
  ASSERT_TRUE(cap.is_capable("foo/bar", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
}

TEST(MDSAuthCaps, AllowUid) {
  MDSAuthCaps cap;
  ASSERT_TRUE(cap.parse("allow * uid=10", NULL));
  ASSERT_FALSE(cap.allow_all());

  // uid/gid must be valid
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0777, 10, 0, NULL, MAY_READ, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0777, 10, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0777, 12, 12, NULL, MAY_READ, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0777, 10, 13, NULL, MAY_READ, 0, 0, addr));
}

TEST(MDSAuthCaps, AllowUidGid) {
  MDSAuthCaps cap;
  ASSERT_TRUE(cap.parse("allow * uid=10 gids=10,11,12; allow * uid=12 gids=12,10", NULL));
  ASSERT_FALSE(cap.allow_all());

  // uid/gid must be valid
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0777, 10, 0, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0777, 9, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0777, 10, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0777, 12, 12, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0777, 10, 13, NULL, MAY_READ, 0, 0, addr));

  // user
  ASSERT_TRUE(cap.is_capable("foo", 10, 10, 0500, 10, 11, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 10, 10, 0500, 10, 11, NULL, MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 10, 10, 0500, 10, 11, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 10, 10, 0700, 10, 11, NULL, MAY_READ, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 10, 10, 0700, 10, 11, NULL, MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 10, 10, 0700, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 10, 0, 0700, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 12, 0, 0700, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 12, 0, 0700, 12, 12, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0700, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));

  // group
  vector<uint64_t> glist10;
  glist10.push_back(10);
  vector<uint64_t> dglist10;
  dglist10.push_back(8);
  dglist10.push_back(10);
  vector<uint64_t> glist11;
  glist11.push_back(11);
  vector<uint64_t> glist12;
  glist12.push_back(12);
  ASSERT_TRUE(cap.is_capable("foo", 0, 10, 0750, 10, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 10, 0750, 10, 10, NULL, MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 10, 0770, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 10, 0770, 10, 11, &glist10, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 11, 0770, 10, 10, &glist11, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 11, 0770, 10, 11, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 12, 0770, 12, 12, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 10, 0770, 12, 12, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 10, 0770, 12, 12, &glist10, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 10, 0770, 12, 12, &dglist10, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 11, 0770, 12, 12, &glist11, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 12, 0770, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 12, 0770, 10, 10, &glist12, MAY_READ | MAY_WRITE, 0, 0, addr));

  // user > group
  ASSERT_TRUE(cap.is_capable("foo", 10, 10, 0570, 10, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 10, 10, 0570, 10, 10, NULL, MAY_WRITE, 0, 0, addr));

  // other
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0775, 10, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0770, 10, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0775, 10, 10, NULL, MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0775, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0777, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0773, 10, 10, NULL, MAY_READ, 0, 0, addr));

  // group > other
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0557, 10, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 10, 0557, 10, 10, NULL, MAY_WRITE, 0, 0, addr));

  // user > other
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0557, 10, 10, NULL, MAY_READ, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 10, 0, 0557, 10, 10, NULL, MAY_WRITE, 0, 0, addr));
}

TEST(MDSAuthCaps, AllowPath) {
  MDSAuthCaps cap;
  ASSERT_TRUE(cap.parse("allow * path=/sandbox", NULL));
  ASSERT_FALSE(cap.allow_all());
  ASSERT_TRUE(cap.is_capable("sandbox/foo", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(cap.is_capable("sandbox", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("sandboxed", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
}

TEST(MDSAuthCaps, AllowPathChars) {
  MDSAuthCaps unquo_cap;
  ASSERT_TRUE(unquo_cap.parse("allow * path=/sandbox-._foo", NULL));
  ASSERT_FALSE(unquo_cap.allow_all());
  ASSERT_TRUE(unquo_cap.is_capable("sandbox-._foo/foo", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(unquo_cap.is_capable("sandbox", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(unquo_cap.is_capable("sandbox-._food", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(unquo_cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
}


TEST(MDSAuthCaps, AllowPathCharsQuoted) {
  MDSAuthCaps quo_cap;
  ASSERT_TRUE(quo_cap.parse("allow * path=\"/sandbox-._foo\"", NULL));
  ASSERT_FALSE(quo_cap.allow_all());
  ASSERT_TRUE(quo_cap.is_capable("sandbox-._foo/foo", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(quo_cap.is_capable("sandbox", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(quo_cap.is_capable("sandbox-._food", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(quo_cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
}

TEST(MDSAuthCaps, RootSquash) {
  MDSAuthCaps rs_cap;
  ASSERT_TRUE(rs_cap.parse("allow rw root_squash, allow rw path=/sandbox", NULL));
  ASSERT_TRUE(rs_cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ, 0, 0, addr));
  ASSERT_TRUE(rs_cap.is_capable("foo", 0, 0, 0777, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_FALSE(rs_cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(rs_cap.is_capable("sandbox", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(rs_cap.is_capable("sandbox/foo", 0, 0, 0777, 0, 0, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
  ASSERT_TRUE(rs_cap.is_capable("sandbox/foo", 0, 0, 0777, 10, 10, NULL, MAY_READ | MAY_WRITE, 0, 0, addr));
}

TEST(MDSAuthCaps, OutputParsed) {
  struct CapsTest {
    const char *input;
    const char *output;
  };
  CapsTest test_values[] = {
    {"allow",
     "MDSAuthCaps[allow rwps]"},
    {"allow *",
     "MDSAuthCaps[allow *]"},
    {"allow r",
     "MDSAuthCaps[allow r]"},
    {"allow rw",
     "MDSAuthCaps[allow rw]"},
    {"allow * uid=1",
     "MDSAuthCaps[allow * uid=1]"},
    {"allow * uid=1 gids=1",
     "MDSAuthCaps[allow * uid=1 gids=1]"},
    {"allow * uid=1 gids=1,2,3",
     "MDSAuthCaps[allow * uid=1 gids=1,2,3]"},
    {"allow * path=/foo",
     "MDSAuthCaps[allow * path=\"/foo\"]"},
    {"allow * path=\"/foo\"",
     "MDSAuthCaps[allow * path=\"/foo\"]"},
    {"allow rw root_squash",
     "MDSAuthCaps[allow rw root_squash]"},
    {"allow rw fsname=a root_squash",
     "MDSAuthCaps[allow rw fsname=a root_squash]"},
    {"allow * path=\"/foo\" root_squash",
     "MDSAuthCaps[allow * path=\"/foo\" root_squash]"},
    {"allow * path=\"/foo\" uid=1",
     "MDSAuthCaps[allow * path=\"/foo\" uid=1]"},
    {"allow * path=\"/foo\" uid=1 gids=1,2,3",
     "MDSAuthCaps[allow * path=\"/foo\" uid=1 gids=1,2,3]"},
    {"allow r uid=1 gids=1,2,3, allow * uid=2",
     "MDSAuthCaps[allow r uid=1 gids=1,2,3, allow * uid=2]"},
    {"allow r uid=1 gids=1,2,3, allow * uid=2 network 10.0.0.0/8",
     "MDSAuthCaps[allow r uid=1 gids=1,2,3, allow * uid=2 network 10.0.0.0/8]"},
    {"allow rw fsname=b, allow rw fsname=a root_squash",
     "MDSAuthCaps[allow rw fsname=b, allow rw fsname=a root_squash]"},
  };
  size_t num_tests = sizeof(test_values) / sizeof(*test_values);
  for (size_t i = 0; i < num_tests; ++i) {
    MDSAuthCaps cap;
    std::cout << "Testing input '" << test_values[i].input << "'" << std::endl;
    ASSERT_TRUE(cap.parse(test_values[i].input, &cout));
    ASSERT_EQ(test_values[i].output, stringify(cap));
  }
}

TEST(MDSAuthCaps, network) {
  entity_addr_t a, b, c;
  a.parse("10.1.2.3");
  b.parse("192.168.2.3");
  c.parse("192.167.2.3");

  MDSAuthCaps cap;
  ASSERT_TRUE(cap.parse("allow * network 192.168.0.0/16, allow * network 10.0.0.0/8", NULL));

  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ, 0, 0, a));
  ASSERT_TRUE(cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ, 0, 0, b));
  ASSERT_FALSE(cap.is_capable("foo", 0, 0, 0777, 0, 0, NULL, MAY_READ, 0, 0, c));
}
