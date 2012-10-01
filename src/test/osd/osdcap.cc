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
#include "osd/OSDCap.h"

#include "gtest/gtest.h"

const char *parse_good[] = {
  "allow *",
  "allow r",
  "allow rwx",
  "allow r pool foo ",
  "allow r pool=foo",
  " allow wx pool taco",
  "allow pool foo r",
  "allow pool taco wx",
  "allow wx pool taco object_prefix obj",
  "allow wx pool taco object_prefix obj_with_underscores_and_no_quotes",
  "allow pool taco object_prefix obj wx",
  "allow pool taco object_prefix obj_with_underscores_and_no_quotes wx",
  "allow rwx pool 'weird name'",
  "allow rwx pool \"weird name with ''s\"",
  "allow rwx auid 123",
  "allow rwx pool foo, allow r pool bar",
  "allow rwx pool foo ; allow r pool bar",
  "allow rwx pool foo ;allow r pool bar",
  "allow rwx pool foo; allow r pool bar",
  "allow auid 123 rwx",
  "allow pool foo rwx, allow pool bar r",
  "allow pool foo rwx ; allow pool bar r",
  "allow pool foo rwx ;allow pool bar r",
  "allow pool foo rwx; allow pool bar r",
  "  allow rwx pool foo; allow r pool bar  ",
  "  allow   rwx   pool foo; allow r pool bar  ",
  "  allow pool foo rwx; allow pool bar r  ",
  "  allow     pool foo rwx; allow pool bar r  ",
  "allow pool data rw, allow pool rbd rwx, allow pool images class rbd foo",
  "allow class foo",
  "allow class clsname \"clsthingidon'tunderstand\"",
  0
};

TEST(OSDCap, ParseGood) {
  for (int i=0; parse_good[i]; i++) {
    string str = parse_good[i];
    OSDCap cap;
    bool r = cap.parse(str, &cout);
    ASSERT_TRUE(r);
  }
}

const char *parse_bad[] = {
  "ALLOW r",
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
  0
};

TEST(OSDCap, ParseBad) {
  for (int i=0; parse_bad[i]; i++) {
    string str = parse_bad[i];
    OSDCap cap;
    bool r = cap.parse(str, &cout);
    ASSERT_FALSE(r);
  }
}

TEST(OSDCap, AllowAll) {
  OSDCap cap;
  ASSERT_FALSE(cap.allow_all());

  ASSERT_TRUE(cap.parse("allow r", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow w", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow x", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rwx", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rw", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rx", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow wx", NULL));
  ASSERT_FALSE(cap.allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow *", NULL));
  ASSERT_TRUE(cap.allow_all());
  ASSERT_TRUE(cap.is_capable("foo", 0, "asdf", true, true, true));
}

TEST(OSDCap, AllowPool) {
  OSDCap cap;
  bool r = cap.parse("allow rwx pool foo", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("foo", 0, "", true, true, true));
  ASSERT_FALSE(cap.is_capable("bar", 0, "", true, true, true));
}

TEST(OSDCap, AllowPools) {
  OSDCap cap;
  bool r = cap.parse("allow rwx pool foo, allow r pool bar", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("foo", 0, "", true, true, true));
  ASSERT_TRUE(cap.is_capable("bar", 0, "", true, false, false));
  ASSERT_FALSE(cap.is_capable("bar", 0, "", true, true, true));
  ASSERT_FALSE(cap.is_capable("baz", 0, "", true, false, false));
}

TEST(OSDCap, AllowPools2) {
  OSDCap cap;
  bool r = cap.parse("allow r, allow rwx pool foo", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("foo", 0, "", true, true, true));
  ASSERT_FALSE(cap.is_capable("bar", 0, "", true, true, true));
  ASSERT_TRUE(cap.is_capable("bar", 0, "", true, false, false));
}

TEST(OSDCap, ObjectPrefix) {
  OSDCap cap;
  bool r = cap.parse("allow rwx object_prefix foo", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("bar", 0, "foo", true, true, true));
  ASSERT_TRUE(cap.is_capable("bar", 0, "food", true, true, true));
  ASSERT_TRUE(cap.is_capable("bar", 0, "foo_bar", true, true, true));

  ASSERT_FALSE(cap.is_capable("bar", 0, "_foo", true, true, true));
  ASSERT_FALSE(cap.is_capable("bar", 0, " foo ", true, true, true));
  ASSERT_FALSE(cap.is_capable("bar", 0, "fo", true, true, true));
}

TEST(OSDCap, ObjectPoolAndPrefix) {
  OSDCap cap;
  bool r = cap.parse("allow rwx pool bar object_prefix foo", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("bar", 0, "foo", true, true, true));
  ASSERT_TRUE(cap.is_capable("bar", 0, "food", true, true, true));
  ASSERT_TRUE(cap.is_capable("bar", 0, "foo_bar", true, true, true));

  ASSERT_FALSE(cap.is_capable("baz", 0, "foo", true, true, true));
  ASSERT_FALSE(cap.is_capable("baz", 0, "food", true, true, true));
  ASSERT_FALSE(cap.is_capable("baz", 0, "fo", true, true, true));
}

TEST(OSDCap, OutputParsed)
{
  struct CapsTest {
    const char *input;
    const char *output;
  };
  CapsTest test_values[] = {
    {"allow *",
     "osdcap[grant(*)]"},
    {"allow r",
     "osdcap[grant(r)]"},
    {"allow rx",
     "osdcap[grant(rx)]"},
    {"allow rwx",
     "osdcap[grant(rwx)]"},
    {"allow rwx pool images",
     "osdcap[grant(pool images rwx)]"},
    {"allow r pool images",
     "osdcap[grant(pool images r)]"},
    {"allow pool images rwx",
     "osdcap[grant(pool images rwx)]"},
    {"allow pool images r",
     "osdcap[grant(pool images r)]"},
    {"allow pool images w",
     "osdcap[grant(pool images w)]"},
    {"allow pool images x",
     "osdcap[grant(pool images x)]"},
    {"allow pool images r; allow pool rbd rwx",
     "osdcap[grant(pool images r),grant(pool rbd rwx)]"},
    {"allow pool images r, allow pool rbd rwx",
     "osdcap[grant(pool images r),grant(pool rbd rwx)]"}
  };

  size_t num_tests = sizeof(test_values) / sizeof(*test_values);
  for (size_t i = 0; i < num_tests; ++i) {
    OSDCap cap;
    std::cout << "Testing input '" << test_values[i].input << "'" << std::endl;
    ASSERT_TRUE(cap.parse(test_values[i].input));
    ASSERT_EQ(test_values[i].output, stringify(cap));
  }
}
