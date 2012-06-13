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

#include "osd/OSDCap.h"

#include "gtest/gtest.h"

const char *parse_good[] = {
  "allow *",
  "allow r",
  "allow rwx",
  "allow pool foo r",
  "allow pool taco wx",
  "allow pool taco object_prefix obj wx",
  "allow pool taco object_prefix obj_with_underscores_and_no_quotes wx",
  "allow pool 'weird name' rwx",
  "allow pool \"weird name with ''s\" rwx",
  "allow auid 123 rwx",
  "allow pool foo rwx, allow pool bar r",
  "allow pool foo rwx ; allow pool bar r",
  "allow pool foo rwx ;allow pool bar r",
  "allow pool foo rwx; allow pool bar r",
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
  "allow r pool foo",
  "allow pool taco wwx",
  "allow pool taco^funny&chars wwx",
  "allow pool 'weird name'' rwx",
  "allow object_prefix \"beforepool\" pool weird rwx",
  "allow auid 123 pool asdf rwx",
  "allow pool foo xrwx,, allow pool bar r",
  ";allow pool foo rwx ; allow pool bar r",
  "allow pool foo rwx ;allow pool bar r gibberish",
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

  bool r = cap.parse("allow *", NULL);
  ASSERT_TRUE(r);
  ASSERT_TRUE(cap.allow_all());

  const OSDCapSpec *s;
  s = cap.get_cap(0, "foo", 0, "asdf");
  ASSERT_TRUE(!!s);
  ASSERT_TRUE((s->allow & (OSD_CAP_R|OSD_CAP_W|OSD_CAP_X)) == (OSD_CAP_R|OSD_CAP_W|OSD_CAP_X));
}

TEST(OSDCap, AllowPool) {
  OSDCap cap;
  bool r = cap.parse("allow pool foo rwx", NULL);
  ASSERT_TRUE(r);

  const OSDCapSpec *s;
  s = cap.get_cap(0, "foo", 0, "");
  ASSERT_TRUE(!!s);
  ASSERT_EQ(s->allow, OSD_CAP_R|OSD_CAP_W|OSD_CAP_X);
  s = cap.get_cap(0, "bar", 0, "");
  ASSERT_TRUE(!s || !s->allow);
}

TEST(OSDCap, AllowPools) {
  OSDCap cap;
  bool r = cap.parse("allow pool foo rwx, allow pool bar r", NULL);
  ASSERT_TRUE(r);

  const OSDCapSpec *s;
  s = cap.get_cap(0, "foo", 0, "");
  ASSERT_TRUE(!!s);
  ASSERT_EQ(s->allow, OSD_CAP_R|OSD_CAP_W|OSD_CAP_X);
  s = cap.get_cap(0, "bar", 0, "");
  ASSERT_TRUE(!!s);
  ASSERT_EQ(s->allow, OSD_CAP_R);
  s = cap.get_cap(0, "baz", 0, "");
  ASSERT_TRUE(!s || !s->allow);
}

TEST(OSDCap, ObjectPrefix) {
  OSDCap cap;
  bool r = cap.parse("allow object_prefix foo rwx", NULL);
  ASSERT_TRUE(r);

  const OSDCapSpec *s;
  s = cap.get_cap(0, "foo", 0, "foo");
  ASSERT_TRUE(!!s);
  ASSERT_EQ(s->allow, OSD_CAP_R|OSD_CAP_W|OSD_CAP_X);
  s = cap.get_cap(0, "foo", 0, "food");
  ASSERT_TRUE(!!s);
  ASSERT_EQ(s->allow, OSD_CAP_R|OSD_CAP_W|OSD_CAP_X);
  s = cap.get_cap(0, "foo", 0, "foo_bar");
  ASSERT_TRUE(!!s);
  ASSERT_EQ(s->allow, OSD_CAP_R|OSD_CAP_W|OSD_CAP_X);

  s = cap.get_cap(0, "foo", 0, "_foo");
  ASSERT_TRUE(!s || !s->allow);
  s = cap.get_cap(0, "foo", 0, " foo ");
  ASSERT_TRUE(!s || !s->allow);
  s = cap.get_cap(0, "foo", 0, " foo ");
  ASSERT_TRUE(!s || !s->allow);
}

