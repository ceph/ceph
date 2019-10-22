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
#include "mgr/MgrCap.h"

#include "gtest/gtest.h"

const char *parse_good[] = {

  // MgrCapMatch
  "allow *",
  "allow r",
  "allow rwx",
  "allow  r",
  " allow rwx",
  "allow rwx ",
  " allow rwx ",
  " allow\t   rwx ",
  "\tallow\nrwx\t",
  "allow service=foo x",
  "allow service=\"froo\" x",
  "allow profile read-only",
  "allow profile read-write",
  "allow profile \"rbd-read-only\", allow *",
  "allow command \"a b c\"",
  "allow command abc",
  "allow command abc with arg=foo",
  "allow command abc with arg=foo arg2=bar",
  "allow command abc with arg=foo arg2=bar",
  "allow command abc with arg=foo arg2 prefix bar arg3 prefix baz",
  "allow command abc with arg=foo arg2 prefix \"bar bingo\" arg3 prefix baz",
  "allow command abc with arg regex \"^[0-9a-z.]*$\"",
  "allow command abc with arg regex \"\(invaluid regex\"",
  "allow service foo x",
  "allow service foo x; allow service bar x",
  "allow service foo w ;allow service bar x",
  "allow service foo  w , allow service bar x",
  "allow service foo r , allow service bar x",
  "allow service foo_foo r, allow service bar r",
  "allow service foo-foo r, allow service bar r",
  "allow service \" foo \" w, allow service bar r",
  "allow module foo x",
  "allow module=foo x",
  "allow module foo_foo r",
  "allow module \" foo \" w",
  "allow module foo with arg1=value1 x",
  "allow command abc with arg=foo arg2=bar, allow service foo r",
  "allow command abc.def with arg=foo arg2=bar, allow service foo r",
  "allow command \"foo bar\" with arg=\"baz\"",
  "allow command \"foo bar\" with arg=\"baz.xx\"",
  "allow command \"foo bar\" with arg = \"baz.xx\"",
  "profile crash",
  "profile osd",
  "profile mds",
  "profile rbd pool=ABC namespace=NS",
  "profile \"rbd-read-only\", profile crash",
  "allow * network 1.2.3.4/24",
  "allow * network ::1/128",
  "allow * network [aa:bb::1]/128",
  "allow service=foo x network 1.2.3.4/16",
  "allow command abc network 1.2.3.4/8",
  "profile crash network 1.2.3.4/32",
  "allow profile crash network 1.2.3.4/32",
  0
};

TEST(MgrCap, ParseGood) {
  for (int i=0; parse_good[i]; ++i) {
    string str = parse_good[i];
    MgrCap cap;
    std::cout << "Testing good input: '" << str << "'" << std::endl;
    ASSERT_TRUE(cap.parse(str, &cout));
    std::cout << "                                         -> " << cap
              << std::endl;
  }
}

// these should stringify to the input value
const char *parse_identity[] = {
  "allow *",
  "allow r",
  "allow rwx",
  "allow service foo x",
  "profile crash",
  "profile rbd-read-only, allow *",
  "profile rbd namespace=NS pool=ABC",
  "allow command abc",
  "allow command \"a b c\"",
  "allow command abc with arg=foo",
  "allow command abc with arg=foo arg2=bar",
  "allow command abc with arg=foo arg2=bar",
  "allow command abc with arg=foo arg2 prefix bar arg3 prefix baz",
  "allow command abc with arg=foo arg2 prefix \"bar bingo\" arg3 prefix baz",
  "allow service foo x",
  "allow service foo x, allow service bar x",
  "allow service foo w, allow service bar x",
  "allow service foo r, allow service bar x",
  "allow service foo_foo r, allow service bar r",
  "allow service foo-foo r, allow service bar r",
  "allow service \" foo \" w, allow service bar r",
  "allow module foo x",
  "allow module \" foo_foo \" r",
  "allow module foo with arg1=value1 x",
  "allow command abc with arg=foo arg2=bar, allow service foo r",
  0
};

TEST(MgrCap, ParseIdentity)
{
  for (int i=0; parse_identity[i]; ++i) {
    string str = parse_identity[i];
    MgrCap cap;
    std::cout << "Testing good input: '" << str << "'" << std::endl;
    ASSERT_TRUE(cap.parse(str, &cout));
    string out = stringify(cap);
    ASSERT_EQ(out, str);
  }
}

const char *parse_bad[] = {
  "allow r foo",
  "allow*",
  "foo allow *",
  "profile foo rwx",
  "profile",
  "profile foo bar rwx",
  "allow profile foo rwx",
  "allow profile",
  "allow profile foo bar rwx",
  "allow service bar",
  "allow command baz x",
  "allow r w",
  "ALLOW r",
  "allow rwx,",
  "allow rwx x",
  "allow r pool foo r",
  "allow wwx pool taco",
  "allow wwx pool taco^funny&chars",
  "allow rwx pool 'weird name''",
  "allow rwx object_prefix \"beforepool\" pool weird",
  "allow rwx auid 123 pool asdf",
  "allow command foo a prefix b",
  "allow command foo with a prefixb",
  "allow command foo with a = prefix b",
  "allow command foo with a prefix b c",
  0
};

TEST(MgrCap, ParseBad) {
  for (int i=0; parse_bad[i]; ++i) {
    string str = parse_bad[i];
    MgrCap cap;
    std::cout << "Testing bad input: '" << str << "'" << std::endl;
    ASSERT_FALSE(cap.parse(str, &cout));
  }
}

TEST(MgrCap, AllowAll) {
  MgrCap cap;
  ASSERT_FALSE(cap.is_allow_all());

  ASSERT_TRUE(cap.parse("allow r", nullptr));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow w", nullptr));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow x", nullptr));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rwx", nullptr));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rw", nullptr));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rx", nullptr));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow wx", nullptr));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow *", nullptr));
  ASSERT_TRUE(cap.is_allow_all());
  ASSERT_TRUE(cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true,
                             true, {}));

  MgrCap cap2;
  ASSERT_FALSE(cap2.is_allow_all());
  cap2.set_allow_all();
  ASSERT_TRUE(cap2.is_allow_all());
}

TEST(MgrCap, Network) {
  MgrCap cap;
  bool r = cap.parse("allow * network 192.168.0.0/16, allow * network 10.0.0.0/8", nullptr);
  ASSERT_TRUE(r);

  entity_addr_t a, b, c;
  a.parse("10.1.2.3");
  b.parse("192.168.2.3");
  c.parse("192.167.2.3");

  ASSERT_TRUE(cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true,
                             true, a));
  ASSERT_TRUE(cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true,
                             true, b));
  ASSERT_FALSE(cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true,
                              true, c));
}

TEST(MgrCap, CommandRegEx) {
  MgrCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(cap.parse("allow command abc with arg regex \"^[0-9a-z.]*$\"",
                        nullptr));

  EntityName name;
  name.from_str("osd.123");
  ASSERT_TRUE(cap.is_capable(nullptr, name, "", "", "abc",
                             {{"arg", "12345abcde"}}, true, true, true, {}));
  ASSERT_FALSE(cap.is_capable(nullptr, name, "", "", "abc", {{"arg", "~!@#$"}},
                              true, true, true, {}));

  ASSERT_TRUE(cap.parse("allow command abc with arg regex \"[*\"", nullptr));
  ASSERT_FALSE(cap.is_capable(nullptr, name, "", "", "abc", {{"arg", ""}}, true,
                              true, true, {}));
}

TEST(MgrCap, Module) {
  MgrCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(cap.parse("allow module abc r, allow module bcd w", nullptr));

  ASSERT_FALSE(cap.is_capable(nullptr, {}, "", "abc", "", {}, true, true, false,
                              {}));
  ASSERT_TRUE(cap.is_capable(nullptr, {}, "", "abc", "", {}, true, false, false,
                             {}));
  ASSERT_FALSE(cap.is_capable(nullptr, {}, "", "bcd", "", {}, true, true, false,
                              {}));
  ASSERT_TRUE(cap.is_capable(nullptr, {}, "", "bcd", "", {}, false, true, false,
                             {}));
}

TEST(MgrCap, Profile) {
  MgrCap cap;
  ASSERT_FALSE(cap.is_allow_all());

  ASSERT_FALSE(cap.parse("profile unknown"));
  ASSERT_FALSE(cap.parse("profile rbd invalid-key=value"));

  ASSERT_TRUE(cap.parse("profile rbd", nullptr));
  ASSERT_FALSE(cap.is_capable(nullptr, {}, "", "abc", "", {}, true, false,
                              false, {}));
  ASSERT_TRUE(cap.is_capable(nullptr, {}, "", "rbd_support", "", {}, true,
                             true, false, {}));
  ASSERT_TRUE(cap.is_capable(nullptr, {}, "", "rbd_support", "", {}, true,
                             false, false, {}));

  ASSERT_TRUE(cap.parse("profile rbd pool=abc namespace prefix def", nullptr));
  ASSERT_FALSE(cap.is_capable(nullptr, {}, "", "rbd_support", "", {},
                              true, true, false, {}));
  ASSERT_FALSE(cap.is_capable(nullptr, {}, "", "rbd_support", "",
                              {{"pool", "abc"}},
                              true, true, false, {}));
  ASSERT_TRUE(cap.is_capable(nullptr, {}, "", "rbd_support", "",
                             {{"pool", "abc"}, {"namespace", "defghi"}},
                             true, true, false, {}));

  ASSERT_TRUE(cap.parse("profile rbd-read-only", nullptr));
  ASSERT_FALSE(cap.is_capable(nullptr, {}, "", "abc", "", {}, true, false,
                              false, {}));
  ASSERT_FALSE(cap.is_capable(nullptr, {}, "", "rbd_support", "", {}, true,
                              true, false, {}));
  ASSERT_TRUE(cap.is_capable(nullptr, {}, "", "rbd_support", "", {}, true,
                             false, false, {}));
}
