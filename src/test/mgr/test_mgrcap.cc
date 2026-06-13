// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "gtest/gtest.h"
#include "include/stringify.h"
#include "mgr/MgrCap.h"

using namespace std;

const std::vector<std::string> parse_good = {
    // MgrCapMatch
    "allow *"s,
    "allow r"s,
    "allow rwx"s,
    "allow  r"s,
    " allow rwx"s,
    "allow rwx "s,
    " allow rwx "s,
    " allow\t   rwx "s,
    "\tallow\nrwx\t"s,
    "allow service=foo x"s,
    "allow service=\"froo\" x"s,
    "allow profile read-only"s,
    "allow profile read-write"s,
    "allow profile \"rbd-read-only\", allow *"s,
    "allow command \"a b c\""s,
    "allow command abc"s,
    "allow command abc with arg=foo"s,
    "allow command abc with arg=foo arg2=bar"s,
    "allow command abc with arg=foo arg2=bar"s,
    "allow command abc with arg=foo arg2 prefix bar arg3 prefix baz"s,
    "allow command abc with arg=foo arg2 prefix \"bar bingo\" arg3 prefix baz"s,
    "allow command abc with arg regex \"^[0-9a-z.]*$\""s,
    "allow command abc with arg regex \"\(invaluid regex\""s,
    "allow service foo x"s,
    "allow service foo x; allow service bar x"s,
    "allow service foo w ;allow service bar x"s,
    "allow service foo  w , allow service bar x"s,
    "allow service foo r , allow service bar x"s,
    "allow service foo_foo r, allow service bar r"s,
    "allow service foo-foo r, allow service bar r"s,
    "allow service \" foo \" w, allow service bar r"s,
    "allow module foo x"s,
    "allow module=foo x"s,
    "allow module foo_foo r"s,
    "allow module \" foo \" w"s,
    "allow module foo with arg1=value1 x"s,
    "allow command abc with arg=foo arg2=bar, allow service foo r"s,
    "allow command abc.def with arg=foo arg2=bar, allow service foo r"s,
    "allow command \"foo bar\" with arg=\"baz\""s,
    "allow command \"foo bar\" with arg=\"baz.xx\""s,
    "allow command \"foo bar\" with arg = \"baz.xx\""s,
    "profile crash"s,
    "profile osd"s,
    "profile mds"s,
    "profile rbd pool=ABC namespace=NS"s,
    "profile \"rbd-read-only\", profile crash"s,
    "allow * network 1.2.3.4/24"s,
    "allow * network ::1/128"s,
    "allow * network [aa:bb::1]/128"s,
    "allow service=foo x network 1.2.3.4/16"s,
    "allow command abc network 1.2.3.4/8"s,
    "profile crash network 1.2.3.4/32"s,
    "allow profile crash network 1.2.3.4/32"s,
};

TEST(MgrCap, ParseGood)
{
  for (const auto& parseable : parse_good) {
    MgrCap cap;
    std::cout << "Testing good input: '" << parseable << "'" << std::endl;
    ASSERT_TRUE(cap.parse(parseable, &cout));
    std::cout << "                                         -> " << cap
              << std::endl;
  }
}

// these should stringify to the input value
const std::vector<std::string> parse_identity = {
    "allow *"s,
    "allow r"s,
    "allow rwx"s,
    "allow service foo x"s,
    "profile crash"s,
    "profile rbd-read-only, allow *"s,
    "profile rbd namespace=NS pool=ABC"s,
    "allow command abc"s,
    "allow command \"a b c\""s,
    "allow command abc with arg=foo"s,
    "allow command abc with arg=foo arg2=bar"s,
    "allow command abc with arg=foo arg2=bar"s,
    "allow command abc with arg=foo arg2 prefix bar arg3 prefix baz"s,
    "allow command abc with arg=foo arg2 prefix \"bar bingo\" arg3 prefix baz"s,
    "allow service foo x"s,
    "allow service foo x, allow service bar x"s,
    "allow service foo w, allow service bar x"s,
    "allow service foo r, allow service bar x"s,
    "allow service foo_foo r, allow service bar r"s,
    "allow service foo-foo r, allow service bar r"s,
    "allow service \" foo \" w, allow service bar r"s,
    "allow module foo x"s,
    "allow module \" foo_foo \" r"s,
    "allow module foo with arg1=value1 x"s,
    "allow command abc with arg=foo arg2=bar, allow service foo r"s,
};

TEST(MgrCap, ParseIdentity)
{
  for (const auto& parseable : parse_identity) {
    MgrCap cap;
    std::cout << "Testing good input: '" << parseable << "'" << std::endl;
    ASSERT_TRUE(cap.parse(parseable, &cout));
    string out = stringify(cap);
    ASSERT_EQ(out, parseable);
  }
}

const std::vector<std::string> parse_bad = {
    "allow r foo"s,
    "allow*"s,
    "foo allow *"s,
    "profile foo rwx"s,
    "profile"s,
    "profile foo bar rwx"s,
    "allow profile foo rwx"s,
    "allow profile"s,
    "allow profile foo bar rwx"s,
    "allow service bar"s,
    "allow command baz x"s,
    "allow r w"s,
    "ALLOW r"s,
    "allow rwx,"s,
    "allow rwx x"s,
    "allow r pool foo r"s,
    "allow wwx pool taco"s,
    "allow wwx pool taco^funny&chars"s,
    "allow rwx pool 'weird name''"s,
    "allow rwx object_prefix \"beforepool\" pool weird"s,
    "allow rwx auid 123 pool asdf"s,
    "allow command foo a prefix b"s,
    "allow command foo with a prefixb"s,
    "allow command foo with a = prefix b"s,
    "allow command foo with a prefix b c"s,
    // Empty and whitespace strings
    ""s,
    "   "s,
    "\t\n"s,
    // Empty names
    "allow command \"\""s,
    "allow service \"\" r"s,
    "allow module \"\" r"s,
    // Invalid profile parameters
    "profile unknown"s,
    "profile rbd invalid-key=value"s,
    "profile rbd pool"s,
    "profile rbd pool="s,
    "profile rbd =value"s,
};

TEST(MgrCap, ParseBad)
{
  for (const auto& unparseable : parse_bad) {
    MgrCap cap;
    std::cout << "Testing bad input: '" << unparseable << "'" << std::endl;
    ASSERT_FALSE(cap.parse(unparseable, &cout));
  }
}

TEST(MgrCap, AllowAll)
{
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
  ASSERT_TRUE(
      cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true, true, {}));

  MgrCap cap2;
  ASSERT_FALSE(cap2.is_allow_all());
  cap2.set_allow_all();
  ASSERT_TRUE(cap2.is_allow_all());
}

TEST(MgrCap, Network)
{
  MgrCap cap;
  bool r = cap.parse(
      "allow * network 192.168.0.0/16, allow * network 10.0.0.0/8", nullptr);
  ASSERT_TRUE(r);

  entity_addr_t a, b, c;
  a.parse("10.1.2.3");
  b.parse("192.168.2.3");
  c.parse("192.167.2.3");

  ASSERT_TRUE(
      cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true, true, a));
  ASSERT_TRUE(
      cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true, true, b));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true, true, c));
}

TEST(MgrCap, NetworkIPv6)
{
  MgrCap cap;
  bool r = cap.parse(
      "allow * network 2001:db8::/32, allow * network fe80::/10", nullptr);
  ASSERT_TRUE(r);

  entity_addr_t a, b, c;
  a.parse("[2001:db8::1]");
  b.parse("[fe80::1]");
  c.parse("[2001:db9::1]");

  ASSERT_TRUE(
      cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true, true, a));
  ASSERT_TRUE(
      cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true, true, b));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "foo", "", "asdf", {}, true, true, true, c));
}

TEST(MgrCap, CommandRegEx)
{
  MgrCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(
      cap.parse("allow command abc with arg regex \"^[0-9a-z.]*$\"", nullptr));

  EntityName name;
  name.from_str("osd.123");
  ASSERT_TRUE(cap.is_capable(
      nullptr, name, "", "", "abc", {{"arg", "12345abcde"}}, true, true, true,
      {}));
  ASSERT_FALSE(cap.is_capable(
      nullptr, name, "", "", "abc", {{"arg", "~!@#$"}}, true, true, true, {}));

  ASSERT_TRUE(cap.parse("allow command abc with arg regex \"[*\"", nullptr));
  ASSERT_FALSE(cap.is_capable(
      nullptr, name, "", "", "abc", {{"arg", ""}}, true, true, true, {}));
}

TEST(MgrCap, Module)
{
  MgrCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(cap.parse("allow module abc r, allow module bcd w", nullptr));

  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "abc", "", {}, true, true, false, {}));
  ASSERT_TRUE(
      cap.is_capable(nullptr, {}, "", "abc", "", {}, true, false, false, {}));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "bcd", "", {}, true, true, false, {}));
  ASSERT_TRUE(
      cap.is_capable(nullptr, {}, "", "bcd", "", {}, false, true, false, {}));
}

TEST(MgrCap, Profile)
{
  MgrCap cap;
  ASSERT_FALSE(cap.is_allow_all());

  ASSERT_FALSE(cap.parse("profile unknown"));
  ASSERT_FALSE(cap.parse("profile rbd invalid-key=value"));

  ASSERT_TRUE(cap.parse("profile rbd", nullptr));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "abc", "", {}, true, false, false, {}));
  ASSERT_TRUE(cap.is_capable(
      nullptr, {}, "", "rbd_support", "", {}, true, true, false, {}));
  ASSERT_TRUE(cap.is_capable(
      nullptr, {}, "", "rbd_support", "", {}, true, false, false, {}));

  ASSERT_TRUE(cap.parse("profile rbd pool=abc namespace prefix def", nullptr));
  ASSERT_FALSE(cap.is_capable(
      nullptr, {}, "", "rbd_support", "", {}, true, true, false, {}));
  ASSERT_FALSE(cap.is_capable(
      nullptr, {}, "", "rbd_support", "", {{"pool", "abc"}}, true, true, false,
      {}));
  ASSERT_TRUE(cap.is_capable(
      nullptr, {}, "", "rbd_support", "",
      {{"pool", "abc"}, {"namespace", "defghi"}}, true, true, false, {}));

  ASSERT_TRUE(cap.parse("profile rbd-read-only", nullptr));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "abc", "", {}, true, false, false, {}));
  ASSERT_FALSE(cap.is_capable(
      nullptr, {}, "", "rbd_support", "", {}, true, true, false, {}));
  ASSERT_TRUE(cap.is_capable(
      nullptr, {}, "", "rbd_support", "", {}, true, false, false, {}));
}

/* Begin Negative Tests */

TEST(MgrCap, IsCapableEmptyCommand)
{
  MgrCap cap;
  ASSERT_TRUE(cap.parse("allow command abc", nullptr));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "", "", {}, true, false, false, {}));
}

TEST(MgrCap, IsCapableEmptyModule)
{
  MgrCap cap;
  ASSERT_TRUE(cap.parse("allow module xyz r", nullptr));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "", "", {}, true, false, false, {}));
}

TEST(MgrCap, IsCapableEmptyService)
{
  MgrCap cap;
  ASSERT_TRUE(cap.parse("allow service foo r", nullptr));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "", "", {}, true, false, false, {}));
}

TEST(MgrCap, CommandEmptyArguments)
{
  MgrCap cap;
  ASSERT_TRUE(cap.parse("allow command abc with arg=foo", nullptr));
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "", "abc", {}, true, false, false, {}));
}

TEST(MgrCap, CommandMismatchedArguments)
{
  MgrCap cap;
  ASSERT_TRUE(cap.parse("allow command abc with arg=foo arg2=bar", nullptr));
  ASSERT_FALSE(cap.is_capable(
      nullptr, {}, "", "", "abc", {{"arg", "foo"}}, true, false, false, {}));
  ASSERT_FALSE(cap.is_capable(
      nullptr, {}, "", "", "abc", {{"arg", "wrong"}, {"arg2", "bar"}}, true,
      false, false, {}));
}

TEST(MgrCap, RegexWithInvalidPattern)
{
  MgrCap cap;
  // Invalid regex should be accepted during parse but fail during matching
  ASSERT_TRUE(cap.parse("allow command abc with arg regex \"[*\"", nullptr));
  ASSERT_FALSE(cap.is_capable(
      nullptr, {}, "", "", "abc", {{"arg", "test"}}, true, false, false, {}));
}

TEST(MgrCap, PermissionsWithNoCapability)
{
  MgrCap cap;
  ASSERT_TRUE(cap.parse("allow r", nullptr));
  // Asking for write when only read is granted
  ASSERT_FALSE(
      cap.is_capable(nullptr, {}, "", "", "", {}, false, true, false, {}));
}

/* End Negative Tests */
