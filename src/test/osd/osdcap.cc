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

using namespace std;

const char *parse_good[] = {
  "allow *",
  "allow r",
  "allow rwx",
  "allow r pool foo ",
  "allow r pool=foo",
  "allow wx pool taco",
  "allow pool foo r",
  "allow pool taco wx",
  "allow wx pool taco object_prefix obj",
  "allow wx pool taco object_prefix obj_with_underscores_and_no_quotes",
  "allow pool taco object_prefix obj wx",
  "allow pool taco object_prefix obj_with_underscores_and_no_quotes wx",
  "allow rwx pool 'weird name'",
  "allow rwx pool \"weird name with ''s\"",
  "allow rwx pool foo, allow r pool bar",
  "allow rwx pool foo ; allow r pool bar",
  "allow rwx pool foo ;allow r pool bar",
  "allow rwx pool foo; allow r pool bar",
  "allow pool foo rwx, allow pool bar r",
  "allow pool foo.froo.foo rwx, allow pool bar r",
  "allow pool foo rwx ; allow pool bar r",
  "allow pool foo rwx ;allow pool bar r",
  "allow pool foo rwx; allow pool bar r",
  "allow pool data rw, allow pool rbd rwx, allow pool images class rbd foo",
  "allow class-read",
  "allow class-write",
  "allow class-read class-write",
  "allow r class-read pool foo",
  "allow rw class-read class-write pool foo",
  "allow r class-read pool foo",
  "allow pool bar rwx; allow pool baz r class-read",
  "allow class foo",
  "allow class clsname \"clsthingidon'tunderstand\"",
  "  allow rwx pool foo; allow r pool bar  ",
  "  allow   rwx   pool foo; allow r pool bar  ",
  "  allow pool foo rwx; allow pool bar r  ",
  "  allow     pool foo rwx; allow pool bar r  ",
  " allow wx pool taco",
  "\tallow\nwx\tpool \n taco\t",
  "allow class-read object_prefix rbd_children, allow pool libvirt-pool-test rwx",
  "allow class-read object_prefix rbd-children, allow pool libvirt_pool_test rwx",
  "allow pool foo namespace nfoo rwx, allow pool bar namespace=nbar r",
  "allow pool foo namespace=nfoo rwx ; allow pool bar namespace=nbar r",
  "allow pool foo namespace nfoo rwx ;allow pool bar namespace nbar r",
  "allow pool foo namespace=nfoo rwx; allow pool bar namespace nbar object_prefix rbd r",
  "allow rwx namespace=nfoo tag cephfs data=cephfs_a",
  "allow rwx namespace foo tag cephfs data =cephfs_a",
  "allow pool foo namespace=nfoo* rwx",
  "allow pool foo namespace=\"\" rwx; allow pool bar namespace='' object_prefix rbd r",
  "allow pool foo namespace \"\" rwx; allow pool bar namespace '' object_prefix rbd r",
  "profile abc, profile abc pool=bar, profile abc pool=bar namespace=foo",
  "allow rwx tag application key=value",
  "allow rwx tag application key = value",
  "allow rwx tag application key =value",
  "allow rwx tag application key= value",
  "allow rwx tag application key  =   value",
  "allow all tag application all=all",
  "allow rwx network 127.0.0.1/8",
  "allow rwx network ::1/128",
  "allow rwx network [ff::1]/128",
  "profile foo network 127.0.0.1/8",
  "allow rwx namespace foo tag cephfs data =cephfs_a network 127.0.0.1/8",
  "allow pool foo rwx network 1.2.3.4/24",
  0
};

TEST(OSDCap, ParseGood) {
  for (int i=0; parse_good[i]; i++) {
    string str = parse_good[i];
    OSDCap cap;
    std::cout << "Testing good input: '" << str << "'" << std::endl;
    ASSERT_TRUE(cap.parse(str, &cout));
  }
}

const char *parse_bad[] = {
  "allow r poolfoo",
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
  "allow xrwx pool foo,, allow r pool bar",
  ";allow rwx pool foo rwx ; allow r pool bar",
  "allow rwx pool foo ;allow r pool bar gibberish",
  "allow rwx auid 123 pool asdf namespace=foo",
  "allow rwx auid 123 namespace",
  "allow rwx namespace",
  "allow namespace",
  "allow namespace=foo",
  "allow namespace=f*oo",
  "allow rwx auid 123 namespace asdf",
  "allow wwx pool ''",
  "allow rwx tag application key value",
  "allow rwx auid 123",
  "allow auid 123 rwx",
  "allow r   pool    foo    object_prefix   blah   ;   allow   w   auid  5",
  0
};

TEST(OSDCap, ParseBad) {
  for (int i=0; parse_bad[i]; i++) {
    string str = parse_bad[i];
    OSDCap cap;
    std::cout << "Testing bad input: '" << str << "'" << std::endl;
    ASSERT_FALSE(cap.parse(str, &cout));
  }
}

TEST(OSDCap, AllowAll) {
  OSDCap cap;
  entity_addr_t addr;
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
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "asdf", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "anamespace", {}, "asdf", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "asdf", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "anamespace", {}, "asdf", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "asdf", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "anamespace", {{"application", {{"key", "value"}}}}, "asdf", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "asdf", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "anamespace", {{"application", {{"key", "value"}}}}, "asdf", true, true, {{"cls", "", true, true, true}}, addr));
  // 'allow *' overrides allow list
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "asdf", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "anamespace", {}, "asdf", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "asdf", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "anamespace", {}, "asdf", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "asdf", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "anamespace", {{"application", {{"key", "value"}}}}, "asdf", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "asdf", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "anamespace", {{"application", {{"key", "value"}}}}, "asdf", true, true, {{"cls", "", true, true, false}}, addr));
}

TEST(OSDCap, AllowPool) {
  OSDCap cap;
  entity_addr_t addr;
  bool r = cap.parse("allow rwx pool foo", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("foo", "", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, true}}, addr));
  // true->false for classes not on allow list
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, false}}, addr));


  ASSERT_FALSE(cap.is_capable("bar", "", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, true}}, addr));
}

TEST(OSDCap, AllowPools) {
  entity_addr_t addr;
  OSDCap cap;
  bool r = cap.parse("allow rwx pool foo, allow r pool bar", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("foo", "", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, true}}, addr));
  // true-false for classes not on allow list
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "ns", {}, "", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "ns", {{"application", {{"key", "value"}}}}, "", true, false, {}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {{"application", {{"key", "value"}}}}, "", true, true, {{"cls", "", true, true, true}}, addr));

  ASSERT_FALSE(cap.is_capable("baz", "", {}, "", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "ns", {}, "", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {{"application", {{"key", "value"}}}}, "", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "ns", {{"application", {{"key", "value"}}}}, "", true, false, {}, addr));
}

TEST(OSDCap, AllowPools2) {
  entity_addr_t addr;
  OSDCap cap;
  bool r = cap.parse("allow r, allow rwx pool foo", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("foo", "", {}, "", true, true, {{"cls", "", true, true, true}}, addr));
  // true-false for classes not on allow list
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "", true, true, {{"cls", "", true, true, true}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "", true, false, {}, addr));
}

TEST(OSDCap, ObjectPrefix) {
  entity_addr_t addr;
  OSDCap cap;
  bool r = cap.parse("allow rwx object_prefix foo", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "food", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo_bar", true, true, {{"cls", "", true, true, true}}, addr));
  // true-false for classes not on allow list
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "food", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo_bar", true, true, {{"cls", "", true, true, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "_foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, " foo ", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "fo", true, true, {{"cls", "", true, true, true}}, addr));
}

TEST(OSDCap, ObjectPoolAndPrefix) {
  entity_addr_t addr;
  OSDCap cap;
  bool r = cap.parse("allow rwx pool bar object_prefix foo", NULL);
  ASSERT_TRUE(r);

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "food", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo_bar", true, true, {{"cls", "", true, true, true}}, addr));
  // true-false for classes not on allow list
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "food", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo_bar", true, true, {{"cls", "", true, true, false}}, addr));

  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "food", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "fo", true, true, {{"cls", "", true, true, true}}, addr));
}

TEST(OSDCap, Namespace) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rw namespace=nfoo"));

  ASSERT_TRUE(cap.is_capable("bar", "nfoo", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "nfoobar", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, NamespaceGlob) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rw namespace=nfoo*"));

  ASSERT_TRUE(cap.is_capable("bar", "nfoo", {}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "nfoobar", {}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "nfo", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, BasicR) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow r", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, BasicW) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow w", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, BasicX) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow x", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  // true->false when class not on allow list
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));


  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, BasicRW) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rw", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
}

TEST(OSDCap, BasicRX) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rx", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, true, true}}, addr));
  // true->false for class not on allow list
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, true, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, BasicWX) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow wx", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  // true->false for class not on allow list
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, BasicRWX) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  // true->false for class not on allow list
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, false, false}}, addr));
}

TEST(OSDCap, BasicRWClassRClassW) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rw class-read class-write", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, false, false}}, addr));
}

TEST(OSDCap, ClassR) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow class-read", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
}

TEST(OSDCap, ClassW) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow class-write", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
}

TEST(OSDCap, ClassRW) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow class-read class-write", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
}

TEST(OSDCap, BasicRClassR) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow r class-read", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, false, {}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, false, {{"cls", "", true, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "any", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "any", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "any", {}, "foo", true, false, {}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "any", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "any", {}, "foo", true, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "any", {{"application", {{"key", "value"}}}}, "foo", true, false, {{"cls", "", true, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "any", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "any", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "any", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "any", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "any", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));
}

TEST(OSDCap, PoolClassR) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow pool bar r class-read, allow pool foo rwx", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, false, {}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, false, {{"cls", "", true, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "ns", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "ns", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "ns", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "ns", {{"application", {{"key", "value"}}}}, "foo", true, false, {}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", true, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {{"application", {{"key", "value"}}}}, "foo", true, false, {{"cls", "", true, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", false, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", false, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", false, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", false, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
}

TEST(OSDCap, PoolClassRNS) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow pool bar namespace='' r class-read, allow pool foo namespace=ns rwx", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, false, {}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, false, {{"cls", "", true, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "ns", {{"application", {{"key", "value"}}}}, "foo", true, false, {}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "other", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "other", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "other", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "other", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "other", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, true}}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", false, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", false, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", false, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("baz", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
}

TEST(OSDCap, NSClassR) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow namespace '' rw class-read class-write, allow namespace test r", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", true, true, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", false, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", false, false, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, false, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, false, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {{"cls", "", true, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "test", {}, "foo", true, false, {}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "test", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "test", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "test", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "test", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "test", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "test", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "test", {}, "foo", true, false, {}, addr));

  ASSERT_FALSE(cap.is_capable("foo", "test", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "test", {}, "foo", true, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "test", {}, "foo", true, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "test", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "test", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "test", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("foo", "bad", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "bad", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "bad", {}, "foo", false, false, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "bad", {}, "foo", false, false, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "bad", {{"application", {{"key", "value"}}}}, "foo", false, false, {{"cls", "", false, true, true}}, addr));
}

TEST(OSDCap, PoolTagBasic) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx tag application key=value", NULL));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key", "value"}}}, {"app2", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key2", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"app2", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key2", "value"}}}, {"app2", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, false, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {{"cls", "", false, false, true}}, addr));
  // true->false when class not allow listed
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {{"cls", "", false, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {{"cls", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {{"cls", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", false, true, {{"cls", "", false, true, false}}, addr));

  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", false, true, {{"cls", "", false, false, true}}, addr));
}

TEST(OSDCap, PoolTagWildK)
{
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx tag application *=value", NULL));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key", "value"}}}, {"app2", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key2", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"app2", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key2", "value"}}}, {"app2", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));
}

TEST(OSDCap, PoolTagWildV)
{
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx tag application key=*", NULL));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key", "value"}}}, {"app2", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key2", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"app2", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key2", "value"}}}, {"app2", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));
}

TEST(OSDCap, PoolTagWildKV)
{
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx tag application *=*", NULL));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key", "value"}}}, {"app2", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {}}}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"key2", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"app2", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {{"application", {{"foo", "bar"}, {"key2", "value"}}}, {"app2", {{"foo", "bar"}}}}, "foo", true, true, {}, addr));
}

TEST(OSDCap, NSPool)
{
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx namespace ns tag application key=value", NULL));

  ASSERT_TRUE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns2", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {{"application", {{"key", "value2"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {{"application", {{"key2", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, NSPoolGlob)
{
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx namespace ns* tag application key=value", NULL));

  ASSERT_TRUE(cap.is_capable("foo", "ns", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));

  ASSERT_TRUE(cap.is_capable("foo", "ns2", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {{"application", {{"key", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {{"application", {{"key", "value2"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "ns", {{"application", {{"key2", "value"}}}}, "foo", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "foo", true, true, {}, addr));
}

TEST(OSDCap, OutputParsed)
{
  entity_addr_t addr;
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
    {"allow rw class-read class-write",
     "osdcap[grant(rwx)]"},
    {"allow rw class-read",
     "osdcap[grant(rw class-read)]"},
    {"allow rw class-write",
     "osdcap[grant(rw class-write)]"},
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
    {"allow r pool images namespace ''",
     "osdcap[grant(pool images namespace \"\" r)]"},
    {"allow r pool images namespace foo",
     "osdcap[grant(pool images namespace foo r)]"},
    {"allow r pool images namespace \"\"",
     "osdcap[grant(pool images namespace \"\" r)]"},
    {"allow r namespace foo",
     "osdcap[grant(namespace foo r)]"},
    {"allow pool images r; allow pool rbd rwx",
     "osdcap[grant(pool images r),grant(pool rbd rwx)]"},
    {"allow pool images r, allow pool rbd rwx",
     "osdcap[grant(pool images r),grant(pool rbd rwx)]"},
    {"allow class-read object_prefix rbd_children, allow pool libvirt-pool-test rwx",
     "osdcap[grant(object_prefix rbd_children  class-read),grant(pool libvirt-pool-test rwx)]"},
    {"allow rwx tag application key=value",
     "osdcap[grant(app application key key val value rwx)]"},
    {"allow rwx namespace ns* tag application key=value",
     "osdcap[grant(namespace ns* app application key key val value rwx)]"},
    {"allow all",
     "osdcap[grant(*)]"},
    {"allow rwx tag application all=all",
     "osdcap[grant(app application key * val * rwx)]"},
    {"allow rwx network 1.2.3.4/24",
     "osdcap[grant(rwx network 1.2.3.4/24)]"},
  };

  size_t num_tests = sizeof(test_values) / sizeof(*test_values);
  for (size_t i = 0; i < num_tests; ++i) {
    OSDCap cap;
    std::cout << "Testing input '" << test_values[i].input << "'" << std::endl;
    ASSERT_TRUE(cap.parse(test_values[i].input));
    ASSERT_EQ(test_values[i].output, stringify(cap));
  }
}

TEST(OSDCap, AllowClass) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow class foo", NULL));

  // can call any method on class foo regardless of allow list status
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}}, addr));

  // does not permit invoking class bar
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, true, false}}, addr));
}

TEST(OSDCap, AllowClassMethod) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow class foo xyz", NULL));

  // can call the xyz method on class foo regardless of allow list status
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "xyz", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "xyz", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "xyz", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "xyz", true, false, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "xyz", false, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "xyz", true, true, false}}, addr));

  // does not permit invoking class bar
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "xyz", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "xyz", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "xyz", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "xyz", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "xyz", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "xyz", true, true, false}}, addr));
}

TEST(OSDCap, AllowClass2) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow class foo, allow class bar", NULL));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, true, false}}, addr));
}

TEST(OSDCap, AllowClassRWX) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx, allow class foo", NULL));

  // can call any method on class foo regardless of allow list status
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}}, addr));

  // does not permit invoking class bar
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, true, false}}, addr));

  // allows class bar if it is allow listed
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"bar", "", true, true, true}}, addr));
}

TEST(OSDCap, AllowClassMulti) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow class foo", NULL));

  // can call any method on foo, but not bar, so the entire op is rejected
  // bar with allow list is rejected because it still needs rwx/class-read,write
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, false, false}}, addr));

  // these are OK because 'bar' is on the allow list BUT the calls don't read or write
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, false, true}}, addr));

  // can call any method on foo or bar regardless of allow list status
  OSDCap cap2;
  ASSERT_TRUE(cap2.parse("allow class foo, allow class bar", NULL));

  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, false, true}}, addr));
  ASSERT_TRUE(cap2.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, false, false}}, addr));
}

TEST(OSDCap, AllowClassMultiRWX) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow rwx, allow class foo", NULL));

  // can call anything on foo, but only allow listed methods on bar
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, false, true}}, addr));

  // fails because bar not allow listed
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, false, true}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, true, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, false, true}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, false, true}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", true, false, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, false, true}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, false, true}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, true, false}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, false, true}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, true}, {"bar", "", false, false, false}}, addr));

  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, false, true}}, addr));

  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", true, false, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, true, false}}, addr));
  ASSERT_FALSE(cap.is_capable("bar", "", {}, "foo", false, false, {{"foo", "", false, false, false}, {"bar", "", false, false, false}}, addr));
}

TEST(OSDCap, AllowProfile) {
  entity_addr_t addr;
  OSDCap cap;
  ASSERT_TRUE(cap.parse("profile read-only, profile read-write pool abc", NULL));
  ASSERT_FALSE(cap.allow_all());
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "asdf", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "asdf", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("abc", "", {}, "asdf", false, true, {}, addr));

  // RBD
  cap.grants.clear();
  ASSERT_TRUE(cap.parse("profile rbd pool abc", NULL));
  ASSERT_FALSE(cap.allow_all());
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "asdf", true, true, {}, addr));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "rbd_children", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "rbd_children", false, false,
                             {{"rbd", "", true, false, true}}, addr));
  ASSERT_TRUE(cap.is_capable("abc", "", {}, "asdf", true, true,
                             {{"rbd", "", true, true, true}}, addr));

  cap.grants.clear();
  ASSERT_TRUE(cap.parse("profile rbd-read-only pool abc", NULL));
  ASSERT_FALSE(cap.allow_all());
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "rbd_children", true, false, {}, addr));
  ASSERT_TRUE(cap.is_capable("abc", "", {}, "asdf", true, false,
                             {{"rbd", "", true, false, true}}, addr));
  ASSERT_FALSE(cap.is_capable("abc", "", {}, "asdf", true, true, {}, addr));
  ASSERT_TRUE(cap.is_capable("abc", "", {}, "rbd_header.ABC", false, false,
                             {{"rbd", "child_attach", true, true, true}}, addr));
  ASSERT_TRUE(cap.is_capable("abc", "", {}, "rbd_header.ABC", false, false,
                             {{"rbd", "child_detach", true, true, true}}, addr));
  ASSERT_FALSE(cap.is_capable("abc", "", {}, "rbd_header.ABC", false, false,
                              {{"rbd", "other function", true, true, true}}, addr));

  cap.grants.clear();
  ASSERT_TRUE(cap.parse("profile rbd pool pool1 namespace ns1", nullptr));
  ASSERT_TRUE(cap.is_capable("pool1", "", {}, "rbd_info", false, false,
                             {{"rbd", "metadata_list", true, false, true}},
                             addr));
  ASSERT_TRUE(cap.is_capable("pool1", "ns1", {}, "rbd_info", false, false,
                             {{"rbd", "metadata_list", true, false, true}},
                             addr));
  ASSERT_FALSE(cap.is_capable("pool1", "ns2", {}, "rbd_info", false, false,
                              {{"rbd", "metadata_list", true, false, true}},
                              addr));
  ASSERT_FALSE(cap.is_capable("pool2", "", {}, "rbd_info", false, false,
                              {{"rbd", "metadata_list", true, false, true}},
                              addr));
  ASSERT_FALSE(cap.is_capable("pool1", "", {}, "asdf", false, false,
                              {{"rbd", "metadata_list", true, false, true}},
                              addr));
  ASSERT_FALSE(cap.is_capable("pool1", "", {}, "rbd_info", false, false,
                              {{"rbd", "other_method", true, false, true}},
                              addr));

  cap.grants.clear();
  ASSERT_TRUE(cap.parse("profile rbd-read-only pool pool1 namespace ns1",
                        nullptr));
  ASSERT_TRUE(cap.is_capable("pool1", "", {}, "rbd_info", false, false,
                             {{"rbd", "metadata_list", true, false, true}},
                             addr));
  ASSERT_TRUE(cap.is_capable("pool1", "ns1", {}, "rbd_info", false, false,
                             {{"rbd", "metadata_list", true, false, true}},
                             addr));
  ASSERT_FALSE(cap.is_capable("pool1", "ns2", {}, "rbd_info", false, false,
                              {{"rbd", "metadata_list", true, false, true}},
                              addr));
  ASSERT_FALSE(cap.is_capable("pool2", "", {}, "rbd_info", false, false,
                              {{"rbd", "metadata_list", true, false, true}},
                              addr));
  ASSERT_FALSE(cap.is_capable("pool1", "", {}, "asdf", false, false,
                              {{"rbd", "metadata_list", true, false, true}},
                              addr));
  ASSERT_FALSE(cap.is_capable("pool1", "", {}, "rbd_info", false, false,
                              {{"rbd", "other_method", true, false, true}},
                              addr));
}

TEST(OSDCap, network) {
  entity_addr_t a, b, c;
  a.parse("10.1.2.3");
  b.parse("192.168.2.3");
  c.parse("192.167.2.3");

  OSDCap cap;
  ASSERT_TRUE(cap.parse("allow * network 192.168.0.0/16, allow * network 10.0.0.0/8", NULL));

  ASSERT_TRUE(cap.is_capable("foo", "", {}, "asdf", true, true, {{"cls", "", true, true, true}}, a));
  ASSERT_TRUE(cap.is_capable("foo", "", {}, "asdf", true, true, {{"cls", "", true, true, true}}, b));
  ASSERT_FALSE(cap.is_capable("foo", "", {}, "asdf", true, true, {{"cls", "", true, true, true}}, c));
}
