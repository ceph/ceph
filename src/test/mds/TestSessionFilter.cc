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
#include "mds/SessionMap.h"

#include "gtest/gtest.h"

typedef std::vector<std::string> args_eg;
typedef std::vector<args_eg> args_eg_set;

TEST(MDSSessionFilter, ParseGood)
{
  args_eg_set examples = {
    {"id=34"},
    {"auth_name=foxtrot"},
    {"state=reconnecting"},
    {"reconnecting=true"},
    {"client_metadata.root=/foo/bar"},
    {},
    {"id=123"},
    {"id=34", "client_metadata.root=/foo/bar", "auth_name=foxtrot",
      "state=reconnecting", "reconnecting=true"}
  };

  for (auto ex : examples) {
    SessionFilter f;
    std::stringstream ss;

    std::cout << "Testing '" << ex << "'" << std::endl;
    int r = f.parse(ex, &ss);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(ss.str().empty());
  }
}

TEST(MDSSessionFilter, ParseBad)
{
  args_eg_set examples = {
    {"rhubarb"},
    {"id="},
    {"id=custard"},
    {"=custard"},
    {"reconnecting=MAYBE"},
    {"reconnecting=2"}
  };

  for (auto ex : examples) {
    SessionFilter f;
    std::stringstream ss;

    std::cout << "Testing '" << ex << "'" << std::endl;
    int r = f.parse(ex, &ss);
    ASSERT_EQ(r, -EINVAL);
    ASSERT_FALSE(ss.str().empty());
  }
}

TEST(MDSSessionFilter, IdEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  filter.parse({"id=123"}, &ss);
  auto a = ceph::make_ref<Session>(nullptr);;
  auto b = ceph::make_ref<Session>(nullptr);;
  a->info.inst.name.parse("client.123");
  b->info.inst.name.parse("client.456");

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return false;}));
  ASSERT_FALSE(filter.match(*b, [](client_t c) -> bool {return false;}));
}

TEST(MDSSessionFilter, StateEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  filter.parse({"state=closing"}, &ss);
  auto a = ceph::make_ref<Session>(nullptr);
  a->set_state(Session::STATE_CLOSING);
  auto b = ceph::make_ref<Session>(nullptr);
  b->set_state(Session::STATE_OPENING);

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return false;}));
  ASSERT_FALSE(filter.match(*b, [](client_t c) -> bool {return false;}));
}

TEST(MDSSessionFilter, AuthEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  filter.parse({"auth_name=rhubarb"}, &ss);
  auto a = ceph::make_ref<Session>(nullptr);
  a->info.auth_name.set_id("rhubarb");
  auto b = ceph::make_ref<Session>(nullptr);
  b->info.auth_name.set_id("custard");

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return false;}));
  ASSERT_FALSE(filter.match(*b, [](client_t c) -> bool {return false;}));
}

TEST(MDSSessionFilter, MetadataEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  int r = filter.parse({"client_metadata.root=/rhubarb"}, &ss);
  ASSERT_EQ(r, 0);
  client_metadata_t meta;
  auto a = ceph::make_ref<Session>(nullptr);
  meta.kv_map = {{"root", "/rhubarb"}};
  a->set_client_metadata(meta);
  auto b = ceph::make_ref<Session>(nullptr);
  meta.kv_map = {{"root", "/custard"}};
  b->set_client_metadata(meta);

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return false;}));
  ASSERT_FALSE(filter.match(*b, [](client_t c) -> bool {return false;}));
}

TEST(MDSSessionFilter, ReconnectingEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  int r = filter.parse({"reconnecting=true"}, &ss);
  ASSERT_EQ(r, 0);
  auto a = ceph::make_ref<Session>(nullptr);

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return true;}));
  ASSERT_FALSE(filter.match(*a, [](client_t c) -> bool {return false;}));
}
