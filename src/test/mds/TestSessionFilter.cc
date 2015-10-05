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
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "global/global_init.h"

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
  Session *a = new Session();;
  Session *b = new Session();;
  a->info.inst.name.parse("client.123");
  b->info.inst.name.parse("client.456");

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return false;}));
  ASSERT_FALSE(filter.match(*b, [](client_t c) -> bool {return false;}));
  a->put();
  b->put();
}

TEST(MDSSessionFilter, StateEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  filter.parse({"state=closing"}, &ss);
  Session *a = new Session();
  a->set_state(Session::STATE_CLOSING);
  Session *b = new Session();
  b->set_state(Session::STATE_OPENING);

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return false;}));
  ASSERT_FALSE(filter.match(*b, [](client_t c) -> bool {return false;}));
  a->put();
  b->put();
}

TEST(MDSSessionFilter, AuthEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  filter.parse({"auth_name=rhubarb"}, &ss);
  Session *a = new Session();
  a->info.auth_name.set_id("rhubarb");
  Session *b = new Session();
  b->info.auth_name.set_id("custard");

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return false;}));
  ASSERT_FALSE(filter.match(*b, [](client_t c) -> bool {return false;}));
  a->put();
  b->put();
}

TEST(MDSSessionFilter, MetadataEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  int r = filter.parse({"client_metadata.root=/rhubarb"}, &ss);
  ASSERT_EQ(r, 0);
  Session *a = new Session();
  a->set_client_metadata({{"root", "/rhubarb"}});
  Session *b = new Session();
  b->set_client_metadata({{"root", "/custard"}});

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return false;}));
  ASSERT_FALSE(filter.match(*b, [](client_t c) -> bool {return false;}));
  a->put();
  b->put();
}

TEST(MDSSessionFilter, ReconnectingEquality)
{
  SessionFilter filter;
  std::stringstream ss;
  int r = filter.parse({"reconnecting=true"}, &ss);
  ASSERT_EQ(r, 0);
  Session *a = new Session();

  ASSERT_TRUE(filter.match(*a, [](client_t c) -> bool {return true;}));
  ASSERT_FALSE(filter.match(*a, [](client_t c) -> bool {return false;}));
  a->put();
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0); 
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
} 
