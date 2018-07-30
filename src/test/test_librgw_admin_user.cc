// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <string.h>
#include "gtest/gtest.h"
#include "common/ceph_argparse.h"

#include "include/rgw/librgw_admin_user.h"

#define dout_subsys ceph_subsys_rgw

namespace {
  librgw_admin_user_t rgw = nullptr;
  std::string uid("testuser");
  std::string display_name("testuser");
  std::string access_key("");
  std::string secret_key("");
  std::string email;
  std::string caps;
  std::string access_str;
  bool admin;
  bool system_user;

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(RGWLibAdmin, INIT) {
  int ret = librgw_admin_user_create(&rgw, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw, nullptr);
}

TEST(RGWLibAdmin, CREATE) {
  int ret = rgw_admin_create_user(rgw, uid.c_str(), display_name.c_str(),
				  access_key.c_str(), secret_key.c_str(), caps.c_str(), access_str.c_str(), email.c_str(),
				  admin, system_user);
  ASSERT_EQ(ret, 0);
}

TEST(RGWLibAdmin, INFO) {
  struct rgw_user_info user_info;
  int ret = rgw_admin_user_info(rgw, uid.c_str(), &user_info);
  ASSERT_EQ(ret, 0);
}

TEST(RGWLibAdmin, SHUTDOWN) {
  librgw_admin_user_shutdown(rgw);
}

int main(int argc, char *argv[])
{
  char *v{nullptr};
  std::string val;
  std::vector<const char*> args;

  argv_to_vec(argc, const_cast<const char**>(argv), args);
  env_to_vec(args);

  v = getenv("AWS_ACCESS_KEY_ID");
  if (v) {
    access_key = v;
  }

  v = getenv("AWS_SECRET_ACCESS_KEY");
  if (v) {
    secret_key = v;
  }

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_witharg(args, arg_iter, &val, "--access",
			      (char*) nullptr)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--secret",
				     (char*) nullptr)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      uid = val;
    } else {
      ++arg_iter;
    }
  }

  saved_args.argc = argc;
  saved_args.argv = argv;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

