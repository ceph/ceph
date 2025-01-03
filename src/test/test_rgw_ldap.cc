// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdint.h>
#include <tuple>
#include <iostream>
#include <vector>
#include <map>
#include <random>

#include "rgw_ldap.h"
#include "rgw_token.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {

  struct {
    int argc;
    char **argv;
  } saved_args;

  bool do_hexdump = false;

  string access_key("ewogICAgIlJHV19UT0tFTiI6IHsKICAgICAgICAidmVyc2lvbiI6IDEsCiAgICAgICAgInR5cGUiOiAibGRhcCIsCiAgICAgICAgImlkIjogImFkbWluIiwKICAgICAgICAia2V5IjogImxpbnV4Ym94IgogICAgfQp9Cg=="); // {admin,linuxbox}
  string other_key("ewogICAgIlJHV19UT0tFTiI6IHsKICAgICAgICAidmVyc2lvbiI6IDEsCiAgICAgICAgInR5cGUiOiAibGRhcCIsCiAgICAgICAgImlkIjogImFkbWluIiwKICAgICAgICAia2V5IjogImJhZHBhc3MiCiAgICB9Cn0K"); // {admin,badpass}

  string ldap_uri = "ldaps://f23-kdc.rgw.com";
  string ldap_binddn = "uid=admin,cn=users,cn=accounts,dc=rgw,dc=com";
  string ldap_bindpw = "supersecret";
  string ldap_searchdn = "cn=users,cn=accounts,dc=rgw,dc=com";
  string ldap_searchfilter = "";
  string ldap_dnattr = "uid";

  rgw::LDAPHelper ldh(ldap_uri, ldap_binddn, ldap_bindpw, ldap_searchdn,
		      ldap_searchfilter, ldap_dnattr);

} /* namespace */

TEST(RGW_LDAP, INIT) {
  int ret = ldh.init();
  ASSERT_EQ(ret, 0);
}

TEST(RGW_LDAP, BIND) {
  int ret = ldh.bind();
  ASSERT_EQ(ret, 0);
}

TEST(RGW_LDAP, AUTH) {
  using std::get;
  using namespace rgw;
  int ret = 0;
  {
    RGWToken token{from_base64(access_key)};
    ret = ldh.auth(token.id, token.key);
    ASSERT_EQ(ret, 0);
  }
  {
    RGWToken token{from_base64(other_key)};
    ret = ldh.auth(token.id, token.key);
    ASSERT_NE(ret, 0);
  }
}

TEST(RGW_LDAP, SHUTDOWN) {
  // nothing
}

int main(int argc, char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  string val;
  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_witharg(args, arg_iter, &val, "--access",
			      (char*) nullptr)) {
      access_key = val;
    } else if (ceph_argparse_flag(args, arg_iter, "--hexdump",
					    (char*) nullptr)) {
      do_hexdump = true;
    } else {
      ++arg_iter;
    }
  }

  /* don't accidentally run as anonymous */
  if (access_key == "") {
    std::cout << argv[0] << " no AWS credentials, exiting" << std::endl;
    return EPERM;
  }

  saved_args.argc = argc;
  saved_args.argv = argv;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
