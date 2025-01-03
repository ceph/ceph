// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "include/ceph_assert.h"
#include "gtest/gtest.h"
#include "rgw_token.h"
#include "rgw_b64.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw;
  using std::get;
  using std::string;

  string access_key{"Smonny"};
  string secret_key{"Turjan of Miir"};

  std::vector<RGWToken> tokens;

  std::string enc_ad{"ewogICAgIlJHV19UT0tFTiI6IHsKICAgICAgICAidmVyc2lvbiI6IDEsCiAgICAgICAgInR5cGUiOiAiYWQiLAogICAgICAgICJpZCI6ICJTbW9ubnkiLAogICAgICAgICJrZXkiOiAiVHVyamFuIG9mIE1paXIiCiAgICB9Cn0K"};

  std::string enc_ldap{"ewogICAgIlJHV19UT0tFTiI6IHsKICAgICAgICAidmVyc2lvbiI6IDEsCiAgICAgICAgInR5cGUiOiAibGRhcCIsCiAgICAgICAgImlkIjogIlNtb25ueSIsCiAgICAgICAgImtleSI6ICJUdXJqYW4gb2YgTWlpciIKICAgIH0KfQo="};

  std::string non_base64{"stuff here"};
  std::string non_base64_sploded{"90KLscc0Dz4U49HX-7Tx"};

  Formatter* token_formatter{nullptr};
  bool verbose {false};
}

using namespace std;

TEST(TOKEN, INIT) {
  token_formatter = new JSONFormatter(true /* pretty */);
  ASSERT_NE(token_formatter, nullptr);
}

TEST(TOKEN, ENCODE) {
  // encode the two supported types
  RGWToken token_ad(RGWToken::TOKEN_AD, access_key, secret_key);
  ASSERT_EQ(token_ad.encode_json_base64(token_formatter), enc_ad);
  tokens.push_back(token_ad); // provies copiable

  RGWToken token_ldap(RGWToken::TOKEN_LDAP, access_key, secret_key);
  ASSERT_EQ(token_ldap.encode_json_base64(token_formatter), enc_ldap);
  tokens.push_back(token_ldap);
}

TEST(TOKEN, DECODE) {
  for (const auto& enc_tok : {enc_ad, enc_ldap}) {
    RGWToken token{from_base64(enc_tok)}; // decode ctor
    ASSERT_EQ(token.id, access_key);
    ASSERT_EQ(token.key, secret_key);
  }
}

TEST(TOKEN, EMPTY) {
    std::string empty{""};
    RGWToken token{from_base64(empty)}; // decode ctor
    ASSERT_FALSE(token.valid());
}

TEST(TOKEN, BADINPUT) {
    RGWToken token{from_base64(non_base64)}; // decode ctor
    ASSERT_FALSE(token.valid());
}

TEST(TOKEN, BADINPUT2) {
    RGWToken token{from_base64(non_base64_sploded)}; // decode ctor
    ASSERT_FALSE(token.valid());
}

TEST(TOKEN, BADINPUT3) {
  try {
    std::string stuff = from_base64(non_base64_sploded); // decode
  } catch(...) {
    // do nothing
  }
  ASSERT_EQ(1, 1);
}

TEST(TOKEN, SHUTDOWN) {
  delete token_formatter;
}

int main(int argc, char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  string val;
  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_flag(args, arg_iter, "--verbose",
			      (char*) nullptr)) {
      verbose = true;
    } else {
      ++arg_iter;
    }
  }

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
