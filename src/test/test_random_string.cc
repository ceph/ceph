// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/random_string.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include <gtest/gtest.h>

inline bool is_alphanumeric_lower(char c) {
  return std::islower(c) || std::isdigit(c);
}
inline bool is_alphanumeric_upper(char c) {
  return std::isupper(c) || std::isdigit(c);
}
inline bool is_alphanumeric_plain(char c) {
  return std::islower(c) || std::isupper(c) || std::isdigit(c);
}
inline bool is_alphanumeric_no_underscore(char c) {
  return is_alphanumeric_plain(c) || c == '-' || c == '.';
}
inline bool is_alphanumeric(char c) {
  return is_alphanumeric_plain(c) || c == '-' || c == '_';
}
inline bool is_base64(char c) {
  return is_alphanumeric_plain(c) || c == '+' || c == '/';
}

TEST(RandomString, base64)
{
  char arr[65] = {};
  ASSERT_EQ(0, gen_rand_base64(g_ceph_context, arr, sizeof(arr)));
  EXPECT_EQ(0, arr[64]); // must be null terminated
  EXPECT_TRUE(std::all_of(arr, arr + 64, is_base64));
}

TEST(RandomString, alphanumeric)
{
  char arr[65] = {};
  gen_rand_alphanumeric(g_ceph_context, arr, sizeof(arr));
  EXPECT_EQ(0, arr[64]);
  EXPECT_TRUE(std::all_of(arr, arr + 64, is_alphanumeric));
}

TEST(RandomString, alphanumeric_string)
{
  std::string str = gen_rand_alphanumeric(g_ceph_context, 64);
  EXPECT_EQ(64, str.size());
  EXPECT_TRUE(std::all_of(str.begin(), str.end(), is_alphanumeric));
}

TEST(RandomString, alphanumeric_lower)
{
  char arr[65] = {};
  gen_rand_alphanumeric_lower(g_ceph_context, arr, sizeof(arr));
  EXPECT_EQ(0, arr[64]);
  EXPECT_TRUE(std::all_of(arr, arr + 64, is_alphanumeric_lower));
}

TEST(RandomString, alphanumeric_lower_string)
{
  std::string str = gen_rand_alphanumeric_lower(g_ceph_context, 64);
  EXPECT_EQ(64, str.size());
  EXPECT_TRUE(std::all_of(str.begin(), str.end(), is_alphanumeric_lower));
}

TEST(RandomString, alphanumeric_upper)
{
  char arr[65] = {};
  gen_rand_alphanumeric_upper(g_ceph_context, arr, sizeof(arr));
  EXPECT_EQ(0, arr[64]);
  EXPECT_TRUE(std::all_of(arr, arr + 64, is_alphanumeric_upper));
}

TEST(RandomString, alphanumeric_upper_string)
{
  std::string str = gen_rand_alphanumeric_upper(g_ceph_context, 64);
  EXPECT_EQ(64, str.size());
  EXPECT_TRUE(std::all_of(str.begin(), str.end(), is_alphanumeric_upper));
}

TEST(RandomString, alphanumeric_no_underscore)
{
  char arr[65] = {};
  gen_rand_alphanumeric_no_underscore(g_ceph_context, arr, sizeof(arr));
  EXPECT_EQ(0, arr[64]);
  EXPECT_TRUE(std::all_of(arr, arr + 64, is_alphanumeric_no_underscore));
}

TEST(RandomString, alphanumeric_no_underscore_string)
{
  std::string str = gen_rand_alphanumeric_no_underscore(g_ceph_context, 64);
  EXPECT_EQ(64, str.size());
  EXPECT_TRUE(std::all_of(str.begin(), str.end(), is_alphanumeric_no_underscore));
}

TEST(RandomString, alphanumeric_plain)
{
  char arr[65] = {};
  gen_rand_alphanumeric_plain(g_ceph_context, arr, sizeof(arr));
  EXPECT_EQ(0, arr[64]);
  EXPECT_TRUE(std::all_of(arr, arr + 64, is_alphanumeric_plain));
}

TEST(RandomString, alphanumeric_plain_string)
{
  std::string str = gen_rand_alphanumeric_plain(g_ceph_context, 64);
  EXPECT_EQ(64, str.size());
  EXPECT_TRUE(std::all_of(str.begin(), str.end(), is_alphanumeric_plain));
}
