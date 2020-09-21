// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw/rgw_string.h"
#include <gtest/gtest.h>

const std::string abc{"abc"};
const char *def{"def"}; // const char*
char ghi_arr[] = {'g', 'h', 'i', '\0'};
char *ghi{ghi_arr}; // char*
constexpr std::string_view jkl{"jkl", 3};
#define mno "mno" // string literal (char[4])
char pqr[] = {'p', 'q', 'r', '\0'};

TEST(string_size, types)
{
  ASSERT_EQ(3u, string_size(abc));
  ASSERT_EQ(3u, string_size(def));
  ASSERT_EQ(3u, string_size(ghi));
  ASSERT_EQ(3u, string_size(jkl));
  ASSERT_EQ(3u, string_size(mno));
  ASSERT_EQ(3u, string_size(pqr));

  constexpr auto compile_time_string_view_size = string_size(jkl);
  ASSERT_EQ(3u, compile_time_string_view_size);
  constexpr auto compile_time_string_literal_size = string_size(mno);
  ASSERT_EQ(3u, compile_time_string_literal_size);

  char arr[] = {'a', 'b', 'c'}; // not null-terminated
  ASSERT_THROW(string_size(arr), std::invalid_argument);
}

TEST(string_cat_reserve, types)
{
  ASSERT_EQ("abcdefghijklmnopqr",
            string_cat_reserve(abc, def, ghi, jkl, mno, pqr));
}

TEST(string_cat_reserve, count)
{
  ASSERT_EQ("", string_cat_reserve());
  ASSERT_EQ("abc", string_cat_reserve(abc));
  ASSERT_EQ("abcdef", string_cat_reserve(abc, def));
}

TEST(string_join_reserve, types)
{
  ASSERT_EQ("abc, def, ghi, jkl, mno, pqr",
            string_join_reserve(", ", abc, def, ghi, jkl, mno, pqr));
}

TEST(string_join_reserve, count)
{
  ASSERT_EQ("", string_join_reserve(", "));
  ASSERT_EQ("abc", string_join_reserve(", ", abc));
  ASSERT_EQ("abc, def", string_join_reserve(", ", abc, def));
}

TEST(string_join_reserve, delim)
{
  ASSERT_EQ("abcdef", string_join_reserve("", abc, def));
  ASSERT_EQ("abc def", string_join_reserve(' ', abc, def));
  ASSERT_EQ("abc\ndef", string_join_reserve('\n', abc, def));
  ASSERT_EQ("abcfoodef", string_join_reserve(std::string{"foo"}, abc, def));
}
