// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_common.h"
#include <gtest/gtest.h>

using namespace std::string_view_literals;

TEST(rgw_etag_matches, whole_tag)
{
  EXPECT_TRUE(rgw_etag_matches("\"abc\"", "abc", false));
  EXPECT_TRUE(rgw_etag_matches("abc", "abc", false)); // unquoted
  EXPECT_TRUE(rgw_etag_matches("\"abc-2\"", "abc-2", false)); // multipart
  EXPECT_FALSE(rgw_etag_matches("\"abcd\"", "abc", false)); // not a prefix
  EXPECT_FALSE(rgw_etag_matches("\"abc\"x", "abc", false));
  EXPECT_FALSE(rgw_etag_matches("\"ab\"", "abc", false));
  EXPECT_FALSE(rgw_etag_matches("\"\"", "abc", false));
  EXPECT_FALSE(rgw_etag_matches("", "abc", false));
}

TEST(rgw_etag_matches, attr_nul)
{
  // an ETag attribute may end with a NUL
  EXPECT_TRUE(rgw_etag_matches("\"abc\"", "abc\0"sv, false));
  EXPECT_FALSE(rgw_etag_matches("\"abcd\"", "abc\0"sv, false));
}

TEST(rgw_etag_matches, list)
{
  EXPECT_TRUE(rgw_etag_matches("\"x\", \"abc\"", "abc", false));
  EXPECT_TRUE(rgw_etag_matches("\"abc\",\"x\"", "abc", false));
  EXPECT_TRUE(rgw_etag_matches(" \"x\" ,\t\"abc\" ", "abc", false));
  EXPECT_FALSE(rgw_etag_matches("\"x\", \"y\"", "abc", false));
}

TEST(rgw_etag_matches, weak)
{
  // If-Match compares strongly, If-None-Match weakly (RFC 7232, 2.3.2)
  EXPECT_FALSE(rgw_etag_matches("W/\"abc\"", "abc", false));
  EXPECT_TRUE(rgw_etag_matches("W/\"abc\"", "abc", true));
  EXPECT_TRUE(rgw_etag_matches("W/\"x\", \"abc\"", "abc", false));
}
