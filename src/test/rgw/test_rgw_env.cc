// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_common.h"

#include <gtest/gtest.h>

namespace http = boost::beast::http;

// RGWEnv keeps two views of a request: env_map, keyed by CGI meta-variable and
// last-wins, and the header fields the frontend parsed off the wire. Only the
// latter can represent a field that arrived on several field-lines, which is
// what SigV4 canonicalization has to see (tracker #75304).

static http::fields make_fields(
    std::initializer_list<std::pair<std::string_view, std::string_view>> l)
{
  http::fields f;
  for (const auto& [name, value] : l) {
    f.insert(name, value);
  }
  return f;
}

TEST(RGWEnv, SetDoesNotTouchHeaderFields)
{
  RGWEnv env;
  env.set("HTTP_CONTENT_ENCODING", "gzip");

  // env_map and the header fields are independent views; only the frontend
  // populates the latter, so an env-only set is invisible to signing
  EXPECT_STREQ("gzip", env.get("HTTP_CONTENT_ENCODING"));
  EXPECT_FALSE(env.get_combined_header("content-encoding").has_value());
}

TEST(RGWEnv, RemoveDoesNotTouchHeaderFields)
{
  RGWEnv env;
  env.set_raw_headers(make_fields({{"content-encoding", "gzip"}}));
  env.set("HTTP_CONTENT_ENCODING", "gzip");

  env.remove("HTTP_CONTENT_ENCODING");

  EXPECT_EQ(nullptr, env.get("HTTP_CONTENT_ENCODING"));
  EXPECT_EQ("gzip", env.get_combined_header("content-encoding").value_or(""));
}

TEST(RGWEnv, SetRawHeadersWinsOverEarlierSets)
{
  RGWEnv env;
  env.set("HTTP_CONTENT_ENCODING", "stale");
  env.set_raw_headers(make_fields({{"content-encoding", "gzip"},
                                   {"content-encoding", "aws-chunked"}}));

  // the wholesale copy is the authority for the header view
  EXPECT_EQ("gzip,aws-chunked",
            env.get_combined_header("content-encoding").value_or(""));
}

TEST(RGWEnv, CombinedHeaderJoinsEveryFieldLineInOrder)
{
  RGWEnv env;
  env.set_raw_headers(make_fields({{"x-amz-object-attributes", "ETag"},
                                   {"x-amz-object-attributes", "ObjectSize"},
                                   {"x-amz-object-attributes", "StorageClass"}}));

  EXPECT_EQ("ETag,ObjectSize,StorageClass",
            env.get_combined_header("x-amz-object-attributes").value_or(""));
}

TEST(RGWEnv, CombinedHeaderSingleFieldLine)
{
  RGWEnv env;
  env.set_raw_headers(make_fields({{"content-encoding", "gzip"}}));

  EXPECT_EQ("gzip", env.get_combined_header("content-encoding").value_or(""));
}

TEST(RGWEnv, CombinedHeaderIsCaseInsensitive)
{
  RGWEnv env;
  env.set_raw_headers(make_fields({{"Content-Encoding", "gzip"}}));

  EXPECT_EQ("gzip", env.get_combined_header("content-encoding").value_or(""));
}

TEST(RGWEnv, CombinedHeaderKeepsEmptyValues)
{
  RGWEnv env;
  env.set_raw_headers(make_fields({{"x-foo", ""}, {"x-foo", ""}}));

  // present but empty is not the same as absent
  EXPECT_EQ(",", env.get_combined_header("x-foo").value_or("<absent>"));
}

TEST(RGWEnv, CombinedHeaderAbsentIsNullopt)
{
  RGWEnv env;
  EXPECT_FALSE(env.get_combined_header("x-missing").has_value());
}

TEST(RGWEnv, SetHeaderReplacesEveryFieldLine)
{
  RGWEnv env;
  env.set_raw_headers(make_fields({{"range", "bytes=0-1"},
                                   {"range", "bytes=2-3"}}));

  // an override has to replace the list, not extend it, or a signature
  // verified afterwards would cover a value the request no longer has
  env.set_header("range", "bytes=4-5");

  EXPECT_EQ("bytes=4-5", env.get_combined_header("range").value_or(""));
}

TEST(RGWEnv, RemoveHeaderClearsEveryFieldLine)
{
  RGWEnv env;
  env.set_raw_headers(make_fields({{"range", "bytes=0-1"},
                                   {"range", "bytes=2-3"}}));

  env.remove_header("range");

  EXPECT_FALSE(env.get_combined_header("range").has_value());
}

TEST(RGWEnv, SetHeaderRejectsNonTokenNames)
{
  RGWEnv env;
  // override_range_hdr builds names out of x-amz-cache, so a crafted name
  // must not be able to inject "name: value" structure into the container
  env.set_header("foo: bar", "v");
  env.set_header("foo\r\nbar", "v");
  env.set_header("foo bar", "v");
  env.set_header("", "v");

  // assert on the container itself: checking a lookup by some other spelling
  // would pass even if the element had been inserted under its crafted name
  EXPECT_EQ(env.get_raw_headers().begin(), env.get_raw_headers().end());
}

TEST(RGWEnv, SetHeaderAcceptsEveryTokenCharacter)
{
  RGWEnv env;
  // the guard must not be so tight that it rejects legal field names, so
  // cover the whole RFC 7230 tchar set, not a sample of it
  const std::string name = "azAZ09!#$%&'*+-.^_`|~";
  env.set_header(name, "v");

  EXPECT_EQ("v", env.get_combined_header(name).value_or(""));
}

TEST(RGWEnv, SetHeaderDropsOversizedInsteadOfThrowing)
{
  RGWEnv env;
  const std::string huge_val(http::fields::max_value_size + 1, 'a');
  const std::string huge_name(http::fields::max_name_size + 1, 'a');

  // beast throws on either; no caller on the request path handles that
  EXPECT_NO_THROW(env.set_header("x-foo", huge_val));
  EXPECT_NO_THROW(env.set_header(huge_name, "v"));

  EXPECT_EQ(env.get_raw_headers().begin(), env.get_raw_headers().end());
}
