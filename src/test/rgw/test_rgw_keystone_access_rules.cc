// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_auth_keystone.h"
#include <gtest/gtest.h>

using rgw::auth::keystone::detail::path_matches_pattern;

// Exact matches
TEST(PathMatchesPattern, ExactMatch)
{
  EXPECT_TRUE(path_matches_pattern("/v1/AUTH_abc/container/obj",
                                   "/v1/AUTH_abc/container/obj"));
}

TEST(PathMatchesPattern, ExactNoMatch)
{
  EXPECT_FALSE(path_matches_pattern("/v1/AUTH_abc/container/obj",
                                    "/v1/AUTH_abc/container/other"));
}

// Single-segment wildcard '*'
TEST(PathMatchesPattern, SingleStar_MatchesSegment)
{
  EXPECT_TRUE(path_matches_pattern("/v1/AUTH_*/container", "/v1/AUTH_abc/container"));
}

TEST(PathMatchesPattern, SingleStar_NoMatchAcrossSlash)
{
  // '*' must not cross a '/'
  EXPECT_FALSE(path_matches_pattern("/v1/*/obj", "/v1/a/b/obj"));
}

TEST(PathMatchesPattern, SingleStar_TrailingSegment)
{
  EXPECT_TRUE(path_matches_pattern("/v1/AUTH_abc/*", "/v1/AUTH_abc/container"));
}

TEST(PathMatchesPattern, SingleStar_OnlyOneSegmentLeft)
{
  // pattern has trailing *, path has two more segments -> no match
  EXPECT_FALSE(path_matches_pattern("/v1/*", "/v1/AUTH_abc/container"));
}

// Double-star wildcard '**'
TEST(PathMatchesPattern, DoubleStar_MatchesMultipleSegments)
{
  EXPECT_TRUE(path_matches_pattern("/v1/**", "/v1/AUTH_abc/container/obj"));
}

TEST(PathMatchesPattern, DoubleStar_MatchesZeroSegments)
{
  EXPECT_TRUE(path_matches_pattern("/v1/**", "/v1/"));
}

TEST(PathMatchesPattern, DoubleStar_MatchesOneSegment)
{
  EXPECT_TRUE(path_matches_pattern("/v1/**", "/v1/AUTH_abc"));
}

TEST(PathMatchesPattern, DoubleStar_InMiddle)
{
  EXPECT_TRUE(path_matches_pattern("/v1/**/obj", "/v1/AUTH_abc/container/obj"));
}

// Named placeholder '{tag}'
TEST(PathMatchesPattern, NamedPlaceholder_MatchesSingleSegment)
{
  EXPECT_TRUE(path_matches_pattern("/v1/{account}/container",
                                   "/v1/AUTH_abc/container"));
}

TEST(PathMatchesPattern, NamedPlaceholder_NoMatchAcrossSlash)
{
  EXPECT_FALSE(path_matches_pattern("/v1/{account}/obj",
                                    "/v1/AUTH_abc/container/obj"));
}

TEST(PathMatchesPattern, MalformedPlaceholder_UnbalancedBrace)
{
  // Unbalanced '{' should not match
  EXPECT_FALSE(path_matches_pattern("/v1/{account/obj", "/v1/AUTH_abc/obj"));
}

// Query string stripping is done by the caller (check_access_rules), not
// path_matches_pattern itself. Verify the raw function sees '?' as a literal.
TEST(PathMatchesPattern, QueryStringPassedThrough)
{
  // If a caller forgot to strip '?', the literal '?' in path won't match
  // because pattern has no '?' or '**' to absorb it.
  EXPECT_FALSE(path_matches_pattern("/v1/AUTH_abc/container",
                                    "/v1/AUTH_abc/container?param=1"));
}

// Mixed patterns
TEST(PathMatchesPattern, MixedStarAndDoubleStar)
{
  // /v1/*/container/** should match /v1/AUTH_abc/container/a/b/c
  EXPECT_TRUE(path_matches_pattern("/v1/*/container/**",
                                   "/v1/AUTH_abc/container/a/b/c"));
}

TEST(PathMatchesPattern, EmptyPatternEmptyPath)
{
  EXPECT_TRUE(path_matches_pattern("", ""));
}

TEST(PathMatchesPattern, EmptyPatternNonEmptyPath)
{
  EXPECT_FALSE(path_matches_pattern("", "/v1/something"));
}

TEST(PathMatchesPattern, TrailingSlashInPattern)
{
  // A literal trailing '/' in the pattern requires '/' at the end of the path.
  EXPECT_TRUE(path_matches_pattern("/v1/AUTH_abc/", "/v1/AUTH_abc/"));
  EXPECT_FALSE(path_matches_pattern("/v1/AUTH_abc/", "/v1/AUTH_abc"));
}

// Trailing '/**' must not match the bare prefix without the separator.
// Regression for the "container/** matches container" scoping bug.
TEST(PathMatchesPattern, TrailingDoubleStar_RequiresSeparator)
{
  EXPECT_FALSE(path_matches_pattern("/v1/AUTH_p/container/**",
                                    "/v1/AUTH_p/container"));
  EXPECT_TRUE(path_matches_pattern("/v1/AUTH_p/container/**",
                                   "/v1/AUTH_p/container/"));
  EXPECT_TRUE(path_matches_pattern("/v1/AUTH_p/container/**",
                                   "/v1/AUTH_p/container/obj"));
  EXPECT_TRUE(path_matches_pattern("/v1/AUTH_p/container/**",
                                   "/v1/AUTH_p/container/a/b/c"));
}

// '**' in the middle of a pattern keeps its surrounding '/' literals.
TEST(PathMatchesPattern, MiddleDoubleStar_KeepsSeparators)
{
  EXPECT_FALSE(path_matches_pattern("/v1/**/obj", "/v1/obj"));
  EXPECT_TRUE(path_matches_pattern("/v1/**/obj", "/v1//obj"));
  EXPECT_TRUE(path_matches_pattern("/v1/**/obj", "/v1/a/obj"));
  EXPECT_TRUE(path_matches_pattern("/v1/**/obj", "/v1/a/b/obj"));
}

// '*' must backtrack across literal mismatches within a segment.
TEST(PathMatchesPattern, SingleStar_BacktrackOnLiteral)
{
  EXPECT_TRUE(path_matches_pattern("/abc*xyz", "/abc123xyz"));
  EXPECT_FALSE(path_matches_pattern("/abc*xyz", "/abcxyz"));
  EXPECT_TRUE(path_matches_pattern("/a*b*c", "/aXbYc"));
}

// '*' must match a non-empty segment (regex '[^/]+'), not zero chars.
TEST(PathMatchesPattern, SingleStar_RejectsEmptySegment)
{
  EXPECT_FALSE(path_matches_pattern("/v1/*", "/v1/"));
  EXPECT_FALSE(path_matches_pattern("/v1/AUTH_abc/*", "/v1/AUTH_abc/"));
  EXPECT_FALSE(path_matches_pattern("/v1/AUTH_*/container",
                                    "/v1/AUTH_/container"));
}

TEST(PathMatchesPattern, NamedPlaceholder_RejectsEmptyTrailingSegment)
{
  EXPECT_FALSE(path_matches_pattern("/v1/{account}", "/v1/"));
}

TEST(PathMatchesPattern, DoubleStar_MatchesEmptyTrailingSegment)
{
  // '**' (regex '.*') still permits the empty remainder.
  EXPECT_TRUE(path_matches_pattern("/v1/AUTH_abc/**", "/v1/AUTH_abc/"));
}
