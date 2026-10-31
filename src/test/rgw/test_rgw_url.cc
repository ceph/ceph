// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_url.h"
#include "rgw_common.h"
#include <string>
#include <string_view>
#include <gtest/gtest.h>

using namespace rgw;

// The lenient string-returning url_decode() keeps its historical behaviour,
// which RGWPutObj::init_processing's copy-source empty-check (PR #63521) relies
// on: a bad hex digit empties the whole result, and a trailing '%' is dropped.
TEST(TestURLDecode, LenientLegacyBehaviour)
{
  EXPECT_EQ(url_decode("repro/%zz"), "");         // bad hex digit -> empty
  EXPECT_EQ(url_decode("repro/k%"), "repro/k");   // trailing '%' -> dropped
  EXPECT_EQ(url_decode("repro/k%25"), "repro/k%");
  EXPECT_EQ(url_decode("repro/%7A%7A"), "repro/zz");
}

// The strict url_decode() overload reports a malformed percent-escape so that
// request-routing callers can reject it instead of routing on a silently
// altered path that names a different resource.
TEST(TestURLDecode, StrictRejectsMalformed)
{
  std::string dest;
  EXPECT_FALSE(url_decode("repro/%zz", dest));  // bad hex digit
  EXPECT_FALSE(url_decode("repro/k%", dest));   // trailing '%'
  EXPECT_FALSE(url_decode("repro/%2", dest));   // short escape (one hex digit)
  EXPECT_FALSE(url_decode("%", dest));          // lone '%'
}

TEST(TestURLDecode, StrictAcceptsWellFormed)
{
  std::string dest;
  ASSERT_TRUE(url_decode("repro/k", dest));
  EXPECT_EQ(dest, "repro/k");
  ASSERT_TRUE(url_decode("repro/k%25", dest));   // '%25' -> '%'
  EXPECT_EQ(dest, "repro/k%");
  ASSERT_TRUE(url_decode("repro/%7A%7A", dest)); // '%7A%7A' -> 'zz'
  EXPECT_EQ(dest, "repro/zz");
}

TEST(TestURL, SimpleAuthority)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "http://example.com";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
    ASSERT_TRUE(user.empty());
    ASSERT_TRUE(password.empty());
    EXPECT_STREQ(host.c_str(), "example.com"); 
}

TEST(TestURL, SimpleAuthority_1)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "http://example.com/";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
    ASSERT_TRUE(user.empty());
    ASSERT_TRUE(password.empty());
    EXPECT_STREQ(host.c_str(), "example.com");
}

TEST(TestURL, IPAuthority)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "http://1.2.3.4";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
    ASSERT_TRUE(user.empty());
    ASSERT_TRUE(password.empty());
    EXPECT_STREQ(host.c_str(), "1.2.3.4"); 
}

TEST(TestURL, IPv6Authority)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "http://[FE80:CD00:0000:0CDE:1257:0000:211E:729C]";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
    ASSERT_TRUE(user.empty());
    ASSERT_TRUE(password.empty());
    EXPECT_STREQ(host.c_str(), "[FE80:CD00:0000:0CDE:1257:0000:211E:729C]");
}

TEST(TestURL, AuthorityWithUserinfo)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "https://user:password@example.com";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
    EXPECT_STREQ(host.c_str(), "example.com"); 
    EXPECT_STREQ(user.c_str(), "user"); 
    EXPECT_STREQ(password.c_str(), "password"); 
}

TEST(TestURL, AuthorityWithPort)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "http://user:password@example.com:1234";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
    EXPECT_STREQ(host.c_str(), "example.com:1234"); 
    EXPECT_STREQ(user.c_str(), "user"); 
    EXPECT_STREQ(password.c_str(), "password"); 
}

TEST(TestURL, DifferentSchema)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "kafka://example.com";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
    ASSERT_TRUE(user.empty());
    ASSERT_TRUE(password.empty());
    EXPECT_STREQ(host.c_str(), "example.com"); 
}

TEST(TestURL, InvalidUrl)
{
  std::string host, user, password;
  EXPECT_FALSE(parse_url_authority("not a valid url", host, user, password));
}

TEST(TestURL, EmptyString)
{
  std::string host, user, password;
  EXPECT_FALSE(parse_url_authority("", host, user, password));
}

TEST(TestURL, MalformedScheme)
{
  std::string host, user, password;
  EXPECT_FALSE(
      parse_url_authority("ht!tp://example.com", host, user, password));
}

TEST(TestURL, InvalidCharactersInHost)
{
  std::string host, user, password;
  EXPECT_FALSE(
      parse_url_authority("http://exa mple.com", host, user, password));
}

TEST(TestURL, MissingScheme)
{
  std::string host, user, password;
  EXPECT_FALSE(parse_url_authority("example.com", host, user, password));
}

TEST(TestURL, SpecialCharactersInUserinfo)
{
  std::string host, user, password;
  EXPECT_FALSE(
      parse_url_authority(
          "http://us er:pass@example.com", host, user, password));
}

TEST(TestURL, WithPath)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "amqps://www.example.com:1234/vhost_name";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
}

