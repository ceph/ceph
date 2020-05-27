// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_url.h"
#include <string>
#include <gtest/gtest.h>

using namespace rgw;

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
    const std::string url = "http://FE80:CD00:0000:0CDE:1257:0000:211E:729C";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
    ASSERT_TRUE(user.empty());
    ASSERT_TRUE(password.empty());
    EXPECT_STREQ(host.c_str(), "FE80:CD00:0000:0CDE:1257:0000:211E:729C"); 
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

TEST(TestURL, InvalidHost)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "http://exa_mple.com";
    ASSERT_FALSE(parse_url_authority(url, host, user, password));
}

TEST(TestURL, WithPath)
{
    std::string host;
    std::string user;
    std::string password;
    const std::string url = "amqps://www.example.com:1234/vhost_name";
    ASSERT_TRUE(parse_url_authority(url, host, user, password));
}

