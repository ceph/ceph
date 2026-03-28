// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "mgr/DaemonKey.h"

TEST(DaemonKey, Parse)
{
  auto [key, ok] = DaemonKey::parse("osd.1");
  ASSERT_TRUE(ok);
  EXPECT_EQ(key.type, "osd");
  EXPECT_EQ(key.name, "1");
}

TEST(DaemonKey, LessThan)
{
  DaemonKey a{"mon", "a"};
  DaemonKey b{"osd", "1"};
  DaemonKey c{"osd", "2"};

  EXPECT_TRUE(a < b); // mon < osd
  EXPECT_TRUE(b < c); // 1 < 2
  EXPECT_FALSE(c < b); // 2 < 1
  EXPECT_FALSE(b < a); // osd < mon
}

TEST(DaemonKey, Ostream)
{
  DaemonKey key{"osd", "1"};
  std::ostringstream oss;
  oss << key;

  EXPECT_EQ(oss.str(), "osd.1");
}

TEST(DaemonKey, ToString)
{
  DaemonKey key{"osd", "1"};

  EXPECT_EQ(ceph::to_string(key), "osd.1");
}

/* Start Negative Tests */

TEST(DaemonKey, ParseWithEmptyString)
{
  auto [key, ok] = DaemonKey::parse("");
  ASSERT_FALSE(ok);
  EXPECT_EQ(key.type, "");
  EXPECT_EQ(key.name, "");
}

TEST(DaemonKey, ParseWithoutDot)
{
  auto [key, ok] = DaemonKey::parse("osd1");
  ASSERT_FALSE(ok);
  EXPECT_EQ(key.type, "");
  EXPECT_EQ(key.name, "");
}

TEST(DaemonKey, ParseExtraDot)
{
  auto [key, ok] = DaemonKey::parse("osd.1.extra");
  ASSERT_TRUE(ok);
  EXPECT_EQ(key.type, "osd");
  EXPECT_EQ(key.name, "1.extra");
}

TEST(DaemonKey, ParseStartDot)
{
  auto [key, ok] = DaemonKey::parse(".osd");
  ASSERT_TRUE(ok);
  EXPECT_EQ(key.type, "");
  EXPECT_EQ(key.name, "osd");
}

TEST(DaemonKey, ParseEndDot)
{
  auto [key, ok] = DaemonKey::parse("osd.");
  ASSERT_TRUE(ok);
  EXPECT_EQ(key.type, "osd");
  EXPECT_EQ(key.name, "");
}

TEST(DaemonKey, ParseDot)
{
  auto [key, ok] = DaemonKey::parse(".");
  ASSERT_TRUE(ok);
  EXPECT_EQ(key.type, "");
  EXPECT_EQ(key.name, "");
}

TEST(DaemonKey, LessThanEmptyStr)
{
  DaemonKey a{"", ""};
  DaemonKey b{"", ""};
  DaemonKey c{"osd", "1"};

  EXPECT_FALSE(a < b);
  EXPECT_TRUE(a < c);
  EXPECT_FALSE(c < a);
}

TEST(DaemonKey, LessThanEmptyType)
{
  DaemonKey a{"", "1"};
  DaemonKey b{"", "2"};
  DaemonKey c{"osd", "1"};

  EXPECT_TRUE(a < b);
  EXPECT_TRUE(a < c);
}

TEST(DaemonKey, LessThanEmptyName)
{
  DaemonKey a{"mon", ""};
  DaemonKey b{"mon", "a"};
  DaemonKey c{"osd", ""};

  EXPECT_TRUE(a < b);
  EXPECT_TRUE(a < c);
}

TEST(DaemonKey, OstreamEmpty)
{
  DaemonKey key{"", ""};
  std::ostringstream oss;
  oss << key;

  EXPECT_EQ(oss.str(), ".");
}

TEST(DaemonKey, ToStringEmptyType)
{
  DaemonKey key{"", "1"};

  EXPECT_EQ(ceph::to_string(key), ".1");
}

TEST(DaemonKey, ToStringEmptyName)
{
  DaemonKey key{"osd", ""};

  EXPECT_EQ(ceph::to_string(key), "osd.");
}

/* End Negative Tests */
