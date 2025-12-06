// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr/DaemonKey.h"
#include "gtest/gtest.h"

TEST(DaemonKey, Parse)
{
  auto[validKey, valid] = DaemonKey::parse("osd.1");
  ASSERT_TRUE(valid);
  EXPECT_EQ(validKey.type, "osd");
  EXPECT_EQ(validKey.name, "1");
  
  auto[invalidKey, invalid] = DaemonKey::parse("osd1");

  ASSERT_FALSE(invalid);
  ASSERT_EQ(invalidKey.type, "");
  ASSERT_EQ(invalidKey.name, "");
}

TEST(DaemonKey, LessThan)
{
  DaemonKey a{"mon", "a"};
  DaemonKey b{"osd", "1"};
  DaemonKey c{"osd", "2"};

  EXPECT_TRUE(a < b);  // mon < osd
  EXPECT_TRUE(b < c);  // 1 < 2
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
