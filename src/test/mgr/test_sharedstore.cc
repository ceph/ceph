// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "mgr/SharedStore.h"

namespace {
bool add(SharedStorePolicy &p, const std::string &prefix,
         const std::set<std::string> &readers, std::string *err = nullptr)
{
  std::string e;
  bool r = p.add(prefix, readers, &e);
  if (err) {
    *err = e;
  }
  return r;
}
}

TEST(SharedStorePolicy, EmptyPolicyAllowsNothing)
{
  SharedStorePolicy p;
  EXPECT_TRUE(p.empty());
  EXPECT_FALSE(p.allows("telemetry", "telemetry/metrics"));
  EXPECT_FALSE(p.allows("telemetry", ""));
}

TEST(SharedStorePolicy, AllowsListedReaderUnderPrefix)
{
  SharedStorePolicy p;
  ASSERT_TRUE(add(p, "telemetry/", {"telemetry"}));
  EXPECT_EQ(p.size(), 1u);
  EXPECT_TRUE(p.allows("telemetry", "telemetry/metrics"));
  EXPECT_TRUE(p.allows("telemetry", "telemetry/a/b/c"));
}

TEST(SharedStorePolicy, DeniesOtherReaders)
{
  SharedStorePolicy p;
  ASSERT_TRUE(add(p, "telemetry/", {"telemetry"}));
  EXPECT_FALSE(p.allows("prometheus", "telemetry/metrics"));
  EXPECT_FALSE(p.allows("", "telemetry/metrics"));
  // a reader name that only contains the allowed one
  EXPECT_FALSE(p.allows("telemetry2", "telemetry/metrics"));
  EXPECT_FALSE(p.allows("telemetr", "telemetry/metrics"));
}

TEST(SharedStorePolicy, DeniesKeysOutsideThePrefix)
{
  SharedStorePolicy p;
  ASSERT_TRUE(add(p, "telemetry/", {"telemetry"}));
  EXPECT_FALSE(p.allows("telemetry", "password"));
  EXPECT_FALSE(p.allows("telemetry", "jwt_secret"));
  // whole "directories" only: a sibling that merely starts with the same
  // letters is not shared
  EXPECT_FALSE(p.allows("telemetry", "telemetry"));
  EXPECT_FALSE(p.allows("telemetry", "telemetry-other/x"));
  EXPECT_FALSE(p.allows("telemetry", "telemetryx/metrics"));
  // shorter than the prefix
  EXPECT_FALSE(p.allows("telemetry", "telem"));
  EXPECT_FALSE(p.allows("telemetry", ""));
}

TEST(SharedStorePolicy, DotDotIsJustPartOfTheKey)
{
  // Keys are opaque strings in the config-key store, there is no path
  // resolution, so this names a different key that must not be reachable
  // just because it starts with the prefix of a rule for another path.
  SharedStorePolicy p;
  ASSERT_TRUE(add(p, "public/", {"telemetry"}));
  EXPECT_FALSE(p.allows("telemetry", "private/x"));
  EXPECT_TRUE(p.allows("telemetry", "public/../private/x"));
}

TEST(SharedStorePolicy, MultipleRulesAndReaders)
{
  SharedStorePolicy p;
  ASSERT_TRUE(add(p, "telemetry/", {"telemetry", "insights"}));
  ASSERT_TRUE(add(p, "status/", {"prometheus"}));
  EXPECT_EQ(p.size(), 2u);
  EXPECT_TRUE(p.allows("telemetry", "telemetry/x"));
  EXPECT_TRUE(p.allows("insights", "telemetry/x"));
  EXPECT_FALSE(p.allows("prometheus", "telemetry/x"));
  EXPECT_TRUE(p.allows("prometheus", "status/x"));
  EXPECT_FALSE(p.allows("telemetry", "status/x"));
}

TEST(SharedStorePolicy, OverlappingRulesAreUnionNotOverride)
{
  SharedStorePolicy p;
  ASSERT_TRUE(add(p, "a/", {"m1"}));
  ASSERT_TRUE(add(p, "a/b/", {"m2"}));
  EXPECT_TRUE(p.allows("m1", "a/b/c"));
  EXPECT_TRUE(p.allows("m2", "a/b/c"));
  EXPECT_FALSE(p.allows("m2", "a/c"));
}

TEST(SharedStorePolicy, RejectsInvalidRules)
{
  SharedStorePolicy p;
  std::string err;

  EXPECT_FALSE(add(p, "", {"telemetry"}, &err));
  EXPECT_NE(err.find("empty"), std::string::npos);

  EXPECT_FALSE(add(p, "telemetry", {"telemetry"}, &err));
  EXPECT_NE(err.find("end with"), std::string::npos);

  EXPECT_FALSE(add(p, "/telemetry/", {"telemetry"}, &err));
  EXPECT_NE(err.find("start with"), std::string::npos);

  EXPECT_FALSE(add(p, "telemetry/", {}, &err));
  EXPECT_NE(err.find("readers"), std::string::npos);

  EXPECT_FALSE(add(p, "telemetry/", {"*"}, &err));
  EXPECT_NE(err.find("wildcard"), std::string::npos);

  EXPECT_FALSE(add(p, "telemetry/", {"telemetry", "*"}, &err));
  EXPECT_FALSE(add(p, "telemetry/", {""}, &err));
  EXPECT_FALSE(add(p, "telemetry/", {"mgr/telemetry"}, &err));

  // nothing was added by any of the failed calls
  EXPECT_TRUE(p.empty());
  EXPECT_FALSE(p.allows("telemetry", "telemetry/x"));
}

TEST(SharedStorePolicy, FailedAddLeavesEarlierRulesIntact)
{
  SharedStorePolicy p;
  ASSERT_TRUE(add(p, "telemetry/", {"telemetry"}));
  EXPECT_FALSE(add(p, "bad", {"telemetry"}));
  EXPECT_EQ(p.size(), 1u);
  EXPECT_TRUE(p.allows("telemetry", "telemetry/x"));
}
