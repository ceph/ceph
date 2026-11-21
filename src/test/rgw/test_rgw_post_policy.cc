// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_policy_s3.h"
#include "include/buffer.h"
#include <gtest/gtest.h>
#include <cerrno>
#include <string>

namespace {

// parse a POST object policy with the given expiration and no conditions,
// then check it against an empty environment
int check_policy(const std::string& expiration, std::string& err_msg)
{
  bufferlist bl;
  bl.append("{\"expiration\": \"" + expiration + "\", \"conditions\": []}");
  RGWPolicy policy;
  int r = policy.from_json(bl, err_msg);
  if (r < 0) {
    return r;
  }
  RGWPolicyEnv env;
  return policy.check(&env, err_msg);
}

} // anonymous namespace

TEST(RGWPostPolicy, FutureExpirationAccepted)
{
  std::string err_msg;
  EXPECT_EQ(0, check_policy("2999-01-01T00:00:00Z", err_msg)) << err_msg;
}

TEST(RGWPostPolicy, PastExpirationRefused)
{
  std::string err_msg;
  EXPECT_EQ(-EACCES, check_policy("1970-01-01T00:00:00Z", err_msg));
  EXPECT_EQ("Policy expired", err_msg);
  err_msg.clear();
  EXPECT_EQ(-EACCES, check_policy("2001-01-01T00:00:00Z", err_msg));
  EXPECT_EQ("Policy expired", err_msg);
}

TEST(RGWPostPolicy, PreEpochExpirationRefused)
{
  std::string err_msg;
  EXPECT_EQ(-EACCES, check_policy("1969-12-31T23:59:59Z", err_msg));
  EXPECT_EQ("Policy expired", err_msg);
  err_msg.clear();
  EXPECT_EQ(-EACCES, check_policy("1900-01-01T00:00:00Z", err_msg));
  EXPECT_EQ("Policy expired", err_msg);
}
