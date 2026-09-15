#include <gtest/gtest.h>
#include "rgw/rgw_keystone.h"
#include "rgw/rgw_auth_keystone.h"

using rgw::keystone::TokenEnvelope;

namespace rgw::auth::keystone {

TEST(RGWKeystoneAuth, PathMatchesPattern)
{
  struct Case {
    std::string pattern;
    std::string path;
    bool expected;
  };

  const std::vector<Case> cases = {
    {"/v1/a/c", "/v1/a/c", true},
    {"/v1/a/c", "/v1/a/d", false},
    {"/v1/*/c", "/v1/a/c", true},
    {"/v1/*/c", "/v1/a/d", false},
    /* single * must not cross a / */
    {"/v1/*/c", "/v1/a/b/c", false},
    {"/v1/**", "/v1/a/c", true},
    {"/v1/**", "/v1/a/b/c", true},
    {"/v1/**", "/v2/a/b/c", false},
    /* trailing slash in pattern */
    {"/v1/**/", "/v1/a/b", true},
    {"*", "/anything", true},
    /* empty pattern / path edge cases */
    {"", "", true},
    {"/v1", "/v1", true},
    {"/v1", "/v1/extra", false},
  };

  for (const auto& tc : cases) {
    SCOPED_TRACE(tc.pattern + " vs " + tc.path);
    ASSERT_EQ(tc.expected, path_matches_pattern(tc.pattern, tc.path));
  }
}

TEST(RGWKeystoneAuth, CheckAccessRules)
{
  std::vector<TokenEnvelope::AccessRule> rules;
  rules.push_back({"object-store", "GET", "/v1/*"});
  rules.push_back({"compute", "GET", "/v1/*"});

  ASSERT_TRUE(check_access_rules(nullptr, rules, "GET", "/v1/a"));
  ASSERT_FALSE(check_access_rules(nullptr, rules, "PUT", "/v1/a"));
  ASSERT_FALSE(check_access_rules(nullptr, rules, "GET", "/v2/a"));

  /* empty rules = no restriction (regular token, not app cred) */
  std::vector<TokenEnvelope::AccessRule> empty_rules;
  ASSERT_TRUE(check_access_rules(nullptr, empty_rules, "GET", "/v1/a"));
  ASSERT_TRUE(check_access_rules(nullptr, empty_rules, "DELETE", "/anything"));
}

} // namespace rgw::auth::keystone
