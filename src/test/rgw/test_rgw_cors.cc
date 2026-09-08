// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include "rgw_cors.h"

using namespace std;

static RGWCORSRule make_rule(const char *origin, uint8_t methods,
                             const char *allowed_hdr = nullptr)
{
  set<string> origins;
  origins.insert(origin);
  set<string> hdrs;
  if (allowed_hdr) {
    hdrs.insert(allowed_hdr);
  }
  list<string> expose;
  return RGWCORSRule(origins, hdrs, expose, methods, 3000);
}

TEST(RGWCORS, MatchRuleSameOriginDifferentMethods)
{
  RGWCORSConfiguration cfg;
  cfg.get_rules().push_back(make_rule("https://app.example", RGW_CORS_GET));
  cfg.get_rules().push_back(make_rule("https://app.example", RGW_CORS_PUT));

  auto it_get = cfg.get_rules().begin();
  auto it_put = std::next(it_get);

  EXPECT_EQ(cfg.match_rule("https://app.example", "GET", nullptr), &(*it_get));
  EXPECT_EQ(cfg.match_rule("https://app.example", "PUT", nullptr), &(*it_put));
  EXPECT_EQ(cfg.match_rule("https://app.example", "DELETE", nullptr), nullptr);
}

TEST(RGWCORS, MatchRuleSkipsOriginOnlyMatch)
{
  RGWCORSConfiguration cfg;
  cfg.get_rules().push_back(make_rule("https://app.example", RGW_CORS_GET));
  cfg.get_rules().push_back(make_rule("https://app.example", RGW_CORS_PUT));

  EXPECT_NE(nullptr, cfg.match_rule("https://app.example", "PUT", nullptr));
}

TEST(RGWCORS, MatchRulePreflightHeaders)
{
  RGWCORSConfiguration cfg;
  cfg.get_rules().push_back(
      make_rule("https://app.example", RGW_CORS_GET, "content-type"));
  cfg.get_rules().push_back(
      make_rule("https://app.example", RGW_CORS_GET, "*"));

  auto it_strict = cfg.get_rules().begin();
  auto it_wild = std::next(it_strict);

  EXPECT_EQ(cfg.match_rule("https://app.example", "GET",
                           "content-type"),
            &(*it_strict));
  EXPECT_EQ(cfg.match_rule("https://app.example", "GET",
                           "authorization"),
            &(*it_wild));
}

TEST(RGWCORS, HostNameRuleStillOriginOnly)
{
  RGWCORSConfiguration cfg;
  cfg.get_rules().push_back(make_rule("https://app.example", RGW_CORS_GET));
  cfg.get_rules().push_back(make_rule("https://other.example", RGW_CORS_PUT));

  auto it_first = cfg.get_rules().begin();
  EXPECT_EQ(cfg.host_name_rule("https://app.example"), &(*it_first));
}
