// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <list>
#include <vector>
#include <string_view>

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
  vector<string> expose;
  return RGWCORSRule(origins, hdrs, expose, methods, 3000);
}

static bool allows_header(RGWCORSRule& rule, std::string_view header)
{
  return rule.is_header_allowed(header.data(), header.size());
}

TEST(RGWCORS, SequenceEncodingRemainsCompatible)
{
  const list<string> legacy_headers {"x-one", "x-two"};
  const vector<string> headers(std::begin(legacy_headers), std::end(legacy_headers));
  bufferlist legacy_headers_encoding;
  bufferlist headers_encoding;

  encode(legacy_headers, legacy_headers_encoding);
  encode(headers, headers_encoding);

  ASSERT_TRUE(legacy_headers_encoding.contents_equal(headers_encoding));

  const list<RGWCORSRule> legacy_rules {
    make_rule("https://app.example", RGW_CORS_GET),
    make_rule("https://app.example", RGW_CORS_PUT)
  };
  const vector<RGWCORSRule> rules(std::begin(legacy_rules), std::end(legacy_rules));
  bufferlist legacy_rules_encoding;
  bufferlist rules_encoding;

  encode(legacy_rules, legacy_rules_encoding);
  encode(rules, rules_encoding);

  ASSERT_TRUE(legacy_rules_encoding.contents_equal(rules_encoding));

  auto encoded = std::cbegin(legacy_rules_encoding);
  vector<RGWCORSRule> decoded;
  decode(decoded, encoded);

  ASSERT_EQ(2, std::size(decoded));
  EXPECT_TRUE(decoded[0].matches("https://app.example", "GET", nullptr));
  EXPECT_TRUE(decoded[1].matches("https://app.example", "PUT", nullptr));
  EXPECT_EQ(0, encoded.get_remaining());
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

TEST(RGWCORS, HeaderWildcards)
{
  auto prefix = make_rule("https://app.example", RGW_CORS_GET, "x-amz-*");
  EXPECT_TRUE(allows_header(prefix, "X-Amz-Meta-Test"));
  EXPECT_FALSE(allows_header(prefix, "x-other-header"));

  auto suffix = make_rule("https://app.example", RGW_CORS_GET, "*-checksum");
  EXPECT_TRUE(allows_header(suffix, "x-amz-checksum"));
  EXPECT_FALSE(allows_header(suffix, "x-amz-checksum-extra"));

  auto infix = make_rule("https://app.example", RGW_CORS_GET, "x-*-checksum");
  EXPECT_TRUE(allows_header(infix, "x-amz-checksum"));
  EXPECT_FALSE(allows_header(infix, "prefix-x-amz-checksum"));

  auto extra = make_rule("https://app.example", RGW_CORS_GET, "x-*-*-checksum");
  EXPECT_FALSE(allows_header(extra, "x-amz-checksum"));
}

TEST(RGWCORS, HostNameRuleStillOriginOnly)
{
  RGWCORSConfiguration cfg;
  cfg.get_rules().push_back(make_rule("https://app.example", RGW_CORS_GET));
  cfg.get_rules().push_back(make_rule("https://other.example", RGW_CORS_PUT));

  auto it_first = cfg.get_rules().begin();
  EXPECT_EQ(cfg.host_name_rule("https://app.example"), &(*it_first));
}
