// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <algorithm>
#include <string>
#include <unordered_map>

#include "gtest/gtest.h"
#include <jwt-cpp/base.h>

using std::string;

#include "rgw/rgw_rest_sts.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::auth::sts {

struct WebTokenEngineTestAccess {
  template <typename T>
  static void call_recurse_and_insert(const WebTokenEngine& engine,
                                      const string& key,
                                      const jwt::claim& c,
                                      T& out) {
    engine.recurse_and_insert(key, c, out);
  }

  static WebTokenEngine::token_t call_get_token_claims(
      const WebTokenEngine& engine,
      const rgw_decoded_jwt& decoded) {
    return engine.get_token_claims(decoded);
  }

  static auto call_get_principal_tags(const WebTokenEngine& engine,
                                      const DoutPrefixProvider* dpp,
                                      const rgw_decoded_jwt& decoded) {
    return engine.get_principal_tags(dpp, decoded);
  }
};

namespace {

std::string make_test_jwt(const std::string& payload_json) {
  const std::string header_json = R"({"alg":"none","typ":"JWT"})";
  auto enc = [](const std::string& s) {
    const std::string b64 = jwt::base::encode<jwt::alphabet::base64url>(s);
    return jwt::base::trim<jwt::alphabet::base64url>(b64);
  };
  return enc(header_json) + "." + enc(payload_json) + ".sig";
}

picojson::value parse_json(const std::string& json_str) {
  picojson::value v;
  const std::string err = picojson::parse(v, json_str);
  EXPECT_TRUE(err.empty()) << "Failed to parse JSON: " << err;
  return v;
}

TEST(WebTokenEngineTest, RecurseAndInsertScalarArrayAndObject) {
  WebTokenEngine engine(nullptr, nullptr, nullptr, nullptr);
  std::unordered_multimap<string, string> out;

  // flattened claim payload, mimicing OIDC
  const auto val = parse_json(R"({
    "iss": "https://keycloak.example/realms/ceph",
    "sub": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
    "aud": "rgw-client",
    "preferred_username": "alice",
    "email": "alice@example.com",
    "groups": ["engineering", "s3-readers"]
  })");

  WebTokenEngineTestAccess::call_recurse_and_insert(
      engine, "ignored_root_key", jwt::claim(val), out);

  EXPECT_EQ(out.find("ignored_root_key"), out.end());

  const std::vector<std::pair<std::string, std::string>> expected_scalars = {
      {"iss", "https://keycloak.example/realms/ceph"},
      {"sub", "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"},
      {"aud", "rgw-client"},
      {"preferred_username", "alice"},
      {"email", "alice@example.com"},
  };

  for (const auto& [key, expected] : expected_scalars) {
    EXPECT_EQ(out.count(key), 1u) << "missing or duplicate key: " << key;
    const auto it = out.find(key);
    ASSERT_NE(it, out.end()) << "key not found: " << key;
    EXPECT_EQ(it->second, expected) << "mismatch for key: " << key;
  }

  EXPECT_EQ(out.count("groups"), 2u);
  const auto range = out.equal_range("groups");
  std::vector<std::string> groups;
  for (auto it = range.first; it != range.second; ++it) {
    groups.push_back(it->second);
  }
  EXPECT_NE(std::find(groups.begin(), groups.end(), "engineering"), groups.end());
  EXPECT_NE(std::find(groups.begin(), groups.end(), "s3-readers"), groups.end());

}

TEST(WebTokenEngineTest, GetTokenClaimsSkipsPrincipalTagsNamespace) {
  WebTokenEngine engine(nullptr, nullptr, nullptr, nullptr);

  // nested claim payload, mimicing Keycloak/AWS Cognito
  const std::string jwt_str = make_test_jwt(R"({
    "sub": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
    "realm_access": {
      "roles": ["offline_access", "s3-admin"]
    },
    "resource_access": {
      "rgw-client": {
        "roles": ["bucket-writer"]
      }
    },
    "https://aws.amazon.com/tags": {
      "principal_tags": {
        "env": ["prod"]
      }
    }
  })");

  const auto decoded = jwt::decode<jwt::traits::kazuho_picojson>(jwt_str);
  const auto claims =
      WebTokenEngineTestAccess::call_get_token_claims(engine, decoded);

  EXPECT_EQ(claims.count("sub"), 1u);
  const auto sub_it = claims.find("sub");
  ASSERT_NE(sub_it, claims.end());
  EXPECT_EQ(sub_it->second, "f81d4fae-7dec-11d0-a765-00a0c91e6bf6");

  // Wrapper/intermediate keys are not emitted as standalone claims
  EXPECT_EQ(claims.find("realm_access"), claims.end());
  EXPECT_EQ(claims.find("resource_access"), claims.end());
  EXPECT_EQ(claims.find("rgw-client"), claims.end());

  // Nested leaf "roles" arrays are flattened into the claims map
  EXPECT_EQ(claims.count("roles"), 3u);
  const auto range = claims.equal_range("roles");
  std::vector<std::string> roles;
  for (auto it = range.first; it != range.second; ++it) {
    roles.push_back(it->second);
  }
  EXPECT_NE(std::find(roles.begin(), roles.end(), "offline_access"), roles.end());
  EXPECT_NE(std::find(roles.begin(), roles.end(), "s3-admin"), roles.end());
  EXPECT_NE(std::find(roles.begin(), roles.end(), "bucket-writer"), roles.end());

  // Principal tags namespace is skipped by get_token_claims
  EXPECT_EQ(claims.find("https://aws.amazon.com/tags"), claims.end());
  EXPECT_EQ(claims.find("principal_tags"), claims.end());
  EXPECT_EQ(claims.find("env"), claims.end());

}

TEST(WebTokenEngineTest, RecurseAndInsertSkipsNullClaims) {
  WebTokenEngine engine(nullptr, nullptr, nullptr, nullptr);
  std::unordered_multimap<string, string> out;

  const auto null_val = parse_json("null");
  EXPECT_NO_THROW(WebTokenEngineTestAccess::call_recurse_and_insert(
      engine, "null_claim", jwt::claim(null_val), out));
  EXPECT_TRUE(out.empty());
}

TEST(WebTokenEngineTest, RecurseAndInsertSkipsNullFieldsInObjectAndArray) {
  WebTokenEngine engine(nullptr, nullptr, nullptr, nullptr);
  std::unordered_multimap<string, string> out;

  const auto val = parse_json(R"({
    "keep_str": "value",
    "null_field": null,
    "nested": {
      "inner_keep": "ok",
      "inner_null": null
    },
    "arr_with_null": ["first", null, "third"]
  })");

  EXPECT_NO_THROW(WebTokenEngineTestAccess::call_recurse_and_insert(
      engine, "ignored_root_key", jwt::claim(val), out));
  EXPECT_EQ(out.count("null_field"), 0u);
  EXPECT_EQ(out.count("inner_null"), 0u);
}

TEST(WebTokenEngineTest, GetPrincipalTagsExtractsEnvProd) {
  WebTokenEngine engine(nullptr, nullptr, nullptr, nullptr);

  const std::string jwt_str = make_test_jwt(R"({
    "https://aws.amazon.com/tags": {
      "principal_tags": {
        "env": ["prod"]
      }
    }
  })");

  const auto decoded = jwt::decode<jwt::traits::kazuho_picojson>(jwt_str);
  const auto tags =
      WebTokenEngineTestAccess::call_get_principal_tags(engine, nullptr, decoded);

  EXPECT_EQ(tags.count({"env", "prod"}), 1u);
}

}  // namespace
}  // namespace rgw::auth::sts

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
