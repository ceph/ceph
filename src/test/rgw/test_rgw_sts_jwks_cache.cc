// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
#include <gtest/gtest.h>
#include <chrono>
#include <string>
#include <vector>
#include "rgw_sts_jwks_cache.h"

using rgw::auth::sts::detail::JWKSCache;
using rgw::auth::sts::detail::is_jwks_document;
using rgw::auth::sts::detail::jwks_keys;
using rgw::auth::sts::detail::jwks_keys_map;
using namespace std::chrono_literals;

namespace {

// thumbprints as passed by a caller that verifies the JWKS endpoint (or not)
const std::vector<std::string> registered{"aa"};
const std::vector<std::string> unverified{};
const std::string jwks_a = R"({"keys":[{"kid":"a","kty":"RSA"}]})";
const std::string jwks_b = R"({"keys":[{"kid":"b","kty":"RSA"}]})";
const std::string jwks_ab = R"({"keys":[{"kid":"a","kty":"RSA"},{"kid":"b","kty":"RSA"}]})";
const std::string jwks_c = R"({"keys":[{"kid":"c","kty":"RSA"}]})";
const std::string jwks_x5c = R"({"keys":[{"kid":"a","kty":"RSA","x5c":["ca","root"]},{"kid":"b","kty":"RSA","x5c":["cb"]},{"kid":"n","kty":"RSA"}]})";
const auto identity_digest = [](const std::string& cert) { return cert; };
constexpr auto ttl = 300s;
const std::optional<std::string> no_kid;

JWKSCache::Key scoped(const std::string& iss,
                      const std::string& account = "account-a",
                      const std::string& provider_namespace = "account-a")
{
  return {account, provider_namespace, iss};
}

std::optional<std::string> look(JWKSCache& cache, const std::string& iss,
                                const std::vector<std::string>& thumbprints,
                                std::optional<std::string> kid,
                                JWKSCache::time_point now)
{
  return cache.lookup(scoped(iss), thumbprints, kid, ttl, now,
                      !thumbprints.empty());
}

JWKSCache::time_point t0()
{
  return JWKSCache::time_point{} + 1000s;
}

std::optional<std::string> signing_lookup(
    JWKSCache& cache, const std::string& iss,
    const std::vector<std::string>& thumbprints,
    std::optional<std::string> kid, JWKSCache::time_point now)
{
  return cache.lookup(scoped(iss), thumbprints, kid, ttl, now, false);
}

} // anonymous namespace

TEST(STSJwksCache, DocumentShape)
{
  EXPECT_TRUE(is_jwks_document(jwks_ab));
  EXPECT_TRUE(is_jwks_document(R"({"keys":[]})"));
  EXPECT_FALSE(is_jwks_document(""));
  EXPECT_FALSE(is_jwks_document("not json"));
  EXPECT_FALSE(is_jwks_document(R"({"nokeys":[]})"));
  EXPECT_FALSE(is_jwks_document(R"({"keys":"notarray"})"));
}

TEST(STSJwksCache, Keys)
{
  EXPECT_EQ((jwks_keys_map{{"a", {}}, {"b", {}}}), jwks_keys(jwks_ab));
  EXPECT_EQ((jwks_keys_map{{"a", {"ca", "root"}}, {"b", {"cb"}}, {"n", {}}}),
            jwks_keys(jwks_x5c));
  EXPECT_EQ((jwks_keys_map{{"only", {}}, {"", {}}}),
            jwks_keys(R"({"keys":[{"kty":"RSA"},"x",null,{"kid":"only"},{"kid":""}]})"));
  EXPECT_TRUE(jwks_keys("not json").empty());
  EXPECT_TRUE(jwks_keys(R"({"keys":"notarray"})").empty());
}

TEST(STSJwksCache, EmptyCacheMisses)
{
  JWKSCache cache;
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0()));
  EXPECT_FALSE(look(cache, "iss", unverified, no_kid, t0()));
  EXPECT_EQ(0u, cache.size());
}

TEST(STSJwksCache, KnownKidHitsUntilTtl)
{
  JWKSCache cache;
  cache.store(scoped("iss"), "aa", jwks_ab, t0());
  for (auto kid : {"a", "b"}) {
    EXPECT_EQ(jwks_ab, look(cache, "iss", registered, kid, t0()));
  }
  EXPECT_EQ(jwks_ab, look(cache, "iss", registered, "a", t0() + ttl - 1s));
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + ttl));
  EXPECT_FALSE(look(cache, "other", registered, "a", t0()));
}

TEST(STSJwksCache, UnknownOrMissingKidMisses)
{
  JWKSCache cache;
  cache.store(scoped("iss"), "aa", jwks_ab, t0());
  EXPECT_FALSE(look(cache, "iss", registered, "c", t0() + 1s));
  EXPECT_FALSE(look(cache, "iss", registered, "", t0() + 1s));
  EXPECT_FALSE(look(cache, "iss", registered, no_kid, t0() + 1s));
  EXPECT_FALSE(look(cache, "iss", unverified, no_kid, t0() + 1s));
  EXPECT_EQ(jwks_ab, look(cache, "iss", registered, "a", t0() + 2s));
  EXPECT_EQ(1u, cache.size());
}

TEST(STSJwksCache, StoreReplacesDocumentAndKids)
{
  JWKSCache cache;
  cache.store(scoped("iss"), "aa", jwks_ab, t0());
  cache.store(scoped("iss"), "aa", jwks_c, t0() + 41s);
  EXPECT_EQ(1u, cache.size());
  EXPECT_EQ(jwks_c, look(cache, "iss", registered, "c", t0() + 42s));
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + 42s));
  EXPECT_EQ(jwks_c, look(cache, "iss", registered, "c", t0() + 41s + ttl - 1s));
  EXPECT_FALSE(look(cache, "iss", registered, "c", t0() + 41s + ttl));
}

TEST(STSJwksCache, ExpiredEntryMissesRegardlessOfKid)
{
  JWKSCache cache;
  cache.store(scoped("iss"), "aa", jwks_ab, t0());
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + ttl));
  EXPECT_FALSE(look(cache, "iss", registered, "c", t0() + ttl));
  EXPECT_FALSE(look(cache, "iss", registered, no_kid, t0() + ttl));
  EXPECT_EQ(1u, cache.size());
}

TEST(STSJwksCache, IssuersAreIndependent)
{
  JWKSCache cache;
  cache.store(scoped("iss1"), "aa", jwks_a, t0());
  cache.store(scoped("iss2"), "bb", jwks_b, t0() + 100s);
  EXPECT_EQ(jwks_a, look(cache, "iss1", {"aa"}, "a", t0() + 1s));
  EXPECT_EQ(jwks_b, look(cache, "iss2", {"bb"}, "b", t0() + 101s));
  EXPECT_FALSE(look(cache, "iss1", {"aa"}, "b", t0() + 101s));
  EXPECT_FALSE(look(cache, "iss2", {"aa"}, "b", t0() + 101s));
  EXPECT_FALSE(look(cache, "iss1", {"aa"}, "a", t0() + ttl));
  EXPECT_EQ(jwks_b, look(cache, "iss2", {"bb"}, "b", t0() + ttl));
}

TEST(STSJwksCache, EndpointCertificateMustStayRegistered)
{
  JWKSCache cache;
  cache.store(scoped("iss"), "aa", jwks_ab, t0());
  EXPECT_EQ(jwks_ab, look(cache, "iss", {"bb", "AA"}, "a", t0() + 1s)); // case-insensitive
  EXPECT_FALSE(look(cache, "iss", {"bb"}, "a", t0() + 1s)); // no longer registered
  EXPECT_FALSE(look(cache, "iss", {"bb"}, "b", t0() + 1s));
  EXPECT_EQ(jwks_ab, look(cache, "iss", unverified, "a", t0() + 1s));
  // fetched without endpoint verification
  cache.store(scoped("iss2"), "", jwks_ab, t0());
  EXPECT_FALSE(look(cache, "iss2", registered, "a", t0() + 1s));
  EXPECT_EQ(jwks_ab, look(cache, "iss2", unverified, "a", t0() + 1s));
}

TEST(STSJwksCache, SigningCertificateMustStayRegistered)
{
  JWKSCache cache;
  cache.store(scoped("iss"), "", jwks_x5c, t0(), identity_digest);
  EXPECT_EQ(jwks_x5c, signing_lookup(cache, "iss", {"ca"}, "a", t0() + 1s));
  EXPECT_EQ(jwks_x5c, signing_lookup(cache, "iss", {"ROOT"}, "a", t0() + 1s));
  EXPECT_EQ(jwks_x5c, signing_lookup(cache, "iss", {"ca", "cb"}, "b", t0() + 1s));
  EXPECT_FALSE(signing_lookup(cache, "iss", {"ca"}, "b", t0() + 1s));
  EXPECT_FALSE(signing_lookup(cache, "iss", {"cb"}, "a", t0() + 1s));
  EXPECT_FALSE(signing_lookup(cache, "iss", unverified, "a", t0() + 1s));
  EXPECT_EQ(jwks_x5c, signing_lookup(cache, "iss", {"ca"}, "n", t0() + 1s));
  EXPECT_EQ(jwks_x5c, signing_lookup(cache, "iss", unverified, "n", t0() + 1s));
  cache.store(scoped("iss2"), "", jwks_x5c, t0());
  EXPECT_EQ(jwks_x5c, signing_lookup(cache, "iss2", {"other"}, "a", t0() + 1s));
}

TEST(STSJwksCache, SigningCertificateRenewalScenario)
{
  JWKSCache cache;
  const std::string before = R"({"keys":[{"kid":"a","x5c":["c1"]}]})";
  const std::string after = R"({"keys":[{"kid":"a","x5c":["c2"]}]})";
  cache.store(scoped("iss"), "", before, t0(), identity_digest);
  EXPECT_EQ(before, signing_lookup(cache, "iss", {"c1", "c2"}, "a", t0() + 10s));
  EXPECT_FALSE(signing_lookup(cache, "iss", {"c2"}, "a", t0() + 11s));
  cache.store(scoped("iss"), "", after, t0() + 11s, identity_digest);
  EXPECT_EQ(after, signing_lookup(cache, "iss", {"c2"}, "a", t0() + 12s));
}

// provider key rotation: publish [a, b], switch issuing to b, retire a
TEST(STSJwksCache, KeyRotationScenario)
{
  JWKSCache cache;
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0()));
  cache.store(scoped("iss"), "aa", jwks_a, t0());
  EXPECT_EQ(jwks_a, look(cache, "iss", registered, "a", t0() + 100s));
  // [a, b] published, still issuing with a
  EXPECT_EQ(jwks_a, look(cache, "iss", registered, "a", t0() + 150s));
  // first token with kid b: fetch right away
  EXPECT_FALSE(look(cache, "iss", registered, "b", t0() + 200s));
  cache.store(scoped("iss"), "aa", jwks_ab, t0() + 200s);
  EXPECT_EQ(jwks_ab, look(cache, "iss", registered, "b", t0() + 201s));
  EXPECT_EQ(jwks_ab, look(cache, "iss", registered, "a", t0() + 201s));
  // a retired: accepted until the cached copy expires
  EXPECT_EQ(jwks_ab, look(cache, "iss", registered, "a", t0() + 200s + ttl - 1s));
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + 200s + ttl));
  cache.store(scoped("iss"), "aa", jwks_b, t0() + 200s + ttl);
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + 201s + ttl));
  EXPECT_EQ(jwks_b, look(cache, "iss", registered, "b", t0() + 201s + ttl));
}

// endpoint certificate renewal with the new thumbprint registered ahead
TEST(STSJwksCache, EndpointCertificateRenewalScenario)
{
  JWKSCache cache;
  cache.store(scoped("iss"), "aa", jwks_a, t0());
  EXPECT_EQ(jwks_a, look(cache, "iss", {"aa", "bb"}, "a", t0() + 10s));
  // endpoint now presents bb; noticed at the next fetch
  EXPECT_FALSE(look(cache, "iss", {"aa", "bb"}, "a", t0() + ttl));
  cache.store(scoped("iss"), "bb", jwks_a, t0() + ttl);
  EXPECT_EQ(jwks_a, look(cache, "iss", {"aa", "bb"}, "a", t0() + ttl + 1s));
  // aa removed
  EXPECT_EQ(jwks_a, look(cache, "iss", {"bb"}, "a", t0() + ttl + 2s));
  cache.store(scoped("iss2"), "aa", jwks_a, t0());
  EXPECT_FALSE(look(cache, "iss2", {"bb"}, "a", t0() + 1s));
}

TEST(STSJwksCache, ClearDropsEveryIssuer)
{
  JWKSCache cache;
  cache.store(scoped("iss1"), "aa", jwks_a, t0());
  cache.store(scoped("iss2"), "aa", jwks_b, t0());
  cache.clear();
  EXPECT_EQ(0u, cache.size());
  EXPECT_FALSE(look(cache, "iss1", registered, "a", t0() + 1s));
  EXPECT_FALSE(look(cache, "iss2", registered, "b", t0() + 1s));
  cache.store(scoped("iss1"), "aa", jwks_a, t0() + 2s);
  EXPECT_EQ(jwks_a, look(cache, "iss1", registered, "a", t0() + 3s));
}

TEST(STSJwksCache, FullCacheEvictsOldestEntry)
{
  JWKSCache cache(2);
  cache.store(scoped("iss1"), "aa", jwks_a, t0());
  cache.store(scoped("iss2"), "aa", jwks_ab, t0() + 1s);
  cache.store(scoped("iss1"), "aa", jwks_c, t0() + 2s);
  EXPECT_EQ(2u, cache.size());
  EXPECT_EQ(jwks_ab, look(cache, "iss2", registered, "a", t0() + 2s));
  cache.store(scoped("iss3"), "aa", jwks_ab, t0() + 3s);
  EXPECT_EQ(2u, cache.size());
  EXPECT_FALSE(look(cache, "iss2", registered, "a", t0() + 4s));
  EXPECT_EQ(jwks_c, look(cache, "iss1", registered, "c", t0() + 4s));
  EXPECT_EQ(jwks_ab, look(cache, "iss3", registered, "a", t0() + 4s));
}

TEST(STSJwksCache, ZeroCapacityDoesNotStore)
{
  JWKSCache cache(0);
  cache.store(scoped("iss"), "aa", jwks_a, t0());
  EXPECT_EQ(0u, cache.size());
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0()));
}

TEST(STSJwksCache, SameIssuerIsIsolatedAcrossAccounts)
{
  JWKSCache cache;
  const auto a = scoped("same-issuer", "account-a", "account-a");
  const auto b = scoped("same-issuer", "account-b", "account-b");
  cache.store(a, "", jwks_ab, t0());
  EXPECT_EQ(jwks_ab, cache.lookup(a, {}, "a", ttl, t0() + 1s, false));
  EXPECT_FALSE(cache.lookup(b, {"bb"}, "a", ttl, t0() + 1s, true));
  cache.store(b, "bb", jwks_ab, t0() + 2s);
  EXPECT_EQ(2u, cache.size());
  EXPECT_EQ(jwks_ab, cache.lookup(b, {"bb"}, "a", ttl, t0() + 3s, true));
  EXPECT_FALSE(cache.lookup(b, {"cc"}, "a", ttl, t0() + 3s, true));
  EXPECT_EQ(jwks_ab, cache.lookup(a, {}, "a", ttl, t0() + 3s, false));
}

TEST(STSJwksCache, EndpointVerificationRequiresVerifiedCache)
{
  JWKSCache cache;
  const auto key = scoped("iss");
  cache.store(key, "", jwks_ab, t0());
  EXPECT_FALSE(cache.lookup(key, {"aa"}, "a", ttl, t0() + 1s, true));
  EXPECT_EQ(jwks_ab, cache.lookup(key, {"aa"}, "a", ttl, t0() + 1s, false));
  cache.store(key, "aa", jwks_ab, t0() + 2s);
  EXPECT_EQ(jwks_ab, cache.lookup(key, {"AA"}, "a", ttl, t0() + 3s, true));
  EXPECT_FALSE(cache.lookup(key, {"bb"}, "a", ttl, t0() + 3s, true));
}

TEST(STSJwksCache, ProviderNamespacesAreIndependent)
{
  JWKSCache cache;
  const auto local = scoped("iss", "account-a", "account-a");
  const auto global = scoped("iss", "account-a", ".global");
  const auto other = scoped("iss", "account-b", ".global");
  const auto legacy = scoped("iss", "", "");
  cache.store(global, "aa", jwks_a, t0());
  EXPECT_FALSE(cache.lookup(local, {"aa"}, "a", ttl, t0(), true));
  EXPECT_FALSE(cache.lookup(other, {"aa"}, "a", ttl, t0(), true));
  EXPECT_FALSE(cache.lookup(legacy, {"aa"}, "a", ttl, t0(), true));
  cache.store(local, "bb", jwks_b, t0());
  EXPECT_EQ(jwks_a, cache.lookup(global, {"aa"}, "a", ttl, t0(), true));
  EXPECT_EQ(jwks_b, cache.lookup(local, {"bb"}, "b", ttl, t0(), true));
}

TEST(STSJwksCache, SameIssuerSigningTrustIsAccountScoped)
{
  JWKSCache cache;
  const auto a = scoped("iss", "account-a", "account-a");
  const auto b = scoped("iss", "account-b", "account-b");
  cache.store(a, "", jwks_x5c, t0(), identity_digest);
  cache.store(b, "", jwks_x5c, t0(), identity_digest);
  EXPECT_EQ(jwks_x5c, cache.lookup(a, {"ca"}, "a", ttl, t0(), false));
  EXPECT_FALSE(cache.lookup(b, {"cb"}, "a", ttl, t0(), false));
  EXPECT_EQ(jwks_x5c, cache.lookup(b, {"cb"}, "b", ttl, t0(), false));
  EXPECT_FALSE(cache.lookup(a, {"other"}, "a", ttl, t0(), false));
  EXPECT_EQ(jwks_x5c, cache.lookup(b, {"cb"}, "b", ttl, t0(), false));
}
