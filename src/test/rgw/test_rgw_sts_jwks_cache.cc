// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
#include <gtest/gtest.h>
#include <chrono>
#include <set>
#include <string>
#include <vector>
#include "rgw_sts_jwks_cache.h"

using rgw::auth::sts::detail::JWKSCache;
using rgw::auth::sts::detail::is_jwks_document;
using rgw::auth::sts::detail::jwks_key_ids;
using namespace std::chrono_literals;

namespace {

// thumbprints as passed by a caller that verifies the JWKS endpoint (or not)
const std::vector<std::string> registered{"aa"};
const std::vector<std::string> unverified{};
const std::string jwks_a = R"({"keys":[{"kid":"a","kty":"RSA"}]})";
const std::string jwks_b = R"({"keys":[{"kid":"b","kty":"RSA"}]})";
const std::string jwks_ab = R"({"keys":[{"kid":"a","kty":"RSA"},{"kid":"b","kty":"RSA"}]})";
const std::string jwks_c = R"({"keys":[{"kid":"c","kty":"RSA"}]})";
constexpr auto ttl = 300s;
const std::optional<std::string> no_kid;

std::optional<std::string> look(JWKSCache& cache, const std::string& iss,
                                const std::vector<std::string>& thumbprints,
                                std::optional<std::string> kid,
                                JWKSCache::time_point now)
{
  return cache.lookup(iss, thumbprints, kid, ttl, now);
}

JWKSCache::time_point t0()
{
  return JWKSCache::time_point{} + 1000s;
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

TEST(STSJwksCache, KeyIds)
{
  EXPECT_EQ((std::set<std::string>{"a", "b"}), jwks_key_ids(jwks_ab));
  EXPECT_EQ((std::set<std::string>{"only", ""}),
            jwks_key_ids(R"({"keys":[{"kty":"RSA"},"x",null,{"kid":"only"},{"kid":""}]})"));
  EXPECT_TRUE(jwks_key_ids("not json").empty());
  EXPECT_TRUE(jwks_key_ids(R"({"keys":"notarray"})").empty());
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
  cache.store("iss", "aa", jwks_ab, t0());
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
  cache.store("iss", "aa", jwks_ab, t0());
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
  cache.store("iss", "aa", jwks_ab, t0());
  cache.store("iss", "aa", jwks_c, t0() + 41s);
  EXPECT_EQ(1u, cache.size());
  EXPECT_EQ(jwks_c, look(cache, "iss", registered, "c", t0() + 42s));
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + 42s));
  EXPECT_EQ(jwks_c, look(cache, "iss", registered, "c", t0() + 41s + ttl - 1s));
  EXPECT_FALSE(look(cache, "iss", registered, "c", t0() + 41s + ttl));
}

TEST(STSJwksCache, ExpiredEntryMissesRegardlessOfKid)
{
  JWKSCache cache;
  cache.store("iss", "aa", jwks_ab, t0());
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + ttl));
  EXPECT_FALSE(look(cache, "iss", registered, "c", t0() + ttl));
  EXPECT_FALSE(look(cache, "iss", registered, no_kid, t0() + ttl));
  EXPECT_EQ(1u, cache.size());
}

TEST(STSJwksCache, IssuersAreIndependent)
{
  JWKSCache cache;
  cache.store("iss1", "aa", jwks_a, t0());
  cache.store("iss2", "bb", jwks_b, t0() + 100s);
  EXPECT_EQ(jwks_a, look(cache, "iss1", {"aa"}, "a", t0() + 1s));
  EXPECT_EQ(jwks_b, look(cache, "iss2", {"bb"}, "b", t0() + 101s));
  EXPECT_FALSE(look(cache, "iss1", {"aa"}, "b", t0() + 101s));
  EXPECT_FALSE(look(cache, "iss2", {"aa"}, "b", t0() + 101s));
  EXPECT_FALSE(look(cache, "iss1", {"aa"}, "a", t0() + ttl));
  EXPECT_EQ(jwks_b, look(cache, "iss2", {"bb"}, "b", t0() + ttl));
}

TEST(STSJwksCache, ThumbprintCheckedOnlyWhenVerifying)
{
  JWKSCache cache;
  cache.store("iss", "aa", jwks_ab, t0());
  EXPECT_EQ(jwks_ab, look(cache, "iss", {"bb", "AA"}, "a", t0() + 1s)); // case-insensitive
  EXPECT_FALSE(look(cache, "iss", {"bb"}, "a", t0() + 1s)); // no longer registered
  EXPECT_FALSE(look(cache, "iss", {"bb"}, "b", t0() + 1s));
  EXPECT_EQ(jwks_ab, look(cache, "iss", unverified, "a", t0() + 1s));
  // fetched without endpoint verification
  cache.store("iss2", "", jwks_ab, t0());
  EXPECT_FALSE(look(cache, "iss2", registered, "a", t0() + 1s));
  EXPECT_EQ(jwks_ab, look(cache, "iss2", unverified, "a", t0() + 1s));
}

// provider key rotation: publish [a, b], switch issuing to b, retire a
TEST(STSJwksCache, KeyRotationScenario)
{
  JWKSCache cache;
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0()));
  cache.store("iss", "aa", jwks_a, t0());
  EXPECT_EQ(jwks_a, look(cache, "iss", registered, "a", t0() + 100s));
  // [a, b] published, still issuing with a
  EXPECT_EQ(jwks_a, look(cache, "iss", registered, "a", t0() + 150s));
  // first token with kid b: fetch right away
  EXPECT_FALSE(look(cache, "iss", registered, "b", t0() + 200s));
  cache.store("iss", "aa", jwks_ab, t0() + 200s);
  EXPECT_EQ(jwks_ab, look(cache, "iss", registered, "b", t0() + 201s));
  EXPECT_EQ(jwks_ab, look(cache, "iss", registered, "a", t0() + 201s));
  // a retired: accepted until the cached copy expires
  EXPECT_EQ(jwks_ab, look(cache, "iss", registered, "a", t0() + 200s + ttl - 1s));
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + 200s + ttl));
  cache.store("iss", "aa", jwks_b, t0() + 200s + ttl);
  EXPECT_FALSE(look(cache, "iss", registered, "a", t0() + 201s + ttl));
  EXPECT_EQ(jwks_b, look(cache, "iss", registered, "b", t0() + 201s + ttl));
}

// endpoint certificate renewal with the new thumbprint registered ahead
TEST(STSJwksCache, EndpointCertificateRenewalScenario)
{
  JWKSCache cache;
  cache.store("iss", "aa", jwks_a, t0());
  EXPECT_EQ(jwks_a, look(cache, "iss", {"aa", "bb"}, "a", t0() + 10s));
  // endpoint now presents bb; noticed at the next fetch
  EXPECT_FALSE(look(cache, "iss", {"aa", "bb"}, "a", t0() + ttl));
  cache.store("iss", "bb", jwks_a, t0() + ttl);
  EXPECT_EQ(jwks_a, look(cache, "iss", {"aa", "bb"}, "a", t0() + ttl + 1s));
  // aa removed
  EXPECT_EQ(jwks_a, look(cache, "iss", {"bb"}, "a", t0() + ttl + 2s));
  cache.store("iss2", "aa", jwks_a, t0());
  EXPECT_FALSE(look(cache, "iss2", {"bb"}, "a", t0() + 1s));
}

TEST(STSJwksCache, FullCacheIsClearedForNewIssuer)
{
  JWKSCache cache(2);
  cache.store("iss1", "aa", jwks_a, t0());
  cache.store("iss2", "aa", jwks_ab, t0());
  cache.store("iss1", "aa", jwks_c, t0() + 1s); // update does not evict
  EXPECT_EQ(2u, cache.size());
  cache.store("iss3", "aa", jwks_ab, t0() + 2s); // new issuer on a full cache
  EXPECT_EQ(1u, cache.size());
  EXPECT_FALSE(look(cache, "iss1", registered, "c", t0() + 3s));
  EXPECT_EQ(jwks_ab, look(cache, "iss3", registered, "a", t0() + 3s));
}
