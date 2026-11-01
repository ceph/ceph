// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
#pragma once

#include <chrono>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <boost/algorithm/string/predicate.hpp>
#include "common/ceph_json.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"

namespace rgw::auth::sts::detail {

inline bool is_jwks_document(const std::string& jwks)
{
  JSONParser parser;
  if (!parser.parse(jwks.c_str(), jwks.size())) {
    return false;
  }
  JSONObj* keys = parser.find_obj("keys");
  return keys && keys->is_array();
}

inline std::set<std::string> jwks_key_ids(const std::string& jwks)
{
  std::set<std::string> kids;
  JSONParser parser;
  if (!parser.parse(jwks.c_str(), jwks.size())) {
    return kids;
  }
  JSONObj* keys = parser.find_obj("keys");
  if (!keys || !keys->is_array()) {
    return kids;
  }
  for (auto key = keys->find_first(); !key.end(); ++key) {
    std::string kid;
    if ((*key)->is_object() && JSONDecoder::decode_json("kid", kid, *key)) {
      kids.insert(std::move(kid));
    }
  }
  return kids;
}

// Per-issuer cache of OIDC provider JWKS documents. Time is passed in by
// the caller so the policy can be tested without a clock.
class JWKSCache {
 public:
  using clock = ceph::coarse_mono_clock;
  using time_point = clock::time_point;

  explicit JWKSCache(size_t max_entries = 1024) : max_entries(max_entries) {}

  // the cached document, if it is within ttl, its endpoint certificate is
  // still registered (when thumbprints are given) and token_kid is in it.
  // Otherwise the caller fetches as it always did; an unknown or missing kid
  // may mean the provider rotated keys (OpenID Connect Core 10.1.1).
  std::optional<std::string> lookup(const std::string& iss,
                                    const std::vector<std::string>& thumbprints,
                                    const std::optional<std::string>& token_kid,
                                    std::chrono::seconds ttl,
                                    time_point now) const
  {
    std::lock_guard l{lock};
    auto it = entries.find(iss);
    if (it == entries.end() || now - it->second.stored_at >= ttl) {
      return std::nullopt;
    }
    const auto& entry = it->second;
    if (!thumbprints.empty() &&
        !is_registered(entry.cert_thumbprint, thumbprints)) {
      return std::nullopt;
    }
    if (!token_kid || entry.kids.count(*token_kid) == 0) {
      return std::nullopt;
    }
    return entry.jwks;
  }

  // cert_thumbprint: the endpoint certificate seen at fetch time, empty if
  // the endpoint was not verified
  void store(const std::string& iss,
             std::string cert_thumbprint,
             std::string jwks,
             time_point now)
  {
    auto kids = jwks_key_ids(jwks);
    std::lock_guard l{lock};
    if (entries.size() >= max_entries && entries.find(iss) == entries.end()) {
      // issuers are registered providers, this is not expected in practice
      entries.clear();
    }
    entries[iss] = Entry{std::move(jwks), std::move(kids),
                         std::move(cert_thumbprint), now};
  }

  size_t size() const
  {
    std::lock_guard l{lock};
    return entries.size();
  }

 private:
  static bool is_registered(const std::string& cert_thumbprint,
                            const std::vector<std::string>& thumbprints)
  {
    if (cert_thumbprint.empty()) {
      return false;
    }
    for (const auto& t : thumbprints) {
      if (boost::iequals(t, cert_thumbprint)) {
        return true;
      }
    }
    return false;
  }

  struct Entry {
    std::string jwks;
    std::set<std::string> kids;
    std::string cert_thumbprint;
    time_point stored_at;
  };

  const size_t max_entries;
  mutable ceph::mutex lock = ceph::make_mutex("rgw::auth::sts::JWKSCache");
  std::map<std::string, Entry> entries;
};

} // namespace rgw::auth::sts::detail
