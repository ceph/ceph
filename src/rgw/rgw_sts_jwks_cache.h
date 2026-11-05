// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
#pragma once

#include <algorithm>
#include <chrono>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <tuple>
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

using jwks_keys_map = std::map<std::string, std::vector<std::string>>;

inline jwks_keys_map jwks_keys(const std::string& jwks)
{
  jwks_keys_map result;
  JSONParser parser;
  if (!parser.parse(jwks.c_str(), jwks.size())) {
    return result;
  }
  JSONObj* keys = parser.find_obj("keys");
  if (!keys || !keys->is_array()) {
    return result;
  }
  for (auto key = keys->find_first(); !key.end(); ++key) {
    std::string kid;
    if ((*key)->is_object() && JSONDecoder::decode_json("kid", kid, *key)) {
      auto& x5c = result[std::move(kid)];
      JSONDecoder::decode_json("x5c", x5c, *key);
    }
  }
  return result;
}

using cert_digest_fn = std::function<std::string(const std::string&)>;

// Per-issuer cache of OIDC provider JWKS documents. Time is passed in by
// the caller so the policy can be tested without a clock.
class JWKSCache {
 public:
  struct Key {
    std::string account;
    std::string provider_namespace;
    std::string issuer;

    bool operator<(const Key& other) const
    {
      return std::tie(account, provider_namespace, issuer) <
             std::tie(other.account, other.provider_namespace, other.issuer);
    }
  };

  using clock = ceph::coarse_mono_clock;
  using time_point = clock::time_point;

  explicit JWKSCache(size_t max_entries = 1024) : max_entries(max_entries) {}

  // Reuse only while the endpoint certificate and the token key's stored
  // x5c thumbprints are still registered, and the entry is within TTL.
  std::optional<std::string> lookup(const Key& cache_key,
                                    const std::vector<std::string>& thumbprints,
                                    const std::optional<std::string>& token_kid,
                                    std::chrono::seconds ttl,
                                    time_point now,
                                    bool verify_endpoint) const
  {
    std::lock_guard l{lock};
    auto it = entries.find(cache_key);
    if (it == entries.end() || now - it->second.stored_at >= ttl) {
      return std::nullopt;
    }
    const auto& entry = it->second;
    if (verify_endpoint &&
        (entry.cert_thumbprint.empty() ||
         !is_registered(entry.cert_thumbprint, thumbprints))) {
      return std::nullopt;
    }
    if (!token_kid) {
      return std::nullopt;
    }
    auto key = entry.keys.find(*token_kid);
    if (key == entry.keys.end()) {
      return std::nullopt;
    }
    if (!key->second.empty() && !any_registered(key->second, thumbprints)) {
      return std::nullopt;
    }
    return entry.jwks;
  }

  // cert_thumbprint: the endpoint certificate seen at fetch time, empty if
  // the endpoint was not verified. digest computes each x5c thumbprint.
  void store(const Key& cache_key,
             std::string cert_thumbprint,
             std::string jwks,
             time_point now,
             const cert_digest_fn& digest = {})
  {
    if (max_entries == 0) {
      return;
    }
    jwks_keys_map keys;
    for (auto& [kid, x5c] : jwks_keys(jwks)) {
      auto& thumbprints = keys[kid];
      if (!digest) {
        continue;
      }
      for (const auto& cert : x5c) {
        if (auto t = digest(cert); !t.empty()) {
          thumbprints.push_back(std::move(t));
        }
      }
    }
    std::lock_guard l{lock};
    if (entries.size() >= max_entries && entries.find(cache_key) == entries.end()) {
      auto oldest = std::min_element(entries.begin(), entries.end(),
          [](const auto& a, const auto& b) {
            return a.second.stored_at < b.second.stored_at;
          });
      entries.erase(oldest);
    }
    entries[cache_key] = Entry{std::move(jwks), std::move(keys),
                         std::move(cert_thumbprint), now};
  }

  void clear()
  {
    std::lock_guard l{lock};
    entries.clear();
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
    for (const auto& t : thumbprints) {
      if (boost::iequals(t, cert_thumbprint)) {
        return true;
      }
    }
    return false;
  }

  static bool any_registered(const std::vector<std::string>& cert_thumbprints,
                             const std::vector<std::string>& thumbprints)
  {
    for (const auto& c : cert_thumbprints) {
      if (is_registered(c, thumbprints)) {
        return true;
      }
    }
    return false;
  }

  struct Entry {
    std::string jwks;
    jwks_keys_map keys;
    std::string cert_thumbprint;
    time_point stored_at;
  };

  const size_t max_entries;
  mutable ceph::mutex lock = ceph::make_mutex("rgw::auth::sts::JWKSCache");
  std::map<Key, Entry> entries;
};

} // namespace rgw::auth::sts::detail
