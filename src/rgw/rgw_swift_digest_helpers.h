// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string_view>
#include <vector>
#include "rgw_swift_auth.h"
#include "common/ceph_crypto.h"

namespace rgw::auth::swift {

// Helper template to create digest factory maps.
// This map serves as the single source of truth for supported digest algorithms.
// Usage: make_digest_helper_map<BaseHelperType, HelperImplTemplate>()
template<typename HelperType, template<typename, SignatureFlavor> class HelperImpl>
auto make_digest_helper_map() {
  using Factory = std::function<std::unique_ptr<HelperType>()>;
  return std::map<std::string_view, Factory>{
    {"sha1", []() {
      return std::make_unique<HelperImpl<ceph::crypto::HMACSHA1,
                                         SignatureFlavor::NAMED_BASE64>>();
    }},
    {"sha256", []() {
      return std::make_unique<HelperImpl<ceph::crypto::HMACSHA256,
                                         SignatureFlavor::NAMED_BASE64>>();
    }},
    {"sha512", []() {
      return std::make_unique<HelperImpl<ceph::crypto::HMACSHA512,
                                         SignatureFlavor::NAMED_BASE64>>();
    }},
  };
}

// Get reference to the shared digest helper map for a specific helper type.
// Returns a static instance for signature verification.
template<typename HelperType, template<typename, SignatureFlavor> class HelperImpl>
const auto& get_digest_helper_map() {
  static const auto map = make_digest_helper_map<HelperType, HelperImpl>();
  return map;
}

// Extract just the algorithm names from a digest helper map.
// Use this for the /info API to avoid exposing private helper classes.
template<typename MapType>
std::vector<std::string_view> get_digest_algorithm_names(const MapType& map) {
  std::vector<std::string_view> names;
  names.reserve(map.size());
  for (const auto& [name, factory] : map) {
    names.push_back(name);
  }
  return names;
}

} // namespace rgw::auth::swift
