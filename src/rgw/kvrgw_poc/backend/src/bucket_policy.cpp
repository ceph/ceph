// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "bucket_policy.hpp"

#include <nlohmann/json.hpp>

namespace kvrgw {

namespace {

uint8_t flag_for_action(const std::string &action)
{
  if (action == "s3:GetObject" || action == "s3:HeadObject") {
    return kDenyRead;
  }
  if (action == "s3:PutObject" || action == "s3:DeleteObject") {
    return kDenyWrite;
  }
  if (action == "s3:ListBucket") {
    return kDenyList;
  }
  if (action == "s3:DeleteBucket") {
    return kDenyDeleteBucket;
  }
  if (action == "s3:*") {
    return kDenyRead | kDenyWrite | kDenyList | kDenyDeleteBucket;
  }
  return 0;
}

bool is_wildcard_principal(const nlohmann::json &principal)
{
  if (principal.is_string() && principal.get<std::string>() == "*") {
    return true;
  }
  if (principal.is_object()) {
    auto it = principal.find("AWS");
    if (it != principal.end()) {
      if (it->is_string() && it->get<std::string>() == "*") {
        return true;
      }
      if (it->is_array()) {
        for (const auto &p : *it) {
          if (p.is_string() && p.get<std::string>() == "*") {
            return true;
          }
        }
      }
    }
  }
  return false;
}

} // namespace

uint8_t parse_policy_flags(std::string_view policy_json)
{
  if (policy_json.empty()) {
    return 0;
  }

  nlohmann::json doc;
  try {
    doc = nlohmann::json::parse(policy_json);
  }
  catch (...) {
    return 0;
  }

  auto stmt_it = doc.find("Statement");
  if (stmt_it == doc.end() || !stmt_it->is_array()) {
    return 0;
  }

  uint8_t flags = 0;
  for (const auto &stmt : *stmt_it) {
    auto effect_it = stmt.find("Effect");
    if (effect_it == stmt.end() || effect_it->get<std::string>() != "Deny") {
      continue;
    }

    auto principal_it = stmt.find("Principal");
    if (principal_it == stmt.end() || !is_wildcard_principal(*principal_it)) {
      continue;
    }

    auto action_it = stmt.find("Action");
    if (action_it == stmt.end()) {
      continue;
    }

    if (action_it->is_string()) {
      flags |= flag_for_action(action_it->get<std::string>());
    }
    else if (action_it->is_array()) {
      for (const auto &a : *action_it) {
        if (a.is_string()) {
          flags |= flag_for_action(a.get<std::string>());
        }
      }
    }
  }
  return flags;
}

} // namespace kvrgw
