// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
#pragma once

#include <optional>
#include <string>
#include <vector>
#include "common/ceph_json.h"

namespace rgw::auth::sts::detail {

// Returned keys are owned by the parsed JWKS and must not outlive it.
// Keep the all-key fallback when the token has no kid or no key matches.
inline std::vector<JSONObj*> select_jwks_keys(
    JSONObj& keys, const std::optional<std::string>& token_kid)
{
  std::vector<JSONObj*> all_keys;
  std::vector<JSONObj*> matching_keys;
  for (auto key = keys.find_first(); !key.end(); ++key) {
    all_keys.push_back(*key);
    if (!token_kid) {
      continue;
    }
    std::string kid, use;
    if (!JSONDecoder::decode_json("kid", kid, *key) || kid != *token_kid) {
      continue;
    }
    if (JSONDecoder::decode_json("use", use, *key) && use != "sig") {
      continue;
    }
    matching_keys.push_back(*key);
  }
  if (!matching_keys.empty()) {
    return matching_keys;
  }
  return all_keys;
}

} // namespace rgw::auth::sts::detail
