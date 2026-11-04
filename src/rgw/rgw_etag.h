// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <algorithm>
#include <cctype>
#include <string_view>

#include "common/ceph_crypto.h"

namespace rgw {

/// whether etag has the form of a multipart upload's: an MD5 in hex, a
/// dash and a count. An OSD-direct PUT stores one of that form too, the
/// MD5 of its stripes' CRCs, which is not the MD5 of the data
inline bool is_multipart_etag(std::string_view etag)
{
  // as an attr holds it, with a trailing NUL, or quoted
  while (!etag.empty() && (etag.back() == '\0' || etag.back() == '"')) {
    etag.remove_suffix(1);
  }
  while (!etag.empty() && etag.front() == '"') {
    etag.remove_prefix(1);
  }
  constexpr size_t md5_hex = CEPH_CRYPTO_MD5_DIGESTSIZE * 2;
  if (etag.size() < md5_hex + 2 || etag[md5_hex] != '-') {
    return false;
  }
  const auto hex = [] (char c) { return std::isxdigit((unsigned char)c); };
  const auto digit = [] (char c) { return std::isdigit((unsigned char)c); };
  return std::all_of(etag.begin(), etag.begin() + md5_hex, hex) &&
         std::all_of(etag.begin() + md5_hex + 1, etag.end(), digit);
}

} // namespace rgw
