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

#include "typed_ids.hpp"

#include "error_codes.hpp"

#include <cstdio>
#include <endian.h>
#include <iomanip>
#include <sstream>

namespace kvrgw {
//--------------------------------------------------------------------------------
KvrgwErrorCode PutCondition::encode(std::string_view if_match,
                                    std::string_view if_none_match)
{
  // Reject dual non-wildcard ETags — no legitimate S3 client sends both
  // If-Match and If-None-Match with distinct ETag values in one request.
  const bool match_is_etag     = !if_match.empty()      && if_match     != "*";
  const bool nonematch_is_etag = !if_none_match.empty() && if_none_match != "*";
  if (match_is_etag && nonematch_is_etag) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }

  if (!if_match.empty()) {
    if (if_match == "*") {
      flags.set_etag_star();
    }
    else {
      // Unparseable ETag → sentinel (all 0xFF) → guaranteed miss → 412
      ETag::from_hex(if_match, &etag);
      flags.set_if_match();
    }
  }

  if (!if_none_match.empty()) {
    if (if_none_match == "*") {
      flags.set_if_none_match_star();
    }
    else {
      // Unparseable ETag → sentinel (all 0xFF) → guaranteed miss → 412
      ETag::from_hex(if_none_match, &etag);
      flags.set_if_none_match();
    }
  }

  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode GetCondition::encode(std::string_view if_match, uint32_t mtime_val,
                                    uint64_t size_val, bool has_size)
{
  auto ec = PutCondition::encode(if_match, {});
  if (ec != KVRGW_ERR_OK) {
    return ec;
  }

  if (mtime_val != 0) {
    mtime = mtime_val;
    flags.set_if_modified_since();
  }

  if (has_size) {
    size = size_val;
    flags.set_has_size();
  }

  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
std::string bucket_id_t::to_hex() const
{
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(2 * sizeof(val_)) << val_;
  return out.str();
}

//--------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const bucket_id_t& v)
{
  os << v.val_;
  return os;
}

//--------------------------------------------------------------------------------
bool version_id_t::is_null() const
{
  return *this == kNullVersion;
}

//--------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const version_id_t& v)
{
  os << v.val_;
  return os;
}

//--------------------------------------------------------------------------------
version_id_t version_id_t::next_vid() const
{
  if (this->is_valid()) {
    return version_id_t(this->val_ - 1);
  }
  else {
    return kNullVersion;
  }
}

//--------------------------------------------------------------------------------
version_id_t version_id_t::prev_vid() const
{
  if (!this->is_null()) {
    return version_id_t(this->val_ + 1);
  }
  else {
    return kNullVersion;
  }
}

//--------------------------------------------------------------------------------
version_id_t version_id_t::to_be() const
{
  return version_id_t(htobe32(val_));
}

//--------------------------------------------------------------------------------
version_id_t version_id_t::from_be() const
{
  return version_id_t(be32toh(val_));
}

//--------------------------------------------------------------------------------
void version_id_t::serialize(char *out) const
{
  uint32_t be = htobe32(val_);
  std::memcpy(out, &be, sizeof(be));
}

//--------------------------------------------------------------------------------
version_id_t version_id_t::deserialize(const char *src)
{
  uint32_t be;
  std::memcpy(&be, src, sizeof(be));
  return version_id_t{be32toh(be)};
}

//--------------------------------------------------------------------------------
void bucket_id_t::serialize(void* out) const
{
  uint64_t be = htobe64(val_);
  std::memcpy(out, &be, sizeof(be));
}

//--------------------------------------------------------------------------------
bucket_id_t bucket_id_t::deserialize(const void* src)
{
  uint64_t be;
  std::memcpy(&be, src, sizeof(be));
  return bucket_id_t{be64toh(be)};
}

//--------------------------------------------------------------------------------
version_id_t version_id_t::generate_random_version_id(uint32_t rand_val,
                                                      uint32_t num_versions)
{
  if (num_versions == 0) {
    return {};
  }
  return version_id_t(kFirstVersionId.raw() - (rand_val % num_versions));
}

//--------------------------------------------------------------------------------
std::string version_id_t::to_hex() const
{
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(2 * sizeof(val_)) << val_;
  return out.str();
}

//--------------------------------------------------------------------------------
version_id_t version_id_t::from_hex(std::string_view hex)
{
  uint32_t val = 0;
  for (char c : hex) {
    val <<= 4;
    if (c >= '0' && c <= '9') {
      val |= (c - '0');
    }
    else if (c >= 'a' && c <= 'f') {
      val |= (c - 'a' + 10);
    }
    else if (c >= 'A' && c <= 'F') {
      val |= (c - 'A' + 10);
    }
  }
  return version_id_t{val};
}

} // namespace kvrgw
