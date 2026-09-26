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

#include <cstdio>
#include <endian.h>
#include <iomanip>
#include <sstream>

namespace kvrgw {

std::string etag_t::to_hex(const uint8_t* bytes, uint16_t part_count)
{
  static constexpr char kHex[] = "0123456789abcdef";
  // 32 hex digits + optional "-65535" (6 chars) + NUL
  char buf[40];
  char *p = buf;
  for (int i = 0; i < 16; ++i) {
    *p++ = kHex[bytes[i] >> 4];
    *p++ = kHex[bytes[i] & 0xf];
  }
  if (part_count > 0) {
    p += std::snprintf(p, buf + sizeof(buf) - p, "-%u",
                       static_cast<unsigned>(part_count));
  }
  return std::string(buf, static_cast<size_t>(p - buf));
}

std::string etag_t::to_hex() const
{
  return to_hex(bytes_, part_count_);
}

etag_t etag_t::from_hex(std::string_view hex)
{
  etag_t result{};
  auto dash = hex.find('-');
  std::string_view hex_part =
      (dash != std::string_view::npos) ? hex.substr(0, dash) : hex;

  if (hex_part.size() >= 32) {
    auto nibble = [](char c) -> uint8_t {
      if (c >= '0' && c <= '9') {
        return c - '0';
      }
      if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
      }
      if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
      }
      return 0;
    };
    for (int i = 0; i < 16; ++i) {
      result.bytes_[i] =
          (nibble(hex_part[i * 2]) << 4) | nibble(hex_part[i * 2 + 1]);
    }
  }

  if (dash != std::string_view::npos && dash + 1 < hex.size()) {
    uint16_t pc = 0;
    for (size_t i = dash + 1; i < hex.size(); ++i) {
      if (hex[i] >= '0' && hex[i] <= '9') {
        pc = pc * 10 + (hex[i] - '0');
      }
    }
    result.part_count_ = pc;
  }
  return result;
}

std::string bucket_id_t::to_hex() const
{
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(2 * sizeof(val_)) << val_;
  return out.str();
}

std::ostream& operator<<(std::ostream& os, const bucket_id_t& v)
{
  os << v.val_;
  return os;
}

bool version_id_t::is_null() const
{
  return *this == kNullVersion;
}

std::ostream& operator<<(std::ostream& os, const version_id_t& v)
{
  os << v.val_;
  return os;
}

version_id_t version_id_t::next_vid() const
{
  if (this->is_valid()) {
    return version_id_t(this->val_ - 1);
  }
  else {
    return kNullVersion;
  }
}

version_id_t version_id_t::prev_vid() const
{
  if (!this->is_null()) {
    return version_id_t(this->val_ + 1);
  }
  else {
    return kNullVersion;
  }
}

version_id_t version_id_t::to_be() const
{
  return version_id_t(htobe32(val_));
}

version_id_t version_id_t::from_be() const
{
  return version_id_t(be32toh(val_));
}

void version_id_t::serialize(char *out) const
{
  uint32_t be = htobe32(val_);
  std::memcpy(out, &be, sizeof(be));
}

version_id_t version_id_t::deserialize(const char *src)
{
  uint32_t be;
  std::memcpy(&be, src, sizeof(be));
  return version_id_t{be32toh(be)};
}

void bucket_id_t::serialize(void* out) const
{
  uint64_t be = htobe64(val_);
  std::memcpy(out, &be, sizeof(be));
}

bucket_id_t bucket_id_t::deserialize(const void* src)
{
  uint64_t be;
  std::memcpy(&be, src, sizeof(be));
  return bucket_id_t{be64toh(be)};
}

version_id_t version_id_t::generate_random_version_id(uint32_t rand_val,
                                                      uint32_t num_versions)
{
  if (num_versions == 0) {
    return {};
  }
  return version_id_t(kFirstVersionId.raw() - (rand_val % num_versions));
}

std::string version_id_t::to_hex() const
{
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(2 * sizeof(val_)) << val_;
  return out.str();
}

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
