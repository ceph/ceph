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

#include <endian.h>
#include <iomanip>
#include <sstream>

namespace kvrgw {

std::string etag_t::to_hex() const
{
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (int i = 0; i < 16; ++i) {
    out << std::setw(2) << static_cast<int>(bytes_[i]);
  }
  if (part_count_ > 0) {
    out << '-' << std::dec << part_count_;
  }
  return out.str();
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

bool version_id_t::is_null() const { return val_ == 0xFFFFFFFF; }

void version_id_t::serialize(uint8_t *out) const
{
  uint32_t be = htobe32(val_);
  std::memcpy(out, &be, 4);
}

version_id_t version_id_t::deserialize(const uint8_t *src)
{
  uint32_t be;
  std::memcpy(&be, src, 4);
  return version_id_t{be32toh(be)};
}

std::string version_id_t::to_hex() const
{
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(8) << val_;
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
