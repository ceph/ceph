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

#include "ref_tag.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <cstring>
#include <endian.h>
#include <iomanip>
#include <sstream>

namespace kvrgw {

// --- RefTag ---
//--------------------------------------------------------------------------------
void RefTag::load(const uint8_t* src)
{
  std::memcpy(bytes_, src, kRefTagSize);
}

//--------------------------------------------------------------------------------
std::string_view RefTag::view() const
{
  return {reinterpret_cast<const char*>(bytes_), kRefTagSize};
}

//--------------------------------------------------------------------------------
void RefTag::to_hex(char* dst) const
{
  static constexpr char kHex[] = "0123456789abcdef";
  for (size_t i = 0; i < kRefTagSize; ++i) {
    dst[i * 2]     = kHex[bytes_[i] >> 4];
    dst[i * 2 + 1] = kHex[bytes_[i] & 0xf];
  }
}

//--------------------------------------------------------------------------------
std::string RefTag::to_hex() const
{
  char buf[kRefTagSize * 2];
  to_hex(buf);
  return std::string(buf, sizeof(buf));
}

//--------------------------------------------------------------------------------
bool RefTag::from_hex(std::string_view hex, RefTag* out)
{
  if (hex.size() != 2 * kRefTagSize) {
    return false;
  }
  uint8_t raw[kRefTagSize];
  for (size_t i = 0; i < kRefTagSize; ++i) {
    auto nibble = [](char c) -> int {
      if (c >= '0' && c <= '9') return c - '0';
      if (c >= 'a' && c <= 'f') return c - 'a' + 10;
      if (c >= 'A' && c <= 'F') return c - 'A' + 10;
      return -1;
    };
    const int hi = nibble(hex[2 * i]);
    const int lo = nibble(hex[2 * i + 1]);
    if (hi < 0 || lo < 0) {
      return false;
    }
    raw[i] = static_cast<uint8_t>((hi << 4) | lo);
  }
  out->load(raw);
  return true;
}

//--------------------------------------------------------------------------------
bool RefTag::operator==(const RefTag& o) const
{
  return std::memcmp(bytes_, o.bytes_, kRefTagSize) == 0;
}

//--------------------------------------------------------------------------------
bool RefTag::operator!=(const RefTag& o) const
{
  return !(*this == o);
}

// --- RefTagGenerator ---

static bool s_instance_created = false;

//--------------------------------------------------------------------------------
RefTagGenerator::RefTagGenerator(uint32_t rgw_id) : rgw_id_(rgw_id)
{
  assert(!s_instance_created && "RefTagGenerator must be a singleton");
  s_instance_created = true;
}

//--------------------------------------------------------------------------------
RefTag RefTagGenerator::next()
{
  RefTag ref_tag;
  const uint32_t rgw_net = htonl(rgw_id_);
  std::memcpy(ref_tag.bytes_, &rgw_net, sizeof(rgw_net));
  const uint64_t seq     = seq_id_.fetch_add(1, std::memory_order_relaxed);
  const uint64_t seq_net = htobe64(seq);
  std::memcpy(ref_tag.bytes_ + sizeof(rgw_net), &seq_net, sizeof(seq_net));
  return ref_tag;
}

} // namespace kvrgw
