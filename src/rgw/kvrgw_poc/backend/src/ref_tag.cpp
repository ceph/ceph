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
std::string RefTag::to_hex() const
{
  static constexpr char kHex[] = "0123456789abcdef";
  char buf[kRefTagSize * 2];
  for (size_t i = 0; i < kRefTagSize; ++i) {
    buf[i * 2]     = kHex[bytes_[i] >> 4];
    buf[i * 2 + 1] = kHex[bytes_[i] & 0xf];
  }
  return std::string(buf, sizeof(buf));
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
