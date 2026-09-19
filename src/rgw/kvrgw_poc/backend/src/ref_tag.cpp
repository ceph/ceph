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

RefTagGenerator::RefTagGenerator(uint32_t rgw_id) : rgw_id_(rgw_id) {}

RefTag RefTagGenerator::next()
{
  std::lock_guard lock(mu_);
  RefTag ref_tag{};

  const uint32_t rgw_net = htonl(rgw_id_);
  std::memcpy(ref_tag.data(), &rgw_net, sizeof(rgw_net));

  const uint64_t seq = seq_id_++;
  const uint64_t seq_net = htobe64(seq);
  std::memcpy(ref_tag.data() + 4, &seq_net, sizeof(seq_net));
  return ref_tag;
}

std::string RefTagGenerator::to_hex(std::string_view ref_tag)
{
  assert(ref_tag.size() == 12);
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (unsigned char byte : ref_tag) {
    out << std::setw(2) << static_cast<int>(byte);
  }
  return out.str();
}

std::string RefTagGenerator::from_hex(std::string_view ref_tag_hex)
{
  assert(ref_tag_hex.size() == 24);
  std::string out;
  out.reserve(12);
  for (size_t i = 0; i < 24; i += 2) {
    const char hi = ref_tag_hex[i];
    const char lo = ref_tag_hex[i + 1];
    auto nibble = [](char c) -> int {
      if (c >= '0' && c <= '9') {
        return c - '0';
      }
      if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
      }
      if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
      }
      return -1;
    };
    const int h = nibble(hi);
    const int l = nibble(lo);
    assert(h >= 0 && l >= 0);
    out.push_back(static_cast<char>((h << 4) | l));
  }
  return out;
}

std::string RefTagGenerator::filename_for(std::string_view ref_tag)
{
  assert(ref_tag.size() == 12 || ref_tag.size() == 24);
  if (ref_tag.size() == 12) {
    return to_hex(ref_tag);
  }
  return std::string(ref_tag);
}

bool RefTagGenerator::equal(std::string_view a, std::string_view b)
{
  if (a.size() == b.size()) {
    return a == b;
  }
  if (a.size() == 24 && b.size() == 12) {
    return from_hex(a) == b;
  }
  if (a.size() == 12 && b.size() == 24) {
    return a == from_hex(b);
  }
  return false;
}

} // namespace kvrgw
