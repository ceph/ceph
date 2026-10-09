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

#include "etag.hpp"

#include <cstdio>
#include <endian.h>

namespace kvrgw {

namespace {

constexpr char kHex[] = "0123456789abcdef";

// Returns the nibble value of a valid hex character, or 0xFF on invalid input.
//--------------------------------------------------------------------------------
inline uint8_t nibble(char c)
{
  if (c >= '0' && c <= '9') {
    return static_cast<uint8_t>(c - '0');
  }
  if (c >= 'a' && c <= 'f') {
    return static_cast<uint8_t>(c - 'a' + 10);
  }
  if (c >= 'A' && c <= 'F') {
    return static_cast<uint8_t>(c - 'A' + 10);
  }
  // report invalid input
  return 0xFF;
}

} // namespace

static constexpr uint32_t kMaxPartCount = 10000;

//--------------------------------------------------------------------------------
inline void write_part_count_be(uint8_t* bytes, uint16_t part_count)
{
  const uint16_t be = htobe16(part_count);
  std::memcpy(bytes + kMd5Bytes, &be, sizeof(be));
}

//--------------------------------------------------------------------------------
// Returns true if pc is a valid multipart part count: [1, kMaxPartCount].
// Zero is not a valid multipart part count (it means single-part).
static inline bool legal_part_count(uint32_t pc)
{
  return (pc > 0 && pc <= kMaxPartCount);
}

//--------------------------------------------------------------------------------
void ETag::load_raw(const uint8_t* digest, uint16_t part_count)
{
  std::memcpy(bytes_, digest, kMd5Bytes);
  write_part_count_be(bytes_, part_count);
}

//--------------------------------------------------------------------------------
bool ETag::load(const uint8_t* src)
{
  std::memcpy(bytes_, src, kSize);
  // single-part (pc==0) is always valid; multipart must be in range
  const uint16_t pc = this->part_count();
  return (pc == 0 || legal_part_count(pc));
}

//--------------------------------------------------------------------------------
void ETag::store(uint8_t* dst) const
{
  std::memcpy(dst, bytes_, kSize);
}

//--------------------------------------------------------------------------------
bool ETag::from_hex(std::string_view hex, ETag* out)
{
  // On any parse failure set all bytes to 0xFF — an illegal sentinel value
  // (part_count 0xFFFF is out of range) that never matches a real stored ETag,
  // guaranteeing a 412 Precondition Failed on any conditional check.
  auto set_sentinel = [&]() {
    std::memset(out->bytes_, 0xFF, kSize);
  };

  // Strip optional surrounding quotes: "d41d...427e" or "d41d...427e-5"
  if (hex.size() >= 2 && hex.front() == '"' && hex.back() == '"') [[unlikely]] {
    hex = hex.substr(1, hex.size() - 2);
  }

  // Locate optional '-' delimiter
  const auto dash = hex.find('-');
  const std::string_view md5_part =
    (dash != std::string_view::npos) ? hex.substr(0, dash) : hex;

  // MD5 must be exactly kMd5HexChars hex characters
  if (md5_part.size() != kMd5HexChars) [[unlikely]] {
    set_sentinel();
    return false;
  }

  ETag result{};

  for (size_t i = 0; i < kMd5Bytes; ++i) {
    const uint8_t hi = nibble(md5_part[i * 2]);
    const uint8_t lo = nibble(md5_part[i * 2 + 1]);
    if (hi == 0xFF || lo == 0xFF) [[unlikely]] {
      set_sentinel();
      return false;
    }
    result.bytes_[i] = static_cast<uint8_t>((hi << 4) | lo);
  }

  if (dash != std::string_view::npos) {
    const std::string_view suffix = hex.substr(dash + 1);

    // Suffix must be 1–kMaxPartCountDigits decimal digits
    if (suffix.empty() || suffix.size() > kMaxPartCountDigits) [[unlikely]] {
      set_sentinel();
      return false;
    }
    uint32_t pc = 0;
    for (char c : suffix) {
      if (c < '0' || c > '9') [[unlikely]] {
        set_sentinel();
        return false;
      }
      pc = pc * 10 + static_cast<uint32_t>(c - '0');
    }
    // Part count must be in range [1, 10000]; zero is invalid for multipart
    if (!legal_part_count(pc)) [[unlikely]] {
      set_sentinel();
      return false;
    }
    write_part_count_be(result.bytes_, static_cast<uint16_t>(pc));
  }

  *out = result;
  return true;
}

//--------------------------------------------------------------------------------
int ETag::to_hex(char* out, size_t size) const
{
  const uint16_t pc = part_count();
  const size_t needed = (pc > 0) ? kEtagMaxBufSize : kMd5HexChars + 1;
  if (size < needed) {
    return -1;
  }

  char* p = out;
  for (size_t i = 0; i < kMd5Bytes; ++i) {
    *p++ = kHex[bytes_[i] >> 4];   // bytes_[0..15] are the MD5
    *p++ = kHex[bytes_[i] & 0x0F];
  }
  if (pc > 0) {
    p += std::snprintf(p, out + size - p, "-%u", static_cast<unsigned>(pc));
  }
  *p = '\0';
  return static_cast<int>(p - out);
}

//--------------------------------------------------------------------------------
std::string ETag::to_hex() const
{
  char buf[kEtagMaxBufSize];
  const int len = to_hex(buf, sizeof(buf));
  return std::string(buf, static_cast<size_t>(len));
}

} // namespace kvrgw
