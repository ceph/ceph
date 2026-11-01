// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

/*
 * The checksums the ofi test clients compute of their own bytes, without
 * Ceph's code, so that they test the gateway's: CRC-64/NVME and CRC-32C
 * as S3 defines them, rendered as S3 renders them.
 */

#include <array>
#include <cstdint>
#include <cstring>
#include <string>

namespace ofi_s3 {

/// CRC-64/NVME (reflected 0xad93d23594c93659, init and xorout all ones),
/// as S3's x-amz-checksum-crc64nvme
inline uint64_t crc64nvme(const char* p, size_t n)
{
  static const auto table = [] {
    std::array<uint64_t, 256> t{};
    for (uint64_t i = 0; i < 256; i++) {
      uint64_t c = i;
      for (int k = 0; k < 8; k++) {
        c = (c & 1) ? (c >> 1) ^ 0x9a6c9329ac4bc9b5ull : c >> 1;
      }
      t[i] = c;
    }
    return t;
  }();
  uint64_t crc = ~0ull;
  for (size_t i = 0; i < n; i++) {
    crc = table[(crc ^ static_cast<unsigned char>(p[i])) & 0xff] ^ (crc >> 8);
  }
  return ~crc;
}

/// CRC-32C (Castagnoli, reflected 0x1edc6f41, init and xorout all ones),
/// as S3's x-amz-checksum-crc32c
inline uint32_t crc32c(const char* p, size_t n)
{
  static const auto table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for (int k = 0; k < 8; k++) {
        c = (c & 1) ? (c >> 1) ^ 0x82f63b78u : c >> 1;
      }
      t[i] = c;
    }
    return t;
  }();
  uint32_t crc = ~0u;
  for (size_t i = 0; i < n; i++) {
    crc = table[(crc ^ static_cast<unsigned char>(p[i])) & 0xff] ^ (crc >> 8);
  }
  return ~crc;
}

/// base64 of n big-endian bytes of v, as S3 renders a checksum
inline std::string armor(uint64_t v, int n)
{
  static const char* b64 =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  unsigned char be[8];
  for (int i = 0; i < n; i++) {
    be[i] = static_cast<unsigned char>(v >> (8 * (n - 1 - i)));
  }
  std::string out;
  for (int i = 0; i < n; i += 3) {
    const uint32_t w = (be[i] << 16) | ((i + 1 < n ? be[i + 1] : 0) << 8) |
      (i + 2 < n ? be[i + 2] : 0);
    out += b64[(w >> 18) & 63];
    out += b64[(w >> 12) & 63];
    out += i + 1 < n ? b64[(w >> 6) & 63] : '=';
    out += i + 2 < n ? b64[w & 63] : '=';
  }
  return out;
}

inline std::string armor_crc64nvme(const char* p, size_t n)
{
  return armor(crc64nvme(p, n), 8);
}

inline std::string armor_crc32c(const char* p, size_t n)
{
  return armor(crc32c(p, n), 4);
}

/// the check values of both CRCs, and their rendering
inline bool self_test()
{
  return crc64nvme("123456789", 9) == 0xae8b14860a799888ull &&
    crc32c("123456789", 9) == 0xe3069283u &&
    armor_crc64nvme("123456789", 9) == "rosUhgp5mIg=" &&
    armor_crc32c("123456789", 9) == "4waSgw==";
}

} // namespace ofi_s3
