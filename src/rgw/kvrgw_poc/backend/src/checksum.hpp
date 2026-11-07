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

#pragma once

// S3 additional-checksum support (x-amz-checksum-*): digest computation and
// multipart combination.  Self-contained (OpenSSL + header-only xxHash) —
// rgw_cksum.h pulls in boost/fmt/ceph buffers and cannot be linked into the
// backend.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace kvrgw {

// Values are persisted in the 4-bit ObjectValueHeader.cksum_alg field (<= 15).
enum class CksumAlg : uint8_t {
  NONE      = 0,
  CRC32     = 1,
  CRC32C    = 2,
  SHA1      = 3,
  SHA256    = 4,
  CRC64NVME = 5,
  SHA512    = 6,
  MD5       = 7,
  XXHash3   = 8,
  XXHash64  = 9,
  XXHash128 = 10,
};
static constexpr uint8_t kCksumAlgMax = static_cast<uint8_t>(CksumAlg::XXHash128);

enum class CksumType : uint8_t {
  NONE        = 0,
  COMPOSITE   = 1,
  FULL_OBJECT = 2,
};

static constexpr size_t kCksumMaxDigestBytes = 64;  // SHA512

// Raw digest length in bytes; 0 for NONE.
size_t cksum_digest_len(CksumAlg alg);

// Parse S3 algorithm names ("CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME",
// "SHA512", "MD5", "XXHASH3", "XXHASH64", "XXHASH128") and type names
// ("COMPOSITE", "FULL_OBJECT").  Return NONE on unknown input.
CksumAlg cksum_alg_from_name(std::string_view name);
CksumType cksum_type_from_name(std::string_view name);
const char* cksum_alg_name(CksumAlg alg);
const char* cksum_type_name(CksumType type);

// Multipart rule: CRC64NVME is FULL_OBJECT only; CRC32 and CRC32C accept
// FULL_OBJECT or COMPOSITE; every other algorithm is COMPOSITE only.  (A
// single-part object is always FULL_OBJECT and does not go through this check.)
bool cksum_type_valid_for_alg(CksumAlg alg, CksumType type);
// Default type S3 applies when none is requested.
CksumType cksum_default_type(CksumAlg alg);

std::string base64_encode(const uint8_t* data, size_t len);
// Allocation-free variants. base64_encode_to writes 4*ceil(len/3) chars to `out`
// (no terminator) and returns that count. base64_decode_to returns false on
// malformed input or when the result would not fit in `cap` bytes.
size_t base64_encode_to(const uint8_t* data, size_t len, char* out);
bool base64_decode_to(std::string_view in, uint8_t* out, size_t cap, size_t* n);
// Returns false on malformed input.
bool base64_decode(std::string_view in, std::string* out);

// S3 rendering of a raw (combined) digest into `out` (no terminator): base64,
// plus "-<parts>" for COMPOSITE. `out` needs at least 4*ceil(n/3)+7 bytes
// (64 is always enough). Returns the number of chars written.
size_t cksum_render(CksumType type, const uint8_t* raw, size_t n, uint32_t parts, char* out);

// Incremental digest of one buffer stream.
class CksumHasher {
 public:
  explicit CksumHasher(CksumAlg alg);
  ~CksumHasher();
  CksumHasher(const CksumHasher&) = delete;
  CksumHasher& operator=(const CksumHasher&) = delete;

  void update(const void* data, size_t len);
  // Writes cksum_digest_len(alg) bytes to out and returns that length.
  size_t finalize(uint8_t* out);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// One-shot helper; returns raw digest bytes.
std::string cksum_digest(CksumAlg alg, const void* data, size_t len);

// CRC combine on raw big-endian digests: acc = CRC(A||B) given acc = CRC(A),
// next = CRC(B), len_b = |B|.  Both buffers are cksum_digest_len(alg) bytes.
void crc_combine(CksumAlg alg, uint8_t* acc, const uint8_t* next, uint64_t len_b);

// Folds per-part raw digests (in part order) into the object-level checksum.
//   COMPOSITE:   digest(concat(part digests)), rendered "base64-N"
//   FULL_OBJECT: CRC combine over the part CRCs, rendered "base64"
class CombinedCksumBuilder {
 public:
  CombinedCksumBuilder(CksumAlg alg, CksumType type);
  ~CombinedCksumBuilder();
  CombinedCksumBuilder(const CombinedCksumBuilder&) = delete;
  CombinedCksumBuilder& operator=(const CombinedCksumBuilder&) = delete;

  // part_digest must be cksum_digest_len(alg) bytes.
  void fold(const uint8_t* part_digest, uint64_t part_size);

  // Raw combined digest (without the "-N" composite suffix).
  size_t finalize_raw(uint8_t* out);
  // S3 rendering of the combined checksum.
  std::string finalize_string();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace kvrgw
