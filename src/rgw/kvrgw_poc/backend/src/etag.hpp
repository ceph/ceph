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

#include <cstdint>
#include <cstring>
#include <endian.h>
#include <memory>
#include <string>
#include <string_view>

namespace kvrgw {

static constexpr size_t kMd5Bytes           = 16;
static constexpr size_t kMd5HexChars        = kMd5Bytes * 2;
static constexpr size_t kMaxPartCountDigits = 5;    // max digits in "10000"
static constexpr size_t kEtagMaxBufSize     = kMd5HexChars + kMaxPartCountDigits + 1/*dash*/ + 1/*NUL*/;

// Binary representation of an S3 ETag.
// Opaque 18-byte buffer: bytes[0..15] = MD5 (same byte order as hex string),
// bytes[16..17] = part_count in big-endian wire order (0 = single-part).
//
// Single-part ETag: "d41d8cd98f00b204e9800998ecf8427e"     (bytes[16..17] == 0)
// Multipart ETag:   "d41d8cd98f00b204e9800998ecf8427e-42"  (bytes[16..17] == 0x002a)
class __attribute__((packed)) ETag {
 public:
  static constexpr size_t kSize = kMd5Bytes + sizeof(uint16_t);  // 18

  ETag() = default;

  // Populate from an 18-byte wire buffer.
  // src must point to at least kSize bytes.
  bool load(const uint8_t* src);
  bool load(const char* src) {
    return load(reinterpret_cast<const uint8_t*>(src));
  }

  // Populate from separate MD5 digest bytes and part count.
  // digest must point to at least kMd5Bytes bytes.
  // part_count == 0 means single-part.
  void load_raw(const uint8_t* digest, uint16_t part_count = 0);

  // Serialize to an 18-byte wire buffer.
  // dst must point to at least kSize bytes.
  void store(uint8_t* dst) const;

  // Parse from an S3 ETag string: "32hexchars" or "32hexchars-N".
  // Writes result into *out and returns true on success.
  // On malformed input returns false and sets *out to all-0xFF sentinel —
  // an illegal value that never matches a real stored ETag, ensuring any
  // conditional check produces 412 Precondition Failed rather than 400.
  static bool from_hex(std::string_view hex, ETag* out);

  // Render as an S3 ETag string: "32hexchars" for single-part,
  // "32hexchars-N" for multipart.
  std::string to_hex() const;

  // Write the ETag hex string directly into caller-supplied buffer.
  // Returns the number of characters written (not including NUL terminator).
  // Returns -1 if size is too small (minimum: 33 for single-part, 39 for multipart).
  // The output is NUL-terminated on success.
  int to_hex(char* out, size_t size) const;

  // Returns part_count in host byte order.  Zero means single-part.
  uint16_t part_count() const {
    uint16_t be;
    std::memcpy(&be, bytes_ + kMd5Bytes, sizeof(be));
    if (be != 0xFFFF) {
      return be16toh(be);
    }
    // sentinel triggers -> corrupted value
    return 0;
  }

  bool is_multipart() const { return part_count() > 0; }

  bool operator==(const ETag& o) const {
    return std::memcmp(bytes_, o.bytes_, kSize) == 0;
  }
  bool operator!=(const ETag& o) const { return !(*this == o); }

 private:
  uint8_t bytes_[kSize]{};  // [0..15] MD5, [16..17] part_count BE

  friend class CompositeEtagBuilder;
};
static_assert(sizeof(ETag) == 18);

// Composite (multipart) ETag builder — MD5 of the concatenation of each
// included part's raw MD5 digest, following the same "ETag of ETags"
// convention S3 itself uses. See "Composite ETag — locked design" in
// plans/multipart.md. Declared a friend of ETag for direct access to
// bytes_[0..15] (the raw per-part MD5 digest) — no new public ETag accessor
// needed.
class CompositeEtagBuilder {
 public:
  CompositeEtagBuilder();
  ~CompositeEtagBuilder();

  CompositeEtagBuilder(const CompositeEtagBuilder&) = delete;
  CompositeEtagBuilder& operator=(const CompositeEtagBuilder&) = delete;

  // Fold in one part's ETag, in ascending part_number order.
  void fold(const ETag& part_etag);

  // MD5 of the concatenated per-part digests, tagged with the number of
  // folds so far as the resulting ETag's part_count.
  ETag finalize() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  uint16_t part_count_{0};
};

}  // namespace kvrgw
