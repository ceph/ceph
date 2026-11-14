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

#include "constants.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>

namespace kvrgw {

// Opaque 12-byte reference tag stored in network byte order.
// Created only by RefTagGenerator::next().
// Never byte-swapped after generation.
class RefTag {
 public:
  RefTag() = default;

  // Populate from a raw wire-format byte buffer (e.g. during deserialization).
  // src must point to at least kRefTagSize bytes.
  void load(const uint8_t* src);
  void load(const char* src) { load(reinterpret_cast<const uint8_t*>(src)); }

  // Returns a string_view over the raw bytes for use in key-building and
  // DataStore calls. Valid for the lifetime of this RefTag.
  std::string_view view() const;

  // Filename-safe identifier (= to_hex()).
  std::string filename() const {
    return to_hex();
  }

  bool operator==(const RefTag& o) const;
  bool operator!=(const RefTag& o) const;

 private:
  // Hex representation of the 12 bytes (24 hex chars).
  std::string to_hex() const;

  // Writes exactly kRefTagSize*2 (24) hex chars into dst. No NUL terminator,
  // no allocation. dst must point to at least kRefTagSize*2 bytes.
  void to_hex(char* dst) const;

  // Decodes a 24-char lowercase/uppercase hex string (the upload_id wire
  // format — see "upload_id wire format" note in service_impl_multipart.cpp)
  // into *out. Returns false on any malformed input (wrong length, invalid
  // hex char) and leaves *out unspecified — callers treat decode failure
  // uniformly as NoSuchUpload, mirroring ETag::from_hex's bool-return shape.
  static bool from_hex(std::string_view hex, RefTag* out);

  uint8_t bytes_[kRefTagSize]{};

  friend class RefTagGenerator;
  friend class KvRgwServiceImpl;
};
static_assert(sizeof(RefTag) == kRefTagSize);

// Generates unique RefTags for this RGW instance.
// Exactly one instance per process — asserted in the constructor.
class RefTagGenerator {
 public:
  explicit RefTagGenerator(uint32_t rgw_id);

  // Returns the next unique RefTag. Thread-safe, lock-free.
  RefTag next();

  RefTagGenerator(const RefTagGenerator&) = delete;
  RefTagGenerator& operator=(const RefTagGenerator&) = delete;

 private:
  uint32_t              rgw_id_;
  std::atomic<uint64_t> seq_id_{0};
};

}  // namespace kvrgw
