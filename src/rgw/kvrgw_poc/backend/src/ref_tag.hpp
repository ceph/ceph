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

  uint8_t bytes_[kRefTagSize]{};

  friend class RefTagGenerator;
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
