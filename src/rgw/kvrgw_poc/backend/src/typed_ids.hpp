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
#include "error_codes.hpp"
#include "etag.hpp"

#include <cstdint>
#include <cstring>
#include <iosfwd>
#include <string>
#include <string_view>

namespace kvrgw {

using tenant_id_t = uint32_t;

// Opaque 8-byte bucket identifier stored in network byte order (big-endian).
// Created only via bucket_id_t::from_counter().
// Never byte-swapped after creation.
class __attribute__((packed)) bucket_id_t {
  uint8_t bytes_[kBucketIdSize]{};

 public:
  bucket_id_t() = default;

  // Populate from a raw wire-format byte buffer (e.g. during deserialization).
  // src must point to at least kBucketIdSize bytes.
  void load(const void* src);

  // Returns a string_view over the raw BE bytes for use in key-building.
  // Valid for the lifetime of this bucket_id_t.
  std::string_view view() const;

  bool operator==(const bucket_id_t& o) const;
  bool operator!=(const bucket_id_t& o) const;

 private:
  // Factory: converts a host-format counter value to BE and stores it.
  static bucket_id_t from_counter(uint64_t host_val);

  friend class KvRgwServiceImpl;
};
static_assert(sizeof(bucket_id_t) == kBucketIdSize);
inline const bucket_id_t kNullBucket{};

class __attribute__((packed)) cond_flags_t {
  uint8_t bits_{0};

 public:
  static constexpr uint8_t kIfMatch           = 0x01;
  static constexpr uint8_t kIfNoneMatch       = 0x02;
  static constexpr uint8_t kIfModifiedSince   = 0x04;
  static constexpr uint8_t kIfUnmodifiedSince = 0x08;
  static constexpr uint8_t kHasMtime          = 0x10;
  static constexpr uint8_t kHasSize           = 0x20;
  static constexpr uint8_t kEtagIsStar        = 0x40;
  static constexpr uint8_t kIfNoneMatchStar   = 0x80;

  bool has_any() const { return bits_ != 0; }
  bool if_match() const { return bits_ & kIfMatch; }
  bool if_none_match() const { return bits_ & kIfNoneMatch; }
  bool if_modified_since() const { return bits_ & kIfModifiedSince; }
  bool if_unmodified_since() const { return bits_ & kIfUnmodifiedSince; }
  bool has_mtime() const { return bits_ & kHasMtime; }
  bool has_size() const { return bits_ & kHasSize; }
  bool etag_is_star() const { return bits_ & kEtagIsStar; }
  bool if_none_match_star() const { return bits_ & kIfNoneMatchStar; }

  void set_if_match() { bits_ |= kIfMatch; }
  void set_if_none_match() { bits_ |= kIfNoneMatch; }
  void set_if_modified_since() { bits_ |= kIfModifiedSince | kHasMtime; }
  void set_if_unmodified_since() { bits_ |= kIfUnmodifiedSince | kHasMtime; }
  void set_has_size() { bits_ |= kHasSize; }
  void set_etag_star() { bits_ |= kIfMatch | kEtagIsStar; }
  void set_if_none_match_star() { bits_ |= kIfNoneMatch | kIfNoneMatchStar; }
  void clear() { bits_ = 0; }
};
static_assert(sizeof(cond_flags_t) == 1);

struct __attribute__((packed)) PutCondition {
  cond_flags_t flags{};
  uint8_t _pad{0};
  ETag etag{};

  // Encodes AWS string-form if_match / if_none_match into binary fields.
  // Returns KVRGW_ERR_INVALID_ARGUMENT if both are non-empty non-wildcard ETags.
  // Unparseable single ETag stores sentinel (all 0xFF) → guaranteed 412 miss.
  // Returns KVRGW_ERR_OK on all other cases.
  KvrgwErrorCode encode(std::string_view if_match, std::string_view if_none_match);
};
static_assert(sizeof(PutCondition) == 20);

struct __attribute__((packed)) GetCondition : PutCondition {
  uint32_t mtime{0};
  uint64_t size{0};

  // Encodes AWS string-form if_match plus numeric mtime/size into binary fields.
  // Unparseable if_match stores sentinel (all 0xFF) → guaranteed 412 miss.
  // Returns KVRGW_ERR_OK on all cases.
  KvrgwErrorCode encode(std::string_view if_match, uint32_t mtime, uint64_t size, bool has_size);
};
static_assert(sizeof(GetCondition) == 32);

class __attribute__((packed)) version_id_t {
  uint32_t val_{0};

 public:
  version_id_t() = default;
  explicit version_id_t(uint32_t v) : val_(v) {}

  version_id_t next_vid() const;
  version_id_t prev_vid() const;
  version_id_t to_be() const;
  version_id_t from_be() const;
  uint32_t raw() const { return val_; }
  bool is_null() const;
  bool is_valid() const { return val_ != 0; }
  bool is_invalid() const { return !is_valid(); }

  void serialize(char* out) const;
  static version_id_t deserialize(const char* src);
  std::string to_hex() const;
  static version_id_t from_hex(std::string_view);
  static version_id_t generate_random_version_id(uint32_t rand_val, uint32_t num_versions);

  friend std::ostream& operator<<(std::ostream& os, const version_id_t& v);
  bool operator==(const version_id_t& o) const { return val_ == o.val_; }
  bool operator!=(const version_id_t& o) const { return val_ != o.val_; }
  bool operator<(const version_id_t& o) const { return val_ < o.val_; }
  bool operator<=(const version_id_t& o) const { return val_ <= o.val_; }
};
static_assert(sizeof(version_id_t) == 4);

inline const version_id_t kNullVersion{0xFFFFFFFF};
inline const version_id_t kFirstVersionId{0xFFFFFFFE};

}  // namespace kvrgw
