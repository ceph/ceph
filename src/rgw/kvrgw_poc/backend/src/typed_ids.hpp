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
#include <string>
#include <string_view>

namespace kvrgw {

using tenant_id_t = uint32_t;
using bucket_id_t = uint64_t;

class __attribute__((packed)) etag_t {
  uint8_t bytes_[16]{};
  uint16_t part_count_{0};

 public:
  etag_t() = default;

  void serialize(uint8_t* out) const { std::memcpy(out, bytes_, 16); }
  void deserialize(const uint8_t* src) { std::memcpy(bytes_, src, 16); }

  std::string to_hex() const;
  static etag_t from_hex(std::string_view hex);

  const uint8_t* data() const { return bytes_; }
  uint8_t* data() { return bytes_; }
  uint16_t part_count() const { return part_count_; }
  void set_part_count(uint16_t pc) { part_count_ = pc; }
  bool is_multipart() const { return part_count_ > 0; }

  bool operator==(const etag_t& o) const {
    return std::memcmp(bytes_, o.bytes_, 16) == 0 && part_count_ == o.part_count_;
  }
  bool operator!=(const etag_t& o) const { return !(*this == o); }
};
static_assert(sizeof(etag_t) == 18);

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

  bool has_any() const { return bits_ != 0; }
  bool if_match() const { return bits_ & kIfMatch; }
  bool if_none_match() const { return bits_ & kIfNoneMatch; }
  bool if_modified_since() const { return bits_ & kIfModifiedSince; }
  bool if_unmodified_since() const { return bits_ & kIfUnmodifiedSince; }
  bool has_mtime() const { return bits_ & kHasMtime; }
  bool has_size() const { return bits_ & kHasSize; }
  bool etag_is_star() const { return bits_ & kEtagIsStar; }

  void set_if_match() { bits_ |= kIfMatch; }
  void set_if_none_match() { bits_ |= kIfNoneMatch; }
  void set_if_modified_since() { bits_ |= kIfModifiedSince | kHasMtime; }
  void set_if_unmodified_since() { bits_ |= kIfUnmodifiedSince | kHasMtime; }
  void set_has_size() { bits_ |= kHasSize; }
  void set_etag_star() { bits_ |= kIfMatch | kEtagIsStar; }
  void clear() { bits_ = 0; }
};
static_assert(sizeof(cond_flags_t) == 1);

struct __attribute__((packed)) Condition {
  cond_flags_t flags{};
  uint8_t _pad{0};
  etag_t etag{};
  uint32_t mtime{0};
  uint64_t size{0};
};
static_assert(sizeof(Condition) == 32);

class version_id_t {
  uint32_t val_{0};

 public:
  version_id_t() = default;
  explicit version_id_t(uint32_t v) : val_(v) {}

  uint32_t raw() const { return val_; }
  bool is_null() const;
  bool is_valid() const { return val_ != 0; }

  void serialize(uint8_t* out) const;
  static version_id_t deserialize(const uint8_t* src);
  std::string to_hex() const;
  static version_id_t from_hex(std::string_view);

  bool operator==(const version_id_t& o) const { return val_ == o.val_; }
  bool operator!=(const version_id_t& o) const { return val_ != o.val_; }
  bool operator<(const version_id_t& o) const { return val_ < o.val_; }
};
static_assert(sizeof(version_id_t) == 4);

inline const version_id_t kNullVersion{0xFFFFFFFF};
inline const version_id_t kFirstVersionId{0xFFFFFFFE};

}  // namespace kvrgw
