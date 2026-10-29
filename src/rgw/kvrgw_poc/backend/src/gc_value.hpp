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

#include "fdb.hpp"
#include "object_value.hpp"
#include "ref_tag.hpp"

#include <array>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>

namespace kvrgw {

class KvTransaction;
struct PoKeyParts;

// GC-specific descriptor: wraps a ChunkDescriptor with object sizing info.
// Carries everything the GC worker needs to clean up one object's data tier.
// Wire layout: [chunk ChunkDescriptor][object_size 8B BE][mtime 4B BE]
#pragma pack(push, 1)
struct GcDescriptor {
  ChunkDescriptor chunk{};
  uint64_t        object_size{};
  uint32_t        mtime{};

  void serialize(uint8_t* dst) const {
    chunk.serialize(dst);
    const uint64_t os_be = htobe64(object_size);
    std::memcpy(dst + offsetof(GcDescriptor, object_size), &os_be, sizeof(os_be));
    const uint32_t mt_be = htonl(mtime);
    std::memcpy(dst + offsetof(GcDescriptor, mtime), &mt_be, sizeof(mt_be));
  }

  void load(const uint8_t* src) {
    chunk.load(src);
    uint64_t os_be{};
    std::memcpy(&os_be, src + offsetof(GcDescriptor, object_size), sizeof(os_be));
    object_size = be64toh(os_be);
    uint32_t mt_be{};
    std::memcpy(&mt_be, src + offsetof(GcDescriptor, mtime), sizeof(mt_be));
    mtime = be32toh(mt_be);
  }
};
#pragma pack(pop)
static_assert(sizeof(GcDescriptor) == sizeof(ChunkDescriptor) + sizeof(uint64_t) + sizeof(uint32_t));

struct GcValue {
  GcValueHeader  hdr{};
  GcDescriptor   gc{};
};

struct GcValueBuf {
  uint8_t data[sizeof(GcValueHeader) + sizeof(GcDescriptor)];

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), sizeof(data)};
  }
};
static_assert(sizeof(GcValueBuf) == sizeof(GcValueHeader) + sizeof(GcDescriptor));

struct GroupGcValueBuf {
  uint8_t data[sizeof(uint16_t) + kMaxBatchSize * sizeof(GcDescriptor)];
  size_t len{};

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};
static_assert(sizeof(GroupGcValueBuf::data) == sizeof(uint16_t) + kMaxBatchSize * sizeof(GcDescriptor));

// Result of parsing a single P:O value (parse_po_value_view()). object_name
// is a view into the source buffer (e.g. an FdbRangeHolder's row value) --
// only valid for as long as that buffer remains alive.
// Wire: [GcDescriptor 36B][object_name_len 2B BE][object_name variable]
struct PoEntryView {
  GcDescriptor     gc{};
  std::string_view object_name;
};

// Stack buffer for serializing a single P:O value.
// Max size: sizeof(GcDescriptor) + sizeof(uint16_t) + AWS_MaxObjectNameLen
struct PoValueBuf {
  static constexpr size_t kMaxSize =
      sizeof(GcDescriptor) + sizeof(uint16_t) + AWS_MaxObjectNameLen;
  uint8_t data[kMaxSize];
  size_t  len{};

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};

// Group P:O value — fixed section 304B + variable object names.
// Wire layout:
//   [object_name_len[kMaxGroupPoSize] × sizeof(uint16_t)]  — 16B (0 = unused slot)
//   [GcDescriptor[kMaxGroupPoSize]]                        — 288B
//   [object_name_0]...[object_name_{n-1}]                  — packed, only for len>0
// Total fixed: 304B; variable section bounded by kMaxGroupPoSize × AWS_MaxObjectNameLen.
struct GroupPoEntry {
  GcDescriptor gc{};
  std::string  object_name;
};

struct GroupPoValue {
  std::array<GroupPoEntry, kMaxGroupPoSize> entries{};
  uint8_t  count{};
  uint32_t created_at_unix{};
};

// Zero-copy variant of GroupPoEntry/GroupPoValue — object_name views point
// into the source buffer. Only valid for as long as that buffer remains
// alive (e.g. an FdbRangeHolder's row value for the duration of the loop
// that consumes it).
struct GroupPoEntryView {
  GcDescriptor     gc{};
  std::string_view object_name;
};

struct GroupPoValueView {
  std::array<GroupPoEntryView, kMaxGroupPoSize> entries{};
  uint8_t  count{};
  uint32_t created_at_unix{};
};

// Stack buffer for one Group P:O value wire encoding.
struct GroupPoValueBuf {
  static constexpr size_t kFixedSize =
      kMaxGroupPoSize * sizeof(uint16_t) +
      kMaxGroupPoSize * sizeof(GcDescriptor);
  static constexpr size_t kMaxSize =
      kFixedSize + 2 * AWS_MaxObjectNameLen;
  uint8_t data[kMaxSize];
  size_t  len{};

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};

struct GroupGcValue {
  std::array<GcDescriptor, kMaxBatchSize> entries{};
  uint8_t count{};
};

void make_gc_value(GcValueBuf& buf, const GcValueHeader& hdr, const GcDescriptor& gc);
bool parse_gc_value(std::string_view data, GcValue& val);

void make_po_value(PoValueBuf& buf, const GcDescriptor& gc, std::string_view object_name);
std::optional<PoEntryView> parse_po_value_view(std::string_view data);

void make_group_po_value(GroupPoValueBuf& buf, const GroupPoValue& gpv);
std::optional<GroupPoValueView> parse_group_po_value_view(std::string_view data);

void make_group_gc_value(GroupGcValueBuf& buf, const GcDescriptor* entries, uint16_t count);
bool parse_group_gc_value(std::string_view data, GroupGcValue& val);

fdb_error_t move_po_to_go(KvTransaction& tr, const PoKeyParts& parts,
                           const GcDescriptor& gc, std::string_view object_name);

}  // namespace kvrgw
