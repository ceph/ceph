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

struct PoValue {
  PoValueHeader hdr{};
};

struct GcValue {
  GcValueHeader hdr{};
};

struct GroupPoEntry {
  RefTag ref_tag{};
  uint64_t object_size{};
};

struct GroupPoValue {
  std::array<GroupPoEntry, kMaxBatchSize> entries{};
  uint8_t count{};
  uint32_t created_at_unix{};
};

struct GroupGcEntry {
  RefTag ref_tag{};
  ChunkType chunk{};
  uint8_t flags{};
  uint64_t object_size{};
};

struct GroupGcValue {
  std::array<GroupGcEntry, kMaxBatchSize> entries{};
  uint8_t count{};
};

std::string make_gc_value(const GcValueHeader& hdr);
std::optional<GcValue> parse_gc_value(std::string_view data);

std::string make_po_value(uint64_t estimated_size, uint32_t created_at_unix);
std::optional<PoValue> parse_po_value(std::string_view data);

std::string make_group_po_value(const GroupPoEntry* entries, size_t count, uint32_t created_at);
std::optional<GroupPoValue> parse_group_po_value(std::string_view data);

std::string make_group_gc_value(const GroupGcEntry* entries, size_t count);
std::optional<GroupGcValue> parse_group_gc_value(std::string_view data);

std::expected<void, fdb_error_t>
move_po_to_go(KvTransaction& tr, const PoKeyParts& parts, const PoValue& po_value);

}  // namespace kvrgw
