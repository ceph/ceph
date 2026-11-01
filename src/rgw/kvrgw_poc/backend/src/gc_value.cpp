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

#include "gc_value.hpp"

#include "keys.hpp"
#include "kv_store.hpp"
#include "ref_tag.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <endian.h>

namespace kvrgw {

void make_gc_value(GcValueBuf &buf, const GcValueHeader &hdr, const GcDescriptor &gc)
{
  // GcValueHeader contains only single-byte fields — no endian conversion needed.
  std::memcpy(buf.data, &hdr, sizeof(hdr));
  gc.serialize(buf.data + sizeof(hdr));
}

bool parse_gc_value(std::string_view data, GcValue &val)
{
  if (data.size() < sizeof(GcValueBuf)) {
    return false;
  }
  std::memcpy(&val.hdr, data.data(), sizeof(GcValueHeader));
  const char ct = static_cast<char>(val.hdr.chunk_type);
  if (ct != CHUNK_INLINE && ct != CHUNK_CHILD_D && ct != CHUNK_CHILD_D_REF &&
      ct != CHUNK_STORAGE && ct != CHUNK_STORAGE_REF &&
      ct != CHUNK_MULTIPART && ct != CHUNK_MULTIPART_REF) {
    return false;
  }
  val.gc.load(reinterpret_cast<const uint8_t*>(data.data()) + sizeof(GcValueHeader));
  return true;
}

// Single P:O value: [GcDescriptor 36B][object_name_len 2B BE][object_name variable]
void make_po_value(PoValueBuf& buf, const GcDescriptor& gc, std::string_view object_name)
{
  gc.serialize(buf.data);
  buf.len = sizeof(GcDescriptor);
  const uint16_t len_be = htons(static_cast<uint16_t>(object_name.size()));
  std::memcpy(buf.data + buf.len, &len_be, sizeof(len_be));
  buf.len += sizeof(len_be);
  std::memcpy(buf.data + buf.len, object_name.data(), object_name.size());
  buf.len += object_name.size();
}

std::optional<PoEntryView> parse_po_value_view(std::string_view data)
{
  constexpr size_t kMinSize = sizeof(GcDescriptor) + sizeof(uint16_t);
  if (data.size() < kMinSize) {
    return std::nullopt;
  }
  PoEntryView entry;
  entry.gc.load(reinterpret_cast<const uint8_t*>(data.data()));
  uint16_t len_be{};
  std::memcpy(&len_be, data.data() + sizeof(GcDescriptor), sizeof(len_be));
  const uint16_t name_len = be16toh(len_be);
  if (data.size() != kMinSize + name_len) {
    return std::nullopt;
  }
  entry.object_name = data.substr(kMinSize, name_len);
  return entry;
}

// Group P:O value:
//   [object_name_len[kMaxGroupPoSize] × 2B BE]  — 16B fixed
//   [GcDescriptor[kMaxGroupPoSize]]              — 288B fixed
//   [object_name_0]...[object_name_{n-1}]        — packed variable
void make_group_po_value(GroupPoValueBuf& buf, const GroupPoValue& gpv)
{
  // Write name-length array
  size_t off = 0;
  for (size_t i = 0; i < kMaxGroupPoSize; ++i) {
    const uint16_t l = (i < gpv.count)
        ? static_cast<uint16_t>(gpv.entries[i].object_name.size())
        : 0;
    const uint16_t l_be = htons(l);
    std::memcpy(buf.data + off, &l_be, sizeof(l_be));
    off += sizeof(l_be);
  }
  // Write GcDescriptor array
  for (size_t i = 0; i < kMaxGroupPoSize; ++i) {
    if (i < gpv.count) {
      gpv.entries[i].gc.serialize(buf.data + off);
    } else {
      std::memset(buf.data + off, 0, sizeof(GcDescriptor));
    }
    off += sizeof(GcDescriptor);
  }
  // Write packed variable object names
  for (size_t i = 0; i < gpv.count; ++i) {
    const auto& nm = gpv.entries[i].object_name;
    std::memcpy(buf.data + off, nm.data(), nm.size());
    off += nm.size();
  }
  buf.len = off;
}

std::optional<GroupPoValueView> parse_group_po_value_view(std::string_view data)
{
  constexpr size_t kFixedSize = GroupPoValueBuf::kFixedSize;
  if (data.size() < kFixedSize) {
    return std::nullopt;
  }
  GroupPoValueView val;
  val.created_at_unix = 0;
  // Read name-length array
  uint16_t name_lens[kMaxGroupPoSize]{};
  size_t off = 0;
  for (size_t i = 0; i < kMaxGroupPoSize; ++i) {
    uint16_t l_be{};
    std::memcpy(&l_be, data.data() + off, sizeof(l_be));
    name_lens[i] = be16toh(l_be);
    off += sizeof(l_be);
  }
  // Read GcDescriptor array; count = slots with name_len > 0
  uint8_t count = 0;
  for (size_t i = 0; i < kMaxGroupPoSize; ++i) {
    val.entries[i].gc.load(reinterpret_cast<const uint8_t*>(data.data()) + off);
    off += sizeof(GcDescriptor);
    if (name_lens[i] > 0) {
      ++count;
    }
  }
  // Verify total size
  size_t var_size = 0;
  for (size_t i = 0; i < kMaxGroupPoSize; ++i) {
    var_size += name_lens[i];
  }
  if (data.size() != kFixedSize + var_size) {
    return std::nullopt;
  }
  // Read packed object names (views into data, no copy)
  for (size_t i = 0; i < kMaxGroupPoSize; ++i) {
    if (name_lens[i] > 0) {
      val.entries[i].object_name = data.substr(off, name_lens[i]);
      off += name_lens[i];
    }
  }
  val.count = count;
  return val;
}

//--------------------------------------------------------------------------------
void make_group_gc_value(GroupGcValueBuf &buf, const GcDescriptor *entries, uint16_t count)
{
  const uint16_t count_be = htons(static_cast<uint16_t>(count));
  std::memcpy(buf.data, &count_be, sizeof(count_be));
  buf.len = sizeof(count_be);
  for (auto i = 0; i < count; ++i) {
    entries[i].serialize(buf.data + buf.len);
    buf.len += sizeof(GcDescriptor);
  }
}

//--------------------------------------------------------------------------------
bool parse_group_gc_value(std::string_view data, GroupGcValue &val)
{
  uint16_t count_be{};
  if (data.size() < sizeof(count_be)) {
    return false;
  }
  std::memcpy(&count_be, data.data(), sizeof(count_be));
  const uint16_t count = be16toh(count_be);
  if (count == 0 || count > kMaxBatchSize) {
    return false;
  }
  const size_t expected = sizeof(count_be) + static_cast<size_t>(count) * sizeof(GcDescriptor);
  if (data.size() != expected) {
    return false;
  }
  val.count = static_cast<uint8_t>(count);
  for (size_t i = 0; i < count; ++i) {
    const size_t offset = sizeof(count_be) + i * sizeof(GcDescriptor);
    val.entries[i].load(reinterpret_cast<const uint8_t*>(data.data()) + offset);
  }
  return true;
}

//--------------------------------------------------------------------------------
fdb_error_t move_po_to_go(KvTransaction &tr,
                           const PoKeyParts &parts,
                           const GcDescriptor &gc,
                           std::string_view object_name)
{
  const auto rt_view = parts.ref_tag.view();
  KeyBuf po_key;
  make_po_key(parts.bucket_id, rt_view, po_key);
  auto po_exists = tr.kv_get(po_key.view());
  if (!po_exists) {
    return po_exists.error();
  }
  if (!*po_exists) {
    return 0;
  }
  KeyBuf object_key;
  make_object_key(parts.bucket_id, object_name, object_key);
  auto existing = tr.kv_get(object_key.view());
  if (!existing) {
    return existing.error();
  }

  if (*existing) {
    const auto object_value = parse_object_value(**existing);
    if (object_value && object_value->hdr.ref_tag == parts.ref_tag) {
      tr.kv_del(po_key.view());
      return 0;
    }
  }

  KeyBuf go_key;
  make_go_key(kShardCount, kShardId,
              parts.bucket_id, rt_view, gc.object_size, go_key);
  GcValueHeader gc_hdr{};
  gc_hdr.chunk_type = CHUNK_STORAGE;
  GcValueBuf gc_buf;
  make_gc_value(gc_buf, gc_hdr, gc);
  tr.kv_put(go_key.view(), gc_buf.view());
  tr.kv_del(po_key.view());
  return 0;
}

} // namespace kvrgw
