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

namespace {

void gc_hdr_to_be(GcValueHeader &hdr)
{
  hdr.object_size = htobe64(hdr.object_size);
  hdr.mtime = htonl(hdr.mtime);
}

void gc_hdr_from_be(GcValueHeader &hdr)
{
  hdr.object_size = be64toh(hdr.object_size);
  hdr.mtime = be32toh(hdr.mtime);
}

void po_hdr_to_be(PoValueHeader &hdr)
{
  hdr.estimated_size = htobe64(hdr.estimated_size);
  hdr.created_at_unix = htonl(hdr.created_at_unix);
}

void po_hdr_from_be(PoValueHeader &hdr)
{
  hdr.estimated_size = be64toh(hdr.estimated_size);
  hdr.created_at_unix = be32toh(hdr.created_at_unix);
}

} // namespace

std::string make_gc_value(const GcValueHeader &hdr)
{
  GcValueHeader wire = hdr;
  gc_hdr_to_be(wire);
  std::string out;
  out.append(reinterpret_cast<const char *>(&wire), sizeof(wire));
  return out;
}

std::optional<GcValue> parse_gc_value(std::string_view data)
{
  if (data.size() < sizeof(GcValueHeader)) {
    return std::nullopt;
  }
  GcValue val;
  std::memcpy(&val.hdr, data.data(), sizeof(GcValueHeader));
  gc_hdr_from_be(val.hdr);
  const char ct = static_cast<char>(val.hdr.chunk.type);
  if (ct != CHUNK_INLINE && ct != CHUNK_CHILD_D && ct != CHUNK_CHILD_D_REF &&
      ct != CHUNK_STORAGE && ct != CHUNK_STORAGE_REF) {
    return std::nullopt;
  }
  return val;
}

std::string make_po_value(uint64_t estimated_size, uint32_t created_at_unix)
{
  PoValueHeader wire{};
  wire.estimated_size = estimated_size;
  wire.created_at_unix = created_at_unix;
  po_hdr_to_be(wire);
  std::string out;
  out.append(reinterpret_cast<const char *>(&wire), sizeof(wire));
  return out;
}

std::optional<PoValue> parse_po_value(std::string_view data)
{
  if (data.size() < sizeof(PoValueHeader)) {
    return std::nullopt;
  }
  PoValue val;
  std::memcpy(&val.hdr, data.data(), sizeof(PoValueHeader));
  po_hdr_from_be(val.hdr);
  return val;
}

std::string make_group_po_value(const GroupPoEntry *entries, size_t count,
                                uint32_t created_at)
{
  std::string out;
  out.reserve(2 + count * 20 + 4);
  uint16_t count_be = htons(static_cast<uint16_t>(count));
  out.append(reinterpret_cast<const char *>(&count_be), 2);
  for (size_t i = 0; i < count; ++i) {
    out.append(reinterpret_cast<const char *>(entries[i].ref_tag.data()),
               kRefTagSize);
    uint64_t size_be = htobe64(entries[i].object_size);
    out.append(reinterpret_cast<const char *>(&size_be), 8);
  }
  uint32_t ts_be = htonl(created_at);
  out.append(reinterpret_cast<const char *>(&ts_be), 4);
  return out;
}

std::optional<GroupPoValue> parse_group_po_value(std::string_view data)
{
  if (data.size() < 6) {
    return std::nullopt; // min: 2B count + 4B created_at
  }
  uint16_t count_be{};
  std::memcpy(&count_be, data.data(), 2);
  const uint16_t count = be16toh(count_be);
  if (count == 0 || count > kMaxBatchSize) {
    return std::nullopt;
  }
  const size_t expected = 2 + static_cast<size_t>(count) * 20 + 4;
  if (data.size() != expected) {
    return std::nullopt;
  }

  GroupPoValue val;
  val.count = static_cast<uint8_t>(count);
  for (size_t i = 0; i < count; ++i) {
    const size_t offset = 2 + i * 20;
    std::memcpy(val.entries[i].ref_tag.data(), data.data() + offset,
                kRefTagSize);
    uint64_t size_be{};
    std::memcpy(&size_be, data.data() + offset + kRefTagSize, 8);
    val.entries[i].object_size = be64toh(size_be);
  }
  uint32_t ts_be{};
  std::memcpy(&ts_be, data.data() + data.size() - 4, 4);
  val.created_at_unix = be32toh(ts_be);
  return val;
}

std::string make_group_gc_value(const GroupGcEntry *entries, size_t count)
{
  std::string out;
  out.reserve(2 + count * 22);
  uint16_t count_be = htons(static_cast<uint16_t>(count));
  out.append(reinterpret_cast<const char *>(&count_be), 2);
  for (size_t i = 0; i < count; ++i) {
    out.append(reinterpret_cast<const char *>(entries[i].ref_tag.data()),
               kRefTagSize);
    out.push_back(static_cast<char>(entries[i].chunk));
    out.push_back(static_cast<char>(entries[i].flags));
    uint64_t size_be = htobe64(entries[i].object_size);
    out.append(reinterpret_cast<const char *>(&size_be), 8);
  }
  return out;
}

std::optional<GroupGcValue> parse_group_gc_value(std::string_view data)
{
  if (data.size() < 2) {
    return std::nullopt;
  }
  uint16_t count_be{};
  std::memcpy(&count_be, data.data(), 2);
  const uint16_t count = be16toh(count_be);
  if (count == 0 || count > kMaxBatchSize) {
    return std::nullopt;
  }
  const size_t expected = 2 + static_cast<size_t>(count) * 22;
  if (data.size() != expected) {
    return std::nullopt;
  }

  GroupGcValue val;
  val.count = static_cast<uint8_t>(count);
  for (size_t i = 0; i < count; ++i) {
    const size_t offset = 2 + i * 22;
    std::memcpy(val.entries[i].ref_tag.data(), data.data() + offset,
                kRefTagSize);
    val.entries[i].chunk = static_cast<ChunkType>(data[offset + kRefTagSize]);
    val.entries[i].flags = static_cast<uint8_t>(data[offset + kRefTagSize + 1]);
    uint64_t size_be{};
    std::memcpy(&size_be, data.data() + offset + kRefTagSize + 2, 8);
    val.entries[i].object_size = be64toh(size_be);
  }
  return val;
}

std::expected<void, fdb_error_t> move_po_to_go(KvTransaction &tr,
                                               const PoKeyParts &parts,
                                               const PoValue &po_value)
{
  const auto rt_view = ref_tag_view(parts.ref_tag);
  const auto po_key = make_po_key(parts.bucket_id, parts.object_name, rt_view);
  auto po_exists = tr.kv_get(po_key.view());
  if (!po_exists) {
    return std::unexpected(po_exists.error());
  }
  if (!*po_exists) {
    return {};
  }
  const auto object_key = make_object_key(parts.bucket_id, parts.object_name);
  auto existing = tr.kv_get(object_key.view());
  if (!existing) {
    return std::unexpected(existing.error());
  }
  if (*existing) {
    const auto object_value = parse_object_value(**existing);
    if (object_value) {
      std::string_view existing_ref(
          reinterpret_cast<const char *>(object_value->hdr.ref_tag), 12);
      if (RefTagGenerator::equal(existing_ref, rt_view)) {
        tr.kv_del(po_key.view());
        return {};
      }
    }
  }

  ObjectKeyParts object_parts;
  object_parts.shard_count = parts.shard_count;
  object_parts.shard_id = parts.shard_id;
  object_parts.bucket_id = parts.bucket_id;
  object_parts.object_name = parts.object_name;
  const auto go_key =
      make_go_key(object_parts, rt_view, po_value.hdr.estimated_size);
  GcValueHeader gc_hdr{};
  gc_hdr.chunk.type = CHUNK_STORAGE;
  gc_hdr.object_size = po_value.hdr.estimated_size;
  gc_hdr.mtime = 0;
  tr.kv_put(go_key.view(), make_gc_value(gc_hdr));
  tr.kv_del(po_key.view());
  return {};
}

} // namespace kvrgw
