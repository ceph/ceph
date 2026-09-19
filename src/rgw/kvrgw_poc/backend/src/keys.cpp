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

#include "keys.hpp"

#include "constants.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <cmath>
#include <cstring>
#include <endian.h>

namespace kvrgw {

namespace {

void encode_bucket_id(bucket_id_t id, void *out)
{
  const uint64_t be = htobe64(id);
  std::memcpy(out, &be, 8);
}

bucket_id_t decode_bucket_id_bytes(const void *data)
{
  uint64_t be;
  std::memcpy(&be, data, 8);
  return be64toh(be);
}

} // namespace

uint8_t size_tier_from_size(uint64_t object_size_bytes)
{
  if (object_size_bytes == 0) {
    return 0;
  }
  const int log2_val = static_cast<int>(
      std::floor(std::log2(static_cast<double>(object_size_bytes))));
  const int tier = log2_val - 10;
  if (tier < 0) {
    return 0;
  }
  if (tier > 34) {
    return 34;
  }
  return static_cast<uint8_t>(tier);
}

uint64_t size_tier_min_bytes(uint8_t tier)
{
  if (tier == 0) {
    return 0;
  }
  return 1ULL << (static_cast<unsigned>(tier) + 10);
}

uint64_t size_tier_max_bytes(uint8_t tier)
{
  if (tier >= 34) {
    return UINT64_MAX;
  }
  return (1ULL << (static_cast<unsigned>(tier) + 11)) - 1;
}

uint8_t d_size_tier_from_size(uint64_t object_size_bytes)
{
  if (object_size_bytes <= 1) {
    return 0;
  }
  int log2_val = 0;
  uint64_t v = object_size_bytes - 1;
  while (v >>= 1) {
    ++log2_val;
  }
  return static_cast<uint8_t>(log2_val);
}

uint8_t d_hash_prefix(std::string_view ref_tag)
{
  if (ref_tag.size() < 4) {
    return 0;
  }
  uint32_t hash = 2166136261u;
  for (size_t i = 0; i < ref_tag.size(); ++i) {
    hash ^= static_cast<uint8_t>(ref_tag[i]);
    hash *= 16777619u;
  }
  return static_cast<uint8_t>(hash % 32);
}

bool is_valid_l_type(char type)
{
  return type == kLocalTypeNumeric || type == kLocalTypeIdMap;
}

KeyBuf make_l_key(char type, std::string_view name)
{
  assert(is_valid_l_type(type));
  assert(!name.empty() && name.size() <= 64);
  KeyBuf key;
  KeyHeaderL hdr(type);
  key.set_header(hdr);
  key.append(name.data(), name.size());
  return key;
}

std::optional<LKeyParts> parse_l_key(std::string_view key)
{
  if (key.size() < 3 || key[0] != kNamespaceLocal) {
    return std::nullopt;
  }
  if (!is_valid_l_type(key[1])) {
    return std::nullopt;
  }
  const std::string_view name = key.substr(2);
  if (name.empty() || name.size() > 64) {
    return std::nullopt;
  }
  LKeyParts parts;
  parts.type = key[1];
  parts.name.assign(name);
  return parts;
}

KeyBuf make_bucket_key(tenant_id_t tenant_id, std::string_view bucket_name)
{
  KeyBuf key;
  KeyHeaderB hdr(tenant_id);
  key.set_header(hdr);
  key.append(bucket_name.data(), bucket_name.size());
  return key;
}

KeyBuf make_bucket_prefix(tenant_id_t tenant_id)
{
  KeyBuf key;
  KeyHeaderB hdr(tenant_id);
  key.set_header(hdr);
  return key;
}

KeyBuf make_tenant_key(std::string_view tenant_name)
{
  KeyBuf key;
  key.data[0] = static_cast<uint8_t>(kNamespaceTenant);
  key.len = 1;
  key.append(tenant_name.data(), tenant_name.size());
  return key;
}

std::optional<BucketKeyParts> parse_bucket_key(std::string_view key)
{
  if (key.size() < 6 || key[0] != kNamespaceBucket) {
    return std::nullopt;
  }
  BucketKeyParts parts;
  uint32_t net{};
  std::memcpy(&net, key.data() + 1, 4);
  parts.tenant_id = ntohl(net);
  parts.bucket_name.assign(key.substr(5));
  if (parts.bucket_name.empty() || parts.bucket_name.size() > 63) {
    return std::nullopt;
  }
  return parts;
}

KeyBuf make_object_key(bucket_id_t bucket_id, std::string_view object_name)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bid_be, kCategoryObject);
  key.set_header(hdr);
  key.append(object_name.data(), object_name.size());
  return key;
}

KeyBuf make_object_prefix(bucket_id_t bucket_id)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bid_be, kCategoryObject);
  key.set_header(hdr);
  return key;
}

KeyBuf make_version_prefix(bucket_id_t bucket_id)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bid_be, kCategoryVersion);
  key.set_header(hdr);
  return key;
}

std::optional<ObjectKeyParts> parse_object_key(std::string_view key)
{
  if (key.size() < 15 || key[0] != kNamespaceObject ||
      key[13] != kCategoryObject) {
    return std::nullopt;
  }
  ObjectKeyParts parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + 1, 2);
  std::memcpy(&si_net, key.data() + 3, 2);
  parts.shard_count = ntohs(sc_net);
  parts.shard_id = ntohs(si_net);
  parts.bucket_id = decode_bucket_id_bytes(key.data() + 5);
  parts.object_name.assign(key.substr(14));
  if (parts.object_name.empty() || parts.object_name.size() > 1024) {
    return std::nullopt;
  }
  return parts;
}

KeyBuf make_po_key(bucket_id_t bucket_id, std::string_view object_name,
                   std::string_view ref_tag_bytes)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('P', kShardCount, kShardId, bid_be, kOpTypeObject);
  key.set_header(hdr);
  key.append(object_name.data(), object_name.size());
  key.append(ref_tag_bytes.data(), ref_tag_bytes.size());
  return key;
}

KeyBuf make_group_po_key(bucket_id_t bucket_id, std::string_view group_ref_tag)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('P', kShardCount, kShardId, bid_be, kOpTypeGroup);
  key.set_header(hdr);
  key.append(group_ref_tag.data(), group_ref_tag.size());
  return key;
}

KeyBuf make_p_prefix(uint16_t shard_count, uint16_t shard_id)
{
  KeyBuf key;
  key.data[0] = static_cast<uint8_t>(kNamespacePending);
  key.len = 1;
  uint16_t sc_net = htons(shard_count);
  uint16_t si_net = htons(shard_id);
  key.append(&sc_net, 2);
  key.append(&si_net, 2);
  return key;
}

KeyBuf make_p_bucket_prefix(bucket_id_t bucket_id)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('P', kShardCount, kShardId, bid_be, kOpTypeObject);
  key.set_header(hdr);
  return key;
}

std::optional<PoKeyParts> parse_po_key(std::string_view key)
{
  constexpr size_t kPoFixedSuffixSize = 12;
  if (key.size() < 14 + kPoFixedSuffixSize || key[0] != kNamespacePending ||
      key[13] != kOpTypeObject) {
    return std::nullopt;
  }
  PoKeyParts parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + 1, 2);
  std::memcpy(&si_net, key.data() + 3, 2);
  parts.shard_count = ntohs(sc_net);
  parts.shard_id = ntohs(si_net);
  parts.bucket_id = decode_bucket_id_bytes(key.data() + 5);
  parts.object_name.assign(
      key.substr(14, key.size() - 14 - kPoFixedSuffixSize));
  std::memcpy(parts.ref_tag.data(),
              key.data() + key.size() - kPoFixedSuffixSize, kRefTagSize);
  if (parts.object_name.empty()) {
    return std::nullopt;
  }
  return parts;
}

KeyBuf make_go_key(const ObjectKeyParts &object_key,
                   std::string_view ref_tag_bytes, uint64_t object_size)
{
  uint8_t bid_be[8];
  encode_bucket_id(object_key.bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderG hdr(size_tier_from_size(object_size), object_key.shard_count,
                 object_key.shard_id, bid_be, kCategoryObject);
  key.set_header(hdr);
  key.append(ref_tag_bytes.data(), ref_tag_bytes.size());
  return key;
}

KeyBuf make_g_prefix()
{
  KeyBuf key;
  key.data[0] = static_cast<uint8_t>(kNamespaceGc);
  key.len = 1;
  return key;
}

std::optional<GoKeyParts> parse_go_key(std::string_view key)
{
  constexpr size_t kGoFixedKeySize = 27;
  if (key.size() != kGoFixedKeySize || key[0] != kNamespaceGc ||
      key[14] != kCategoryObject) {
    return std::nullopt;
  }
  GoKeyParts parts;
  parts.size_tier = static_cast<uint8_t>(key[1]);
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + 2, 2);
  std::memcpy(&si_net, key.data() + 4, 2);
  parts.shard_count = ntohs(sc_net);
  parts.shard_id = ntohs(si_net);
  parts.bucket_id = decode_bucket_id_bytes(key.data() + 6);
  std::memcpy(parts.ref_tag.data(), key.data() + 15, kRefTagSize);
  return parts;
}

KeyBuf make_d_key(bucket_id_t bucket_id, uint8_t size_tier,
                  std::string_view ref_tag, uint32_t mtime)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderD hdr(kShardCount, kShardId, bid_be, size_tier,
                 d_hash_prefix(ref_tag), mtime, ref_tag.data());
  key.set_header(hdr);
  return key;
}

KeyBuf make_d_bucket_prefix(bucket_id_t bucket_id)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  key.data[0] = static_cast<uint8_t>(kNamespaceData);
  key.len = 1;
  uint16_t sc_net = htons(kShardCount);
  uint16_t si_net = htons(kShardId);
  key.append(&sc_net, 2);
  key.append(&si_net, 2);
  key.append(bid_be, 8);
  return key;
}

KeyBuf make_d_bucket_tier_prefix(bucket_id_t bucket_id, uint8_t size_tier)
{
  KeyBuf key = make_d_bucket_prefix(bucket_id);
  key.append(&size_tier, 1);
  return key;
}

std::optional<DKeyParts> parse_d_key(std::string_view key)
{
  constexpr size_t kDKeySize = 31;
  if (key.size() != kDKeySize || key[0] != kNamespaceData) {
    return std::nullopt;
  }
  DKeyParts parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + 1, 2);
  std::memcpy(&si_net, key.data() + 3, 2);
  parts.shard_count = ntohs(sc_net);
  parts.shard_id = ntohs(si_net);
  parts.bucket_id = decode_bucket_id_bytes(key.data() + 5);
  parts.size_tier = static_cast<uint8_t>(key[13]);
  parts.hash_prefix = static_cast<uint8_t>(key[14]);
  uint32_t mt_net{};
  std::memcpy(&mt_net, key.data() + 15, 4);
  parts.mtime = ntohl(mt_net);
  std::memcpy(parts.ref_tag.data(), key.data() + 19, kRefTagSize);
  return parts;
}

std::optional<bucket_id_t> extract_bucket_id(std::string_view bucket_value)
{
  if (bucket_value.size() < 8) {
    return std::nullopt;
  }
  return decode_bucket_id_bytes(bucket_value.data());
}

uint8_t extract_access_flags(std::string_view bucket_value)
{
  if (bucket_value.size() < 17) {
    return 0;
  }
  return static_cast<uint8_t>(bucket_value[16]);
}

VersioningState extract_versioning_state(std::string_view bucket_value)
{
  if (bucket_value.size() < 18) {
    return VERSIONING_DISABLED;
  }
  return static_cast<VersioningState>(bucket_value[17]);
}

KeyBuf make_v_key(bucket_id_t bucket_id, std::string_view object_name,
                  version_id_t version_id)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bid_be, kCategoryVersion);
  key.set_header(hdr);
  key.append(object_name.data(), object_name.size());
  key.append_byte('\0');
  uint32_t vid_be = htonl(version_id.raw());
  key.append(&vid_be, 4);
  return key;
}

KeyBuf make_v_prefix(bucket_id_t bucket_id, std::string_view object_name)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bid_be, kCategoryVersion);
  key.set_header(hdr);
  key.append(object_name.data(), object_name.size());
  key.append_byte('\0');
  return key;
}

std::optional<VersionKeyParts> parse_v_key(std::string_view key)
{
  if (key.size() < 20 || key[0] != kNamespaceObject ||
      key[13] != kCategoryVersion) {
    return std::nullopt;
  }
  VersionKeyParts parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + 1, 2);
  std::memcpy(&si_net, key.data() + 3, 2);
  parts.shard_count = ntohs(sc_net);
  parts.shard_id = ntohs(si_net);
  parts.bucket_id = decode_bucket_id_bytes(key.data() + 5);
  parts.object_name.assign(key.substr(14, key.size() - 14 - 5));
  uint32_t vid_net{};
  std::memcpy(&vid_net, key.data() + key.size() - 4, 4);
  parts.version_id = version_id_t{ntohl(vid_net)};
  return parts;
}

KeyBuf make_r_key(std::string_view ref_tag)
{
  KeyBuf key;
  const char ns = 'R';
  key.append(&ns, 1);
  key.append(ref_tag.data(), ref_tag.size());
  return key;
}

KeyBuf make_ct_key(bucket_id_t bucket_id, std::string_view ref_tag)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bid_be, kCategoryChild);
  key.set_header(hdr);
  key.append(ref_tag.data(), ref_tag.size());
  const char child_type = kChildTypeTags;
  key.append(&child_type, 1);
  return key;
}

KeyBuf make_c_prefix(bucket_id_t bucket_id, std::string_view ref_tag)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bid_be, kCategoryChild);
  key.set_header(hdr);
  key.append(ref_tag.data(), ref_tag.size());
  return key;
}

std::optional<GroupPoKeyParts> parse_group_po_key(std::string_view key)
{
  constexpr size_t kGroupPoKeySize =
      14 + kRefTagSize; // header(14) + group_ref_tag(12)
  if (key.size() != kGroupPoKeySize || key[0] != kNamespacePending ||
      key[13] != kOpTypeGroup) {
    return std::nullopt;
  }
  GroupPoKeyParts parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + 1, 2);
  std::memcpy(&si_net, key.data() + 3, 2);
  parts.shard_count = ntohs(sc_net);
  parts.shard_id = ntohs(si_net);
  parts.bucket_id = decode_bucket_id_bytes(key.data() + 5);
  std::memcpy(parts.group_ref_tag.data(), key.data() + 14, kRefTagSize);
  return parts;
}

KeyBuf make_group_go_key(bucket_id_t bucket_id, std::string_view group_ref_tag)
{
  uint8_t bid_be[8];
  encode_bucket_id(bucket_id, bid_be);
  KeyBuf key;
  KeyHeaderG hdr(0, kShardCount, kShardId, bid_be, kOpTypeGroup);
  key.set_header(hdr);
  key.append(group_ref_tag.data(), group_ref_tag.size());
  return key;
}

std::optional<GroupGoKeyParts> parse_group_go_key(std::string_view key)
{
  constexpr size_t kGroupGoKeySize =
      15 + kRefTagSize; // header(15) + group_ref_tag(12)
  if (key.size() != kGroupGoKeySize || key[0] != kNamespaceGc ||
      key[14] != kOpTypeGroup) {
    return std::nullopt;
  }
  GroupGoKeyParts parts;
  parts.size_tier = static_cast<uint8_t>(key[1]);
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + 2, 2);
  std::memcpy(&si_net, key.data() + 4, 2);
  parts.shard_count = ntohs(sc_net);
  parts.shard_id = ntohs(si_net);
  parts.bucket_id = decode_bucket_id_bytes(key.data() + 6);
  std::memcpy(parts.group_ref_tag.data(), key.data() + 15, kRefTagSize);
  return parts;
}

} // namespace kvrgw
