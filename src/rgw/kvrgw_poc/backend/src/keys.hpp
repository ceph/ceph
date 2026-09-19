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

#include "key_buf.hpp"
#include "ref_tag.hpp"
#include "typed_ids.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace kvrgw {

struct BucketKeyParts {
  tenant_id_t tenant_id{};
  std::string bucket_name;
};

struct ObjectKeyParts {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  std::string object_name;
};

struct PoKeyParts {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  std::string object_name;
  RefTag ref_tag{};
};

struct GoKeyParts {
  uint8_t size_tier{};
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  RefTag ref_tag{};
};

struct DKeyParts {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  uint8_t size_tier{};
  uint8_t hash_prefix{};
  uint32_t mtime{};
  RefTag ref_tag{};
};

struct LKeyParts {
  char type{};
  std::string name;
};

KeyBuf make_l_key(char type, std::string_view name);
std::optional<LKeyParts> parse_l_key(std::string_view key);
bool is_valid_l_type(char type);

KeyBuf make_bucket_key(tenant_id_t tenant_id, std::string_view bucket_name);
KeyBuf make_bucket_prefix(tenant_id_t tenant_id);
KeyBuf make_tenant_key(std::string_view tenant_name);
std::optional<BucketKeyParts> parse_bucket_key(std::string_view key);

KeyBuf make_object_key(bucket_id_t bucket_id, std::string_view object_name);
KeyBuf make_object_prefix(bucket_id_t bucket_id);
KeyBuf make_version_prefix(bucket_id_t bucket_id);
std::optional<ObjectKeyParts> parse_object_key(std::string_view key);

KeyBuf make_po_key(
    bucket_id_t bucket_id,
    std::string_view object_name,
    std::string_view ref_tag_bytes);
KeyBuf make_group_po_key(
    bucket_id_t bucket_id,
    std::string_view group_ref_tag);
KeyBuf make_p_prefix(uint16_t shard_count, uint16_t shard_id);
KeyBuf make_p_bucket_prefix(bucket_id_t bucket_id);
std::optional<PoKeyParts> parse_po_key(std::string_view key);

KeyBuf make_go_key(
    const ObjectKeyParts& object_key,
    std::string_view ref_tag_bytes,
    uint64_t object_size);
KeyBuf make_g_prefix();
std::optional<GoKeyParts> parse_go_key(std::string_view key);

uint8_t size_tier_from_size(uint64_t object_size_bytes);
uint64_t size_tier_min_bytes(uint8_t tier);
uint64_t size_tier_max_bytes(uint8_t tier);

KeyBuf make_d_key(
    bucket_id_t bucket_id,
    uint8_t size_tier,
    std::string_view ref_tag,
    uint32_t mtime);
KeyBuf make_d_bucket_prefix(bucket_id_t bucket_id);
KeyBuf make_d_bucket_tier_prefix(bucket_id_t bucket_id, uint8_t size_tier);
std::optional<DKeyParts> parse_d_key(std::string_view key);
uint8_t d_size_tier_from_size(uint64_t object_size_bytes);
uint8_t d_hash_prefix(std::string_view ref_tag);

std::optional<bucket_id_t> extract_bucket_id(std::string_view bucket_value);
uint8_t extract_access_flags(std::string_view bucket_value);
VersioningState extract_versioning_state(std::string_view bucket_value);

struct VersionKeyParts {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  std::string object_name;
  version_id_t version_id{};
};

KeyBuf make_v_key(bucket_id_t bucket_id, std::string_view object_name, version_id_t version_id);
KeyBuf make_v_prefix(bucket_id_t bucket_id, std::string_view object_name);
std::optional<VersionKeyParts> parse_v_key(std::string_view key);

KeyBuf make_r_key(std::string_view ref_tag);

KeyBuf make_ct_key(bucket_id_t bucket_id, std::string_view ref_tag);
KeyBuf make_c_prefix(bucket_id_t bucket_id, std::string_view ref_tag);

struct GroupPoKeyParts {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  RefTag group_ref_tag{};
};

std::optional<GroupPoKeyParts> parse_group_po_key(std::string_view key);

struct GroupGoKeyParts {
  uint8_t size_tier{};
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  RefTag group_ref_tag{};
};

KeyBuf make_group_go_key(bucket_id_t bucket_id, std::string_view group_ref_tag);
std::optional<GroupGoKeyParts> parse_group_go_key(std::string_view key);

}  // namespace kvrgw
