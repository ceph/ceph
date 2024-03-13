// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

/* N.B., this header defines fundamental serialized types.  Do not
 * introduce changes or include files which can only be compiled in
 * radosgw or OSD contexts (e.g., rgw_sal.h, rgw_common.h)
 */

#pragma once

#include <string>
#include <set>
#include <map>
#include <list>
#include <boost/optional.hpp>

#include <fmt/format.h>

#include "include/types.h"
#include "rgw_bucket_layout.h"
#include "rgw_zone_features.h"
#include "rgw_pool_types.h"
#include "rgw_acl_types.h"
#include "rgw_placement_types.h"

#include "common/Formatter.h"

class JSONObj;

namespace rgw_zone_defaults {

extern std::string zone_names_oid_prefix;
extern std::string region_info_oid_prefix;
extern std::string realm_names_oid_prefix;
extern std::string zone_group_info_oid_prefix;
extern std::string realm_info_oid_prefix;
extern std::string default_region_info_oid;
extern std::string default_zone_group_info_oid;
extern std::string region_map_oid;
extern std::string default_realm_info_oid;
extern std::string default_zonegroup_name;
extern std::string default_zone_name;
extern std::string zonegroup_names_oid_prefix;
extern std::string RGW_DEFAULT_ZONE_ROOT_POOL;
extern std::string RGW_DEFAULT_ZONEGROUP_ROOT_POOL;
extern std::string RGW_DEFAULT_REALM_ROOT_POOL;
extern std::string RGW_DEFAULT_PERIOD_ROOT_POOL;
extern std::string avail_pools;
extern std::string default_storage_pool_suffix;

} /* namespace rgw_zone_defaults */

struct RGWNameToId {
  std::string obj_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(obj_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(obj_id, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWNameToId*>& o);
};
WRITE_CLASS_ENCODER(RGWNameToId)

struct RGWDefaultSystemMetaObjInfo {
  std::string default_id;
  std::string default_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(default_id, bl);
    encode(default_name, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(default_id, bl);
    decode(default_name, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWDefaultSystemMetaObjInfo)

struct RGWZoneStorageClass {
  boost::optional<rgw_pool> data_pool;
  boost::optional<std::string> compression_type;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(data_pool, bl);
    encode(compression_type, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(data_pool, bl);
    decode(compression_type, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZoneStorageClass*>& o);
};
WRITE_CLASS_ENCODER(RGWZoneStorageClass)

class RGWZoneStorageClasses {
  std::map<std::string, RGWZoneStorageClass> m;

  /* in memory only */
  RGWZoneStorageClass *standard_class;

public:
  RGWZoneStorageClasses() {
    standard_class = &m[RGW_STORAGE_CLASS_STANDARD];
  }
  RGWZoneStorageClasses(const RGWZoneStorageClasses& rhs) {
    m = rhs.m;
    standard_class = &m[RGW_STORAGE_CLASS_STANDARD];
  }
  RGWZoneStorageClasses& operator=(const RGWZoneStorageClasses& rhs) {
    m = rhs.m;
    standard_class = &m[RGW_STORAGE_CLASS_STANDARD];
    return *this;
  }

  const RGWZoneStorageClass& get_standard() const {
    return *standard_class;
  }

  bool find(const std::string& sc, const RGWZoneStorageClass** pstorage_class) const {
    auto iter = m.find(sc);
    if (iter == m.end()) {
      return false;
    }
    *pstorage_class = &iter->second;
    return true;
  }

  bool exists(const std::string& sc) const {
    if (sc.empty()) {
      return true;
    }
    auto iter = m.find(sc);
    return (iter != m.end());
  }

  const std::map<std::string, RGWZoneStorageClass>& get_all() const {
    return m;
  }

  std::map<std::string, RGWZoneStorageClass>& get_all() {
    return m;
  }

  void set_storage_class(const std::string& sc, const rgw_pool* data_pool, const std::string* compression_type) {
    const std::string *psc = &sc;
    if (sc.empty()) {
      psc = &RGW_STORAGE_CLASS_STANDARD;
    }
    RGWZoneStorageClass& storage_class = m[*psc];
    if (data_pool) {
      storage_class.data_pool = *data_pool;
    }
    if (compression_type) {
      storage_class.compression_type = *compression_type;
    }
  }

  void remove_storage_class(const std::string& sc) {
    if (!sc.empty()) {
      m.erase(sc);
    }
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(m, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(m, bl);
    standard_class = &m[RGW_STORAGE_CLASS_STANDARD];
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZoneStorageClasses*>& o);
};
WRITE_CLASS_ENCODER(RGWZoneStorageClasses)

struct RGWZonePlacementInfo {
  rgw_pool index_pool;
  rgw_pool data_extra_pool; /* if not set we should use data_pool */
  RGWZoneStorageClasses storage_classes;
  rgw::BucketIndexType index_type;
  bool inline_data;

  RGWZonePlacementInfo() : index_type(rgw::BucketIndexType::Normal), inline_data(true) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(8, 1, bl);
    encode(index_pool.to_str(), bl);
    rgw_pool standard_data_pool = get_data_pool(RGW_STORAGE_CLASS_STANDARD);
    encode(standard_data_pool.to_str(), bl);
    encode(data_extra_pool.to_str(), bl);
    encode((uint32_t)index_type, bl);
    std::string standard_compression_type = get_compression_type(RGW_STORAGE_CLASS_STANDARD);
    encode(standard_compression_type, bl);
    encode(storage_classes, bl);
    encode(inline_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(8, bl);
    std::string index_pool_str;
    std::string data_pool_str;
    decode(index_pool_str, bl);
    index_pool = rgw_pool(index_pool_str);
    decode(data_pool_str, bl);
    rgw_pool standard_data_pool(data_pool_str);
    if (struct_v >= 4) {
      std::string data_extra_pool_str;
      decode(data_extra_pool_str, bl);
      data_extra_pool = rgw_pool(data_extra_pool_str);
    }
    if (struct_v >= 5) {
      uint32_t it;
      decode(it, bl);
      index_type = (rgw::BucketIndexType)it;
    }
    std::string standard_compression_type;
    if (struct_v >= 6) {
      decode(standard_compression_type, bl);
    }
    if (struct_v >= 7) {
      decode(storage_classes, bl);
    } else {
      storage_classes.set_storage_class(RGW_STORAGE_CLASS_STANDARD, &standard_data_pool,
                                        (!standard_compression_type.empty() ? &standard_compression_type : nullptr));
    }
    if (struct_v >= 8) {
      decode(inline_data, bl);
    }
    DECODE_FINISH(bl);
  }
  const rgw_pool& get_data_extra_pool() const {
    static rgw_pool no_pool;
    if (data_extra_pool.empty()) {
      return storage_classes.get_standard().data_pool.get_value_or(no_pool);
    }
    return data_extra_pool;
  }
  const rgw_pool& get_data_pool(const std::string& sc) const {
    const RGWZoneStorageClass *storage_class;
    static rgw_pool no_pool;

    if (!storage_classes.find(sc, &storage_class)) {
      return storage_classes.get_standard().data_pool.get_value_or(no_pool);
    }

    return storage_class->data_pool.get_value_or(no_pool);
  }
  const rgw_pool& get_standard_data_pool() const {
    return get_data_pool(RGW_STORAGE_CLASS_STANDARD);
  }

  const std::string& get_compression_type(const std::string& sc) const {
    const RGWZoneStorageClass *storage_class;
    static std::string no_compression;

    if (!storage_classes.find(sc, &storage_class)) {
      return no_compression;
    }
    return storage_class->compression_type.get_value_or(no_compression);
  }

  bool storage_class_exists(const std::string& sc) const {
    return storage_classes.exists(sc);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZonePlacementInfo*>& o);

};
WRITE_CLASS_ENCODER(RGWZonePlacementInfo)

struct RGWZone {
  std::string id;
  std::string name;
  std::list<std::string> endpoints; // std::vector?
  bool log_meta;
  bool log_data;
  bool read_only;
  std::string tier_type;
  std::string redirect_zone;

/**
 * Represents the number of shards for the bucket index object, a value of zero
 * indicates there is no sharding. By default (no sharding, the name of the object
 * is '.dir.{marker}', with sharding, the name is '.dir.{marker}.{sharding_id}',
 * sharding_id is zero-based value. It is not recommended to set a too large value
 * (e.g. thousand) as it increases the cost for bucket listing.
 */
  uint32_t bucket_index_max_shards;

  // pre-shard buckets on creation to enable some write-parallelism by default,
  // delay the need to reshard as the bucket grows, and (in multisite) get some
  // bucket index sharding where dynamic resharding is not supported
  static constexpr uint32_t default_bucket_index_max_shards = 11;

  bool sync_from_all;
  std::set<std::string> sync_from; /* list of zones to sync from */

  rgw::zone_features::set supported_features;

  RGWZone()
    : log_meta(false), log_data(false), read_only(false),
      bucket_index_max_shards(default_bucket_index_max_shards),
      sync_from_all(true) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(8, 1, bl);
    encode(name, bl);
    encode(endpoints, bl);
    encode(log_meta, bl);
    encode(log_data, bl);
    encode(bucket_index_max_shards, bl);
    encode(id, bl);
    encode(read_only, bl);
    encode(tier_type, bl);
    encode(sync_from_all, bl);
    encode(sync_from, bl);
    encode(redirect_zone, bl);
    encode(supported_features, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(8, bl);
    decode(name, bl);
    if (struct_v < 4) {
      id = name;
    }
    decode(endpoints, bl);
    if (struct_v >= 2) {
      decode(log_meta, bl);
      decode(log_data, bl);
    }
    if (struct_v >= 3) {
      decode(bucket_index_max_shards, bl);
    }
    if (struct_v >= 4) {
      decode(id, bl);
      decode(read_only, bl);
    }
    if (struct_v >= 5) {
      decode(tier_type, bl);
    }
    if (struct_v >= 6) {
      decode(sync_from_all, bl);
      decode(sync_from, bl);
    }
    if (struct_v >= 7) {
      decode(redirect_zone, bl);
    }
    if (struct_v >= 8) {
      decode(supported_features, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZone*>& o);

  bool is_read_only() const { return read_only; }

  bool syncs_from(const std::string& zone_name) const {
    return (sync_from_all || sync_from.find(zone_name) != sync_from.end());
  }

  bool supports(std::string_view feature) const {
    return supported_features.contains(feature);
  }
};
WRITE_CLASS_ENCODER(RGWZone)

struct RGWDefaultZoneGroupInfo {
  std::string default_zonegroup;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(default_zonegroup, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(default_zonegroup, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  //todo: implement ceph-dencoder
};
WRITE_CLASS_ENCODER(RGWDefaultZoneGroupInfo)

struct RGWTierACLMapping {
  ACLGranteeTypeEnum type{ACL_TYPE_CANON_USER};
  std::string source_id;
  std::string dest_id;

  RGWTierACLMapping() = default;

  RGWTierACLMapping(ACLGranteeTypeEnum t,
             const std::string& s,
             const std::string& d) : type(t),
  source_id(s),
  dest_id(d) {}

  void init(const JSONFormattable& config) {
    const std::string& t = config["type"];

    if (t == "email") {
      type = ACL_TYPE_EMAIL_USER;
    } else if (t == "uri") {
      type = ACL_TYPE_GROUP;
    } else {
      type = ACL_TYPE_CANON_USER;
    }

    source_id = config["source_id"];
    dest_id = config["dest_id"];
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)type, bl);
    encode(source_id, bl);
    encode(dest_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    uint32_t it;
    decode(it, bl);
    type = (ACLGranteeTypeEnum)it;
    decode(source_id, bl);
    decode(dest_id, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWTierACLMapping)

enum HostStyle {
  PathStyle = 0,
  VirtualStyle = 1,
};

struct RGWZoneGroupPlacementTierS3 {
#define DEFAULT_MULTIPART_SYNC_PART_SIZE (32 * 1024 * 1024)
  std::string endpoint;
  RGWAccessKey key;
  std::string region;
  HostStyle host_style{PathStyle};
  std::string target_storage_class;

  /* Should below be bucket/zone specific?? */
  std::string target_path;
  std::map<std::string, RGWTierACLMapping> acl_mappings;

  uint64_t multipart_sync_threshold{DEFAULT_MULTIPART_SYNC_PART_SIZE};
  uint64_t multipart_min_part_size{DEFAULT_MULTIPART_SYNC_PART_SIZE};

  int update_params(const JSONFormattable& config);
  int clear_params(const JSONFormattable& config);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(endpoint, bl);
    encode(key, bl);
    encode(region, bl);
    encode((uint32_t)host_style, bl); // XXX kill C-style casts
    encode(target_storage_class, bl);
    encode(target_path, bl);
    encode(acl_mappings, bl);
    encode(multipart_sync_threshold, bl);
    encode(multipart_min_part_size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(endpoint, bl);
    decode(key, bl);
    decode(region, bl);

    uint32_t it;
    decode(it, bl);
    host_style = (HostStyle)it; // XXX can't this be HostStyle(it)?

    decode(target_storage_class, bl);
    decode(target_path, bl);
    decode(acl_mappings, bl);
    decode(multipart_sync_threshold, bl);
    decode(multipart_min_part_size, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZoneGroupPlacementTierS3)

struct RGWZoneGroupPlacementTier {
  std::string tier_type;
  std::string storage_class;
  bool retain_head_object = false;

  struct _tier {
    RGWZoneGroupPlacementTierS3 s3;
  } t;

  int update_params(const JSONFormattable& config);
  int clear_params(const JSONFormattable& config);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tier_type, bl);
    encode(storage_class, bl);
    encode(retain_head_object, bl);
    if (tier_type == "cloud-s3") {
      encode(t.s3, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tier_type, bl);
    decode(storage_class, bl);
    decode(retain_head_object, bl);
    if (tier_type == "cloud-s3") {
      decode(t.s3, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZoneGroupPlacementTier*>& o) {
    o.push_back(new RGWZoneGroupPlacementTier);
    o.push_back(new RGWZoneGroupPlacementTier);
    o.back()->tier_type = "cloud-s3";
    o.back()->storage_class = "STANDARD";
  }
};
WRITE_CLASS_ENCODER(RGWZoneGroupPlacementTier)

struct RGWZoneGroupPlacementTarget {
  std::string name;
  std::set<std::string> tags;
  std::set<std::string> storage_classes;
  std::map<std::string, RGWZoneGroupPlacementTier> tier_targets;

  bool user_permitted(const std::list<std::string>& user_tags) const {
    if (tags.empty()) {
      return true;
    }
    for (auto& rule : user_tags) {
      if (tags.find(rule) != tags.end()) {
        return true;
      }
    }
    return false;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    encode(name, bl);
    encode(tags, bl);
    encode(storage_classes, bl);
    encode(tier_targets, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(name, bl);
    decode(tags, bl);
    if (struct_v >= 2) {
      decode(storage_classes, bl);
    }
    if (storage_classes.empty()) {
      storage_classes.insert(RGW_STORAGE_CLASS_STANDARD);
    }
    if (struct_v >= 3) {
      decode(tier_targets, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZoneGroupPlacementTarget*>& o) {
    o.push_back(new RGWZoneGroupPlacementTarget);
    o.back()->storage_classes.insert("STANDARD");
    o.push_back(new RGWZoneGroupPlacementTarget);
    o.back()->name = "target";
    o.back()->tags.insert("tag1");
    o.back()->tags.insert("tag2");
    o.back()->storage_classes.insert("STANDARD_IA");
    o.back()->tier_targets["cloud-s3"].tier_type = "cloud-s3";
    o.back()->tier_targets["cloud-s3"].storage_class = "STANDARD";
  }
};
WRITE_CLASS_ENCODER(RGWZoneGroupPlacementTarget)
