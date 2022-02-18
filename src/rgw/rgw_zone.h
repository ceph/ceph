// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_ZONE_H
#define CEPH_RGW_ZONE_H

#include "rgw_common.h"
#include "rgw_sync_policy.h"

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

}

class JSONObj;
class RGWSyncModulesManager;


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
};
WRITE_CLASS_ENCODER(RGWNameToId)

struct RGWDefaultSystemMetaObjInfo {
  std::string default_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(default_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(default_id, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWDefaultSystemMetaObjInfo)

class RGWSI_SysObj;
class RGWSI_Zone;

class RGWSystemMetaObj {
protected:
  std::string id;
  std::string name;

  CephContext *cct{nullptr};
  RGWSI_SysObj *sysobj_svc{nullptr};
  RGWSI_Zone *zone_svc{nullptr};

  int store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int read_info(const DoutPrefixProvider *dpp, const std::string& obj_id, optional_yield y, bool old_format = false);
  int read_id(const DoutPrefixProvider *dpp, const std::string& obj_name, std::string& obj_id, optional_yield y);
  int read_default(const DoutPrefixProvider *dpp, 
                   RGWDefaultSystemMetaObjInfo& default_info,
		   const std::string& oid,
		   optional_yield y);
  /* read and use default id */
  int use_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format = false);

public:
  RGWSystemMetaObj() {}
  RGWSystemMetaObj(const std::string& _name): name(_name) {}
  RGWSystemMetaObj(const std::string& _id, const std::string& _name) : id(_id), name(_name) {}
  RGWSystemMetaObj(CephContext *_cct, RGWSI_SysObj *_sysobj_svc) {
    reinit_instance(_cct, _sysobj_svc);
  }
  RGWSystemMetaObj(const std::string& _name, CephContext *_cct, RGWSI_SysObj *_sysobj_svc): name(_name) {
    reinit_instance(_cct, _sysobj_svc);
  }

  const std::string& get_name() const { return name; }
  const std::string& get_id() const { return id; }

  void set_name(const std::string& _name) { name = _name;}
  void set_id(const std::string& _id) { id = _id;}
  void clear_id() { id.clear(); }

  virtual ~RGWSystemMetaObj() {}

  virtual void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(name, bl);
    ENCODE_FINISH(bl);
  }

  virtual void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(name, bl);
    DECODE_FINISH(bl);
  }

  void reinit_instance(CephContext *_cct, RGWSI_SysObj *_sysobj_svc);
  int init(const DoutPrefixProvider *dpp, CephContext *_cct, RGWSI_SysObj *_sysobj_svc,
	   optional_yield y,
	   bool setup_obj = true, bool old_format = false);
  virtual int read_default_id(const DoutPrefixProvider *dpp, std::string& default_id, optional_yield y,
			      bool old_format = false);
  virtual int set_as_default(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive = false);
  int delete_default();
  virtual int create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive = true);
  int delete_obj(const DoutPrefixProvider *dpp, optional_yield y, bool old_format = false);
  int rename(const DoutPrefixProvider *dpp, const std::string& new_name, optional_yield y);
  int update(const DoutPrefixProvider *dpp, optional_yield y) { return store_info(dpp, false, y);}
  int update_name(const DoutPrefixProvider *dpp, optional_yield y) { return store_name(dpp, false, y);}
  int read(const DoutPrefixProvider *dpp, optional_yield y);
  int write(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);

  virtual rgw_pool get_pool(CephContext *cct) const = 0;
  virtual const std::string get_default_oid(bool old_format = false) const = 0;
  virtual const std::string& get_names_oid_prefix() const = 0;
  virtual const std::string& get_info_oid_prefix(bool old_format = false) const = 0;
  virtual std::string get_predefined_id(CephContext *cct) const = 0;
  virtual const std::string& get_predefined_name(CephContext *cct) const = 0;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWSystemMetaObj)

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

  bool find(const std::string& sc, const RGWZoneStorageClass **pstorage_class) const {
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

  void set_storage_class(const std::string& sc, const rgw_pool *data_pool, const std::string *compression_type) {
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
};
WRITE_CLASS_ENCODER(RGWZoneStorageClasses)

struct RGWZonePlacementInfo {
  rgw_pool index_pool;
  rgw_pool data_extra_pool; /* if not set we should use data_pool */
  RGWZoneStorageClasses storage_classes;
  rgw::BucketIndexType index_type;

  RGWZonePlacementInfo() : index_type(rgw::BucketIndexType::Normal) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(7, 1, bl);
    encode(index_pool.to_str(), bl);
    rgw_pool standard_data_pool = get_data_pool(RGW_STORAGE_CLASS_STANDARD);
    encode(standard_data_pool.to_str(), bl);
    encode(data_extra_pool.to_str(), bl);
    encode((uint32_t)index_type, bl);
    std::string standard_compression_type = get_compression_type(RGW_STORAGE_CLASS_STANDARD);
    encode(standard_compression_type, bl);
    encode(storage_classes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(7, bl);
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

};
WRITE_CLASS_ENCODER(RGWZonePlacementInfo)

struct RGWZoneParams : RGWSystemMetaObj {
  rgw_pool domain_root;
  rgw_pool control_pool;
  rgw_pool gc_pool;
  rgw_pool lc_pool;
  rgw_pool log_pool;
  rgw_pool intent_log_pool;
  rgw_pool usage_log_pool;

  rgw_pool user_keys_pool;
  rgw_pool user_email_pool;
  rgw_pool user_swift_pool;
  rgw_pool user_uid_pool;
  rgw_pool roles_pool;
  rgw_pool reshard_pool;
  rgw_pool otp_pool;
  rgw_pool oidc_pool;

  RGWAccessKey system_key;

  std::map<std::string, RGWZonePlacementInfo> placement_pools;

  std::string realm_id;

  JSONFormattable tier_config;

  rgw_pool notif_pool;

  RGWZoneParams() : RGWSystemMetaObj() {}
  explicit RGWZoneParams(const std::string& name) : RGWSystemMetaObj(name){}
  RGWZoneParams(const rgw_zone_id& id, const std::string& name) : RGWSystemMetaObj(id.id, name) {}
  RGWZoneParams(const rgw_zone_id& id, const std::string& name, const std::string& _realm_id)
    : RGWSystemMetaObj(id.id, name), realm_id(_realm_id) {}

  rgw_pool get_pool(CephContext *cct) const override;
  const std::string get_default_oid(bool old_format = false) const override;
  const std::string& get_names_oid_prefix() const override;
  const std::string& get_info_oid_prefix(bool old_format = false) const override;
  std::string get_predefined_id(CephContext *cct) const override;
  const std::string& get_predefined_name(CephContext *cct) const override;

  int init(const DoutPrefixProvider *dpp, 
           CephContext *_cct, RGWSI_SysObj *_sysobj_svc, optional_yield y,
	   bool setup_obj = true, bool old_format = false);
  using RGWSystemMetaObj::init;
  int read_default_id(const DoutPrefixProvider *dpp, std::string& default_id, optional_yield y, bool old_format = false) override;
  int set_as_default(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive = false) override;
  int create_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format = false);
  int create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive = true) override;
  int fix_pool_names(const DoutPrefixProvider *dpp, optional_yield y);

  const std::string& get_compression_type(const rgw_placement_rule& placement_rule) const;
  
  void encode(bufferlist& bl) const override {
    ENCODE_START(14, 1, bl);
    encode(domain_root, bl);
    encode(control_pool, bl);
    encode(gc_pool, bl);
    encode(log_pool, bl);
    encode(intent_log_pool, bl);
    encode(usage_log_pool, bl);
    encode(user_keys_pool, bl);
    encode(user_email_pool, bl);
    encode(user_swift_pool, bl);
    encode(user_uid_pool, bl);
    RGWSystemMetaObj::encode(bl);
    encode(system_key, bl);
    encode(placement_pools, bl);
    rgw_pool unused_metadata_heap;
    encode(unused_metadata_heap, bl);
    encode(realm_id, bl);
    encode(lc_pool, bl);
    std::map<std::string, std::string, ltstr_nocase> old_tier_config;
    encode(old_tier_config, bl);
    encode(roles_pool, bl);
    encode(reshard_pool, bl);
    encode(otp_pool, bl);
    encode(tier_config, bl);
    encode(oidc_pool, bl);
    encode(notif_pool, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
    DECODE_START(14, bl);
    decode(domain_root, bl);
    decode(control_pool, bl);
    decode(gc_pool, bl);
    decode(log_pool, bl);
    decode(intent_log_pool, bl);
    decode(usage_log_pool, bl);
    decode(user_keys_pool, bl);
    decode(user_email_pool, bl);
    decode(user_swift_pool, bl);
    decode(user_uid_pool, bl);
    if (struct_v >= 6) {
      RGWSystemMetaObj::decode(bl);
    } else if (struct_v >= 2) {
      decode(name, bl);
      id = name;
    }
    if (struct_v >= 3)
      decode(system_key, bl);
    if (struct_v >= 4)
      decode(placement_pools, bl);
    if (struct_v >= 5) {
      rgw_pool unused_metadata_heap;
      decode(unused_metadata_heap, bl);
    }
    if (struct_v >= 6) {
      decode(realm_id, bl);
    }
    if (struct_v >= 7) {
      decode(lc_pool, bl);
    } else {
      lc_pool = log_pool.name + ":lc";
    }
    std::map<std::string, std::string, ltstr_nocase> old_tier_config;
    if (struct_v >= 8) {
      decode(old_tier_config, bl);
    }
    if (struct_v >= 9) {
      decode(roles_pool, bl);
    } else {
      roles_pool = name + ".rgw.meta:roles";
    }
    if (struct_v >= 10) {
      decode(reshard_pool, bl);
    } else {
      reshard_pool = log_pool.name + ":reshard";
    }
    if (struct_v >= 11) {
      ::decode(otp_pool, bl);
    } else {
      otp_pool = name + ".rgw.otp";
    }
    if (struct_v >= 12) {
      ::decode(tier_config, bl);
    } else {
      for (auto& kv : old_tier_config) {
        tier_config.set(kv.first, kv.second);
      }
    }
    if (struct_v >= 13) {
      ::decode(oidc_pool, bl);
    } else {
      oidc_pool = name + ".rgw.meta:oidc";
    }
    if (struct_v >= 14) {
      decode(notif_pool, bl);
    } else {
      notif_pool = log_pool.name + ":notif";
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZoneParams*>& o);

  bool get_placement(const std::string& placement_id, RGWZonePlacementInfo *placement) const {
    auto iter = placement_pools.find(placement_id);
    if (iter == placement_pools.end()) {
      return false;
    }
    *placement = iter->second;
    return true;
  }

  /*
   * return data pool of the head object
   */
  bool get_head_data_pool(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_pool *pool) const {
    const rgw_data_placement_target& explicit_placement = obj.bucket.explicit_placement;
    if (!explicit_placement.data_pool.empty()) {
      if (!obj.in_extra_data) {
        *pool = explicit_placement.data_pool;
      } else {
        *pool = explicit_placement.get_data_extra_pool();
      }
      return true;
    }
    if (placement_rule.empty()) {
      return false;
    }
    auto iter = placement_pools.find(placement_rule.name);
    if (iter == placement_pools.end()) {
      return false;
    }
    if (!obj.in_extra_data) {
      *pool = iter->second.get_data_pool(placement_rule.storage_class);
    } else {
      *pool = iter->second.get_data_extra_pool();
    }
    return true;
  }

  bool valid_placement(const rgw_placement_rule& rule) const {
    auto iter = placement_pools.find(rule.name);
    if (iter == placement_pools.end()) {
      return false;
    }
    return iter->second.storage_class_exists(rule.storage_class);
  }
};
WRITE_CLASS_ENCODER(RGWZoneParams)

struct RGWZone {
  std::string id;
  std::string name;
  std::list<std::string> endpoints;
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

  // pre-shard buckets on creation to enable some write-parallism by default,
  // delay the need to reshard as the bucket grows, and (in multisite) get some
  // bucket index sharding where dynamic resharding is not supported
  static constexpr uint32_t default_bucket_index_max_shards = 11;

  bool sync_from_all;
  std::set<std::string> sync_from; /* list of zones to sync from */

  RGWZone()
    : log_meta(false), log_data(false), read_only(false),
      bucket_index_max_shards(default_bucket_index_max_shards),
      sync_from_all(true) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(7, 1, bl);
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
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(7, bl);
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
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZone*>& o);

  bool is_read_only() const { return read_only; }

  bool syncs_from(const std::string& zone_name) const {
    return (sync_from_all || sync_from.find(zone_name) != sync_from.end());
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
    encode((uint32_t)host_style, bl);
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
    host_style = (HostStyle)it;

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
};
WRITE_CLASS_ENCODER(RGWZoneGroupPlacementTarget)

struct RGWZoneGroup : public RGWSystemMetaObj {
  std::string api_name;
  std::list<std::string> endpoints;
  bool is_master = false;

  rgw_zone_id master_zone;
  std::map<rgw_zone_id, RGWZone> zones;

  std::map<std::string, RGWZoneGroupPlacementTarget> placement_targets;
  rgw_placement_rule default_placement;

  std::list<std::string> hostnames;
  std::list<std::string> hostnames_s3website;
  // TODO: Maybe convert hostnames to a map<std::string,std::list<std::string>> for
  // endpoint_type->hostnames
/*
20:05 < _robbat21irssi> maybe I do someting like: if (hostname_map.empty()) { populate all map keys from hostnames; };
20:05 < _robbat21irssi> but that's a later compatability migration planning bit
20:06 < yehudasa> more like if (!hostnames.empty()) {
20:06 < yehudasa> for (std::list<std::string>::iterator iter = hostnames.begin(); iter != hostnames.end(); ++iter) {
20:06 < yehudasa> hostname_map["s3"].append(iter->second);
20:07 < yehudasa> hostname_map["s3website"].append(iter->second);
20:07 < yehudasa> s/append/push_back/g
20:08 < _robbat21irssi> inner loop over APIs
20:08 < yehudasa> yeah, probably
20:08 < _robbat21irssi> s3, s3website, swift, swith_auth, swift_website
*/
  std::map<std::string, std::list<std::string> > api_hostname_map;
  std::map<std::string, std::list<std::string> > api_endpoints_map;

  std::string realm_id;

  rgw_sync_policy_info sync_policy;

  RGWZoneGroup(): is_master(false){}
  RGWZoneGroup(const std::string &id, const std::string &name):RGWSystemMetaObj(id, name) {}
  explicit RGWZoneGroup(const std::string &_name):RGWSystemMetaObj(_name) {}
  RGWZoneGroup(const std::string &_name, bool _is_master, CephContext *cct, RGWSI_SysObj* sysobj_svc,
	       const std::string& _realm_id, const std::list<std::string>& _endpoints)
    : RGWSystemMetaObj(_name, cct , sysobj_svc), endpoints(_endpoints), is_master(_is_master),
      realm_id(_realm_id) {}

  bool is_master_zonegroup() const { return is_master;}
  void update_master(const DoutPrefixProvider *dpp, bool _is_master, optional_yield y) {
    is_master = _is_master;
    post_process_params(dpp, y);
  }
  void post_process_params(const DoutPrefixProvider *dpp, optional_yield y);

  void encode(bufferlist& bl) const override {
    ENCODE_START(5, 1, bl);
    encode(name, bl);
    encode(api_name, bl);
    encode(is_master, bl);
    encode(endpoints, bl);
    encode(master_zone, bl);
    encode(zones, bl);
    encode(placement_targets, bl);
    encode(default_placement, bl);
    encode(hostnames, bl);
    encode(hostnames_s3website, bl);
    RGWSystemMetaObj::encode(bl);
    encode(realm_id, bl);
    encode(sync_policy, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
    DECODE_START(5, bl);
    decode(name, bl);
    decode(api_name, bl);
    decode(is_master, bl);
    decode(endpoints, bl);
    decode(master_zone, bl);
    decode(zones, bl);
    decode(placement_targets, bl);
    decode(default_placement, bl);
    if (struct_v >= 2) {
      decode(hostnames, bl);
    }
    if (struct_v >= 3) {
      decode(hostnames_s3website, bl);
    }
    if (struct_v >= 4) {
      RGWSystemMetaObj::decode(bl);
      decode(realm_id, bl);
    } else {
      id = name;
    }
    if (struct_v >= 5) {
      decode(sync_policy, bl);
    }
    DECODE_FINISH(bl);
  }

  int read_default_id(const DoutPrefixProvider *dpp, std::string& default_id, optional_yield y, bool old_format = false) override;
  int set_as_default(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive = false) override;
  int create_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format = false);
  int equals(const std::string& other_zonegroup) const;
  int add_zone(const DoutPrefixProvider *dpp, 
               const RGWZoneParams& zone_params, bool *is_master, bool *read_only,
               const std::list<std::string>& endpoints, const std::string *ptier_type,
               bool *psync_from_all, std::list<std::string>& sync_from,
               std::list<std::string>& sync_from_rm, std::string *predirect_zone,
               std::optional<int> bucket_index_max_shards, RGWSyncModulesManager *sync_mgr,
	       optional_yield y);
  int remove_zone(const DoutPrefixProvider *dpp, const std::string& zone_id, optional_yield y);
  int rename_zone(const DoutPrefixProvider *dpp, const RGWZoneParams& zone_params, optional_yield y);
  rgw_pool get_pool(CephContext *cct) const override;
  const std::string get_default_oid(bool old_region_format = false) const override;
  const std::string& get_info_oid_prefix(bool old_region_format = false) const override;
  const std::string& get_names_oid_prefix() const override;
  std::string get_predefined_id(CephContext *cct) const override;
  const std::string& get_predefined_name(CephContext *cct) const override;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWZoneGroup*>& o);
};
WRITE_CLASS_ENCODER(RGWZoneGroup)

struct RGWPeriodMap
{
  std::string id;
  std::map<std::string, RGWZoneGroup> zonegroups;
  std::map<std::string, RGWZoneGroup> zonegroups_by_api;
  std::map<std::string, uint32_t> short_zone_ids;

  std::string master_zonegroup;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);

  int update(const RGWZoneGroup& zonegroup, CephContext *cct);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  void reset() {
    zonegroups.clear();
    zonegroups_by_api.clear();
    master_zonegroup.clear();
  }

  uint32_t get_zone_short_id(const std::string& zone_id) const;

  bool find_zone_by_id(const rgw_zone_id& zone_id,
                       RGWZoneGroup *zonegroup,
                       RGWZone *zone) const;
  bool find_zone_by_name(const std::string& zone_id,
                       RGWZoneGroup *zonegroup,
                       RGWZone *zone) const;
};
WRITE_CLASS_ENCODER(RGWPeriodMap)

struct RGWPeriodConfig
{
  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;
  RGWRateLimitInfo user_ratelimit;
  RGWRateLimitInfo bucket_ratelimit;
  // rate limit unauthenticated user
  RGWRateLimitInfo anon_ratelimit;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(bucket_quota, bl);
    encode(user_quota, bl);
    encode(bucket_ratelimit, bl);
    encode(user_ratelimit, bl);
    encode(anon_ratelimit, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(bucket_quota, bl);
    decode(user_quota, bl);
    if (struct_v >= 2) {
      decode(bucket_ratelimit, bl);
      decode(user_ratelimit, bl);
      decode(anon_ratelimit, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  // the period config must be stored in a local object outside of the period,
  // so that it can be used in a default configuration where no realm/period
  // exists
  int read(const DoutPrefixProvider *dpp, RGWSI_SysObj *sysobj_svc, const std::string& realm_id, optional_yield y);
  int write(const DoutPrefixProvider *dpp, RGWSI_SysObj *sysobj_svc, const std::string& realm_id, optional_yield y);

  static std::string get_oid(const std::string& realm_id);
  static rgw_pool get_pool(CephContext *cct);
};
WRITE_CLASS_ENCODER(RGWPeriodConfig)

/* for backward comaptability */
struct RGWRegionMap {

  std::map<std::string, RGWZoneGroup> regions;

  std::string master_region;

  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRegionMap)

struct RGWZoneGroupMap {

  std::map<std::string, RGWZoneGroup> zonegroups;
  std::map<std::string, RGWZoneGroup> zonegroups_by_api;

  std::string master_zonegroup;

  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  /* construct the map */
  int read(const DoutPrefixProvider *dpp, CephContext *cct, RGWSI_SysObj *sysobj_svc, optional_yield y);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZoneGroupMap)

class RGWRealm;
class RGWPeriod;

class RGWRealm : public RGWSystemMetaObj
{
  std::string current_period;
  epoch_t epoch{0}; //< realm epoch, incremented for each new period

  int create_control(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int delete_control(const DoutPrefixProvider *dpp, optional_yield y);
public:
  RGWRealm() {}
  RGWRealm(const std::string& _id, const std::string& _name = "") : RGWSystemMetaObj(_id, _name) {}
  RGWRealm(CephContext *_cct, RGWSI_SysObj *_sysobj_svc): RGWSystemMetaObj(_cct, _sysobj_svc) {}
  RGWRealm(const std::string& _name, CephContext *_cct, RGWSI_SysObj *_sysobj_svc): RGWSystemMetaObj(_name, _cct, _sysobj_svc){}
  virtual ~RGWRealm() override;

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    RGWSystemMetaObj::encode(bl);
    encode(current_period, bl);
    encode(epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
    DECODE_START(1, bl);
    RGWSystemMetaObj::decode(bl);
    decode(current_period, bl);
    decode(epoch, bl);
    DECODE_FINISH(bl);
  }

  int create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive = true) override;
  int delete_obj(const DoutPrefixProvider *dpp, optional_yield y);
  rgw_pool get_pool(CephContext *cct) const override;
  const std::string get_default_oid(bool old_format = false) const override;
  const std::string& get_names_oid_prefix() const override;
  const std::string& get_info_oid_prefix(bool old_format = false) const override;
  std::string get_predefined_id(CephContext *cct) const override;
  const std::string& get_predefined_name(CephContext *cct) const override;

  using RGWSystemMetaObj::read_id; // expose as public for radosgw-admin

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWRealm*>& o);

  const std::string& get_current_period() const {
    return current_period;
  }
  int set_current_period(const DoutPrefixProvider *dpp, RGWPeriod& period, optional_yield y);
  void clear_current_period_and_epoch() {
    current_period.clear();
    epoch = 0;
  }
  epoch_t get_epoch() const { return epoch; }

  std::string get_control_oid() const;
  /// send a notify on the realm control object
  int notify_zone(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y);
  /// notify the zone of a new period
  int notify_new_period(const DoutPrefixProvider *dpp, const RGWPeriod& period, optional_yield y);

  int find_zone(const DoutPrefixProvider *dpp,
                const rgw_zone_id& zid,
                RGWPeriod *pperiod,
                RGWZoneGroup *pzonegroup,
                bool *pfound,
                optional_yield y) const;
};
WRITE_CLASS_ENCODER(RGWRealm)

struct RGWPeriodLatestEpochInfo {
  epoch_t epoch = 0;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(epoch, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWPeriodLatestEpochInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWPeriodLatestEpochInfo)


/*
 * The RGWPeriod object contains the entire configuration of a
 * RGWRealm, including its RGWZoneGroups and RGWZones. Consistency of
 * this configuration is maintained across all zones by passing around
 * the RGWPeriod object in its JSON representation.
 *
 * If a new configuration changes which zone is the metadata master
 * zone (i.e., master zone of the master zonegroup), then a new
 * RGWPeriod::id (a uuid) is generated, its RGWPeriod::realm_epoch is
 * incremented, and the RGWRealm object is updated to reflect that new
 * current_period id and epoch. If the configuration changes BUT which
 * zone is the metadata master does NOT change, then only the
 * RGWPeriod::epoch is incremented (and the RGWPeriod::id remains the
 * same).
 *
 * When a new RGWPeriod is created with a new RGWPeriod::id (uuid), it
 * is linked back to its predecessor RGWPeriod through the
 * RGWPeriod::predecessor_uuid field, thus creating a "linked
 * list"-like structure of RGWPeriods back to the cluster's creation.
 */
class RGWPeriod
{
  std::string id; //< a uuid
  epoch_t epoch{0};
  std::string predecessor_uuid;
  std::vector<std::string> sync_status;
  RGWPeriodMap period_map;
  RGWPeriodConfig period_config;
  std::string master_zonegroup;
  rgw_zone_id master_zone;

  std::string realm_id;
  std::string realm_name;
  epoch_t realm_epoch{1}; //< realm epoch when period was made current

  CephContext *cct{nullptr};
  RGWSI_SysObj *sysobj_svc{nullptr};

  int read_info(const DoutPrefixProvider *dpp, optional_yield y);
  int read_latest_epoch(const DoutPrefixProvider *dpp,
                        RGWPeriodLatestEpochInfo& epoch_info,
			optional_yield y,
                        RGWObjVersionTracker *objv = nullptr);
  int use_latest_epoch(const DoutPrefixProvider *dpp, optional_yield y);
  int use_current_period();

  const std::string get_period_oid() const;
  const std::string get_period_oid_prefix() const;

  // gather the metadata sync status for each shard; only for use on master zone
  int update_sync_status(const DoutPrefixProvider *dpp, 
                         rgw::sal::Store* store,
                         const RGWPeriod &current_period,
                         std::ostream& error_stream, bool force_if_stale);

public:
  RGWPeriod() {}

  explicit RGWPeriod(const std::string& period_id, epoch_t _epoch = 0)
    : id(period_id), epoch(_epoch) {}

  const std::string& get_id() const { return id; }
  epoch_t get_epoch() const { return epoch; }
  epoch_t get_realm_epoch() const { return realm_epoch; }
  const std::string& get_predecessor() const { return predecessor_uuid; }
  const rgw_zone_id& get_master_zone() const { return master_zone; }
  const std::string& get_master_zonegroup() const { return master_zonegroup; }
  const std::string& get_realm() const { return realm_id; }
  const std::string& get_realm_name() const { return realm_name; }
  const RGWPeriodMap& get_map() const { return period_map; }
  RGWPeriodConfig& get_config() { return period_config; }
  const RGWPeriodConfig& get_config() const { return period_config; }
  const std::vector<std::string>& get_sync_status() const { return sync_status; }
  rgw_pool get_pool(CephContext *cct) const;
  const std::string& get_latest_epoch_oid() const;
  const std::string& get_info_oid_prefix() const;

  void set_user_quota(RGWQuotaInfo& user_quota) {
    period_config.user_quota = user_quota;
  }

  void set_bucket_quota(RGWQuotaInfo& bucket_quota) {
    period_config.bucket_quota = bucket_quota;
  }

  void set_id(const std::string& _id) {
    this->id = _id;
    period_map.id = _id;
  }
  void set_epoch(epoch_t epoch) { this->epoch = epoch; }
  void set_realm_epoch(epoch_t epoch) { realm_epoch = epoch; }

  void set_predecessor(const std::string& predecessor)
  {
    predecessor_uuid = predecessor;
  }

  void set_realm_id(const std::string& _realm_id) {
    realm_id = _realm_id;
  }

  int reflect(const DoutPrefixProvider *dpp, optional_yield y);

  int get_zonegroup(RGWZoneGroup& zonegroup,
		    const std::string& zonegroup_id) const;

  bool is_single_zonegroup() const
  {
      return (period_map.zonegroups.size() <= 1);
  }

  /*
    returns true if there are several zone groups with a least one zone
   */
  bool is_multi_zonegroups_with_zones() const
  {
    int count = 0;
    for (const auto& zg:  period_map.zonegroups) {
      if (zg.second.zones.size() > 0) {
	if (count++ > 0) {
	  return true;
	}
      }
    }
    return false;
  }

  bool find_zone(const DoutPrefixProvider *dpp,
                const rgw_zone_id& zid,
                RGWZoneGroup *pzonegroup,
                optional_yield y) const;

  int get_latest_epoch(const DoutPrefixProvider *dpp, epoch_t& epoch, optional_yield y);
  int set_latest_epoch(const DoutPrefixProvider *dpp, optional_yield y,
		       epoch_t epoch, bool exclusive = false,
                       RGWObjVersionTracker *objv = nullptr);
  // update latest_epoch if the given epoch is higher, else return -EEXIST
  int update_latest_epoch(const DoutPrefixProvider *dpp, epoch_t epoch, optional_yield y);

  int init(const DoutPrefixProvider *dpp, CephContext *_cct, RGWSI_SysObj *_sysobj_svc, const std::string &period_realm_id, optional_yield y,
	   const std::string &period_realm_name = "", bool setup_obj = true);
  int init(const DoutPrefixProvider *dpp, CephContext *_cct, RGWSI_SysObj *_sysobj_svc, optional_yield y, bool setup_obj = true);  

  int create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive = true);
  int delete_obj(const DoutPrefixProvider *dpp, optional_yield y);
  int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int add_zonegroup(const DoutPrefixProvider *dpp, const RGWZoneGroup& zonegroup, optional_yield y);

  void fork();
  int update(const DoutPrefixProvider *dpp, optional_yield y);

  // commit a staging period; only for use on master zone
  int commit(const DoutPrefixProvider *dpp,
	     rgw::sal::Store* store,
             RGWRealm& realm, const RGWPeriod &current_period,
             std::ostream& error_stream, optional_yield y,
	     bool force_if_stale = false);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(epoch, bl);
    encode(realm_epoch, bl);
    encode(predecessor_uuid, bl);
    encode(sync_status, bl);
    encode(period_map, bl);
    encode(master_zone, bl);
    encode(master_zonegroup, bl);
    encode(period_config, bl);
    encode(realm_id, bl);
    encode(realm_name, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(epoch, bl);
    decode(realm_epoch, bl);
    decode(predecessor_uuid, bl);
    decode(sync_status, bl);
    decode(period_map, bl);
    decode(master_zone, bl);
    decode(master_zonegroup, bl);
    decode(period_config, bl);
    decode(realm_id, bl);
    decode(realm_name, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWPeriod*>& o);

  static std::string get_staging_id(const std::string& realm_id) {
    return realm_id + ":staging";
  }
};
WRITE_CLASS_ENCODER(RGWPeriod)

#endif
