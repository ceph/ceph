#ifndef CEPH_RGW_ZONE_H
#define CEPH_RGW_ZONE_H

struct RGWNameToId {
  string obj_id;

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
  string default_id;

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

class RGWSystemMetaObj {
protected:
  string id;
  string name;

  CephContext *cct{nullptr};
  RGWSI_SysObj *sysobj_svc{nullptr};

  int store_name(bool exclusive);
  int store_info(bool exclusive);
  int read_info(const string& obj_id, bool old_format = false);
  int read_id(const string& obj_name, string& obj_id);
  int read_default(RGWDefaultSystemMetaObjInfo& default_info,
		   const string& oid);
  /* read and use default id */
  int use_default(bool old_format = false);

public:
  RGWSystemMetaObj() {}
  RGWSystemMetaObj(const string& _name): name(_name) {}
  RGWSystemMetaObj(const string& _id, const string& _name) : id(_id), name(_name) {}
  RGWSystemMetaObj(CephContext *_cct, RGWSI_SysObj *_sysobj_svc): cct(_cct), sysobj_svc(_sysobj_svc){}
  RGWSystemMetaObj(const string& _name, CephContext *_cct, RGWSI_SysObj *_sysobj_svc): name(_name), cct(_cct), sysobj_svc(_sysobj_svc){}

  const string& get_name() const { return name; }
  const string& get_id() const { return id; }

  void set_name(const string& _name) { name = _name;}
  void set_id(const string& _id) { id = _id;}
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

  void reinit_instance(CephContext *_cct, RGWSI_SysObj *_sysobj_svc) {
    cct = _cct;
    sysobj_svc = _sysobj_svc;
  }
  int init(CephContext *_cct, RGWSI_SysObj *_sysobj_svc, bool setup_obj = true, bool old_format = false);
  virtual int read_default_id(string& default_id, bool old_format = false);
  virtual int set_as_default(bool exclusive = false);
  int delete_default();
  virtual int create(bool exclusive = true);
  int delete_obj(bool old_format = false);
  int rename(const string& new_name);
  int update() { return store_info(false);}
  int update_name() { return store_name(false);}
  int read();
  int write(bool exclusive);

  virtual rgw_pool get_pool(CephContext *cct) = 0;
  virtual const string get_default_oid(bool old_format = false) = 0;
  virtual const string& get_names_oid_prefix() = 0;
  virtual const string& get_info_oid_prefix(bool old_format = false) = 0;
  virtual const string& get_predefined_name(CephContext *cct) = 0;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWSystemMetaObj)

struct RGWZonePlacementInfo {
  rgw_pool index_pool;
  rgw_pool data_pool;
  rgw_pool data_extra_pool; /* if not set we should use data_pool */
  RGWBucketIndexType index_type;
  std::string compression_type;

  RGWZonePlacementInfo() : index_type(RGWBIType_Normal) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(6, 1, bl);
    encode(index_pool.to_str(), bl);
    encode(data_pool.to_str(), bl);
    encode(data_extra_pool.to_str(), bl);
    encode((uint32_t)index_type, bl);
    encode(compression_type, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(6, bl);
    string index_pool_str;
    string data_pool_str;
    decode(index_pool_str, bl);
    index_pool = rgw_pool(index_pool_str);
    decode(data_pool_str, bl);
    data_pool = rgw_pool(data_pool_str);
    if (struct_v >= 4) {
      string data_extra_pool_str;
      decode(data_extra_pool_str, bl);
      data_extra_pool = rgw_pool(data_extra_pool_str);
    }
    if (struct_v >= 5) {
      uint32_t it;
      decode(it, bl);
      index_type = (RGWBucketIndexType)it;
    }
    if (struct_v >= 6) {
      decode(compression_type, bl);
    }
    DECODE_FINISH(bl);
  }
  const rgw_pool& get_data_extra_pool() const {
    if (data_extra_pool.empty()) {
      return data_pool;
    }
    return data_extra_pool;
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZonePlacementInfo)

struct RGWZoneParams : RGWSystemMetaObj {
  rgw_pool domain_root;
  rgw_pool metadata_heap;
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

  RGWAccessKey system_key;

  map<string, RGWZonePlacementInfo> placement_pools;

  string realm_id;

  JSONFormattable tier_config;

  RGWZoneParams() : RGWSystemMetaObj() {}
  explicit RGWZoneParams(const string& name) : RGWSystemMetaObj(name){}
  RGWZoneParams(const string& id, const string& name) : RGWSystemMetaObj(id, name) {}
  RGWZoneParams(const string& id, const string& name, const string& _realm_id)
    : RGWSystemMetaObj(id, name), realm_id(_realm_id) {}

  rgw_pool get_pool(CephContext *cct) override;
  const string get_default_oid(bool old_format = false) override;
  const string& get_names_oid_prefix() override;
  const string& get_info_oid_prefix(bool old_format = false) override;
  const string& get_predefined_name(CephContext *cct) override;

  int init(CephContext *_cct, RGWSI_SysObj *_sysobj_svc, bool setup_obj = true,
	   bool old_format = false);
  using RGWSystemMetaObj::init;
  int read_default_id(string& default_id, bool old_format = false) override;
  int set_as_default(bool exclusive = false) override;
  int create_default(bool old_format = false);
  int create(bool exclusive = true) override;
  int fix_pool_names();

  const string& get_compression_type(const string& placement_rule) const;
  
  void encode(bufferlist& bl) const override {
    ENCODE_START(12, 1, bl);
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
    encode(metadata_heap, bl);
    encode(realm_id, bl);
    encode(lc_pool, bl);
    map<string, string, ltstr_nocase> old_tier_config;
    encode(old_tier_config, bl);
    encode(roles_pool, bl);
    encode(reshard_pool, bl);
    encode(otp_pool, bl);
    encode(tier_config, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
    DECODE_START(12, bl);
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
    if (struct_v >= 5)
      decode(metadata_heap, bl);
    if (struct_v >= 6) {
      decode(realm_id, bl);
    }
    if (struct_v >= 7) {
      decode(lc_pool, bl);
    } else {
      lc_pool = log_pool.name + ":lc";
    }
    map<string, string, ltstr_nocase> old_tier_config;
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
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<RGWZoneParams*>& o);

  bool get_placement(const string& placement_id, RGWZonePlacementInfo *placement) const {
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
  bool get_head_data_pool(const string& placement_id, const rgw_obj& obj, rgw_pool *pool) const {
    const rgw_data_placement_target& explicit_placement = obj.bucket.explicit_placement;
    if (!explicit_placement.data_pool.empty()) {
      if (!obj.in_extra_data) {
        *pool = explicit_placement.data_pool;
      } else {
        *pool = explicit_placement.get_data_extra_pool();
      }
      return true;
    }
    if (placement_id.empty()) {
      return false;
    }
    auto iter = placement_pools.find(placement_id);
    if (iter == placement_pools.end()) {
      return false;
    }
    if (!obj.in_extra_data) {
      *pool = iter->second.data_pool;
    } else {
      *pool = iter->second.get_data_extra_pool();
    }
    return true;
  }
};
WRITE_CLASS_ENCODER(RGWZoneParams)

struct RGWZone {
  string id;
  string name;
  list<string> endpoints;
  bool log_meta;
  bool log_data;
  bool read_only;
  string tier_type;

  string redirect_zone;

/**
 * Represents the number of shards for the bucket index object, a value of zero
 * indicates there is no sharding. By default (no sharding, the name of the object
 * is '.dir.{marker}', with sharding, the name is '.dir.{marker}.{sharding_id}',
 * sharding_id is zero-based value. It is not recommended to set a too large value
 * (e.g. thousand) as it increases the cost for bucket listing.
 */
  uint32_t bucket_index_max_shards;

  bool sync_from_all;
  set<string> sync_from; /* list of zones to sync from */

  RGWZone() : log_meta(false), log_data(false), read_only(false), bucket_index_max_shards(0),
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
  static void generate_test_instances(list<RGWZone*>& o);

  bool is_read_only() { return read_only; }

  bool syncs_from(const string& zone_id) const {
    return (sync_from_all || sync_from.find(zone_id) != sync_from.end());
  }
};
WRITE_CLASS_ENCODER(RGWZone)

struct RGWDefaultZoneGroupInfo {
  string default_zonegroup;

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

struct RGWZoneGroupPlacementTarget {
  string name;
  set<string> tags;

  bool user_permitted(list<string>& user_tags) const {
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
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(tags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    decode(tags, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZoneGroupPlacementTarget)


struct RGWZoneGroup : public RGWSystemMetaObj {
  string api_name;
  list<string> endpoints;
  bool is_master = false;

  string master_zone;
  map<string, RGWZone> zones;

  map<string, RGWZoneGroupPlacementTarget> placement_targets;
  string default_placement;

  list<string> hostnames;
  list<string> hostnames_s3website;
  // TODO: Maybe convert hostnames to a map<string,list<string>> for
  // endpoint_type->hostnames
/*
20:05 < _robbat21irssi> maybe I do someting like: if (hostname_map.empty()) { populate all map keys from hostnames; };
20:05 < _robbat21irssi> but that's a later compatability migration planning bit
20:06 < yehudasa> more like if (!hostnames.empty()) {
20:06 < yehudasa> for (list<string>::iterator iter = hostnames.begin(); iter != hostnames.end(); ++iter) {
20:06 < yehudasa> hostname_map["s3"].append(iter->second);
20:07 < yehudasa> hostname_map["s3website"].append(iter->second);
20:07 < yehudasa> s/append/push_back/g
20:08 < _robbat21irssi> inner loop over APIs
20:08 < yehudasa> yeah, probably
20:08 < _robbat21irssi> s3, s3website, swift, swith_auth, swift_website
*/
  map<string, list<string> > api_hostname_map;
  map<string, list<string> > api_endpoints_map;

  string realm_id;

  RGWZoneGroup(): is_master(false){}
  RGWZoneGroup(const std::string &id, const std::string &name):RGWSystemMetaObj(id, name) {}
  explicit RGWZoneGroup(const std::string &_name):RGWSystemMetaObj(_name) {}
  RGWZoneGroup(const std::string &_name, bool _is_master, CephContext *cct, RGWSI_SysObj* sysobj_svc,
	       const string& _realm_id, const list<string>& _endpoints)
    : RGWSystemMetaObj(_name, cct , sysobj_svc), endpoints(_endpoints), is_master(_is_master),
      realm_id(_realm_id) {}

  bool is_master_zonegroup() const { return is_master;}
  void update_master(bool _is_master) {
    is_master = _is_master;
    post_process_params();
  }
  void post_process_params();

  void encode(bufferlist& bl) const override {
    ENCODE_START(4, 1, bl);
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
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
    DECODE_START(4, bl);
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
    DECODE_FINISH(bl);
  }

  int read_default_id(string& default_id, bool old_format = false) override;
  int set_as_default(bool exclusive = false) override;
  int create_default(bool old_format = false);
  int equals(const string& other_zonegroup) const;
  int add_zone(const RGWZoneParams& zone_params, bool *is_master, bool *read_only,
               const list<string>& endpoints, const string *ptier_type,
               bool *psync_from_all, list<string>& sync_from, list<string>& sync_from_rm,
               string *predirect_zone);
  int remove_zone(const std::string& zone_id);
  int rename_zone(const RGWZoneParams& zone_params);
  rgw_pool get_pool(CephContext *cct) override;
  const string get_default_oid(bool old_region_format = false) override;
  const string& get_info_oid_prefix(bool old_region_format = false) override;
  const string& get_names_oid_prefix() override;
  const string& get_predefined_name(CephContext *cct) override;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<RGWZoneGroup*>& o);
};
WRITE_CLASS_ENCODER(RGWZoneGroup)

struct RGWPeriodMap
{
  string id;
  map<string, RGWZoneGroup> zonegroups;
  map<string, RGWZoneGroup> zonegroups_by_api;
  map<string, uint32_t> short_zone_ids;

  string master_zonegroup;

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

  uint32_t get_zone_short_id(const string& zone_id) const;
};
WRITE_CLASS_ENCODER(RGWPeriodMap)

struct RGWPeriodConfig
{
  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket_quota, bl);
    encode(user_quota, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket_quota, bl);
    decode(user_quota, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  // the period config must be stored in a local object outside of the period,
  // so that it can be used in a default configuration where no realm/period
  // exists
  int read(RGWSI_SysObj *sysobj_svc, const std::string& realm_id);
  int write(RGWSI_SysObj *sysobj_svc, const std::string& realm_id);

  static std::string get_oid(const std::string& realm_id);
  static rgw_pool get_pool(CephContext *cct);
};
WRITE_CLASS_ENCODER(RGWPeriodConfig)

/* for backward comaptability */
struct RGWRegionMap {

  map<string, RGWZoneGroup> regions;

  string master_region;

  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRegionMap)

struct RGWZoneGroupMap {

  map<string, RGWZoneGroup> zonegroups;
  map<string, RGWZoneGroup> zonegroups_by_api;

  string master_zonegroup;

  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  /* construct the map */
  int read(CephContext *cct, RGWSI_SysObj *sysobj_svc);

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
  string current_period;
  epoch_t epoch{0}; //< realm epoch, incremented for each new period

  int create_control(bool exclusive);
  int delete_control();
public:
  RGWRealm() {}
  RGWRealm(const string& _id, const string& _name = "") : RGWSystemMetaObj(_id, _name) {}
  RGWRealm(CephContext *_cct, RGWSI_SysObj *_sysobj_svc): RGWSystemMetaObj(_cct, _sysobj_svc) {}
  RGWRealm(const string& _name, CephContext *_cct, RGWSI_SysObj *_sysobj_svc): RGWSystemMetaObj(_name, _cct, _sysobj_svc){}

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

  int create(bool exclusive = true) override;
  int delete_obj();
  rgw_pool get_pool(CephContext *cct) override;
  const string get_default_oid(bool old_format = false) override;
  const string& get_names_oid_prefix() override;
  const string& get_info_oid_prefix(bool old_format = false) override;
  const string& get_predefined_name(CephContext *cct) override;

  using RGWSystemMetaObj::read_id; // expose as public for radosgw-admin

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  const string& get_current_period() const {
    return current_period;
  }
  int set_current_period(RGWPeriod& period);
  void clear_current_period_and_epoch() {
    current_period.clear();
    epoch = 0;
  }
  epoch_t get_epoch() const { return epoch; }

  string get_control_oid();
  /// send a notify on the realm control object
  int notify_zone(bufferlist& bl);
  /// notify the zone of a new period
  int notify_new_period(const RGWPeriod& period);
};
WRITE_CLASS_ENCODER(RGWRealm)

struct RGWPeriodLatestEpochInfo {
  epoch_t epoch;

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
};
WRITE_CLASS_ENCODER(RGWPeriodLatestEpochInfo)

class RGWPeriod
{
  string id;
  epoch_t epoch{0};
  string predecessor_uuid;
  std::vector<std::string> sync_status;
  RGWPeriodMap period_map;
  RGWPeriodConfig period_config;
  string master_zonegroup;
  string master_zone;

  string realm_id;
  string realm_name;
  epoch_t realm_epoch{1}; //< realm epoch when period was made current

  CephContext *cct{nullptr};
  RGWSI_SysObj *sysobj_svc{nullptr};

  int read_info();
  int read_latest_epoch(RGWPeriodLatestEpochInfo& epoch_info,
                        RGWObjVersionTracker *objv = nullptr);
  int use_latest_epoch();
  int use_current_period();

  const string get_period_oid();
  const string get_period_oid_prefix();

  // gather the metadata sync status for each shard; only for use on master zone
  int update_sync_status(const RGWPeriod &current_period,
                         std::ostream& error_stream, bool force_if_stale);

public:
  RGWPeriod() {}

  RGWPeriod(const string& period_id, epoch_t _epoch = 0)
    : id(period_id), epoch(_epoch) {}

  const string& get_id() const { return id; }
  epoch_t get_epoch() const { return epoch; }
  epoch_t get_realm_epoch() const { return realm_epoch; }
  const string& get_predecessor() const { return predecessor_uuid; }
  const string& get_master_zone() const { return master_zone; }
  const string& get_master_zonegroup() const { return master_zonegroup; }
  const string& get_realm() const { return realm_id; }
  const RGWPeriodMap& get_map() const { return period_map; }
  RGWPeriodConfig& get_config() { return period_config; }
  const RGWPeriodConfig& get_config() const { return period_config; }
  const std::vector<std::string>& get_sync_status() const { return sync_status; }
  rgw_pool get_pool(CephContext *cct);
  const string& get_latest_epoch_oid();
  const string& get_info_oid_prefix();

  void set_user_quota(RGWQuotaInfo& user_quota) {
    period_config.user_quota = user_quota;
  }

  void set_bucket_quota(RGWQuotaInfo& bucket_quota) {
    period_config.bucket_quota = bucket_quota;
  }

  void set_id(const string& id) {
    this->id = id;
    period_map.id = id;
  }
  void set_epoch(epoch_t epoch) { this->epoch = epoch; }
  void set_realm_epoch(epoch_t epoch) { realm_epoch = epoch; }

  void set_predecessor(const string& predecessor)
  {
    predecessor_uuid = predecessor;
  }

  void set_realm_id(const string& _realm_id) {
    realm_id = _realm_id;
  }

  int reflect();

  int get_zonegroup(RGWZoneGroup& zonegroup,
		    const string& zonegroup_id);

  bool is_single_zonegroup() const
  {
      return (period_map.zonegroups.size() == 1);
  }

  /*
    returns true if there are several zone groups with a least one zone
   */
  bool is_multi_zonegroups_with_zones()
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

  int get_latest_epoch(epoch_t& epoch);
  int set_latest_epoch(epoch_t epoch, bool exclusive = false,
                       RGWObjVersionTracker *objv = nullptr);
  // update latest_epoch if the given epoch is higher, else return -EEXIST
  int update_latest_epoch(epoch_t epoch);

  int init(CephContext *_cct, RGWSI_SysObj *_sysobj_svc, const string &period_realm_id, const string &period_realm_name = "",
	   bool setup_obj = true);
  int init(CephContext *_cct, RGWSI_SysObj *_sysobj_svc, bool setup_obj = true);  

  int create(bool exclusive = true);
  int delete_obj();
  int store_info(bool exclusive);
  int add_zonegroup(const RGWZoneGroup& zonegroup);

  void fork();
  int update();

  // commit a staging period; only for use on master zone
  int commit(RGWRealm& realm, const RGWPeriod &current_period,
             std::ostream& error_stream, bool force_if_stale = false);

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

  static string get_staging_id(const string& realm_id) {
    return realm_id + ":staging";
  }
};
WRITE_CLASS_ENCODER(RGWPeriod)

#endif
