// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <ostream>
#include "rgw_zone_types.h"
#include "rgw_common.h"
#include "rgw_sal_fwd.h"
#include "rgw_sync_policy.h"


class RGWSyncModulesManager;

class RGWSI_SysObj;
class RGWSI_Zone;

class RGWSystemMetaObj {
public:
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
  rgw_pool notif_pool;

  RGWAccessKey system_key;

  std::map<std::string, RGWZonePlacementInfo> placement_pools;

  std::string realm_id;

  JSONFormattable tier_config;

  RGWZoneParams() : RGWSystemMetaObj() {}
  explicit RGWZoneParams(const std::string& name) : RGWSystemMetaObj(name){}
  RGWZoneParams(const rgw_zone_id& id, const std::string& name) : RGWSystemMetaObj(id.id, name) {}
  RGWZoneParams(const rgw_zone_id& id, const std::string& name, const std::string& _realm_id)
    : RGWSystemMetaObj(id.id, name), realm_id(_realm_id) {}
  virtual ~RGWZoneParams();

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
  bool get_head_data_pool(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_pool* pool) const {
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
20:05 < _robbat21irssi> maybe I do something like: if (hostname_map.empty()) { populate all map keys from hostnames; };
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
  rgw::zone_features::set enabled_features;

  RGWZoneGroup(): is_master(false){}
  RGWZoneGroup(const std::string &id, const std::string &name):RGWSystemMetaObj(id, name) {}
  explicit RGWZoneGroup(const std::string &_name):RGWSystemMetaObj(_name) {}
  RGWZoneGroup(const std::string &_name, bool _is_master, CephContext *cct, RGWSI_SysObj* sysobj_svc,
	       const std::string& _realm_id, const std::list<std::string>& _endpoints)
    : RGWSystemMetaObj(_name, cct , sysobj_svc), endpoints(_endpoints), is_master(_is_master),
      realm_id(_realm_id) {}
  virtual ~RGWZoneGroup();

  bool is_master_zonegroup() const { return is_master;}
  void update_master(const DoutPrefixProvider *dpp, bool _is_master, optional_yield y) {
    is_master = _is_master;
    post_process_params(dpp, y);
  }
  void post_process_params(const DoutPrefixProvider *dpp, optional_yield y);

  void encode(bufferlist& bl) const override {
    ENCODE_START(6, 1, bl);
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
    encode(enabled_features, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
    DECODE_START(6, bl);
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
    if (struct_v >= 6) {
      decode(enabled_features, bl);
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
               const rgw::zone_features::set& enable_features,
               const rgw::zone_features::set& disable_features,
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

  bool supports(std::string_view feature) const {
    return enabled_features.contains(feature);
  }
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
  RGWQuota quota;
  RGWRateLimitInfo user_ratelimit;
  RGWRateLimitInfo bucket_ratelimit;
  // rate limit unauthenticated user
  RGWRateLimitInfo anon_ratelimit;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(quota.bucket_quota, bl);
    encode(quota.user_quota, bl);
    encode(bucket_ratelimit, bl);
    encode(user_ratelimit, bl);
    encode(anon_ratelimit, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(quota.bucket_quota, bl);
    decode(quota.user_quota, bl);
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

class RGWRealm;
class RGWPeriod;

class RGWRealm : public RGWSystemMetaObj
{
public:
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
public:
  std::string id; //< a uuid
  epoch_t epoch{0};
  std::string predecessor_uuid;
  std::vector<std::string> sync_status;
  RGWPeriodMap period_map;
  RGWPeriodConfig period_config;
  std::string master_zonegroup;
  rgw_zone_id master_zone;

  std::string realm_id;
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
                         rgw::sal::Driver* driver,
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
  const RGWPeriodMap& get_map() const { return period_map; }
  RGWPeriodConfig& get_config() { return period_config; }
  const RGWPeriodConfig& get_config() const { return period_config; }
  const std::vector<std::string>& get_sync_status() const { return sync_status; }
  rgw_pool get_pool(CephContext *cct) const;
  const std::string& get_latest_epoch_oid() const;
  const std::string& get_info_oid_prefix() const;

  void set_user_quota(RGWQuotaInfo& user_quota) {
    period_config.quota.user_quota = user_quota;
  }

  void set_bucket_quota(RGWQuotaInfo& bucket_quota) {
    period_config.quota.bucket_quota = bucket_quota;
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
	    bool setup_obj = true);
  int init(const DoutPrefixProvider *dpp, CephContext *_cct, RGWSI_SysObj *_sysobj_svc, optional_yield y, bool setup_obj = true);  

  int create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive = true);
  int delete_obj(const DoutPrefixProvider *dpp, optional_yield y);
  int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int add_zonegroup(const DoutPrefixProvider *dpp, const RGWZoneGroup& zonegroup, optional_yield y);

  void fork();
  int update(const DoutPrefixProvider *dpp, optional_yield y);

  // commit a staging period; only for use on master zone
  int commit(const DoutPrefixProvider *dpp,
	     rgw::sal::Driver* driver,
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
    std::string realm_name; // removed
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
    std::string realm_name; // removed
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

namespace rgw {

/// Look up a realm by its id. If no id is given, look it up by name.
/// If no name is given, fall back to the cluster's default realm.
int read_realm(const DoutPrefixProvider* dpp, optional_yield y,
               sal::ConfigStore* cfgstore,
               std::string_view realm_id,
               std::string_view realm_name,
               RGWRealm& info,
               std::unique_ptr<sal::RealmWriter>* writer = nullptr);

/// Create a realm and its initial period. If the info.id is empty, a
/// random uuid will be generated.
int create_realm(const DoutPrefixProvider* dpp, optional_yield y,
                 sal::ConfigStore* cfgstore, bool exclusive,
                 RGWRealm& info,
                 std::unique_ptr<sal::RealmWriter>* writer = nullptr);

/// Set the given realm as the cluster's default realm.
int set_default_realm(const DoutPrefixProvider* dpp, optional_yield y,
                      sal::ConfigStore* cfgstore, const RGWRealm& info,
                      bool exclusive = false);

/// Update the current_period of an existing realm.
int realm_set_current_period(const DoutPrefixProvider* dpp, optional_yield y,
                             sal::ConfigStore* cfgstore,
                             sal::RealmWriter& writer, RGWRealm& realm,
                             const RGWPeriod& period);

/// Overwrite the local zonegroup and period config objects with the new
/// configuration contained in the given period.
int reflect_period(const DoutPrefixProvider* dpp, optional_yield y,
                   sal::ConfigStore* cfgstore, const RGWPeriod& info);

/// Return the staging period id for the given realm.
std::string get_staging_period_id(std::string_view realm_id);

/// Convert the given period into a separate staging period, where
/// radosgw-admin can make changes to it without effecting the running
/// configuration.
void fork_period(const DoutPrefixProvider* dpp, RGWPeriod& info);

/// Read all zonegroups in the period's realm and add them to the period.
int update_period(const DoutPrefixProvider* dpp, optional_yield y,
                  sal::ConfigStore* cfgstore, RGWPeriod& info);

/// Validates the given 'staging' period and tries to commit it as the
/// realm's new current period.
int commit_period(const DoutPrefixProvider* dpp, optional_yield y,
                  sal::ConfigStore* cfgstore, sal::Driver* driver,
                  RGWRealm& realm, sal::RealmWriter& realm_writer,
                  const RGWPeriod& current_period,
                  RGWPeriod& info, std::ostream& error_stream,
                  bool force_if_stale);


/// Look up a zonegroup by its id. If no id is given, look it up by name.
/// If no name is given, fall back to the cluster's default zonegroup.
int read_zonegroup(const DoutPrefixProvider* dpp, optional_yield y,
                   sal::ConfigStore* cfgstore,
                   std::string_view zonegroup_id,
                   std::string_view zonegroup_name,
                   RGWZoneGroup& info,
                   std::unique_ptr<sal::ZoneGroupWriter>* writer = nullptr);

/// Initialize and create the given zonegroup. If the given info.id is empty,
/// a random uuid will be generated. May fail with -EEXIST.
int create_zonegroup(const DoutPrefixProvider* dpp, optional_yield y,
                     sal::ConfigStore* cfgstore, bool exclusive,
                     RGWZoneGroup& info);

/// Set the given zonegroup as its realm's default zonegroup.
int set_default_zonegroup(const DoutPrefixProvider* dpp, optional_yield y,
                          sal::ConfigStore* cfgstore, const RGWZoneGroup& info,
                          bool exclusive = false);

/// Return an endpoint from the zonegroup or its master zone.
std::string get_zonegroup_endpoint(const RGWZoneGroup& info);

/// Add a zone to the zonegroup, or update an existing zone entry.
int add_zone_to_group(const DoutPrefixProvider* dpp,
                      RGWZoneGroup& zonegroup,
                      const RGWZoneParams& zone_params,
                      const bool *pis_master, const bool *pread_only,
                      const std::list<std::string>& endpoints,
                      const std::string *ptier_type,
                      const bool *psync_from_all,
                      const std::list<std::string>& sync_from,
                      const std::list<std::string>& sync_from_rm,
                      const std::string *predirect_zone,
                      std::optional<int> bucket_index_max_shards,
                      const rgw::zone_features::set& enable_features,
                      const rgw::zone_features::set& disable_features);

/// Remove a zone by id from its zonegroup, promoting a new master zone if
/// necessary.
int remove_zone_from_group(const DoutPrefixProvider* dpp,
                           RGWZoneGroup& info,
                           const rgw_zone_id& zone_id);


/// Look up a zone by its id. If no id is given, look it up by name. If no name
/// is given, fall back to the realm's default zone.
int read_zone(const DoutPrefixProvider* dpp, optional_yield y,
              sal::ConfigStore* cfgstore,
              std::string_view zone_id,
              std::string_view zone_name,
              RGWZoneParams& info,
              std::unique_ptr<sal::ZoneWriter>* writer = nullptr);

/// Initialize and create a new zone. If the given info.id is empty, a random
/// uuid will be generated. Pool names are initialized with the zone name as a
/// prefix. If any pool names conflict with existing zones, a random suffix is
/// added.
int create_zone(const DoutPrefixProvider* dpp, optional_yield y,
                sal::ConfigStore* cfgstore, bool exclusive,
                RGWZoneParams& info,
                std::unique_ptr<sal::ZoneWriter>* writer = nullptr);

/// Initialize the zone's pool names using the zone name as a prefix. If a pool
/// name conflicts with an existing zone's pool, add a unique suffix.
int init_zone_pool_names(const DoutPrefixProvider *dpp, optional_yield y,
                         const std::set<rgw_pool>& pools, RGWZoneParams& info);

/// Set the given zone as its realm's default zone.
int set_default_zone(const DoutPrefixProvider* dpp, optional_yield y,
                      sal::ConfigStore* cfgstore, const RGWZoneParams& info,
                      bool exclusive = false);

/// Delete an existing zone and remove it from any zonegroups that contain it.
int delete_zone(const DoutPrefixProvider* dpp, optional_yield y,
                sal::ConfigStore* cfgstore, const RGWZoneParams& info,
                sal::ZoneWriter& writer);

/// Return the zone placement corresponding to the given rule, or nullptr.
auto find_zone_placement(const DoutPrefixProvider* dpp,
                         const RGWZoneParams& info,
                         const rgw_placement_rule& rule)
    -> const RGWZonePlacementInfo*;


/// Global state about the site configuration. Initialized once during
/// startup and may be reinitialized by RGWRealmReloader, but is otherwise
/// immutable at runtime.
class SiteConfig {
 public:
  /// Return the local zone params.
  const RGWZoneParams& get_zone_params() const { return zone_params; }
  /// Return the current realm configuration, if a realm is present.
  const std::optional<RGWRealm>& get_realm() const { return realm; }
  /// Return the current period configuration, if a period is present.
  const std::optional<RGWPeriod>& get_period() const { return period; }
  /// Return the zonegroup configuration.
  const RGWZoneGroup& get_zonegroup() const { return *zonegroup; }
  /// Return the public zone configuration.
  const RGWZone& get_zone() const { return *zone; }
  /// Return true if the local zone can write metadata.
  bool is_meta_master() const {
    return zonegroup->is_master && zonegroup->master_zone == zone->id;
  }

  /// Load or reload the multisite configuration from storage. This is not
  /// thread-safe, so requires careful coordination with the RGWRealmReloader.
  int load(const DoutPrefixProvider* dpp, optional_yield y,
           sal::ConfigStore* cfgstore, bool force_local_zonegroup = false);

  /// Create a fake site config to be used by tests and similar, just
  /// to have a site config.
  ///
  /// \warning Do not use this anywhere but unittests where we need to
  /// bring up parts of RGW that require a SiteConfig exist, but need
  /// to run without a cluster.
  static std::unique_ptr<SiteConfig> make_fake();

  virtual ~SiteConfig() = default;

 private:
  int load_period_zonegroup(const DoutPrefixProvider* dpp, optional_yield y,
                            sal::ConfigStore* cfgstore, const RGWRealm& realm,
                            const rgw_zone_id& zone_id);
  int load_local_zonegroup(const DoutPrefixProvider* dpp, optional_yield y,
                           sal::ConfigStore* cfgstore,
                           const rgw_zone_id& zone_id);

  RGWZoneParams zone_params;
  std::optional<RGWRealm> realm;
  std::optional<RGWPeriod> period;
  std::optional<RGWZoneGroup> local_zonegroup;
  const RGWZoneGroup* zonegroup = nullptr;
  const RGWZone* zone = nullptr;
};


} // namespace rgw
