// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"


class RGWSI_RADOS;
class RGWSI_SysObj;
class RGWSI_SyncModules;
class RGWSI_Bucket_Sync;

class RGWRealm;
class RGWZoneGroup;
class RGWZone;
class RGWZoneParams;
class RGWPeriod;
class RGWZonePlacementInfo;

class RGWBucketSyncPolicyHandler;

class RGWRESTConn;

struct rgw_sync_policy_info;

class RGWSI_Zone : public RGWServiceInstance
{
  friend struct RGWServices_Def;

  RGWSI_SysObj *sysobj_svc{nullptr};
  RGWSI_RADOS *rados_svc{nullptr};
  RGWSI_SyncModules *sync_modules_svc{nullptr};
  RGWSI_Bucket_Sync *bucket_sync_svc{nullptr};

  RGWRealm *realm{nullptr};
  RGWZoneGroup *zonegroup{nullptr};
  RGWZone *zone_public_config{nullptr}; /* external zone params, e.g., entrypoints, log flags, etc. */  
  RGWZoneParams *zone_params{nullptr}; /* internal zone params, e.g., rados pools */
  RGWPeriod *current_period{nullptr};
  rgw_zone_id cur_zone_id;
  uint32_t zone_short_id{0};
  bool writeable_zone{false};
  bool exports_data{false};

  std::shared_ptr<RGWBucketSyncPolicyHandler> sync_policy_handler;
  std::map<rgw_zone_id, std::shared_ptr<RGWBucketSyncPolicyHandler> > sync_policy_handlers;

  RGWRESTConn *rest_master_conn{nullptr};
  map<rgw_zone_id, RGWRESTConn *> zone_conn_map;
  std::vector<const RGWZone*> data_sync_source_zones;
  map<rgw_zone_id, RGWRESTConn *> zone_data_notify_to_map;
  map<string, RGWRESTConn *> zonegroup_conn_map;

  map<string, rgw_zone_id> zone_id_by_name;
  map<rgw_zone_id, RGWZone> zone_by_id;

  std::unique_ptr<rgw_sync_policy_info> sync_policy;

  void init(RGWSI_SysObj *_sysobj_svc,
	    RGWSI_RADOS *_rados_svc,
	    RGWSI_SyncModules *_sync_modules_svc,
	    RGWSI_Bucket_Sync *_bucket_sync_svc);
  int do_start(optional_yield y, const DoutPrefixProvider *dpp) override;
  void shutdown() override;

  int replace_region_with_zonegroup(const DoutPrefixProvider *dpp, optional_yield y);
  int init_zg_from_period(bool *initialized, optional_yield y);
  int init_zg_from_local(const DoutPrefixProvider *dpp, bool *creating_defaults, optional_yield y);
  int convert_regionmap(const DoutPrefixProvider *dpp, optional_yield y);

  int update_placement_map(optional_yield y);
public:
  RGWSI_Zone(CephContext *cct);
  ~RGWSI_Zone();

  const RGWZoneParams& get_zone_params() const;
  const RGWPeriod& get_current_period() const;
  const RGWRealm& get_realm() const;
  const RGWZoneGroup& get_zonegroup() const;
  int get_zonegroup(const string& id, RGWZoneGroup& zonegroup) const;
  const RGWZone& get_zone() const;

  std::shared_ptr<RGWBucketSyncPolicyHandler> get_sync_policy_handler(std::optional<rgw_zone_id> zone = nullopt) const;

  const string& zone_name() const;
  const rgw_zone_id& zone_id() const {
    return cur_zone_id;
  }
  uint32_t get_zone_short_id() const;

  const string& get_current_period_id() const;
  bool has_zonegroup_api(const std::string& api) const;

  bool zone_is_writeable();
  bool zone_syncs_from(const RGWZone& target_zone, const RGWZone& source_zone) const;
  bool get_redirect_zone_endpoint(string *endpoint);
  bool sync_module_supports_writes() const { return writeable_zone; }
  bool sync_module_exports_data() const { return exports_data; }

  RGWRESTConn *get_master_conn() {
    return rest_master_conn;
  }

  map<string, RGWRESTConn *>& get_zonegroup_conn_map() {
    return zonegroup_conn_map;
  }

  map<rgw_zone_id, RGWRESTConn *>& get_zone_conn_map() {
    return zone_conn_map;
  }

  std::vector<const RGWZone*>& get_data_sync_source_zones() {
    return data_sync_source_zones;
  }

  map<rgw_zone_id, RGWRESTConn *>& get_zone_data_notify_to_map() {
    return zone_data_notify_to_map;
  }

  bool find_zone(const rgw_zone_id& id, RGWZone **zone);

  RGWRESTConn *get_zone_conn(const rgw_zone_id& zone_id);
  RGWRESTConn *get_zone_conn_by_name(const string& name);
  bool find_zone_id_by_name(const string& name, rgw_zone_id *id);

  int select_bucket_placement(const DoutPrefixProvider *dpp, const RGWUserInfo& user_info, const string& zonegroup_id,
                              const rgw_placement_rule& rule,
                              rgw_placement_rule *pselected_rule, RGWZonePlacementInfo *rule_info, optional_yield y);
  int select_legacy_bucket_placement(const DoutPrefixProvider *dpp, RGWZonePlacementInfo *rule_info, optional_yield y);
  int select_new_bucket_location(const DoutPrefixProvider *dpp, const RGWUserInfo& user_info, const string& zonegroup_id,
                                 const rgw_placement_rule& rule,
                                 rgw_placement_rule *pselected_rule_name, RGWZonePlacementInfo *rule_info,
				 optional_yield y);
  int select_bucket_location_by_rule(const DoutPrefixProvider *dpp, const rgw_placement_rule& location_rule, RGWZonePlacementInfo *rule_info, optional_yield y);

  int add_bucket_placement(const DoutPrefixProvider *dpp, const rgw_pool& new_pool, optional_yield y);
  int remove_bucket_placement(const rgw_pool& old_pool, optional_yield y);
  int list_placement_set(set<rgw_pool>& names, optional_yield y);

  bool is_meta_master() const;

  bool need_to_sync() const;
  bool need_to_log_data() const;
  bool need_to_log_metadata() const;
  bool can_reshard() const;
  bool is_syncing_bucket_meta(const rgw_bucket& bucket);

  int list_zonegroups(list<string>& zonegroups);
  int list_regions(list<string>& regions);
  int list_zones(list<string>& zones);
  int list_realms(list<string>& realms);
  int list_periods(list<string>& periods);
  int list_periods(const string& current_period, list<string>& periods, optional_yield y);
};
