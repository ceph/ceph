// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_service.h"


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
  librados::Rados* rados{nullptr};
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
  std::map<rgw_zone_id, RGWRESTConn *> zone_conn_map;
  std::vector<const RGWZone*> data_sync_source_zones;
  std::map<rgw_zone_id, RGWRESTConn *> zone_data_notify_to_map;
  std::map<std::string, RGWRESTConn *> zonegroup_conn_map;

  std::map<std::string, rgw_zone_id> zone_id_by_name;
  std::map<rgw_zone_id, RGWZone> zone_by_id;

  std::unique_ptr<rgw_sync_policy_info> sync_policy;

  void init(RGWSI_SysObj *_sysobj_svc,
	    librados::Rados* rados_,
	    RGWSI_SyncModules *_sync_modules_svc,
	    RGWSI_Bucket_Sync *_bucket_sync_svc);
  int do_start(optional_yield y, const DoutPrefixProvider *dpp) override;
  void shutdown() override;

  int init_zg_from_period(const DoutPrefixProvider *dpp, optional_yield y);
  int init_zg_from_local(const DoutPrefixProvider *dpp, optional_yield y);

  int create_default_zg(const DoutPrefixProvider *dpp, optional_yield y);
  int init_default_zone(const DoutPrefixProvider *dpp, optional_yield y);

  int search_realm_with_zone(const DoutPrefixProvider *dpp,
                             const rgw_zone_id& zid,
                             RGWRealm *prealm,
                             RGWPeriod *pperiod,
                             RGWZoneGroup *pzonegroup,
                             bool *pfound,
                             optional_yield y);
public:
  RGWSI_Zone(CephContext *cct);
  ~RGWSI_Zone();

  const RGWZoneParams& get_zone_params() const;
  const RGWPeriod& get_current_period() const;
  const RGWRealm& get_realm() const;
  const RGWZoneGroup& get_zonegroup() const;
  int get_zonegroup(const std::string& id, RGWZoneGroup& zonegroup) const;
  const RGWZone& get_zone() const;

  std::shared_ptr<RGWBucketSyncPolicyHandler> get_sync_policy_handler(std::optional<rgw_zone_id> zone = std::nullopt) const;

  const std::string& zone_name() const;
  const rgw_zone_id& zone_id() const {
    return cur_zone_id;
  }
  uint32_t get_zone_short_id() const;

  const std::string& get_current_period_id() const;
  bool has_zonegroup_api(const std::string& api) const;

  bool zone_is_writeable();
  bool zone_syncs_from(const RGWZone& target_zone, const RGWZone& source_zone) const;
  bool zone_syncs_from(const RGWZone& source_zone) const;
  bool get_redirect_zone_endpoint(std::string *endpoint);
  bool sync_module_supports_writes() const { return writeable_zone; }
  bool sync_module_exports_data() const { return exports_data; }

  RGWRESTConn *get_master_conn() {
    return rest_master_conn;
  }

  std::map<std::string, RGWRESTConn *>& get_zonegroup_conn_map() {
    return zonegroup_conn_map;
  }

  std::map<rgw_zone_id, RGWRESTConn *>& get_zone_conn_map() {
    return zone_conn_map;
  }

  std::vector<const RGWZone*>& get_data_sync_source_zones() {
    return data_sync_source_zones;
  }

  std::map<rgw_zone_id, RGWRESTConn *>& get_zone_data_notify_to_map() {
    return zone_data_notify_to_map;
  }

  RGWZone* find_zone(const rgw_zone_id& id);

  RGWRESTConn *get_zone_conn(const rgw_zone_id& zone_id);
  RGWRESTConn *get_zone_conn_by_name(const std::string& name);
  bool find_zone_id_by_name(const std::string& name, rgw_zone_id *id);

  int select_bucket_placement(const DoutPrefixProvider *dpp, const RGWUserInfo& user_info, const std::string& zonegroup_id,
                              const rgw_placement_rule& rule,
                              rgw_placement_rule *pselected_rule, RGWZonePlacementInfo *rule_info, optional_yield y);
  int select_new_bucket_location(const DoutPrefixProvider *dpp, const RGWUserInfo& user_info, const std::string& zonegroup_id,
                                 const rgw_placement_rule& rule,
                                 rgw_placement_rule *pselected_rule_name, RGWZonePlacementInfo *rule_info,
				 optional_yield y);
  int select_bucket_location_by_rule(const DoutPrefixProvider *dpp, const rgw_placement_rule& location_rule, RGWZonePlacementInfo *rule_info, optional_yield y);

  bool is_meta_master() const;

  bool need_to_sync() const;
  bool need_to_log_data() const;
  bool need_to_log_metadata() const;
  bool can_reshard() const;
  bool is_syncing_bucket_meta(const rgw_bucket& bucket);

  int list_zonegroups(const DoutPrefixProvider *dpp, std::list<std::string>& zonegroups);
  int list_regions(const DoutPrefixProvider *dpp, std::list<std::string>& regions);
  int list_zones(const DoutPrefixProvider *dpp, std::list<std::string>& zones);
  int list_realms(const DoutPrefixProvider *dpp, std::list<std::string>& realms);
  int list_periods(const DoutPrefixProvider *dpp, std::list<std::string>& periods);
  int list_periods(const DoutPrefixProvider *dpp, const std::string& current_period, std::list<std::string>& periods, optional_yield y);
};
