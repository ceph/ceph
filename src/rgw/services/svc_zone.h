// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"


class RGWSI_RADOS;
class RGWSI_SysObj;
class RGWSI_SyncModules;

class RGWRealm;
class RGWZoneGroup;
class RGWZone;
class RGWZoneParams;
class RGWPeriod;
class RGWZonePlacementInfo;

class RGWRESTConn;

class RGWSI_Zone : public RGWServiceInstance
{
  friend struct RGWServices_Def;

  RGWSI_SysObj *sysobj_svc{nullptr};
  RGWSI_RADOS *rados_svc{nullptr};
  RGWSI_SyncModules *sync_modules_svc{nullptr};

  RGWRealm *realm{nullptr};
  RGWZoneGroup *zonegroup{nullptr};
  RGWZone *zone_public_config{nullptr}; /* external zone params, e.g., entrypoints, log flags, etc. */  
  RGWZoneParams *zone_params{nullptr}; /* internal zone params, e.g., rados pools */
  RGWPeriod *current_period{nullptr};
  uint32_t zone_short_id{0};
  bool writeable_zone{false};

  RGWRESTConn *rest_master_conn{nullptr};
  std::map<std::string, RGWRESTConn *> zone_conn_map;
  std::vector<const RGWZone*> data_sync_source_zones;
  std::map<std::string, RGWRESTConn *> zone_data_notify_to_map;
  std::map<std::string, RGWRESTConn *> zonegroup_conn_map;

  map<string, string> zone_id_by_name;
  map<string, RGWZone> zone_by_id;

  void init(RGWSI_SysObj *_sysobj_svc,
           RGWSI_RADOS *_rados_svc,
           RGWSI_SyncModules *_sync_modules_svc);
  boost::system::error_code do_start() override;
  void shutdown() override;

  boost::system::error_code replace_region_with_zonegroup(optional_yield y);
  boost::system::error_code init_zg_from_period(bool *initialized);
  boost::system::error_code init_zg_from_local(bool *creating_defaults);
  boost::system::error_code convert_regionmap();

  boost::system::error_code update_placement_map();
  boost::system::error_code lister(std::string prefix,
				   std::vector<std::string>& v,
				   optional_yield y);

public:
  RGWSI_Zone(CephContext *cct, boost::asio::io_context& ioc);
  ~RGWSI_Zone();

  const RGWZoneParams& get_zone_params() const;
  const RGWPeriod& get_current_period() const;
  const RGWRealm& get_realm() const;
  const RGWZoneGroup& get_zonegroup() const;
  boost::system::error_code get_zonegroup(const string& id, RGWZoneGroup& zonegroup) const;
  const RGWZone& get_zone() const;

  const string& zone_name();
  const string& zone_id();
  uint32_t get_zone_short_id() const;

  const string& get_current_period_id();
  bool has_zonegroup_api(const std::string& api) const;

  bool zone_is_writeable();
  bool zone_syncs_from(const RGWZone& target_zone, const RGWZone& source_zone) const;
  bool get_redirect_zone_endpoint(string *endpoint);
  bool sync_module_supports_writes() const;

  RGWRESTConn *get_master_conn() {
    return rest_master_conn;
  }

  map<string, RGWRESTConn *>& get_zonegroup_conn_map() {
    return zonegroup_conn_map;
  }

  map<string, RGWRESTConn *>& get_zone_conn_map() {
    return zone_conn_map;
  }

  std::vector<const RGWZone*>& get_data_sync_source_zones() {
    return data_sync_source_zones;
  }

  map<string, RGWRESTConn *>& get_zone_data_notify_to_map() {
    return zone_data_notify_to_map;
  }

  bool find_zone_by_id(const string& id, RGWZone **zone);

  RGWRESTConn *get_zone_conn_by_id(const string& id);
  RGWRESTConn *get_zone_conn_by_name(const string& name);
  bool find_zone_id_by_name(const string& name, string *id);

  boost::system::error_code select_bucket_placement(const RGWUserInfo& user_info, const string& zonegroup_id,
                                                    const rgw_placement_rule& rule,
                                                    rgw_placement_rule *pselected_rule,
                                                    RGWZonePlacementInfo *rule_info);
  boost::system::error_code select_legacy_bucket_placement(RGWZonePlacementInfo *rule_info);
  boost::system::error_code select_new_bucket_location(const RGWUserInfo& user_info, const string& zonegroup_id,
                                                       const rgw_placement_rule& rule,
                                                       rgw_placement_rule *pselected_rule_name,
                                                       RGWZonePlacementInfo *rule_info);
  boost::system::error_code select_bucket_location_by_rule(const rgw_placement_rule& location_rule, RGWZonePlacementInfo *rule_info);

  boost::system::error_code add_bucket_placement(const rgw_pool& new_pool);
  boost::system::error_code remove_bucket_placement(const rgw_pool& old_pool);
  boost::system::error_code list_placement_set(boost::container::flat_set<rgw_pool>& names);

  bool is_meta_master() const;

  bool need_to_sync() const;
  bool need_to_log_data() const;
  bool need_to_log_metadata() const;
  bool can_reshard() const;
  bool is_syncing_bucket_meta(const rgw_bucket& bucket);

  boost::system::error_code list_zonegroups(std::vector<string>& zonegroups,
					    optional_yield y);
  boost::system::error_code list_regions(std::vector<std::string>& regions,
					 optional_yield y);
  boost::system::error_code list_zones(std::vector<string>& zones,
				       optional_yield y);
  boost::system::error_code list_realms(std::vector<string>& realms,
					optional_yield y);
  boost::system::error_code list_periods(std::vector<string>& periods,
					 optional_yield y);
  boost::system::error_code list_periods(const std::string& current_period,
					 std::vector<string>& periods);
};
