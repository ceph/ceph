// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <list>
#include <map>
#include <optional>
#include <string>
#include "rgw/rgw_string.h"
#include "radosgw-admin/radosgw-admin.h"
#ifdef WITH_RADOSGW_RADOS
#include "rgw_zone_features.h"
#endif

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class ConfigStore; }

struct rgw_admin_zonegroup_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string zonegroup_id;
  std::string zonegroup_name;
  std::string zonegroup_new_name;
  std::string zone_id;
  std::string zone_name;
  std::string realm_id;
  std::string realm_name;
  std::string placement_id;
  std::string infile;
  std::list<std::string> tags;
  std::list<std::string> tags_add;
  std::list<std::string> tags_rm;
  std::list<std::string> sync_from;
  std::list<std::string> sync_from_rm;
  std::list<std::string> endpoints;
  std::string master_zone;
  std::string api_name;
  std::string tier_type;
  std::string redirect_zone;
  std::map<std::string, std::string, ltstr_nocase> tier_config_add;
  std::map<std::string, std::string, ltstr_nocase> tier_config_rm;
  std::optional<std::string> opt_storage_class;
  std::optional<int> bucket_index_max_shards;
  bool tier_type_specified = false;
  int sync_from_all_specified = false;
  bool redirect_zone_set = false;
  bool set_default = false;
  bool read_only = false;
  bool is_master = false;
  bool sync_from_all = false;
  bool is_master_set = false;
  int is_read_only_set = 0;
  bool yes_i_really_mean_it = false;
#ifdef WITH_RADOSGW_RADOS
  rgw::zone_features::set enable_features;
  rgw::zone_features::set disable_features;
#endif
};

int rgw_admin_zonegroup(const DoutPrefixProvider* dpp,
                        rgw::sal::Driver* driver,
                        rgw::sal::ConfigStore* cfgstore,
                        rgw::SiteConfig& site,
                        ceph::Formatter* formatter,
                        rgw_admin_zonegroup_options& opts);
