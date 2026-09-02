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
#include "rgw_basic_types.h"
#endif

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class ConfigStore; }

struct rgw_admin_zone_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string* zonegroup_id = nullptr;
  std::string* zonegroup_name = nullptr;
  std::string* zone_id = nullptr;
  std::string* zone_name = nullptr;
  std::string* zone_new_name = nullptr;
  std::string* realm_id = nullptr;
  std::string* realm_name = nullptr;
  std::string* placement_id = nullptr;
  std::string* url = nullptr;
  std::string* access_key = nullptr;
  std::string* secret_key = nullptr;
  std::string* infile = nullptr;
  std::list<std::string>* sync_from = nullptr;
  std::list<std::string>* sync_from_rm = nullptr;
  std::list<std::string>* endpoints = nullptr;
  std::string* master_zone = nullptr;
  std::string* format = nullptr;
  std::string* api_name = nullptr;
  std::string* tier_type = nullptr;
  std::string* redirect_zone = nullptr;
  std::map<std::string, std::string, ltstr_nocase>* tier_config_add = nullptr;
  std::map<std::string, std::string, ltstr_nocase>* tier_config_rm = nullptr;
  std::optional<std::string>* index_pool = nullptr;
  std::optional<std::string>* data_pool = nullptr;
  std::optional<std::string>* data_extra_pool = nullptr;
  std::optional<std::string>* compression_type = nullptr;
  std::optional<int>* bucket_index_max_shards = nullptr;
  std::optional<std::string>* opt_storage_class = nullptr;
  std::optional<std::string>* opt_region = nullptr;
  bool tier_type_specified = false;
  int sync_from_all_specified = false;
  bool redirect_zone_set = false;
  bool placement_inline_data = false;
  bool placement_inline_data_specified = false;
  bool set_default = false;
  bool read_only = false;
  bool is_master = false;
  bool is_master_set = false;
  int is_read_only_set = 0;
  bool sync_from_all = false;
  bool yes_i_really_mean_it = false;
  bool num_shards_specified = false;
  int num_shards = 0;
#ifdef WITH_RADOSGW_RADOS
  rgw::BucketIndexType* placement_index_type = nullptr;
  bool index_type_specified = false;
  rgw::zone_features::set* enable_features = nullptr;
  rgw::zone_features::set* disable_features = nullptr;
#endif
};

int rgw_admin_zone(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   rgw::sal::ConfigStore* cfgstore,
                   rgw::SiteConfig& site,
                   ceph::Formatter* formatter,
                   const rgw_admin_zone_options& opts);
