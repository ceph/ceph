// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rgw_basic_types.h"
#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWUserAdminOpState;
namespace ceph { class Formatter; }
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class User; class Bucket; class ConfigStore; }

struct rgw_admin_sync_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  rgw_zone_id* source_zone = nullptr;
  std::string* marker = nullptr;
  std::string* start_marker = nullptr;
  std::string* end_marker = nullptr;
  std::string* start_date = nullptr;
  std::string* end_date = nullptr;
  std::string* period_id = nullptr;
  std::string* realm_id = nullptr;
  std::string* realm_name = nullptr;
  std::string* zonegroup_id = nullptr;
  std::string* zonegroup_name = nullptr;
  std::optional<rgw_zone_id>* opt_effective_zone_id = nullptr;
  std::optional<rgw_bucket>* opt_bucket = nullptr;
  std::optional<std::string>* opt_bucket_name = nullptr;
  std::optional<rgw_zone_id>* opt_source_zone_id = nullptr;
  std::optional<rgw_zone_id>* opt_dest_zone_id = nullptr;
  std::optional<std::string>* opt_source_zone_name = nullptr;
  std::optional<std::string>* opt_dest_zone_name = nullptr;
  std::optional<std::vector<rgw_zone_id>>* opt_zone_ids = nullptr;
  std::optional<std::vector<rgw_zone_id>>* opt_source_zone_ids = nullptr;
  std::optional<std::vector<rgw_zone_id>>* opt_dest_zone_ids = nullptr;
  std::optional<rgw_bucket>* opt_source_bucket = nullptr;
  std::optional<rgw_bucket>* opt_dest_bucket = nullptr;
  std::optional<std::string>* opt_source_tenant = nullptr;
  std::optional<std::string>* opt_dest_tenant = nullptr;
  std::optional<std::string>* opt_source_bucket_name = nullptr;
  std::optional<std::string>* opt_dest_bucket_name = nullptr;
  std::optional<std::string>* opt_source_bucket_id = nullptr;
  std::optional<std::string>* opt_dest_bucket_id = nullptr;
  std::optional<std::string>* opt_pipe_id = nullptr;
  std::optional<std::string>* opt_group_id = nullptr;
  std::optional<std::string>* opt_flow_id = nullptr;
  std::optional<std::string>* opt_flow_type = nullptr;
  std::optional<std::string>* opt_status = nullptr;
  std::optional<std::string>* opt_prefix = nullptr;
  std::optional<std::string>* opt_prefix_rm = nullptr;
  std::optional<rgw_user>* opt_dest_owner = nullptr;
  std::optional<std::string>* opt_storage_class = nullptr;
  std::optional<int>* opt_priority = nullptr;
  std::optional<std::string>* opt_mode = nullptr;
  std::list<std::string>* tags_add = nullptr;
  std::list<std::string>* tags_rm = nullptr;
  std::unique_ptr<rgw::sal::User>* user = nullptr;
  int max_entries = 0;
  int shard_id = 0;
  int trim_delay_ms = 0;
  bool max_entries_specified = false;
  bool specified_shard_id = false;
};

int rgw_admin_sync(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   rgw::sal::ConfigStore* cfgstore,
                   rgw::SiteConfig& site,
                   ceph::Formatter* formatter,
                   ceph::Formatter* zone_formatter,
                   const rgw_admin_sync_options& opts);

void init_optional_bucket(std::optional<rgw_bucket>& opt_bucket,
                          std::optional<std::string>& opt_tenant,
                          std::optional<std::string>& opt_bucket_name,
                          std::optional<std::string>& opt_bucket_id);
