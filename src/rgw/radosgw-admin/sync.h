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
  rgw_zone_id source_zone;
  std::string marker;
  std::string start_marker;
  std::string end_marker;
  std::string start_date;
  std::string end_date;
  std::string period_id;
  std::string realm_id;
  std::string realm_name;
  std::string zonegroup_id;
  std::string zonegroup_name;
  std::optional<rgw_zone_id> opt_effective_zone_id;
  std::optional<rgw_bucket> opt_bucket;
  std::optional<std::string> opt_bucket_name;
  std::optional<rgw_zone_id> opt_source_zone_id;
  std::optional<rgw_zone_id> opt_dest_zone_id;
  std::optional<std::string> opt_source_zone_name;
  std::optional<std::string> opt_dest_zone_name;
  std::optional<std::vector<rgw_zone_id>> opt_zone_ids;
  std::optional<std::vector<rgw_zone_id>> opt_source_zone_ids;
  std::optional<std::vector<rgw_zone_id>> opt_dest_zone_ids;
  std::optional<rgw_bucket> opt_source_bucket;
  std::optional<rgw_bucket> opt_dest_bucket;
  std::optional<std::string> opt_source_tenant;
  std::optional<std::string> opt_dest_tenant;
  std::optional<std::string> opt_source_bucket_name;
  std::optional<std::string> opt_dest_bucket_name;
  std::optional<std::string> opt_source_bucket_id;
  std::optional<std::string> opt_dest_bucket_id;
  std::optional<std::string> opt_pipe_id;
  std::optional<std::string> opt_group_id;
  std::optional<std::string> opt_flow_id;
  std::optional<std::string> opt_flow_type;
  std::optional<std::string> opt_status;
  std::optional<std::string> opt_prefix;
  std::optional<std::string> opt_prefix_rm;
  std::optional<rgw_user> opt_dest_owner;
  std::optional<std::string> opt_storage_class;
  std::optional<int> opt_priority;
  std::optional<std::string> opt_mode;
  std::list<std::string> tags_add;
  std::list<std::string> tags_rm;
  std::unique_ptr<rgw::sal::User> user;
  std::optional<int> max_entries;
  int shard_id = 0;
  int trim_delay_ms = 0;
  bool specified_shard_id = false;
};

int rgw_admin_sync(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   rgw::sal::ConfigStore* cfgstore,
                   rgw::SiteConfig& site,
                   ceph::Formatter* formatter,
                   ceph::Formatter* zone_formatter,
                   rgw_admin_sync_options& opts);

void init_optional_bucket(std::optional<rgw_bucket>& opt_bucket,
                          std::optional<std::string>& opt_tenant,
                          std::optional<std::string>& opt_bucket_name,
                          std::optional<std::string>& opt_bucket_id);
