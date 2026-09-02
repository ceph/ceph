// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>
#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class ConfigStore; }

struct rgw_admin_period_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string realm_id;
  std::string realm_name;
  std::string period_id;
  std::string period_epoch;
  std::string url;
  std::string access_key;
  std::string secret_key;
  std::string remote;
  std::string quota_scope;
  std::string ratelimit_scope;
  std::optional<std::string> opt_region;
  bool commit = false;
  bool staging = false;
  bool yes_i_really_mean_it = false;
  bool have_max_read_ops = false;
  bool have_max_write_ops = false;
  bool have_max_list_ops = false;
  bool have_max_delete_ops = false;
  bool have_max_read_bytes = false;
  bool have_max_write_bytes = false;
  bool have_max_size = false;
  bool have_max_objects = false;
  int64_t max_read_ops = 0;
  int64_t max_write_ops = 0;
  int64_t max_list_ops = 0;
  int64_t max_delete_ops = 0;
  int64_t max_read_bytes = 0;
  int64_t max_write_bytes = 0;
  int64_t max_size = 0;
  int64_t max_objects = 0;
};

int rgw_admin_period(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     rgw::sal::ConfigStore* cfgstore,
                     rgw::SiteConfig& site,
                     ceph::Formatter* formatter,
                     rgw_admin_period_options& opts);
