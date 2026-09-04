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

struct rgw_admin_realm_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string realm_id;
  std::string realm_name;
  std::string realm_new_name;
  std::string period_id;
  std::string period_epoch;
  std::string url;
  std::string access_key;
  std::string secret_key;
  std::string remote;
  std::string infile;
  std::optional<std::string> opt_region;
  bool set_default = false;
  bool yes_i_really_mean_it = false;
};

int rgw_admin_realm(const DoutPrefixProvider* dpp,
                    rgw::sal::Driver* driver,
                    rgw::sal::ConfigStore* cfgstore,
                    rgw::SiteConfig& site,
                    ceph::Formatter* formatter,
                    rgw_admin_realm_options& opts);
