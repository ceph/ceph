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
  std::string* realm_id = nullptr;
  std::string* realm_name = nullptr;
  std::string* realm_new_name = nullptr;
  std::string* period_id = nullptr;
  std::string* period_epoch = nullptr;
  std::string* url = nullptr;
  std::string* access_key = nullptr;
  std::string* secret_key = nullptr;
  std::string* remote = nullptr;
  std::string* infile = nullptr;
  std::optional<std::string>* opt_region = nullptr;
  bool set_default = false;
  bool yes_i_really_mean_it = false;
};

int rgw_admin_realm(const DoutPrefixProvider* dpp,
                    rgw::sal::Driver* driver,
                    rgw::sal::ConfigStore* cfgstore,
                    rgw::SiteConfig& site,
                    ceph::Formatter* formatter,
                    const rgw_admin_realm_options& opts);
