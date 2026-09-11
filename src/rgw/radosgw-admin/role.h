// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

#include "rgw_basic_types.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; }

struct rgw_admin_role_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string role_name;
  std::string tenant;
  rgw_account_id account_id;
  std::string path;
  std::string assume_role_doc;
  std::string perm_policy_doc;
  std::string policy_name;
  std::string policy_arn;
  std::string description;
  std::string path_prefix;
  std::string max_session_duration;
  std::string marker;
  std::string infile;
  std::optional<int> max_entries;
};

int rgw_admin_role(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   ceph::Formatter* formatter,
                   rgw_admin_role_options& opts);
