// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>

#include "radosgw-admin/radosgw-admin.h"

#include "rgw_basic_types.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; }

struct rgw_admin_role_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string* role_name = nullptr;
  std::string* tenant = nullptr;
  rgw_account_id* account_id = nullptr;
  std::string* path = nullptr;
  std::string* assume_role_doc = nullptr;
  std::string* perm_policy_doc = nullptr;
  std::string* policy_name = nullptr;
  std::string* policy_arn = nullptr;
  std::string* description = nullptr;
  std::string* path_prefix = nullptr;
  std::string* max_session_duration = nullptr;
  std::string* marker = nullptr;
  std::string* infile = nullptr;
  int max_entries = 0;
  bool max_entries_specified = false;
};

int rgw_admin_role(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   ceph::Formatter* formatter,
                   const rgw_admin_role_options& opts);
