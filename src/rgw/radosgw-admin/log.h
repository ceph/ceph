// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; }

struct rgw_admin_log_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string date;
  std::string object;
  std::string bucket_name;
  std::string bucket_id;
  bool show_log_entries = true;
  bool show_log_sum = true;
  bool skip_zero_entries = false;
};

int rgw_admin_log(const DoutPrefixProvider* dpp,
                  rgw::sal::Driver* driver,
                  ceph::Formatter* formatter,
                  const rgw_admin_log_options& opts);
