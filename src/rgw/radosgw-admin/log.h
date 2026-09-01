// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class Formatter;
namespace rgw::sal { class Driver; }

struct rgw_admin_log_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  const std::string* date = nullptr;
  const std::string* object = nullptr;
  const std::string* bucket_name = nullptr;
  const std::string* bucket_id = nullptr;
  bool show_log_entries = true;
  bool show_log_sum = true;
  bool skip_zero_entries = false;
};

int rgw_admin_log(const DoutPrefixProvider* dpp,
                  rgw::sal::Driver* driver,
                  Formatter* formatter,
                  const rgw_admin_log_options& opts);
