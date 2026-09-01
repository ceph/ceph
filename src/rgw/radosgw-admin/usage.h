// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWFormatterFlusher;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; class User; class Bucket; }

struct rgw_admin_usage_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  const std::string* tenant = nullptr;
  const std::string* bucket_name = nullptr;
  const std::string* bucket_id = nullptr;
  const std::string* start_date = nullptr;
  const std::string* end_date = nullptr;
  std::map<std::string, bool>* categories = nullptr;
  bool show_log_entries = true;
  bool show_log_sum = true;
  bool yes_i_really_mean_it = false;
};

int rgw_admin_usage(const DoutPrefixProvider* dpp,
                    rgw::sal::Driver* driver,
                    ceph::Formatter* formatter,
                    RGWFormatterFlusher& stream_flusher,
                    std::unique_ptr<rgw::sal::User>& user,
                    std::unique_ptr<rgw::sal::Bucket>& bucket,
                    const rgw_admin_usage_options& opts);
