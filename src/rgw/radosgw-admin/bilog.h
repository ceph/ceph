// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <memory>
#include <optional>
#include <string>
#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; class Bucket; }

struct rgw_admin_bilog_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;
  std::string marker;
  std::string start_marker;
  std::string end_marker;
  std::optional<uint64_t> gen;
  int max_entries = -1;
  int shard_id = 0;
  bool yes_i_really_mean_it = false;
};


int rgw_admin_bilog(const DoutPrefixProvider* dpp,
                    rgw::sal::Driver* driver,
                    ceph::Formatter* formatter,
                    std::unique_ptr<rgw::sal::Bucket>& bucket,
                    rgw_admin_bilog_options& opts);

