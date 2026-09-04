// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include "common/ceph_time.h"
#include "rgw_basic_types.h"
#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWBucketAdminOpState;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; class Bucket; }

struct rgw_admin_bucket_sync_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;
  rgw_zone_id source_zone;
  std::optional<rgw_bucket> opt_source_bucket;
  RGWBucketAdminOpState* bucket_op = nullptr;
  ceph::timespan opt_retry_delay_ms = std::chrono::milliseconds(2000);
  ceph::timespan opt_timeout_sec = std::chrono::seconds(60);
  bool extra_info = false;
  bool format_arg_passed = false;
};


int rgw_admin_bucket_sync(const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver,
                            ceph::Formatter* formatter,
                            RGWBucketAdminOpState& bucket_op,
                            std::unique_ptr<rgw::sal::Bucket>& bucket,
                            const rgw_admin_bucket_sync_options& opts);
