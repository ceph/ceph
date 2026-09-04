// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <memory>
#include <optional>
#include <string>
#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWStreamFlusher;
class RGWBucketAdminOpState;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; class Bucket; }

struct rgw_admin_reshard_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;
  std::string marker;
  std::optional<int> max_entries;
  int num_shards = 0;
  int shard_id = 0;
  bool num_shards_specified = false;
  bool specified_shard_id = false;
  bool yes_i_really_mean_it = false;
};


int rgw_admin_reshard(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      ceph::Formatter* formatter,
                      RGWStreamFlusher& stream_flusher,
                      RGWBucketAdminOpState& bucket_op,
                      std::unique_ptr<rgw::sal::Bucket>& bucket,
                      rgw_admin_reshard_options& opts);

