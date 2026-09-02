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

#ifdef WITH_RADOSGW_RADOS
#include "cls/rgw/cls_rgw_types.h"
#endif

struct rgw_admin_bi_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;
  std::string object;
  std::string object_version;
  std::string infile;
  std::string marker;
  std::optional<int> max_entries;
  int shard_id = 0;
#ifdef WITH_RADOSGW_RADOS
  BIIndexType bi_index_type = BIIndexType::Plain;
#endif
  bool specified_shard_id = false;
  bool yes_i_really_mean_it = false;
};


int rgw_admin_bi(const DoutPrefixProvider* dpp,
                 rgw::sal::Driver* driver,
                 ceph::Formatter* formatter,
                 std::unique_ptr<rgw::sal::Bucket>& bucket,
                 const rgw_admin_bi_options& opts);

