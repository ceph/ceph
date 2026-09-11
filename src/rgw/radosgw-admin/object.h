// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <memory>
#include <optional>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

#ifdef WITH_RADOSGW_RADOS
#include "cls/rgw/cls_rgw_types.h"
#endif

class DoutPrefixProvider;
namespace ceph { class Formatter; }
class RGWStreamFlusher;
class RGWBucketAdminOpState;
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class Bucket; }

struct rgw_admin_object_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;
  std::string object;
  std::string object_version;
  std::string infile;
  std::string objects_file;
  std::string end_date;
  std::string start_date;
  std::string marker;
  std::optional<int> max_entries;
  int shard_id = 0;
  int64_t min_rewrite_size = 0;
  int64_t max_rewrite_size = 0;
  uint64_t min_rewrite_stripe_size = 0;
  bool specified_shard_id = false;
  bool yes_i_really_mean_it = false;
  bool fix = false;
  bool remove_bad = false;
};

int rgw_admin_object(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     rgw::SiteConfig& site,
                     ceph::Formatter* formatter,
                     RGWStreamFlusher& stream_flusher,
                     RGWBucketAdminOpState& bucket_op,
                     std::unique_ptr<rgw::sal::Bucket>& bucket,
                     const rgw_admin_object_options& opts);

int do_check_object_locator(const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver,
                            const std::string& tenant_name,
                            const std::string& bucket_name,
                            bool fix, bool remove_bad,
                            ceph::Formatter* f);
