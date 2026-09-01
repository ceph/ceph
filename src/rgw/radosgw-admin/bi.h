// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <memory>
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
  std::string* tenant = nullptr;
  std::string* bucket_name = nullptr;
  std::string* bucket_id = nullptr;
  std::string* object = nullptr;
  std::string* object_version = nullptr;
  std::string* infile = nullptr;
  std::string* marker = nullptr;
  int max_entries = -1;
  int shard_id = 0;
#ifdef WITH_RADOSGW_RADOS
  BIIndexType bi_index_type = BIIndexType::Plain;
#endif
  bool max_entries_specified = false;
  bool specified_shard_id = false;
  bool yes_i_really_mean_it = false;
};


int rgw_admin_bi(const DoutPrefixProvider* dpp,
                 rgw::sal::Driver* driver,
                 ceph::Formatter* formatter,
                 std::unique_ptr<rgw::sal::Bucket>& bucket,
                 const rgw_admin_bi_options& opts);

