// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
class RGWStreamFlusher;
class RGWUser;
class RGWUserAdminOpState;
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class User; class Bucket; class ConfigStore; }

struct rgw_admin_dedup_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string* allow_bucket_list_file = nullptr;
  std::string* deny_bucket_list_file = nullptr;
  std::string* allow_storage_class_list_file = nullptr;
  std::string* deny_storage_class_list_file = nullptr;
  bool yes_i_really_mean_it = false;
  bool throttle_stat = false;
  bool have_max_bucket_index_ops = false;
  bool have_max_metadata_ops = false;
  int max_bucket_index_ops = 0;
  int max_metadata_ops = 0;
};

int rgw_admin_dedup(const DoutPrefixProvider* dpp,
                    rgw::sal::Driver* driver,
                    ceph::Formatter* formatter,
                    const rgw_admin_dedup_options& opts);
