// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "common/ceph_time.h"
#include "rgw_basic_types.h"
#include "radosgw-admin/radosgw-admin.h"

#ifdef WITH_RADOSGW_RADOS
#include "cls/rgw/cls_rgw_types.h"
#endif

class DoutPrefixProvider;
class RGWFormatterFlusher;
class RGWUserAdminOpState;
class RGWBucketAdminOpState;
namespace ceph { class Formatter; }
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class User; class Bucket; class Object; }

int rgw_admin_init_bucket(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          const rgw_bucket& b,
                          std::unique_ptr<rgw::sal::Bucket>* bucket);
int rgw_admin_init_bucket(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          const std::string& tenant_name,
                          const std::string& bucket_name,
                          const std::string& bucket_id,
                          std::unique_ptr<rgw::sal::Bucket>* bucket);

int rgw_admin_check_reshard_bucket_params(const DoutPrefixProvider* dpp,
                                            rgw::sal::Driver* driver,
                                            const std::string& bucket_name,
                                            const std::string& tenant,
                                            const std::string& bucket_id,
                                            bool num_shards_specified,
                                            int num_shards,
                                            int yes_i_really_mean_it,
                                            std::unique_ptr<rgw::sal::Bucket>* bucket);

int rgw_admin_check_min_obj_stripe_size(const DoutPrefixProvider* dpp,
                                          rgw::sal::Driver* driver,
                                          rgw::sal::Object* obj,
                                          uint64_t min_stripe_size,
                                          bool* need_rewrite);

struct rgw_admin_bucket_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;

  std::string* tenant = nullptr;
  std::string* bucket_name = nullptr;
  std::string* bucket_id = nullptr;
  std::string* object = nullptr;
  std::string* object_version = nullptr;
  std::string* marker = nullptr;
  rgw_zone_id* source_zone = nullptr;
  std::string* metadata_key = nullptr;
  std::string* err = nullptr;
  std::string* new_bucket_name = nullptr;
  std::string* account_id = nullptr;
  std::string* format = nullptr;
  std::string* start_date = nullptr;
  std::string* end_date = nullptr;

  std::optional<std::string>* opt_prefix = nullptr;
  std::optional<rgw_bucket>* opt_source_bucket = nullptr;
  std::optional<std::string>* inject_error_at = nullptr;
  std::optional<int>* inject_error_code = nullptr;
  std::optional<std::string>* inject_abort_at = nullptr;
  std::optional<std::string>* inject_delay_at = nullptr;
  ceph::timespan* inject_delay = nullptr;
  std::optional<std::string>* rgw_obj_fs = nullptr;

  int* ret = nullptr;
  int max_entries = 0;
  int max_concurrent_ios = 0;
  int orphan_stale_secs = 0;
  int num_shards = 0;
  int shard_id = 0;
  ceph::timespan min_age{};
  uint64_t min_rewrite_size = 0;
  uint64_t max_rewrite_size = 0;
  uint64_t min_rewrite_stripe_size = 0;
  ceph::timespan opt_retry_delay_ms{};
  ceph::timespan opt_timeout_sec{};

  bool max_entries_specified = false;
  bool warnings_only = false;
  bool allow_unordered = false;
  bool show_restore_stats = false;
  bool yes_i_really_mean_it = false;
  bool bypass_gc = false;
  bool inconsistent_index = false;
  bool num_shards_specified = false;
  bool specified_shard_id = false;
  bool fix = false;
  bool dump_keys = false;
  bool hide_progress = false;
  bool extra_info = false;
  bool verbose = false;
  bool format_arg_passed = false;
  bool check_head_obj_locator = false;
  bool remove_bad = false;
};

int rgw_admin_bucket(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     rgw::SiteConfig& site,
                     ceph::Formatter* formatter,
                     RGWFormatterFlusher& stream_flusher,
                     std::unique_ptr<rgw::sal::User>& user,
                     RGWUserAdminOpState& user_op,
                     RGWBucketAdminOpState& bucket_op,
                     std::unique_ptr<rgw::sal::Bucket>& bucket,
                     const rgw_admin_bucket_options& opts);
