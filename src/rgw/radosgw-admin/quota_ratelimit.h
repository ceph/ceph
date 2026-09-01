// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "rgw_common.h"
#include "radosgw-admin/radosgw-admin.h"

struct rgw_account_id;

class DoutPrefixProvider;
class RGWStreamFlusher;
class RGWUser;
class RGWUserAdminOpState;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; class User; }

bool set_ratelimit_info(RGWRateLimitInfo& ratelimit, rgw_admin::OPT command,
                        int64_t max_read_ops, int64_t max_write_ops,
                        int64_t max_list_ops, int64_t max_delete_ops,
                        int64_t max_read_bytes, int64_t max_write_bytes,
                        bool have_max_read_ops, bool have_max_write_ops,
                        bool have_max_list_ops, bool have_max_delete_ops,
                        bool have_max_read_bytes, bool have_max_write_bytes);

void set_quota_info(RGWQuotaInfo& quota, rgw_admin::OPT command,
                    int64_t max_size, int64_t max_objects,
                    bool have_max_size, bool have_max_objects);

struct rgw_admin_quota_ratelimit_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string* tenant = nullptr;
  std::string* bucket_name = nullptr;
  rgw_account_id* account_id = nullptr;
  std::string* account_name = nullptr;
  std::string* quota_scope = nullptr;
  std::string* ratelimit_scope = nullptr;
  int64_t max_size = 0;
  int64_t max_objects = 0;
  int64_t max_read_ops = 0;
  int64_t max_write_ops = 0;
  int64_t max_list_ops = 0;
  int64_t max_delete_ops = 0;
  int64_t max_read_bytes = 0;
  int64_t max_write_bytes = 0;
  bool have_max_size = false;
  bool have_max_objects = false;
  bool have_max_read_ops = false;
  bool have_max_write_ops = false;
  bool have_max_list_ops = false;
  bool have_max_delete_ops = false;
  bool have_max_read_bytes = false;
  bool have_max_write_bytes = false;
};

int rgw_admin_quota_ratelimit(const DoutPrefixProvider* dpp,
                              rgw::sal::Driver* driver,
                              RGWStreamFlusher& stream_flusher,
                              ceph::Formatter* formatter,
                              RGWUser& ruser,
                              RGWUserAdminOpState& user_op,
                              std::unique_ptr<rgw::sal::User>& user,
                              const rgw_admin_quota_ratelimit_options& opts);
