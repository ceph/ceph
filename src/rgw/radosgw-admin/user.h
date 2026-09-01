// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <memory>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWFormatterFlusher;
class RGWUser;
class RGWUserAdminOpState;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; class User; class Bucket; }

struct rgw_admin_user_mutate_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  const std::string* access_key = nullptr;
  const std::string* subuser = nullptr;
  bool yes_i_really_mean_it = false;
  int generate_key = 2; // 0=set-false, 1=set-true, 2=not-set
};

int rgw_admin_user_mutate(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          ceph::Formatter* formatter,
                          RGWUser& ruser,
                          RGWUserAdminOpState& user_op,
                          std::unique_ptr<rgw::sal::User>& user,
                          const rgw_admin_user_mutate_options& opts,
                          std::string& err_msg);

struct rgw_admin_user_policy_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string* policy_arn = nullptr;
};

int rgw_admin_user_policy(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          ceph::Formatter* formatter,
                          std::unique_ptr<rgw::sal::User>& user,
                          const rgw_admin_user_policy_options& opts);

struct rgw_admin_user_query_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  const std::string* tenant = nullptr;
  const std::string* bucket_name = nullptr;
  const std::string* bucket_id = nullptr;
  const std::string* account_id = nullptr;
  const std::string* account_name = nullptr;
  const std::string* path_prefix = nullptr;
  const std::string* marker = nullptr;
  int max_entries = 0;
  bool max_entries_specified = false;
  bool account_root = false;
  bool sync_stats = false;
  bool reset_stats = false;
  bool fix = false;
};

int rgw_admin_user_query(const DoutPrefixProvider* dpp,
                         rgw::sal::Driver* driver,
                         ceph::Formatter* formatter,
                         RGWFormatterFlusher& stream_flusher,
                         std::unique_ptr<rgw::sal::User>& user,
                         std::unique_ptr<rgw::sal::Bucket>& bucket,
                         const rgw_admin_user_query_options& opts);
