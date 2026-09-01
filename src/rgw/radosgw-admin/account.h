// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWStreamFlusher;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; }

struct rgw_admin_account_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  const std::string* tenant = nullptr;
  const std::string* account_id = nullptr;
  const std::string* account_name = nullptr;
  const std::string* user_email = nullptr;
  const std::string* marker = nullptr;
  const std::optional<int>* max_users = nullptr;
  const std::optional<int>* max_roles = nullptr;
  const std::optional<int>* max_groups = nullptr;
  const std::optional<int>* max_access_keys = nullptr;
  const std::optional<int>* max_buckets = nullptr;
  bool purge_data = false;
  bool sync_stats = false;
  bool reset_stats = false;
  int max_entries = 0;
  bool max_entries_specified = false;
};

int rgw_admin_account(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      RGWStreamFlusher& stream_flusher,
                      const rgw_admin_account_options& opts);
