// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWStreamFlusher;
namespace rgw::sal { class Driver; }

struct rgw_admin_account_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string_view tenant;
  std::string_view account_id;
  std::string_view account_name;
  std::string_view user_email;
  std::string_view marker;
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
