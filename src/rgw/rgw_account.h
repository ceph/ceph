// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "include/common_fwd.h"

#include "rgw_sal_fwd.h"

class DoutPrefixProvider;
class RGWFormatterFlusher;
class optional_yield;

namespace rgw::account {

/// generate a randomized account id in a specific format
std::string generate_id(CephContext* cct);

/// validate that an account id matches the generated format
bool validate_id(std::string_view id, std::string* err_msg = nullptr);

/// check an account name for any invalid characters
bool validate_name(std::string_view name, std::string* err_msg = nullptr);


struct AdminOpState {
  std::string account_id;
  std::string tenant;
  std::string account_name;
  std::string email;
  std::optional<int32_t> max_users;
  std::optional<int32_t> max_roles;
  std::optional<int32_t> max_groups;
  std::optional<int32_t> max_access_keys;
  std::optional<int32_t> max_buckets;
  std::string quota_scope;
  std::optional<int64_t> quota_max_size;
  std::optional<int64_t> quota_max_objects;
  std::optional<bool> quota_enabled;
};

/// create an account
int create(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
           AdminOpState& op_state, std::string& err_msg,
           RGWFormatterFlusher& flusher, optional_yield y);

/// modify an existing account
int modify(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
           AdminOpState& op_state, std::string& err_msg,
           RGWFormatterFlusher& flusher, optional_yield y);

/// remove an existing account
int remove(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
           AdminOpState& op_state, std::string& err_msg,
           RGWFormatterFlusher& flusher, optional_yield y);

/// dump RGWAccountInfo
int info(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
         AdminOpState& op_state, std::string& err_msg,
         RGWFormatterFlusher& flusher, optional_yield y);

/// dump account storage stats
int stats(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
          AdminOpState& op_state, bool sync_stats,
          bool reset_stats, std::string& err_msg,
          RGWFormatterFlusher& flusher, optional_yield y);

/// list account users
int list_users(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
               AdminOpState& op_state, const std::string& path_prefix,
               const std::string& marker, bool max_entries_specified,
               int max_entries, std::string& err_msg,
               RGWFormatterFlusher& flusher, optional_yield y);

} // namespace rgw::account
