// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace ceph::async { class io_context_pool; }
namespace rgw::sal { class Driver; }

enum class log_type;

struct rgw_admin_datalog_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string* marker = nullptr;
  std::string* start_marker = nullptr;
  std::string* end_marker = nullptr;
  std::string* start_date = nullptr;
  std::string* end_date = nullptr;
  std::optional<log_type>* opt_log_type = nullptr;
  std::optional<std::uint64_t>* count = nullptr;
  int max_entries = 0;
  int shard_id = 0;
  bool specified_shard_id = false;
  bool extra_info = false;
};

int rgw_admin_datalog(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      ceph::async::io_context_pool& context_pool,
                      ceph::Formatter* formatter,
                      const rgw_admin_datalog_options& opts);
