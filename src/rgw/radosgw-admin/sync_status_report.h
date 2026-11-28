// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

// Declarations below are RADOS-only: the .cc file and radosgw-admin callers are
// built under WITH_RADOSGW_RADOS; the guard keeps stub/standalone admin builds
// from seeing RGW sync types.
#ifdef WITH_RADOSGW_RADOS

#include <cstdint>
#include <list>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include "common/Formatter.h"
#include "include/utime.h"
#include "rgw_basic_types.h"

class DoutPrefixProvider;

namespace rgw::sal {
class Driver;
}

struct rgw_admin_md_sync_report {
  std::string error;
  bool meta_master{false};
  std::string status;
  int num_full{0};
  int num_inc{0};
  int total_shards{0};
  uint64_t full_total{0};
  uint64_t full_complete{0};
  bool period_mismatch{false};
  std::string master_period;
  std::string local_period;
  int total_behind{0};
  std::set<int> shards_behind;
  std::optional<std::pair<int, ceph::real_time>> oldest;
};

struct rgw_admin_data_sync_report {
  std::string error;
  rgw_zone_id source_zone;
  std::string source_zone_name;
  bool not_syncing{false};
  std::string status;
  int num_full{0};
  int num_inc{0};
  int total_shards{0};
  uint64_t full_total{0};
  uint64_t full_complete{0};
  int total_behind{0};
  int total_recovering{0};
  std::set<int> shards_behind;
  std::set<int> recovering_shards;
  std::optional<std::pair<int, ceph::real_time>> oldest;
};

void rgw_admin_collect_md_sync_report(const DoutPrefixProvider* dpp,
                                      rgw::sal::Driver* driver,
                                      rgw_admin_md_sync_report* out);

void rgw_admin_collect_data_sync_report(const DoutPrefixProvider* dpp,
                                        rgw::sal::Driver* driver,
                                        const rgw_zone_id& source_zone,
                                        rgw_admin_data_sync_report* out);

void rgw_admin_format_md_sync_plaintext(const rgw_admin_md_sync_report& report,
                                        std::list<std::string>& lines);

void rgw_admin_format_md_sync_json(const rgw_admin_md_sync_report& report,
                                   Formatter* formatter);

void rgw_admin_format_data_sync_plaintext(const rgw_admin_data_sync_report& report,
                                            int tab,
                                            std::list<std::string>& lines);

void rgw_admin_format_data_sync_json(const rgw_admin_data_sync_report& report,
                                     Formatter* formatter);

#endif
