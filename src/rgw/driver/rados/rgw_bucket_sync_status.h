// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#include "common/Formatter.h"
#include "common/ceph_time.h"
#include "rgw_basic_types.h"
#include "rgw_data_sync.h"
#include "rgw_sync_policy.h"
#include "rgw_zone.h"
#include "rgw_bucket.h"
class RGWRESTConn;
struct RGWZone;

namespace rgw::sal {
class Driver;
class RadosStore;
class Zone;
class ZoneGroup;
}

struct indented {
  int w;
  std::string header;
  indented(int w, std::string header = "") : w(w), header(header) {}
};

std::ostream& operator<<(std::ostream& out, const indented& h);

struct bucket_source_sync_info {
  const RGWZone& _source;
  std::string error;
  std::map<int, std::string> shards_behind;
  int total_shards{0};
  std::string status;
  rgw_bucket bucket_source;

  explicit bucket_source_sync_info(const RGWZone& source);

  void print_plaintext(std::ostream& out, int width) const;
  void print_formatter(std::ostream& out, Formatter* formatter) const;
};

struct bucket_sync_status_info {
  std::vector<bucket_source_sync_info> source_status_info;
  rgw::sal::Zone* _zone{nullptr};
  const rgw::sal::ZoneGroup* _zonegroup{nullptr};
  const RGWBucketInfo& _bucket_info;
  const int width = 15;
  std::string error;

  explicit bucket_sync_status_info(const RGWBucketInfo& bucket_info);

  void print(std::ostream& out, bool use_formatter, Formatter* formatter);

private:
  void print_plaintext(std::ostream& out);
  void print_formatter(std::ostream& out, Formatter* formatter);
};

int bucket_source_sync_status(const DoutPrefixProvider* dpp,
                              rgw::sal::RadosStore* driver,
                              const RGWZone& zone,
                              const RGWZone& source,
                              RGWRESTConn* conn,
                              const RGWBucketInfo& bucket_info,
                              rgw_sync_bucket_pipe pipe,
                              bucket_source_sync_info& source_sync_info);

int bucket_sync_status(const DoutPrefixProvider* dpp,
                       rgw::sal::Driver* driver,
                       const RGWBucketInfo& info,
                       const rgw_zone_id& source_zone_id,
                       std::optional<rgw_bucket>& opt_source_bucket,
                       bucket_sync_status_info& bucket_sync_info);