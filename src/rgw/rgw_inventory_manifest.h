// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <ctime>
#include <string>
#include <string_view>
#include <vector>

#include "rgw_inventory_parquet.h"
#include "rgw_inventory_s3.h"

namespace ceph { class Formatter; }

namespace rgw::inventory {

struct ManifestFile {
  std::string key;          // destination object key
  uint64_t    size;         // parquet file size in bytes
  std::string md5checksum;  // MD5 hex of the parquet object (its ETag)
};

struct Manifest {
  std::string source_bucket;
  std::string destination_bucket_arn;
  std::string version{"2016-11-30"};
  int64_t     creation_timestamp_ms{0};  // epoch millis; caller must set
  Format      file_format{Format::Parquet};
  std::string file_schema;               // Parquet message DSL string
  std::vector<ManifestFile> files;

  void dump(ceph::Formatter* f) const;
  // manifest.json content
  std::string to_json() const;
};

// Parquet message DSL schema string matching AWS manifest.json fileSchema,
// e.g. "message s3.inventory {  required binary bucket (STRING); ...}"
std::string build_file_schema(const FieldSelection& sel);

// md5 hex digest, the content of manifest.checksum
std::string md5_hex(std::string_view s);

// "<prefix>/<source>/<config-id>/", everything a configuration writes
// goes under it. no leading "<prefix>/" if the prefix is empty
std::string inventory_config_prefix(const std::string& prefix,
                                    const std::string& source,
                                    const std::string& config_id);

// "<config prefix>data/<uuid>.parquet"
std::string inventory_data_key(const std::string& config_prefix);

// "<config prefix>YYYY-MM-DDT00-00Z/", the folder for the day's
// manifest.json and manifest.checksum (UTC)
std::string inventory_run_prefix(const std::string& config_prefix, time_t now);

} // namespace rgw::inventory
