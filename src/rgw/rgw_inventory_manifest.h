// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp
#pragma once

#include <string>
#include <vector>
#include "rgw_inventory_s3.h"

namespace rgw::inventory {

struct ManifestFile {
  std::string key;          // destination object key
  uint64_t    size;         // parquet file size in bytes
  std::string md5checksum;  // MD5 hex of parquet file
};

struct Manifest {
  std::string source_bucket;
  std::string destination_bucket_arn;
  std::string version{"2016-11-30"};
  std::string creation_timestamp_ms;  // epoch millis as string
  std::string file_format{"Parquet"};
  std::string file_schema;            // Parquet message DSL string
  std::vector<ManifestFile> files;

  // Serialise to JSON string (manifest.json content)
  std::string to_json() const;
};

// Build the Parquet message DSL schema string matching AWS format
// e.g. "message s3.inventory { required binary bucket (STRING); ... }"
std::string build_file_schema(const FieldSelection& sel);

// Compute MD5 hex digest of a file on disk
std::string md5_of_file(const std::string& path);

// Compute MD5 hex digest of a string
std::string md5_of_string(const std::string& s);

// Generate run timestamp string: "YYYY-MM-DDTHH-MMZ"
std::string inventory_run_timestamp();

// Build the full destination key for a parquet data file:
// <prefix>/<source-bucket>/<config-id>/data/<uuid>.parquet
std::string inventory_data_key(const std::string& prefix,
                               const std::string& source_bucket,
                               const std::string& config_id);

// Build the manifest key:
// <prefix>/<source-bucket>/<config-id>/<timestamp>/manifest.json
std::string inventory_manifest_key(const std::string& prefix,
                                   const std::string& source_bucket,
                                   const std::string& config_id,
                                   const std::string& timestamp);

// Build the manifest checksum key (same path, .checksum suffix)
std::string inventory_checksum_key(const std::string& manifest_key);

} // namespace rgw::inventory
