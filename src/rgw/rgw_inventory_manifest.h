// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <ctime>
#include <string>
#include <vector>

#include "rgw_inventory_parquet.h"

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
  int64_t     creation_timestamp_ms{0};  // epoch millis; caller must set
  std::string file_format{"Parquet"};
  std::string file_schema;               // Parquet message DSL string
  std::vector<ManifestFile> files;

  // Serialise to manifest.json content. Uses creation_timestamp_ms as-is
  // so the output (and therefore manifest.checksum) is deterministic.
  std::string to_json() const;
};

// Current wall-clock time in epoch milliseconds, UTC.
int64_t current_time_millis();

// Parquet message DSL schema string matching AWS manifest.json fileSchema,
// e.g. "message s3.inventory {  required binary bucket (STRING); ...}"
std::string build_file_schema(const FieldSelection& sel);

// MD5 hex digest of a file on disk ("" if the file can't be opened).
std::string md5_of_file(const std::string& path);

// MD5 hex digest of a string.
std::string md5_of_string(const std::string& s);

// Minute-granular run timestamp, UTC: "YYYY-MM-DDTHH-MMZ".
std::string inventory_run_timestamp();

// Day-granular per-run manifest folder, UTC: "YYYY-MM-DDT00-00Z".
// One folder per day makes "does today's manifest.json exist?" the
// idempotency check for a run.
std::string inventory_run_folder(time_t now);

// <prefix>/<source-bucket>/<config-id>/data/<uuid>.parquet
std::string inventory_data_key(const std::string& prefix,
                               const std::string& source_bucket,
                               const std::string& config_id);

// <prefix>/<source-bucket>/<config-id>/<folder>/manifest.json
std::string inventory_manifest_key(const std::string& prefix,
                                   const std::string& source_bucket,
                                   const std::string& config_id,
                                   const std::string& folder);

// Same path as the manifest key, with manifest.checksum as the leaf.
std::string inventory_checksum_key(const std::string& manifest_key);

} // namespace rgw::inventory
