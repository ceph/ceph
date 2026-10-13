// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <ctime>
#include <memory>
#include <string>

#include "common/async/yield_context.h"
#include "common/dout.h"
#include "rgw_sal_fwd.h"
#include "rgw_inventory_s3.h"
#include "rgw_inventory_parquet.h"

class CephContext;

namespace rgw::inventory {

// Principal AWS documents for S3 Inventory destination bucket policies,
// used with the aws:SourceArn / aws:SourceAccount condition keys.
// (Bucket logging uses a different one: logging.s3.amazonaws.com.)
inline const std::string service_principal = "s3.amazonaws.com";

// Map IncludedObjectVersions + OptionalFields onto writer columns.
// Starts from all-false: AWS only emits columns that were requested.
FieldSelection field_selection_from_config(const Configuration& cfg);

// Daily: always due. Weekly: due on one stable weekday per bucket
// (ceph_str_hash_linux, so every RGW computes the same day).
bool is_due(const Configuration& cfg, const std::string& bucket_key,
            time_t now);

// Resolve the destination ARN to a bucket in this cluster.
int load_destination_bucket(const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver,
                            const std::string& source_tenant,
                            const Configuration& cfg,
                            std::unique_ptr<rgw::sal::Bucket>* dest,
                            optional_yield y);

// The destination bucket policy must allow service_principal to PutObject.
// Mirrors verify_target_bucket_policy() in rgw_bucket_logging.cc.
int verify_destination_policy(const DoutPrefixProvider* dpp,
                              CephContext* cct,
                              rgw::sal::Bucket* source,
                              rgw::sal::Bucket* dest,
                              const Configuration& cfg,
                              std::string* err_message);

// Called from RGWLC::bucket_lc_process() for every bucket on the
// lifecycle work list. Runs each enabled, due configuration. Errors are
// logged; they never change the lifecycle result for the bucket.
int process_bucket(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   rgw::sal::Bucket* bucket,
                   optional_yield y);

// Remove temp parquet files left behind by crashed runs.
void cleanup_stale_temp_files(const DoutPrefixProvider* dpp,
                              CephContext* cct);

} // namespace rgw::inventory
