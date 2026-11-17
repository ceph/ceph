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

namespace rgw::inventory {

FieldSelection field_selection_from_config(const Configuration& cfg);

// weekly configs run on a weekday derived from a hash of the bucket key,
// so all gateways agree on it
bool is_due(const Configuration& cfg, const std::string& bucket_key,
            time_t now);

int load_destination_bucket(const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver,
                            const std::string& source_tenant,
                            const Configuration& cfg,
                            std::unique_ptr<rgw::sal::Bucket>* dest,
                            optional_yield y);

// Inventory is delivered as the "s3.amazonaws.com" service principal, the
// same one AWS uses for inventory (there is no dedicated principal, unlike
// bucket logging's "logging.s3.amazonaws.com"). The destination bucket needs
// a policy like:
//
// {
//   "Version": "2012-10-17",
//   "Statement": [{
//     "Effect": "Allow",
//     "Principal": {"Service": "s3.amazonaws.com"},
//     "Action": "s3:PutObject",
//     "Resource": "arn:aws:s3:::<destination>/*",
//     "Condition": {
//       "ArnLike": {"aws:SourceArn": "arn:aws:s3:::<source>"},
//       "StringEquals": {"aws:SourceAccount": "<source bucket owner>"}
//     }
//   }]
// }
//
// The conditions are optional. Resource can be narrowed down to
// "<destination>/<prefix>/<source>/<id>/*", where <source> is
// "<tenant>:<bucket>" for a bucket in a non-default tenant.
//
// similar to verify_target_bucket_policy() in rgw_bucket_logging.cc
int verify_destination_policy(const DoutPrefixProvider* dpp,
                              rgw::sal::Bucket* source,
                              rgw::sal::Bucket* dest,
                              const Configuration& cfg,
                              std::string* err_message);

// called from RGWLC::bucket_lc_process(); runs every enabled config
// that is due. returns the first error, the other configs still run
int process_bucket(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   rgw::sal::Bucket* bucket,
                   optional_yield y);

} // namespace rgw::inventory
