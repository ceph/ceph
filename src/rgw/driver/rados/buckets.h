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
#include <string>
#include <boost/intrusive_ptr.hpp>
#include "include/rados/librados_fwd.hpp"
#include "common/ceph_time.h"
#include "rgw_sal_fwd.h"

class DoutPrefixProvider;
class optional_yield;
struct rgw_bucket;
struct rgw_raw_obj;
struct RGWBucketEnt;
struct RGWStorageStats;

/// Interface for bucket owners (users or accounts) to manage
/// their list of buckets and storage stats with cls_user.
namespace rgwrados::buckets {

/// Add the given bucket to the list.
int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const rgw_bucket& bucket,
        ceph::real_time creation_time);

/// Remove the given bucket from the list.
int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           librados::Rados& rados,
           const rgw_raw_obj& obj,
           const rgw_bucket& bucket);

/// Return a paginated list of buckets.
int list(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw_raw_obj& obj,
         const std::string& tenant,
         const std::string& marker,
         const std::string& end_marker,
         uint64_t max,
         rgw::sal::BucketList& buckets);

/// Update usage stats for the given bucket.
int write_stats(const DoutPrefixProvider* dpp,
                optional_yield y,
                librados::Rados& rados,
                const rgw_raw_obj& obj,
                const RGWBucketEnt& bucket);

/// Read the total usage stats of all buckets.
int read_stats(const DoutPrefixProvider* dpp,
               optional_yield y,
               librados::Rados& rados,
               const rgw_raw_obj& obj,
               RGWStorageStats& stats,
               ceph::real_time* last_synced,
               ceph::real_time* last_updated);

/// Read the total usage stats of all buckets asynchronously.
int read_stats_async(const DoutPrefixProvider* dpp,
                     librados::Rados& rados,
                     const rgw_raw_obj& obj,
                     boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb);

/// Recalculate the sum of bucket usage.
int reset_stats(const DoutPrefixProvider* dpp,
                optional_yield y,
                librados::Rados& rados,
                const rgw_raw_obj& obj);

/// Update the last_synced timestamp.
int complete_flush_stats(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         librados::Rados& rados,
                         const rgw_raw_obj& obj);

} // namespace rgwrados::buckets
