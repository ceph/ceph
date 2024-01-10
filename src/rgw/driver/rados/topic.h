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

#include <memory>
#include <set>
#include <string>
#include "include/rados/librados_fwd.hpp"
#include "common/ceph_time.h"
#include "rgw_pubsub.h"

class DoutPrefixProvider;
class optional_yield;
class RGWMetadataHandler;
class RGWObjVersionTracker;
class RGWSI_MDLog;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWZoneParams;

template <typename T> class RGWChainedCacheImpl;

// Rados interface for v2 topic metadata
namespace rgwrados::topic {

struct cache_entry {
  rgw_pubsub_topic info;
  RGWObjVersionTracker objv;
  ceph::real_time mtime;
};

/// Read topic info by metadata key.
int read(const DoutPrefixProvider* dpp, optional_yield y,
         RGWSI_SysObj& sysobj, RGWSI_SysObj_Cache& cache_svc,
         const RGWZoneParams& zone, const std::string& topic_key,
         rgw_pubsub_topic& info, RGWChainedCacheImpl<cache_entry>& cache,
         ceph::real_time* pmtime = nullptr,
         RGWObjVersionTracker* pobjv = nullptr);

/// Write or overwrite topic info.
int write(const DoutPrefixProvider* dpp, optional_yield y,
          RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog, const RGWZoneParams& zone,
          const rgw_pubsub_topic& info, RGWObjVersionTracker& objv,
          ceph::real_time mtime, bool exclusive);

/// Remove a topic by metadata key.
int remove(const DoutPrefixProvider* dpp, optional_yield y,
           RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
           const RGWZoneParams& zone, const std::string& topic_key,
           RGWObjVersionTracker& objv);


/// Add a bucket key to the topic's list of buckets.
int link_bucket(const DoutPrefixProvider* dpp, optional_yield y,
                librados::Rados& rados, const RGWZoneParams& zone,
                const std::string& topic_key,
                const std::string& bucket_key);

/// Remove a bucket key from the topic's list of buckets.
int unlink_bucket(const DoutPrefixProvider* dpp, optional_yield y,
                  librados::Rados& rados, const RGWZoneParams& zone,
                  const std::string& topic_key,
                  const std::string& bucket_key);

/// List the bucket keys associated with a given topic.
int list_buckets(const DoutPrefixProvider* dpp, optional_yield y,
                 librados::Rados& rados, const RGWZoneParams& zone,
                 const std::string& topic_key,
                 const std::string& marker, int max_items,
                 std::set<std::string>& bucket_keys,
                 std::string& next_marker);


/// Topic metadata handler factory.
auto create_metadata_handler(RGWSI_SysObj& sysobj,
                             RGWSI_SysObj_Cache& cache_svc,
                             RGWSI_MDLog& mdlog, const RGWZoneParams& zone,
                             RGWChainedCacheImpl<cache_entry>& cache)
    -> std::unique_ptr<RGWMetadataHandler>;

} // rgwrados::topic
