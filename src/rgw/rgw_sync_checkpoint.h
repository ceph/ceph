// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <optional>
#include "common/ceph_time.h"
#include "rgw_basic_types.h"

class DoutPrefixProvider;
namespace rgw::sal { class RadosStore; }
class RGWBucketInfo;
class RGWBucketSyncPolicyHandler;

// poll the bucket's sync status until it's caught up against all sync sources
int rgw_bucket_sync_checkpoint(const DoutPrefixProvider* dpp,
                               rgw::sal::RadosStore* store,
                               const RGWBucketSyncPolicyHandler& policy,
                               const RGWBucketInfo& info,
                               std::optional<rgw_zone_id> opt_source_zone,
                               std::optional<rgw_bucket> opt_source_bucket,
                               ceph::timespan retry_delay,
                               ceph::coarse_mono_time timeout_at);
