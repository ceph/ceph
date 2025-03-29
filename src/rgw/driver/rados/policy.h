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
#include <string>
#include <vector>
#include "include/rados/librados_fwd.hpp"
#include "rgw_customer_managed_policy.h"
#include "common/ceph_time.h"

class DoutPrefixProvider;
class optional_yield;
struct rgw_account_id;
struct rgw_cache_entry_info;
class RGWMetadataHandler;
class RGWObjVersionTracker;
struct ManagedPolicyInfo;
class RGWSI_MDLog;
class RGWSI_SysObj;
class RGWZoneParams;


namespace rgwrados::policy {




/// Write or overwrite policy info and update its name/path objects.
int write(const DoutPrefixProvider* dpp, optional_yield y,
          librados::Rados& rados, RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
          const RGWZoneParams& zone, const ManagedPolicyInfo& info,
          RGWObjVersionTracker& objv, ceph::real_time mtime,
          bool exclusive);



/// Role metadata handler factory.
auto create_metadata_handler(librados::Rados& rados,
                             RGWSI_SysObj& sysobj,
                             RGWSI_MDLog& mdlog,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>;

} // rgwrados::policy
