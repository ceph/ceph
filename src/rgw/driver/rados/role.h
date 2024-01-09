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
#include "common/ceph_time.h"

class DoutPrefixProvider;
class optional_yield;
struct rgw_cache_entry_info;
class RGWMetadataHandler;
class RGWObjVersionTracker;
struct RGWRoleInfo;
class RGWSI_MDLog;
class RGWSI_SysObj;
class RGWZoneParams;

namespace rgwrados::role {

/// Read role info by id.
int read_by_id(const DoutPrefixProvider* dpp, optional_yield y,
               RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
               const std::string& role_id, RGWRoleInfo& info,
               ceph::real_time* pmtime = nullptr,
               RGWObjVersionTracker* pobjv = nullptr,
               rgw_cache_entry_info* pcache_info = nullptr);

/// Read role info by name.
int read_by_name(const DoutPrefixProvider* dpp, optional_yield y,
                 RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
                 const std::string& tenant, const std::string& name,
                 RGWRoleInfo& info,
                 ceph::real_time* pmtime = nullptr,
                 RGWObjVersionTracker* pobjv = nullptr,
                 rgw_cache_entry_info* pcache_info = nullptr);

/// Write or overwrite role info and update its name/path objects.
int write(const DoutPrefixProvider* dpp, optional_yield y,
          RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
          const RGWZoneParams& zone, const RGWRoleInfo& info,
          RGWObjVersionTracker& objv, ceph::real_time mtime,
          bool exclusive);

/// Remove a role by name, including its name/path objects.
int remove(const DoutPrefixProvider* dpp, optional_yield y,
           RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
           const RGWZoneParams& zone, const std::string& tenant,
           const std::string& name);

/// Return a paginated listing of roles.
int list(const DoutPrefixProvider* dpp, optional_yield y,
         RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
         const std::string& tenant, const std::string& marker,
         int max_items, const std::string& path_prefix,
         std::vector<RGWRoleInfo>& roles, std::string& next_marker);

/// Role metadata handler factory.
auto create_metadata_handler(RGWSI_SysObj& sysobj,
                             RGWSI_MDLog& mdlog,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>;

} // rgwrados::role
