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
struct RGWOIDCProviderInfo;
class RGWSI_MDLog;
class RGWSI_SysObj;
class RGWZoneParams;

namespace rgwrados::oidc {

int read(const DoutPrefixProvider* dpp, optional_yield y,
         RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
         const std::string_view account, std::string_view url,
         const std::string& id, RGWOIDCProviderInfo& info,
         ceph::real_time* mtime = nullptr,
         RGWObjVersionTracker* objv = nullptr,
         rgw_cache_entry_info* cache_info = nullptr);

/// Write or overwrite provider info
int write(const DoutPrefixProvider* dpp, optional_yield y,
          RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
          const RGWZoneParams& zone, const RGWOIDCProviderInfo& info,
          RGWObjVersionTracker& objv, ceph::real_time mtime,
          bool exclusive);

/// Remove a provider by name
int remove(const DoutPrefixProvider* dpp, optional_yield y,
           RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
           const RGWZoneParams& zone, const std::string_view tenant,
           std::string_view url, RGWObjVersionTracker& objv,
           std::string& id);


auto create_metadata_handler(RGWSI_SysObj& sysobj,
                             RGWSI_MDLog& mdlog,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>;
}