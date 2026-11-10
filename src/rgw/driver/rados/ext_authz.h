// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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
#include <string_view>
#include "common/ceph_time.h"

class DoutPrefixProvider;
class optional_yield;
class RGWMetadataHandler;
class RGWObjVersionTracker;
struct RGWExtAuthzConfig;
class RGWSI_MDLog;
class RGWSI_SysObj;
class RGWZoneParams;

namespace rgwrados::ext_authz {

/// The only config entry. The metadata section is "ext_authz".
inline constexpr std::string_view metadata_key = "default";

/// Read the external authorizer config. Returns -ENOENT if none is set.
int read(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    const RGWZoneParams& zone,
    RGWExtAuthzConfig& config,
    ceph::real_time* pmtime = nullptr,
    RGWObjVersionTracker* pobjv = nullptr);

/// Write or overwrite the config and record it in the metadata log.
int write(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog* mdlog,
    const RGWZoneParams& zone,
    const RGWExtAuthzConfig& config,
    ceph::real_time mtime,
    RGWObjVersionTracker* objv = nullptr);

/// Remove the config and record it in the metadata log.
int remove(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog* mdlog,
    const RGWZoneParams& zone,
    RGWObjVersionTracker* objv = nullptr);

/// Metadata handler factory, makes the config replicate in multisite.
auto create_metadata_handler(
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog& mdlog,
    const RGWZoneParams& zone)
  -> std::unique_ptr<RGWMetadataHandler>;

} // rgwrados::ext_authz
