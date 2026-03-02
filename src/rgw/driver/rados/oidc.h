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
#include <vector>
#include "include/rados/librados_fwd.hpp"
#include "common/ceph_time.h"

class DoutPrefixProvider;
class optional_yield;
class RGWMetadataHandler;
class RGWObjVersionTracker;
struct RGWOIDCProviderInfo;
class RGWSI_MDLog;
class RGWSI_SysObj;
class RGWZoneParams;

namespace rgwrados::oidc {

/// Read OIDC provider info by URL.
int read(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    const RGWZoneParams& zone,
    std::string_view tenant,
    std::string_view url,
    RGWOIDCProviderInfo& info,
    ceph::real_time* pmtime = nullptr,
    RGWObjVersionTracker* pobjv = nullptr);

/// Write or overwrite OIDC provider info.
int write(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog* mdlog,
    librados::Rados& rados,
    const RGWZoneParams& zone,
    const RGWOIDCProviderInfo& info,
    ceph::real_time mtime,
    bool exclusive,
    RGWObjVersionTracker* objv = nullptr);

/// Remove an OIDC provider by URL.
int remove(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog* mdlog,
    librados::Rados& rados,
    const RGWZoneParams& zone,
    std::string_view tenant,
    std::string_view url,
    RGWObjVersionTracker* objv = nullptr);

/// List all OIDC providers for a given tenant.
int list(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    librados::Rados& rados,
    const RGWZoneParams& zone,
    std::string_view tenant,
    std::vector<RGWOIDCProviderInfo>& providers);

/// List OIDC provider URLs for an account (paginated).
int list_oidc_urls(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    librados::Rados& rados,
    const RGWZoneParams& zone,
    std::string_view account_id,
    std::string_view marker,
    uint32_t max_items,
    std::vector<std::string>& urls,
    std::string& next_marker);

/// List all OIDC providers for an account using optimized account index.
int list_account_oidcs(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    librados::Rados& rados,
    const RGWZoneParams& zone,
    std::string_view account_id,
    std::vector<RGWOIDCProviderInfo>& providers);

/// OIDC provider metadata handler factory.
auto create_metadata_handler(
    librados::Rados& rados,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog& mdlog,
    const RGWZoneParams& zone)
  -> std::unique_ptr<RGWMetadataHandler>;

/// Construct the metadata key "{tenant}${url}" from tenant and URL.
std::string get_oidc_metadata_key(
    std::string_view tenant,
    std::string_view url);

/// Parse the metadata key "{tenant}${url}" into tenant and URL.
void parse_oidc_metadata_key(
    const std::string& key,
    std::string& tenant,
    std::string& url);

} // rgwrados::oidc

