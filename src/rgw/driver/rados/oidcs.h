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

#include <cstdint>
#include <string>
#include <vector>
#include "include/rados/librados_fwd.hpp"
#include "rgw_sal_fwd.h"

class DoutPrefixProvider;
class optional_yield;
struct rgw_raw_obj;
struct RGWOIDCProviderInfo;


namespace rgwrados::oidcs {

/// Add the given OIDC provider to the list.
int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const RGWOIDCProviderInfo& info,
        bool exclusive, uint32_t limit);

/// Remove the given OIDC provider from the list.
int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           librados::Rados& rados,
           const rgw_raw_obj& obj,
           std::string_view provider_url);

/// Return a paginated listing of OIDC provider URLs.
int list(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw_raw_obj& obj,
         std::string_view marker,
         uint32_t max_items,
         std::vector<std::string>& provider_urls,
         std::string& next_marker);

} // namespace rgwrados::oidcs


