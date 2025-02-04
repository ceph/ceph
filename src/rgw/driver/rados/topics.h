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
#include <vector>
#include "include/rados/librados_fwd.hpp"
#include "rgw_sal_fwd.h"

class DoutPrefixProvider;
class optional_yield;
struct rgw_raw_obj;
struct rgw_pubsub_topic;


namespace rgwrados::topics {

/// Add the given topic to the list.
int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const rgw_pubsub_topic& info,
        bool exclusive, uint32_t limit);

/// Remove the given topic from the list.
int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           librados::Rados& rados,
           const rgw_raw_obj& obj,
           std::string_view name);

/// Return a paginated listing of topic names.
int list(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw_raw_obj& obj,
         std::string_view marker,
         uint32_t max_items,
         std::vector<std::string>& names,
         std::string& next_marker);

} // namespace rgwrados::topics
