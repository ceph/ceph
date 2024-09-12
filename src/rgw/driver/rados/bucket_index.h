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

#include "include/rados/librados_fwd.hpp"

class DoutPrefixProvider;
class optional_yield;
class RGWBucketInfo;

namespace rgw {
struct bucket_index_layout_generation;
struct bucket_index_normal_layout;
class SiteConfig;
}

namespace rgwrados::bucket_index {

/// Format the rados object name for a shard of the index layout generation.
auto shard_oid(std::string_view prefix, uint64_t gen,
               const rgw::bucket_index_normal_layout& index, uint32_t shard)
    -> std::string;

/// Initialize all of the index shard objects using exclusive create.
///
/// If judge_support_logrecord is true, issue the cls_rgw_bucket_init_index2()
/// op to detect whether the OSD supports the reshard log.
int init(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw::SiteConfig& site,
         const RGWBucketInfo& info,
         const rgw::bucket_index_layout_generation& index,
         bool judge_support_logrecord = false);

} // namespace rgwrados::bucket_index
