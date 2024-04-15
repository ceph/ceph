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

#include <list>
#include <map>
#include <memory>
#include <string>
#include "include/encoding.h"
#include "include/rados/librados_fwd.hpp"
#include "common/async/yield_context.h"

namespace ceph { class Formatter; }
class DoutPrefixProvider;
class JSONObj;
struct rgw_raw_obj;
class RGWAccountInfo;
struct RGWBucketInfo;
class RGWMetadataHandler;
class RGWObjVersionTracker;
class RGWSI_SysObj;
class RGWStorageStats;
class RGWZoneParams;

namespace rgwrados::account {

/// Account metadata handler factory
auto create_metadata_handler(RGWSI_SysObj& sysobj, const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>;

/// Return the rados object that tracks the given account's buckets. This
/// can be used with the cls_user interface in namespace rgwrados::buckets.
rgw_raw_obj get_buckets_obj(const RGWZoneParams& zone,
                            std::string_view account_id);

/// Return the rados object that tracks the given account's users. This
/// can be used with the cls_user interface in namespace rgwrados::users.
rgw_raw_obj get_users_obj(const RGWZoneParams& zone,
                          std::string_view account_id);

/// Return the rados object that tracks the given account's groups. This
/// can be used with the cls_user interface in namespace rgwrados::groups.
rgw_raw_obj get_groups_obj(const RGWZoneParams& zone,
                           std::string_view account_id);

/// Return the rados object that tracks the given account's roles. This
/// can be used with the cls_user interface in namespace rgwrados::roles.
rgw_raw_obj get_roles_obj(const RGWZoneParams& zone,
                          std::string_view account_id);

/// Return the rados object that tracks the given account's topics. This
/// can be used with the cls_user interface in namespace rgwrados::topics.
rgw_raw_obj get_topics_obj(const RGWZoneParams& zone,
                           std::string_view account_id);


/// Read account info by id
int read(const DoutPrefixProvider* dpp,
         optional_yield y,
         RGWSI_SysObj& sysobj,
         const RGWZoneParams& zone,
         std::string_view account_id,
         RGWAccountInfo& info,
         std::map<std::string, ceph::buffer::list>& attrs,
         ceph::real_time& mtime,
         RGWObjVersionTracker& objv);

/// Read account info by name
int read_by_name(const DoutPrefixProvider* dpp,
                 optional_yield y,
                 RGWSI_SysObj& sysobj,
                 const RGWZoneParams& zone,
                 std::string_view tenant,
                 std::string_view name,
                 RGWAccountInfo& info,
                 std::map<std::string, ceph::buffer::list>& attrs,
                 RGWObjVersionTracker& objv);

/// Read account info by email
int read_by_email(const DoutPrefixProvider* dpp,
                  optional_yield y,
                  RGWSI_SysObj& sysobj,
                  const RGWZoneParams& zone,
                  std::string_view email,
                  RGWAccountInfo& info,
                  std::map<std::string, ceph::buffer::list>& attrs,
                  RGWObjVersionTracker& objv);

/// Write account info and update name/email indices
int write(const DoutPrefixProvider* dpp,
          optional_yield y,
          RGWSI_SysObj& sysobj,
          const RGWZoneParams& zone,
          const RGWAccountInfo& info,
          const RGWAccountInfo* old_info,
          const std::map<std::string, ceph::buffer::list>& attrs,
          ceph::real_time mtime,
          bool exclusive,
          RGWObjVersionTracker& objv);

/// Remove account info and name/email indices
int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           RGWSI_SysObj& sysobj,
           const RGWZoneParams& zone,
           const RGWAccountInfo& info,
           RGWObjVersionTracker& objv);


/// Read the resource count from an account index object.
int resource_count(const DoutPrefixProvider* dpp,
                   optional_yield y,
                   librados::Rados& rados,
                   const rgw_raw_obj& obj,
                   uint32_t& count);

} // namespace rgwrados::account
