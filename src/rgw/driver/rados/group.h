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

#include <map>
#include <memory>
#include <string>
#include "include/buffer_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include "common/async/yield_context.h"
#include "common/ceph_time.h"

class DoutPrefixProvider;
struct rgw_raw_obj;
class RGWGroupInfo;
class RGWMetadataHandler;
class RGWObjVersionTracker;
class RGWSI_SysObj;
class RGWZoneParams;

namespace rgwrados::group {

/// Group metadata handler factory
auto create_metadata_handler(RGWSI_SysObj& sysobj, librados::Rados& rados,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>;

/// Return the rados object that tracks the given group's users
rgw_raw_obj get_users_obj(const RGWZoneParams& zone,
                          std::string_view group_id);


/// Read group info by id
int read(const DoutPrefixProvider* dpp,
         optional_yield y,
         RGWSI_SysObj& sysobj,
         const RGWZoneParams& zone,
         std::string_view id,
         RGWGroupInfo& info,
         std::map<std::string, ceph::buffer::list>& attrs,
         ceph::real_time& mtime,
         RGWObjVersionTracker& objv);

/// Read group info by name
int read_by_name(const DoutPrefixProvider* dpp,
                 optional_yield y,
                 RGWSI_SysObj& sysobj,
                 const RGWZoneParams& zone,
                 std::string_view account_id,
                 std::string_view name,
                 RGWGroupInfo& info,
                 std::map<std::string, ceph::buffer::list>& attrs,
                 RGWObjVersionTracker& objv);

/// Write group info and update name index
int write(const DoutPrefixProvider* dpp,
          optional_yield y,
          RGWSI_SysObj& sysobj,
          librados::Rados& rados,
          const RGWZoneParams& zone,
          const RGWGroupInfo& info,
          const RGWGroupInfo* old_info,
          const std::map<std::string, ceph::buffer::list>& attrs,
          ceph::real_time mtime,
          bool exclusive,
          RGWObjVersionTracker& objv);

/// Remove group info and name index
int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           RGWSI_SysObj& sysobj,
           librados::Rados& rados,
           const RGWZoneParams& zone,
           const RGWGroupInfo& info,
           RGWObjVersionTracker& objv);

} // namespace rgwrados::group
