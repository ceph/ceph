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
#include "include/encoding.h"

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

struct resource_metadata {
  std::string policy_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(policy_id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(policy_id, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<resource_metadata*>& o);
};
WRITE_CLASS_ENCODER(resource_metadata);

/// Write or overwrite policy info and update its name/path objects.
int write(const DoutPrefixProvider* dpp, optional_yield y,
          librados::Rados& rados, RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
          const RGWZoneParams& zone, const ManagedPolicyInfo& info,
          RGWObjVersionTracker& objv, ceph::real_time mtime,
          bool exclusive);

int write(const DoutPrefixProvider *dpp, optional_yield y,
          librados::Rados &rados, RGWSI_SysObj &sysobj, RGWSI_MDLog *mdlog,
          const RGWZoneParams &zone, const ManagedPolicyInfo &info,
          const RGWAccountInfo &acc_info, std::map<std::string, bufferlist> &acc_attrs,
          RGWObjVersionTracker &objv, ceph::real_time mtime,
          bool exclusive);

int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const ManagedPolicyInfo& role,
        bool exclusive, uint32_t limit);

/// Role metadata handler factory.
auto create_metadata_handler(librados::Rados& rados,
                             RGWSI_SysObj& sysobj,
                             RGWSI_MDLog& mdlog,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>;

} // rgwrados::policy
