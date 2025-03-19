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

#include "policy.h"

#include <optional>
#include <variant>

#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_role.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "svc_mdlog.h"

#include "account.h"
#include "rgw_customer_managed_policy.h"


namespace rgwrados::policy
{
/// Write or overwrite policy info and update its name/path objects.
int write(const DoutPrefixProvider *dpp, optional_yield y,
          librados::Rados &rados, RGWSI_SysObj &sysobj, RGWSI_MDLog *mdlog,
          const RGWZoneParams &zone, const ManagedPolicyInfo &info,
          RGWObjVersionTracker &objv, ceph::real_time mtime,
          bool exclusive)
{
  int r = 0;

  ManagedPolicyInfo old;
  ManagedPolicyInfo* old_info = nullptr;

  if(!exclusive) {
    //TODO
  }
}
}
