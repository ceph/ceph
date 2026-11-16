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

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_process_env.h"
#include "rgw_zone.h"

#include "rgw_sal_nsfs.h"

/* /admin/nsfs/bucket -- place a directory that is already there as a
 * bucket, NooBaa's `manage_nsfs bucket add --path` (DECISIONS.md
 * 2026-10-14).
 *
 * Under `nsfs-adoption`, for now:  an admin cap cannot be narrow enough
 * for self-service, so whoever calls this is automation.
 *
 *   PUT ?name=<bucket>&path=<directory>&owner=<account id>
 *
 * 400 with no name, path or owner, an invalid bucket name, an owner that
 * is a user rather than an account, or a path that is not an absolute
 * canonical directory;  404 for a path that
 * does not exist or an unknown owner;  403 when, under impersonation,
 * the owner cannot read and write the directory;  409 for a taken name,
 * a directory that is or was a bucket's, or a path nested with another
 * bucket's. */

class RGWOp_NSFS_Bucket_Put : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs-adoption", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    std::string name, path, owner;
    RESTArgs::get_string(s, "name", "", &name);
    RESTArgs::get_string(s, "path", "", &path);
    RESTArgs::get_string(s, "owner", "", &owner);
    if (name.empty() || path.empty() || owner.empty()) {
      op_ret = -EINVAL;
      return;
    }
    op_ret = valid_s3_bucket_name(
      name, s->cct->_conf->rgw_relaxed_s3_bucket_names);
    if (op_ret < 0) {
      return;
    }
    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }
    std::string profile;
    op_ret = nsfs_driver->place_bucket(s, y, name, path, parse_owner(owner),
				       s->penv.site->get_zonegroup(), &profile);
    if (op_ret < 0) {
      return;
    }
    Formatter* f = flusher.get_formatter();
    flusher.start(0);
    f->open_object_section("bucket");
    f->dump_string("name", name);
    f->dump_string("path", path);
    f->dump_string("owner", owner);
    f->dump_string("profile", profile);
    f->close_section();
    flusher.flush();
  }

  const char* name() const override { return "nsfs_bucket_put"; }
};

class RGWHandler_NSFS_Bucket : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_put() override { return new RGWOp_NSFS_Bucket_Put; }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_NSFS_Bucket() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_NSFS_Bucket : public RGWRESTMgr {
public:
  RGWRESTMgr_NSFS_Bucket() = default;
  ~RGWRESTMgr_NSFS_Bucket() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state* const s,
			       const rgw::auth::StrategyRegistry& auth_registry,
			       const std::string& frontend_prefix) override {
    return new RGWHandler_NSFS_Bucket(auth_registry);
  }
};
