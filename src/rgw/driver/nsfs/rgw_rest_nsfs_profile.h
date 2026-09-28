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

#include "rgw_sal_nsfs.h"
#include "bucket_profile.h"

/* GET /admin/nsfs/profile?bucket=<name>
 *
 * Which profile a bucket is in, without changing it.  A migration has to
 * be able to verify what it did, and a test has to be able to assert on
 * a bucket it is not modifying. */
class RGWOp_NSFS_Profile_Get : public RGWRESTOp {
public:
  RGWOp_NSFS_Profile_Get() = default;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    std::string bucket;
    RESTArgs::get_string(s, "bucket", "", &bucket);
    if (bucket.empty()) {
      op_ret = -EINVAL;
      return;
    }

    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }

    uint32_t extensions = rgw::sal::nsfs::EXTENSIONS_NONE;
    std::string pname;
    op_ret = nsfs_driver->get_bucket_profile(s, y, bucket, &extensions,
					     &pname);
    if (op_ret < 0) {
      return;
    }

    Formatter* f = flusher.get_formatter();
    flusher.start(0);
    f->open_object_section("dummy");   /* outermost is not rendered */
    f->dump_string("bucket", bucket);
    f->dump_string("profile", pname);
    f->dump_unsigned("extensions", extensions);
    f->close_section();
    flusher.flush();
  }

  const char* name() const override { return "nsfs_profile_get"; }
};

/* PUT /admin/nsfs/profile?bucket=<name>&profile=base|shared|strong
 *
 * The one way a bucket's profile changes from outside the gateway.
 *
 * Named, not a bitmask:  the mask is how the answer is stored, and a
 * profile is what an operator means.  Moving a base bucket to shared or
 * strong takes a tree we did not write into our management, which is why
 * it is a declared act on a named bucket and never inferred from
 * access. */
class RGWOp_NSFS_Profile_Put : public RGWRESTOp {
public:
  RGWOp_NSFS_Profile_Put() = default;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    std::string bucket, want;
    RESTArgs::get_string(s, "bucket", "", &bucket);
    RESTArgs::get_string(s, "profile", "", &want);
    if (bucket.empty() || want.empty()) {
      op_ret = -EINVAL;
      return;
    }

    auto target = rgw::sal::nsfs::extensions_for_profile(want);
    if (! target) {
      op_ret = -EINVAL;
      return;
    }

    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }

    uint32_t had = rgw::sal::nsfs::EXTENSIONS_NONE;
    std::string had_profile;
    op_ret = nsfs_driver->set_bucket_profile(s, y, bucket, *target, &had,
					     &had_profile);
    if (op_ret < 0) {
      return;
    }

    Formatter* f = flusher.get_formatter();
    flusher.start(0);
    f->open_object_section("dummy");   /* outermost is not rendered */
    f->dump_string("bucket", bucket);
    /* what it was, so a caller can tell a change from a no-op */
    f->dump_string("had_profile", had_profile);
    f->dump_unsigned("had", had);
    f->dump_string("profile", want);
    f->dump_unsigned("extensions", *target);
    f->close_section();
    flusher.flush();
  }

  const char* name() const override { return "nsfs_profile_put"; }
};

class RGWHandler_NSFS_Profile : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_get() override { return new RGWOp_NSFS_Profile_Get; }
  RGWOp* op_put() override { return new RGWOp_NSFS_Profile_Put; }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_NSFS_Profile() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_NSFS_Profile : public RGWRESTMgr {
public:
  RGWRESTMgr_NSFS_Profile() = default;
  ~RGWRESTMgr_NSFS_Profile() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state* const s,
			       const rgw::auth::StrategyRegistry& auth_registry,
			       const std::string& frontend_prefix) override {
    return new RGWHandler_NSFS_Profile(auth_registry);
  }
};
