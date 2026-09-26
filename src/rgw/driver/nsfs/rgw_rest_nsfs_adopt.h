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

/* PUT /admin/nsfs/adopt?bucket=<name> -- mark an existing bucket.
 *
 * A function point, not a driver hint.  The hint endpoint is dev-gated
 * and defaults off because what it reaches is fault injection;  adoption
 * is an operator action on a production tree, performed once during a
 * migration, and gating it behind a debug flag would be the wrong
 * contract.  It carries its own capability for the same reason.
 *
 * Marking a bucket is what ends its ability to be handed back to NooBaa,
 * so this is never inferred.  Marking on first access was considered and
 * rejected:  it would convert a tree an operator was deliberately keeping
 * reversible, and would do it as a side effect of somebody reading it.
 * See bucket_profile.h. */
class RGWOp_NSFS_Adopt : public RGWRESTOp {
public:
  RGWOp_NSFS_Adopt() = default;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs", RGW_CAP_WRITE);
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

    uint32_t had = rgw::sal::nsfs::EXTENSIONS_NONE;
    op_ret = nsfs_driver->adopt_bucket(s, y, bucket, &had);
    if (op_ret < 0) {
      return;
    }

    Formatter* f = flusher.get_formatter();
    flusher.start(0);
    f->open_object_section("dummy");   /* outermost is not rendered */
    f->dump_string("bucket", bucket);
    /* what it was, so a caller can tell an adoption from a no-op */
    f->dump_unsigned("had", had);
    f->dump_unsigned("extensions", rgw::sal::nsfs::EXTENSIONS_DEFAULT);
    f->close_section();
    flusher.flush();
  }

  const char* name() const override { return "nsfs_adopt"; }
};

class RGWHandler_NSFS_Adopt : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_put() override { return new RGWOp_NSFS_Adopt; }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_NSFS_Adopt() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_NSFS_Adopt : public RGWRESTMgr {
public:
  RGWRESTMgr_NSFS_Adopt() = default;
  ~RGWRESTMgr_NSFS_Adopt() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver*,
			       req_state*,
			       const rgw::auth::StrategyRegistry& auth_registry,
			       const std::string&) override {
    return new RGWHandler_NSFS_Adopt(auth_registry);
  }
};
