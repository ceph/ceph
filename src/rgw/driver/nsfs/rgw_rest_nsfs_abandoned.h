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

/* /admin/nsfs/abandoned -- NooBaa records an operator has decided will
 * not be carried further, and why (DECISIONS.md 2026-10-13).
 *
 * Under its own cap, `nsfs-adoption`, not the shared `nsfs`:  an
 * abandoned record counts as consumed, so it lets adoption complete,
 * and a bucket abandoned before its import is not served once it has.
 * Being able to change a bucket's profile does not grant that.
 *
 *   GET                         every abandoned record
 *   PUT ?id=<_id>&reason=<text> abandon one
 *   DELETE ?id=<_id>            take one back, before adoption is complete
 *
 * The stage says what was left undone and so what becomes of a bucket:
 * "not imported" leaves S3 at completion;  "not converted" stays, in
 * NooBaa's format;  "partly converted" stays, half-converted for good.
 *
 * 400 with no id or no reason;  404 for an `_id` NooBaa's store does not
 * hold, no store, or (DELETE) one that is not abandoned;  409 for a
 * record fully consumed or already abandoned, and for any change once
 * adoption is complete. */

static void dump_abandoned(Formatter* f,
			   const rgw::sal::nsfs::AbandonedRecord& r)
{
  f->open_object_section("abandoned");
  f->dump_string("id", r.noobaa_id);
  f->dump_string("kind", r.kind);
  f->dump_string("name", r.name);
  f->dump_string("stage", r.stage);
  f->dump_string("reason", r.reason);
  f->dump_string("at", r.at);
  /* only when abandoning an identity, and only in the PUT's response:
   * what now fails to import because of it */
  if (!r.dependent_buckets.empty()) {
    f->open_array_section("dependent_buckets");
    for (const auto& b : r.dependent_buckets) {
      f->dump_string("bucket", b);
    }
    f->close_section();
  }
  f->close_section();
}

class RGWOp_NSFS_Abandoned_Get : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs-adoption", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }
    std::vector<rgw::sal::nsfs::AbandonedRecord> all;
    op_ret = nsfs_driver->list_abandoned(s, all);
    if (op_ret < 0) {
      return;
    }
    Formatter* f = flusher.get_formatter();
    flusher.start(0);
    f->open_array_section("abandoned");
    for (const auto& r : all) {
      dump_abandoned(f, r);
    }
    f->close_section();
    flusher.flush();
  }

  const char* name() const override { return "nsfs_abandoned_get"; }
};

class RGWOp_NSFS_Abandoned_Put : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs-adoption", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    std::string id, reason;
    RESTArgs::get_string(s, "id", "", &id);
    RESTArgs::get_string(s, "reason", "", &reason);
    if (id.empty() || reason.empty()) {
      op_ret = -EINVAL;
      return;
    }
    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }
    rgw::sal::nsfs::AbandonedRecord r;
    op_ret = nsfs_driver->abandon_noobaa_record(s, y, id, reason, r);
    if (op_ret < 0) {
      return;
    }
    Formatter* f = flusher.get_formatter();
    flusher.start(0);
    dump_abandoned(f, r);
    flusher.flush();
  }

  const char* name() const override { return "nsfs_abandoned_put"; }
};

class RGWOp_NSFS_Abandoned_Delete : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs-adoption", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    std::string id;
    RESTArgs::get_string(s, "id", "", &id);
    if (id.empty()) {
      op_ret = -EINVAL;
      return;
    }
    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }
    op_ret = nsfs_driver->unabandon_noobaa_record(s, id);
  }

  const char* name() const override { return "nsfs_abandoned_delete"; }
};

class RGWHandler_NSFS_Abandoned : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_get() override { return new RGWOp_NSFS_Abandoned_Get; }
  RGWOp* op_put() override { return new RGWOp_NSFS_Abandoned_Put; }
  RGWOp* op_delete() override { return new RGWOp_NSFS_Abandoned_Delete; }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_NSFS_Abandoned() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_NSFS_Abandoned : public RGWRESTMgr {
public:
  RGWRESTMgr_NSFS_Abandoned() = default;
  ~RGWRESTMgr_NSFS_Abandoned() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state* const s,
			       const rgw::auth::StrategyRegistry& auth_registry,
			       const std::string& frontend_prefix) override {
    return new RGWHandler_NSFS_Abandoned(auth_registry);
  }
};
