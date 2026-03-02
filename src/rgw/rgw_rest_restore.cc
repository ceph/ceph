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

#include "rgw_rest_restore.h"
#include "rgw_restore.h"

class RGWOp_Restore_Status : public RGWRESTOp {

public:
  RGWOp_Restore_Status() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "get_restore_status"; }
};

void RGWOp_Restore_Status::execute(optional_yield y)
{
  std::string bucket_name, tenant, object;
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_string(s, "tenant", tenant, &tenant);
  RESTArgs::get_string(s, "object", object, &object);
  rgw::restore::RestoreEntry entry;

  entry.bucket = rgw_bucket {tenant, bucket_name};
  entry.obj_key = rgw_obj_key {object};

  op_ret = driver->get_rgwrestore()->status(this, entry, s->err.message,
                                            flusher, y);
}

class RGWOp_Restore_List : public RGWRESTOp {

public:
  RGWOp_Restore_List() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "get_restore_list"; }
};

void RGWOp_Restore_List::execute(optional_yield y)
{
  std::string bucket_name, tenant, restore_status_filter;
  std::optional<std::string> restore_status_filter_optional;
  bool exists = false;
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_string(s, "tenant", tenant, &tenant);
  RESTArgs::get_string(s, "restore-status-filter", "", &restore_status_filter, &exists);
  rgw::restore::RestoreEntry entry;

  entry.bucket = rgw_bucket {tenant, bucket_name};

  if (exists) {
    restore_status_filter_optional = restore_status_filter;
  }

  op_ret = driver->get_rgwrestore()->list(this, entry, restore_status_filter_optional,
                                          s->err.message, flusher, y);
}

RGWOp* RGWHandler_Restore::op_get()
{
  if (s->info.args.sub_resource_exists("object"))
    return new RGWOp_Restore_Status;

  return new RGWOp_Restore_List;
}
