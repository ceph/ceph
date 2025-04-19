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

#include "driver/rados/rgw_zone.h"
#include "rgw_iam_managed_policy.h"
#include "rgw_rest.h"
#include "rgw_arn.h"
#include "rgw_sal_fwd.h"
#include "rgw_user_types.h"

using namespace std;

class RGWRestIAMPolicy : public RGWRESTOp {
protected:
  RGWRestIAMPolicy(uint64_t action, uint32_t perm);

  uint64_t action;
  uint32_t perm;
  rgw_account_id account_id;
  std::unique_ptr<rgw::sal::User> user;
  rgw::ARN resource;
  std::string user_name;
  std::string policy;

  virtual int get_params();
public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void send_response() override;
  int check_caps(const RGWUserCaps& caps) override;
};

class RGWPutIAMPolicy : public RGWRestIAMPolicy {
  bufferlist post_body;
  int get_params() override;
  int forward_to_master(optional_yield y, const rgw::SiteConfig &site);
  std::string policy_name;
  std::string policy_path;
  std::string description;
  std::string policy_document;
  std::string default_version;
  std::multimap<std::string, std::string> tags;

public:
  RGWPutIAMPolicy(const ceph::bufferlist &post_body);
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char *name() const override { return "put_iam_policy"; }
  RGWOpType get_type() override {return RGW_OP_PUT_IAM_POLICY;}
};
