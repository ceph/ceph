// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_iam_managed_policy.h"

class RGWRestPolicy : public RGWOp {
  const uint64_t action;
  const uint32_t perm;
protected:
  rgw::ARN arn;

  RGWRestPolicy(uint64_t action, uint32_t perm) : action(action), perm(perm) {}
public:
  int verify_permission(optional_yield y) override;
  void send_response() override;
};

class RGWCreatePolicy : public RGWRestPolicy {
  bufferlist post_body;
  rgw::IAM::ManagedPolicyInfo info;
  int forward_to_master(optional_yield y, const rgw::SiteConfig& site, std::string& uid);
public:
  RGWCreatePolicy(const bufferlist& post_body) : RGWRestPolicy(rgw::IAM::iamCreatePolicy, RGW_CAP_WRITE), post_body(post_body){ }
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "create_policy"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_POLICY; }
};

class RGWGetPolicy : public RGWRestPolicy {
  rgw::IAM::ManagedPolicyInfo info;
public:
  RGWGetPolicy() : RGWRestPolicy(rgw::IAM::iamGetPolicy, RGW_CAP_READ){ }
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_policy"; }
  RGWOpType get_type() override { return RGW_OP_GET_POLICY; }
};

class RGWDeletePolicy : public RGWRestPolicy {
public:
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  RGWDeletePolicy() : RGWRestPolicy(rgw::IAM::iamDeletePolicy, RGW_CAP_WRITE){ }
  const char* name() const override { return "delete_policy"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_POLICY; }
};
