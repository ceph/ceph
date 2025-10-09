// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_iam_managed_policy.h"
#include <span>

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

class RGWListPolicies : public RGWRestPolicy {
  std::string account_id;
  rgw::IAM::Scope scope;
  bool only_attached;
  std::string path_prefix;
  rgw::IAM::PolicyUsageFilter policy_usage_filter;
  std::string marker;
  int max_items = 100;

  bool started_response = false;
  void start_response();
  void end_response(std::string_view next_marker);
  void send_response_data(std::span<rgw::IAM::ManagedPolicyInfo> policies);
public:
  RGWListPolicies() : RGWRestPolicy(rgw::IAM::iamListPolicies, RGW_CAP_READ){ }

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_policies"; }
  RGWOpType get_type() override { return RGW_OP_LIST_POLICIES; }
};

class RGWCreatePolicyVersion : public RGWRestPolicy {
  std::string policy_arn;
  std::string policy_document;
  bool set_as_default = false;
public:
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  RGWCreatePolicyVersion() : RGWRestPolicy(rgw::IAM::iamCreatePolicyVersion, RGW_CAP_WRITE){ }
  const char* name() const override { return "create_policy_version"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_POLICY_VERSION; }
};