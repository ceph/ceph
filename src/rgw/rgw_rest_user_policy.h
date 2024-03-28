// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_arn.h"
#include "rgw_rest.h"
#include "rgw_user_types.h"
#include "rgw_sal_fwd.h"

class RGWRestUserPolicy : public RGWRESTOp {
protected:
  RGWRestUserPolicy(uint64_t action, uint32_t perm);

  uint64_t action;
  uint32_t perm;
  rgw_account_id account_id;
  std::unique_ptr<rgw::sal::User> user;
  rgw::ARN user_arn;
  std::string policy_name;
  std::string user_name;
  std::string policy;

  virtual int get_params();
  bool validate_input();

public:
  int init_processing(optional_yield y) override;
  int check_caps(const RGWUserCaps& caps) override;
  int verify_permission(optional_yield y) override;
  void send_response() override;
};

class RGWPutUserPolicy : public RGWRestUserPolicy {
  bufferlist post_body;
  int get_params() override;
  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
public:
  RGWPutUserPolicy(const ceph::bufferlist& post_body);
  void execute(optional_yield y) override;
  const char* name() const override { return "put_user_policy"; }
  RGWOpType get_type() override { return RGW_OP_PUT_USER_POLICY; }
};

class RGWGetUserPolicy : public RGWRestUserPolicy {
  int get_params() override;
public:
  RGWGetUserPolicy();
  void execute(optional_yield y) override;
  const char* name() const override { return "get_user_policy"; }
  RGWOpType get_type() override { return RGW_OP_GET_USER_POLICY; }
};

class RGWListUserPolicies : public RGWRestUserPolicy {
  std::string marker;
  int max_items = 100;
  int get_params() override;
public:
  RGWListUserPolicies();
  void execute(optional_yield y) override;
  const char* name() const override { return "list_user_policies"; }
  RGWOpType get_type() override { return RGW_OP_LIST_USER_POLICIES; }
};

class RGWDeleteUserPolicy : public RGWRestUserPolicy {
  bufferlist post_body;
  int get_params() override;
  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
public:
  RGWDeleteUserPolicy(const ceph::bufferlist& post_body);
  void execute(optional_yield y) override;
  const char* name() const override { return "delete_user_policy"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_USER_POLICY; }
};

RGWOp* make_iam_attach_user_policy_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_detach_user_policy_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_list_attached_user_policies_op(const ceph::bufferlist& unused);
