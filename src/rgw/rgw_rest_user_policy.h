// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_arn.h"
#include "rgw_rest.h"
#include "rgw_sal_fwd.h"

class RGWRestUserPolicy : public RGWRESTOp {
protected:
  std::unique_ptr<rgw::sal::User> user;
  rgw::ARN user_arn;
  std::string policy_name;
  std::string user_name;
  std::string policy;

  virtual int get_params();
  bool validate_input();

public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  virtual uint64_t get_op() = 0;
  void send_response() override;
};

class RGWUserPolicyRead : public RGWRestUserPolicy {
public:
  RGWUserPolicyRead() = default;
  int check_caps(const RGWUserCaps& caps) override;
};

class RGWUserPolicyWrite : public RGWRestUserPolicy {
public:
  RGWUserPolicyWrite() = default;
  int check_caps(const RGWUserCaps& caps) override;
};

class RGWPutUserPolicy : public RGWUserPolicyWrite {
  int get_params() override;
public:
  RGWPutUserPolicy() = default;
  void execute(optional_yield y) override;
  const char* name() const override { return "put_user_policy"; }
  uint64_t get_op() override;
  RGWOpType get_type() override { return RGW_OP_PUT_USER_POLICY; }
};

class RGWGetUserPolicy : public RGWUserPolicyRead {
  int get_params() override;
public:
  RGWGetUserPolicy() = default;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_user_policy"; }
  uint64_t get_op() override;
  RGWOpType get_type() override { return RGW_OP_GET_USER_POLICY; }
};

class RGWListUserPolicies : public RGWUserPolicyRead {
public:
  RGWListUserPolicies() = default;
  void execute(optional_yield y) override;
  const char* name() const override { return "list_user_policies"; }
  uint64_t get_op() override;
  RGWOpType get_type() override { return RGW_OP_LIST_USER_POLICIES; }
};

class RGWDeleteUserPolicy : public RGWUserPolicyWrite {
  int get_params() override;
public:
  RGWDeleteUserPolicy() = default;
  void execute(optional_yield y) override;
  const char* name() const override { return "delete_user_policy"; }
  uint64_t get_op() override;
  RGWOpType get_type() override { return RGW_OP_DELETE_USER_POLICY; }
};
