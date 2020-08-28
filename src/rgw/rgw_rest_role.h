// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_role.h"

class RGWRestRole : public RGWRESTOp {
protected:
  string role_name;
  string role_path;
  string trust_policy;
  string policy_name;
  string perm_policy;
  string path_prefix;
  string max_session_duration;
  RGWRole _role;
public:
 int verify_permission(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override;
  virtual uint64_t get_op() = 0;
};

class RGWRoleRead : public RGWRestRole {
public:
  RGWRoleRead() = default;
  int check_caps(const RGWUserCaps& caps) override;
};

class RGWRoleWrite : public RGWRestRole {
public:
  RGWRoleWrite() = default;
  int check_caps(const RGWUserCaps& caps) override;
};

class RGWCreateRole : public RGWRoleWrite {
public:
  RGWCreateRole() = default;
 int verify_permission(const Span& parent_span = nullptr) override;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "create_role"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_ROLE; }
  uint64_t get_op() { return rgw::IAM::iamCreateRole; }
};

class RGWDeleteRole : public RGWRoleWrite {
public:
  RGWDeleteRole() = default;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "delete_role"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_ROLE; }
  uint64_t get_op() { return rgw::IAM::iamDeleteRole; }
};

class RGWGetRole : public RGWRoleRead {
  int _verify_permission(const RGWRole& role);
public:
  RGWGetRole() = default;
 int verify_permission(const Span& parent_span = nullptr) override;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "get_role"; }
  RGWOpType get_type() override { return RGW_OP_GET_ROLE; }
  uint64_t get_op() { return rgw::IAM::iamGetRole; }
};

class RGWModifyRole : public RGWRoleWrite {
public:
  RGWModifyRole() = default;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "modify_role"; }
  RGWOpType get_type() override { return RGW_OP_MODIFY_ROLE; }
  uint64_t get_op() { return rgw::IAM::iamModifyRole; }
};

class RGWListRoles : public RGWRoleRead {
public:
  RGWListRoles() = default;
 int verify_permission(const Span& parent_span = nullptr) override;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "list_roles"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ROLES; }
  uint64_t get_op() { return rgw::IAM::iamListRoles; }
};

class RGWPutRolePolicy : public RGWRoleWrite {
public:
  RGWPutRolePolicy() = default;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "put_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_PUT_ROLE_POLICY; }
  uint64_t get_op() { return rgw::IAM::iamPutRolePolicy; }
};

class RGWGetRolePolicy : public RGWRoleRead {
public:
  RGWGetRolePolicy() = default;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "get_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_GET_ROLE_POLICY; }
  uint64_t get_op() { return rgw::IAM::iamGetRolePolicy; }
};

class RGWListRolePolicies : public RGWRoleRead {
public:
  RGWListRolePolicies() = default;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "list_role_policies"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ROLE_POLICIES; }
  uint64_t get_op() { return rgw::IAM::iamListRolePolicies; }
};

class RGWDeleteRolePolicy : public RGWRoleWrite {
public:
  RGWDeleteRolePolicy() = default;
  void execute(const Span& parent_span = nullptr) override;
  int get_params();
  const char* name() const override { return "delete_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_ROLE_POLICY; }
  uint64_t get_op() { return rgw::IAM::iamDeleteRolePolicy; }
};
