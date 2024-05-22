// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <boost/optional.hpp>
#include "common/async/yield_context.h"

#include "rgw_arn.h"
#include "rgw_role.h"
#include "rgw_rest.h"

class RGWRestRole : public RGWRESTOp {
  const uint64_t action;
  const uint32_t perm;
 protected:
  rgw_account_id account_id;
  rgw::ARN resource; // must be initialized before verify_permission()
  int check_caps(const RGWUserCaps& caps) override;

  RGWRestRole(uint64_t action, uint32_t perm) : action(action), perm(perm) {}
 public:
  int verify_permission(optional_yield y) override;
};

class RGWCreateRole : public RGWRestRole {
  bufferlist bl_post_body;
  std::string role_name;
  std::string role_path;
  std::string trust_policy;
  std::string description;
  std::string max_session_duration;
  std::multimap<std::string, std::string> tags;
public:
  explicit RGWCreateRole(const bufferlist& bl_post_body)
    : RGWRestRole(rgw::IAM::iamCreateRole, RGW_CAP_WRITE),
      bl_post_body(bl_post_body) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "create_role"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_ROLE; }
};

class RGWDeleteRole : public RGWRestRole {
  bufferlist bl_post_body;
  std::string role_name;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  explicit RGWDeleteRole(const bufferlist& bl_post_body)
    : RGWRestRole(rgw::IAM::iamDeleteRole, RGW_CAP_WRITE),
      bl_post_body(bl_post_body) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "delete_role"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_ROLE; }
};

class RGWGetRole : public RGWRestRole {
  std::string role_name;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  RGWGetRole() : RGWRestRole(rgw::IAM::iamGetRole, RGW_CAP_READ) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_role"; }
  RGWOpType get_type() override { return RGW_OP_GET_ROLE; }
};

class RGWModifyRoleTrustPolicy : public RGWRestRole {
  bufferlist bl_post_body;
  std::string role_name;
  std::string trust_policy;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  explicit RGWModifyRoleTrustPolicy(const bufferlist& bl_post_body)
    : RGWRestRole(rgw::IAM::iamModifyRoleTrustPolicy, RGW_CAP_WRITE),
      bl_post_body(bl_post_body) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "modify_role_trust_policy"; }
  RGWOpType get_type() override { return RGW_OP_MODIFY_ROLE_TRUST_POLICY; }
};

class RGWListRoles : public RGWRestRole {
  std::string path_prefix;
  std::string marker;
  int max_items = 100;
  std::string next_marker;
public:
  RGWListRoles() : RGWRestRole(rgw::IAM::iamListRoles, RGW_CAP_READ) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "list_roles"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ROLES; }
};

class RGWPutRolePolicy : public RGWRestRole {
  bufferlist bl_post_body;
  std::string role_name;
  std::string policy_name;
  std::string perm_policy;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  explicit RGWPutRolePolicy(const bufferlist& bl_post_body)
    : RGWRestRole(rgw::IAM::iamPutRolePolicy, RGW_CAP_WRITE),
      bl_post_body(bl_post_body) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "put_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_PUT_ROLE_POLICY; }
};

class RGWGetRolePolicy : public RGWRestRole {
  std::string role_name;
  std::string policy_name;
  std::string perm_policy;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  RGWGetRolePolicy() : RGWRestRole(rgw::IAM::iamGetRolePolicy, RGW_CAP_READ) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_GET_ROLE_POLICY; }
};

class RGWListRolePolicies : public RGWRestRole {
  std::string role_name;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  RGWListRolePolicies() : RGWRestRole(rgw::IAM::iamListRolePolicies, RGW_CAP_READ) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "list_role_policies"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ROLE_POLICIES; }
};

class RGWDeleteRolePolicy : public RGWRestRole {
  bufferlist bl_post_body;
  std::string role_name;
  std::string policy_name;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  explicit RGWDeleteRolePolicy(const bufferlist& bl_post_body)
    : RGWRestRole(rgw::IAM::iamDeleteRolePolicy, RGW_CAP_WRITE),
      bl_post_body(bl_post_body) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "delete_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_ROLE_POLICY; }
};

class RGWTagRole : public RGWRestRole {
  bufferlist bl_post_body;
  std::string role_name;
  std::multimap<std::string, std::string> tags;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  explicit RGWTagRole(const bufferlist& bl_post_body)
    : RGWRestRole(rgw::IAM::iamTagRole, RGW_CAP_WRITE),
      bl_post_body(bl_post_body) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "tag_role"; }
  RGWOpType get_type() override { return RGW_OP_TAG_ROLE; }
};

class RGWListRoleTags : public RGWRestRole {
  std::string role_name;
  std::multimap<std::string, std::string> tags;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  RGWListRoleTags() : RGWRestRole(rgw::IAM::iamListRoleTags, RGW_CAP_READ) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "list_role_tags"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ROLE_TAGS; }
};

class RGWUntagRole : public RGWRestRole {
  bufferlist bl_post_body;
  std::string role_name;
  std::vector<std::string> untag;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  explicit RGWUntagRole(const bufferlist& bl_post_body)
    : RGWRestRole(rgw::IAM::iamUntagRole, RGW_CAP_WRITE),
      bl_post_body(bl_post_body) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "untag_role"; }
  RGWOpType get_type() override { return RGW_OP_UNTAG_ROLE; }
};

class RGWUpdateRole : public RGWRestRole {
  bufferlist bl_post_body;
  std::string role_name;
  boost::optional<std::string> description;
  std::string max_session_duration;
  std::unique_ptr<rgw::sal::RGWRole> role;
public:
  explicit RGWUpdateRole(const bufferlist& bl_post_body)
    : RGWRestRole(rgw::IAM::iamUpdateRole, RGW_CAP_WRITE),
      bl_post_body(bl_post_body) {}
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "update_role"; }
  RGWOpType get_type() override { return RGW_OP_UPDATE_ROLE; }
};

RGWOp* make_iam_attach_role_policy_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_detach_role_policy_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_list_attached_role_policies_op(const ceph::bufferlist& unused);
