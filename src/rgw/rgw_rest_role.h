// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_RGW_REST_ROLE_H
#define CEPH_RGW_REST_ROLE_H

class RGWRestRole : public RGWRESTOp {
protected:
  string role_name;
  string role_path;
  string trust_policy;
  string policy_name;
  string perm_policy;
  string path_prefix;

public:
  int verify_permission() override;
  void send_response() override;
};

class RGWRoleRead : public RGWRestRole {
public:
  RGWRoleRead() = default;
  int check_caps(RGWUserCaps& caps) override;
};

class RGWRoleWrite : public RGWRestRole {
public:
  RGWRoleWrite() = default;
  int check_caps(RGWUserCaps& caps) override;
};

class RGWCreateRole : public RGWRoleWrite {
public:
  RGWCreateRole() = default;
  void execute() override;
  int get_params();
  const string name() override { return "create_role"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_ROLE; }
};

class RGWDeleteRole : public RGWRoleWrite {
public:
  RGWDeleteRole() = default;
  void execute() override;
  int get_params();
  const string name() override { return "delete_role"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_ROLE; }
};

class RGWGetRole : public RGWRoleRead {
public:
  RGWGetRole() = default;
  void execute() override;
  int get_params();
  const string name() override { return "get_role"; }
  RGWOpType get_type() override { return RGW_OP_GET_ROLE; }
};

class RGWModifyRole : public RGWRoleWrite {
public:
  RGWModifyRole() = default;
  void execute() override;
  int get_params();
  const string name() override { return "modify_role"; }
  RGWOpType get_type() override { return RGW_OP_MODIFY_ROLE; }
};

class RGWListRoles : public RGWRoleRead {
public:
  RGWListRoles() = default;
  void execute() override;
  int get_params();
  const string name() override { return "list_roles"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ROLES; }
};

class RGWPutRolePolicy : public RGWRoleWrite {
public:
  RGWPutRolePolicy() = default;
  void execute() override;
  int get_params();
  const string name() override { return "put_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_PUT_ROLE_POLICY; }
};

class RGWGetRolePolicy : public RGWRoleRead {
public:
  RGWGetRolePolicy() = default;
  void execute() override;
  int get_params();
  const string name() override { return "get_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_GET_ROLE_POLICY; }
};

class RGWListRolePolicies : public RGWRoleRead {
public:
  RGWListRolePolicies() = default;
  void execute() override;
  int get_params();
  const string name() override { return "list_role_policies"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ROLE_POLICIES; }
};

class RGWDeleteRolePolicy : public RGWRoleWrite {
public:
  RGWDeleteRolePolicy() = default;
  void execute() override;
  int get_params();
  const string name() override { return "delete_role_policy"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_ROLE_POLICY; }
};
#endif /* CEPH_RGW_REST_ROLE_H */

