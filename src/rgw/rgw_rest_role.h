#ifndef CEPH_RGW_REST_ROLE_H
#define CEPH_RGW_REST_ROLE_H

class RGWRestRole : public RGWOp {
protected:
  string role_name;
  string role_path;
  string trust_policy;
  string policy_name;
  string perm_policy;
  string path_prefix;

public:
  virtual void send_response() override;
};

class RGWRoleRead : public RGWRestRole {
public:
  RGWRoleRead() = default;
  virtual int verify_permission() override;
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWRoleWrite : public RGWRestRole {
public:
  RGWRoleWrite() = default;
  virtual int verify_permission() override;
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
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
#endif /* CEPH_RGW_REST_ROLE_H */

