// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_RGW_REST_GROUP_H
#define CEPH_RGW_REST_GROUP_H

class RGWRestGroup : public RGWRESTOp {
protected:
  string group_name;
  string group_path;
  string user;
  vector<string> users;
  string path_prefix;
  string new_group_name;
  string new_group_path;

public:
  int verify_permission() override;
  void send_response() override;
};

class RGWGroupRead : public RGWRestGroup {
public:
  RGWGroupRead() = default;
  int check_caps(RGWUserCaps& caps) override;
};

class RGWGroupWrite : public RGWRestGroup {
public:
  RGWGroupWrite() = default;
  int check_caps(RGWUserCaps& caps) override;
};

class RGWCreateGroup : public RGWGroupWrite {
public:
  RGWCreateGroup() = default;
  void execute() override;
  int get_params();
  const string name() override { return "create_group"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_GROUP; }
};

class RGWAddUserToGroup : public RGWGroupWrite {
public:
  RGWAddUserToGroup() = default;
  void execute() override;
  int get_params();
  const string name() override { return "add_user_to_group"; }
  RGWOpType get_type() override { return RGW_OP_ADD_USER_TO_GROUP; }
};

class RGWGetGroup : public RGWGroupRead {
public:
  RGWGetGroup() = default;
  void execute() override;
  int get_params();
  const string name() override { return "get_group"; }
  RGWOpType get_type() override { return RGW_OP_GET_GROUP; }
};

class RGWRemoveUserFromGroup : public RGWGroupWrite {
public:
  RGWRemoveUserFromGroup() = default;
  void execute() override;
  int get_params();
  const string name() override { return "remove_user_from_group"; }
  RGWOpType get_type() override { return RGW_OP_REMOVE_USER_FROM_GROUP; }
};

class RGWUpdateGroup : public RGWGroupWrite {
public:
  RGWUpdateGroup() = default;
  void execute() override;
  int get_params();
  const string name() override { return "update_group"; }
  RGWOpType get_type() override { return RGW_OP_UPDATE_GROUP; }
};

class RGWListGroups : public RGWGroupRead {
public:
  RGWListGroups() = default;
  void execute() override;
  int get_params();
  const string name() override { return "list_groups"; }
  RGWOpType get_type() override { return RGW_OP_LIST_GROUPS; }
};

class RGWListGroupsForUser : public RGWGroupRead {
public:
  RGWListGroupsForUser() = default;
  void execute() override;
  int get_params();
  const string name() override { return "list_groups_for_user"; }
  RGWOpType get_type() override { return RGW_OP_LIST_GROUPS_FOR_USER; }
};

class RGWDeleteGroup : public RGWGroupWrite {
public:
  RGWDeleteGroup() = default;
  void execute() override;
  int get_params();
  const string name() override { return "delete_group"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_GROUP; }
};
#endif /* CEPH_RGW_REST_GROUP_H */

