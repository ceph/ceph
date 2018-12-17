// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_USER_POLICY_H
#define CEPH_RGW_REST_USER_POLICY_H

class RGWRestUserPolicy : public RGWRESTOp {
protected:
  static constexpr int MAX_POLICY_NAME_LEN = 128;
  string policy_name;
  string user_name;
  string policy;

  bool validate_input();

public:
  int verify_permission() override;
  virtual uint64_t get_op() = 0;
  void send_response() override;
  void dump(Formatter *f) const;
};

class RGWUserPolicyRead : public RGWRestUserPolicy {
public:
  RGWUserPolicyRead() = default;
  int check_caps(RGWUserCaps& caps) override;
};

class RGWUserPolicyWrite : public RGWRestUserPolicy {
public:
  RGWUserPolicyWrite() = default;
  int check_caps(RGWUserCaps& caps) override;
};

class RGWPutUserPolicy : public RGWUserPolicyWrite {
public:
  RGWPutUserPolicy() = default;
  void execute() override;
  int get_params();
  const char* name() const override { return "put_user-policy"; }
  uint64_t get_op() override;
  RGWOpType get_type() override { return RGW_OP_PUT_USER_POLICY; }
};

class RGWGetUserPolicy : public RGWUserPolicyRead {
public:
  RGWGetUserPolicy() = default;
  void execute() override;
  int get_params();
  const char* name() const override { return "get_user_policy"; }
  uint64_t get_op() override;
  RGWOpType get_type() override { return RGW_OP_GET_USER_POLICY; }
};

class RGWListUserPolicies : public RGWUserPolicyRead {
public:
  RGWListUserPolicies() = default;
  void execute() override;
  int get_params();
  const char* name() const override { return "list_user_policies"; }
  uint64_t get_op() override;
  RGWOpType get_type() override { return RGW_OP_LIST_USER_POLICIES; }
};

class RGWDeleteUserPolicy : public RGWUserPolicyWrite {
public:
  RGWDeleteUserPolicy() = default;
  void execute() override;
  int get_params();
  const char* name() const override { return "delete_user_policy"; }
  uint64_t get_op() override;
  RGWOpType get_type() override { return RGW_OP_DELETE_USER_POLICY; }
};

#endif /* CEPH_RGW_REST_USER_POLICY_H */

