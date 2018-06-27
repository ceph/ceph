// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "rgw_rest.h"
#include "rgw_rest_s3.h"

#ifndef RGW_REST_OPS_LOG_H
#define RGW_REST_OPS_LOG_H

class RGWRestOpsLog : public RGWRESTOp {
protected:
  string bucket;
  string bucket_id;
  string date;
  string oid;
public:
  int verify_permission() override;
  void send_response() override;
};

class RGWGetOpsLog : public RGWRestOpsLog {
public:
  RGWGetOpsLog() = default;
  void execute() override;
  void send_response() override;
  int check_caps(RGWUserCaps& caps) override;
  const char* name() const override { return "get_ops_log"; }
  RGWOpType get_type() override { return RGW_OP_GET_OPS_LOG; }
};

class RGWDeleteOpsLog : public RGWRestOpsLog {
public:
  RGWDeleteOpsLog() = default;
  void execute() override;
  void send_response() override;
  int check_caps(RGWUserCaps& caps) override;
  const char* name() const override { return "delete_ops_log"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_OPS_LOG; }
};

class RGWHandler_OpsLog : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_delete() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_OpsLog() override = default;

  int read_permissions(RGWOp*) override {
    return 0;
  }
};

class RGWRESTMgr_OpsLog : public RGWRESTMgr {
public:
  RGWRESTMgr_OpsLog() = default;
  ~RGWRESTMgr_OpsLog() override = default;

  RGWHandler_REST* get_handler(struct req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_OpsLog(auth_registry);
  }
};

#endif /* RGW_REST_OPS_LOG_H */
