// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef RGW_REST_REPLICA_LOG_H
#define RGW_REST_REPLICA_LOG_H

class RGWOp_OBJLog_GetBounds : public RGWRESTOp {
  string prefix;
  string obj_type;
  RGWReplicaBounds bounds;

public:
  RGWOp_OBJLog_GetBounds(const char *_prefix, const char *type) 
    : prefix(_prefix), obj_type(type){}
  ~RGWOp_OBJLog_GetBounds() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap(obj_type.c_str(), RGW_CAP_READ);
  }
  int verify_permission() override {
    return check_caps(s->user->caps);
  }
  void execute() override;
  void send_response() override;
  const string name() override {
    string s = "replica";
    s.append(obj_type);
    s.append("_getbounds");
    return s;
  }
};

class RGWOp_OBJLog_SetBounds : public RGWRESTOp {
  string prefix;
  string obj_type;
public:
  RGWOp_OBJLog_SetBounds(const char *_prefix, const char *type) 
    : prefix(_prefix), obj_type(type){}
  ~RGWOp_OBJLog_SetBounds() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap(obj_type.c_str(), RGW_CAP_WRITE);
  }
  void execute() override;
  const string name() override {
    string s = "replica";
    s.append(obj_type);
    s.append("_updatebounds");
    return s;
  }
};

class RGWOp_OBJLog_DeleteBounds : public RGWRESTOp {
  string prefix;
  string obj_type;
public:
  RGWOp_OBJLog_DeleteBounds(const char *_prefix, const char *type) 
    : prefix(_prefix), obj_type(type){}
  ~RGWOp_OBJLog_DeleteBounds() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap(obj_type.c_str(), RGW_CAP_WRITE);
  }
  void execute() override;
  const string name() override {
    string s = "replica";
    s.append(obj_type);
    s.append("_deletebound");
    return s;
  }
};

class RGWOp_BILog_GetBounds : public RGWRESTOp {
  RGWReplicaBounds bounds;
public:
  RGWOp_BILog_GetBounds() {}
  ~RGWOp_BILog_GetBounds() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("bilog", RGW_CAP_READ);
  }
  int verify_permission() override {
    return check_caps(s->user->caps);
  }
  void execute() override;
  void send_response() override;
  const string name() override {
    return "replicabilog_getbounds";
  }
};

class RGWOp_BILog_SetBounds : public RGWRESTOp {
public:
  RGWOp_BILog_SetBounds() {}
  ~RGWOp_BILog_SetBounds() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("bilog", RGW_CAP_WRITE);
  }
  void execute() override;
  const string name() override {
    return "replicabilog_updatebounds";
  }
};

class RGWOp_BILog_DeleteBounds : public RGWRESTOp {
public:
  RGWOp_BILog_DeleteBounds() {}
  ~RGWOp_BILog_DeleteBounds() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("bilog", RGW_CAP_WRITE);
  }
  void execute() override;
  const string name() override {
    return "replicabilog_deletebound";
  }
};

class RGWHandler_ReplicaLog : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_delete() override;
  RGWOp *op_post() override;

  int read_permissions(RGWOp*) override {
    return 0;
  }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_ReplicaLog() override = default;
};

class RGWRESTMgr_ReplicaLog : public RGWRESTMgr {
public:
  RGWRESTMgr_ReplicaLog() = default;
  ~RGWRESTMgr_ReplicaLog() override = default;

  RGWHandler_REST* get_handler(struct req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_ReplicaLog(auth_registry);
  }
};

#endif /*!RGW_REST_REPLICA_LOG_H*/
