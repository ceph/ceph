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
  ~RGWOp_OBJLog_GetBounds() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap(obj_type.c_str(), RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual const string name() {
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
  ~RGWOp_OBJLog_SetBounds() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap(obj_type.c_str(), RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
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
  ~RGWOp_OBJLog_DeleteBounds() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap(obj_type.c_str(), RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
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
  ~RGWOp_BILog_GetBounds() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual const string name() {
    return "replicabilog_getbounds";
  }
};

class RGWOp_BILog_SetBounds : public RGWRESTOp {
public:
  RGWOp_BILog_SetBounds() {}
  ~RGWOp_BILog_SetBounds() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "replicabilog_updatebounds";
  }
};

class RGWOp_BILog_DeleteBounds : public RGWRESTOp {
public:
  RGWOp_BILog_DeleteBounds() {}
  ~RGWOp_BILog_DeleteBounds() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "replicabilog_deletebound";
  }
};

class RGWHandler_ReplicaLog : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_delete();
  RGWOp *op_post();

  int read_permissions(RGWOp*) {
    return 0;
  }
public:
  RGWHandler_ReplicaLog() : RGWHandler_Auth_S3() {}
  virtual ~RGWHandler_ReplicaLog() {}
};

class RGWRESTMgr_ReplicaLog : public RGWRESTMgr {
public:
  RGWRESTMgr_ReplicaLog() {}
  virtual ~RGWRESTMgr_ReplicaLog() {}

  virtual RGWHandler_REST* get_handler(struct req_state *s){
    return new RGWHandler_ReplicaLog;
  }
};

#endif /*!RGW_REST_REPLICA_LOG_H*/
