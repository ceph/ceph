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
#ifndef CEPH_RGW_REST_LOG_H
#define CEPH_RGW_REST_LOG_H

class RGWOp_BILog_List : public RGWRESTOp {
public:
  RGWOp_BILog_List() {}
  ~RGWOp_BILog_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_READ);
  }
  void execute();
  virtual const char *name() {
    return "list bucket index log";
  }
};

class RGWOp_BILog_Delete : public RGWRESTOp {
public:
  RGWOp_BILog_Delete() {}
  ~RGWOp_BILog_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const char *name() {
    return "trim bucket index log";
  }
};

class RGWOp_MDLog_List : public RGWRESTOp {
public:
  RGWOp_MDLog_List() {}
  ~RGWOp_MDLog_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  void execute();
  virtual const char *name() {
    return "list metadata log";
  }
};

class RGWOp_MDLog_Delete : public RGWRESTOp {
public:
  RGWOp_MDLog_Delete() {}
  ~RGWOp_MDLog_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const char *name() {
    return "trim metadata log";
  }
};

class RGWHandler_Log : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_delete();

  int read_permissions(RGWOp*) {
    return 0;
  }
public:
  RGWHandler_Log() : RGWHandler_Auth_S3() {}
  virtual ~RGWHandler_Log() {}
};

class RGWRESTMgr_Log : public RGWRESTMgr {
public:
  RGWRESTMgr_Log() {}
  virtual ~RGWRESTMgr_Log() {}

  virtual RGWHandler *get_handler(struct req_state *s){
    return new RGWHandler_Log;
  }
};

#endif

