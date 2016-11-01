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

#ifndef RGW_REST_METADATA_H
#define RGW_REST_METADATA_H

class RGWOp_Metadata_List : public RGWRESTOp {
public:
  RGWOp_Metadata_List() {}
  ~RGWOp_Metadata_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_READ);
  }
  void execute();
  virtual const string name();
};

class RGWOp_Metadata_Get : public RGWRESTOp {
public:
  RGWOp_Metadata_Get() {}
  ~RGWOp_Metadata_Get() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_READ);
  }
  void execute();
  virtual const string name();
};

class RGWOp_Metadata_Put : public RGWRESTOp {
  int get_data(bufferlist& bl);
  string update_status;
  obj_version ondisk_version;
public:
  RGWOp_Metadata_Put() {}
  ~RGWOp_Metadata_Put() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_WRITE);
  }
  void execute();
  void send_response();
  virtual const string name() { return "set_metadata"; }
  RGWOpType get_type() { return RGW_OP_ADMIN_SET_METADATA; }
};

class RGWOp_Metadata_Delete : public RGWRESTOp {
public:
  RGWOp_Metadata_Delete() {}
  ~RGWOp_Metadata_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() { return "remove_metadata"; }
};

class RGWOp_Metadata_Lock : public RGWRESTOp {
public:
  RGWOp_Metadata_Lock() {}
  ~RGWOp_Metadata_Lock() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "lock_metadata_object";
  }
};

class RGWOp_Metadata_Unlock : public RGWRESTOp {
public:
  RGWOp_Metadata_Unlock() {}
  ~RGWOp_Metadata_Unlock() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "unlock_metadata_object";
  }
};

class RGWHandler_Metadata : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_put();
  RGWOp *op_delete();
  RGWOp *op_post();

  int read_permissions(RGWOp*) {
    return 0;
  }
public:
  RGWHandler_Metadata() : RGWHandler_Auth_S3() {}
  virtual ~RGWHandler_Metadata() {}
};

class RGWRESTMgr_Metadata : public RGWRESTMgr {
public:
  RGWRESTMgr_Metadata() = default;
  virtual ~RGWRESTMgr_Metadata() = default;

  RGWHandler_REST* get_handler(struct req_state* const s,
                               const std::string& frontend_prefix) override {
    return new RGWHandler_Metadata;
  }
};

#endif /* RGW_REST_METADATA_H */
