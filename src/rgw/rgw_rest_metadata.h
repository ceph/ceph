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
#ifndef CEPH_RGW_REST_METADATA_H
#define CEPH_RGW_REST_METADATA_H

class RGWOp_Metadata_Get : public RGWRESTOp {
public:
  RGWOp_Metadata_Get() {}
  ~RGWOp_Metadata_Get() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_READ);
  }
  void execute();
  virtual const char *name();
};

class RGWOp_Metadata_Put : public RGWRESTOp {
  int get_data(bufferlist& bl);
public:
  RGWOp_Metadata_Put() {}
  ~RGWOp_Metadata_Put() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_WRITE);
  }
  void execute();
  virtual const char *name() { return "set_metadata"; }
};

class RGWOp_Metadata_Delete : public RGWRESTOp {
public:
  RGWOp_Metadata_Delete() {}
  ~RGWOp_Metadata_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("metadata", RGW_CAP_WRITE);
  }
  void execute();
  virtual const char *name() { return "remove_metadata"; }
};

class RGWHandler_Metadata : public RGWHandler_ObjStore {
protected:
  int init_from_header(struct req_state *state);
  RGWOp *op_get();
  RGWOp *op_put();
  RGWOp *op_delete();
public:
  RGWHandler_Metadata() : RGWHandler_ObjStore() {}
  virtual ~RGWHandler_Metadata() {}

  virtual int validate_bucket_name(const std::string& bucket) { return 0; }
  virtual int validate_object_name(const std::string& object) { return 0; }

  virtual int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  virtual int authorize();
};

class RGWRESTMgr_Metadata : public RGWRESTMgr {
public:
  RGWRESTMgr_Metadata() {}
  virtual ~RGWRESTMgr_Metadata() {}

  virtual RGWHandler *get_handler(struct req_state *s){
    return new RGWHandler_Metadata;
  }
};


#endif
