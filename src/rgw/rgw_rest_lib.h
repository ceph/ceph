// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_LIB_H
#define CEPH_RGW_REST_LIB_H

class RGWRESTMgr_Lib : public RGWRESTMgr_S3 {
public:
  RGWRESTMgr_Lib() {}
  virtual ~RGWRESTMgr_Lib() {}
  virtual RGWHandler* get_handler(struct req_state* s);
};

class RGWHandler_ObjStore_Lib : public RGWHandler_User {
  friend class RGWRestMgr_Lib;
public:
  RGWHandler_ObjStore_Lib() {}
  virtual ~RGWHandler_ObjStore_Lib() {}
};

#endif /* CEPH_RGW_REST_LIB_H */
