// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_LIB_H
#define CEPH_RGW_REST_LIB_H


class RGWRESTMgr_Lib : public RGWRESTMgr {
public:
  RGWRESTMgr_Lib() {}
  virtual ~RGWRESTMgr_Lib() {}
  virtual RGWHandler* get_handler(struct req_state* s);
}; /* RGWRESTMgr_Lib */

class RGWHandler_ObjStore_Lib : public RGWHandler_User {
  friend class RGWRESTMgr_Lib;
public:
  RGWHandler_ObjStore_Lib() {}
  virtual ~RGWHandler_ObjStore_Lib() {}
}; /* RGWHandler_ObjStore_Lib */

class RGWListBuckets_ObjStore_Lib : public RGWListBuckets_ObjStore {
public:
  RGWListBuckets_ObjStore_Lib() {}
  ~RGWListBuckets_ObjStore_Lib() {}

  int get_params() {
    limit = -1; /* no limit */
    return 0;
  }
}; /* RGWListBuckets_ObjStore_Lib */

class RGWListBucket_ObjStore_Lib : public RGWListBucket_ObjStore {
public:
  RGWListBucket_ObjStore_Lib() {
    default_max = 1000;
  }
  ~RGWListBucket_ObjStore_Lib() {}
}; /* RGWListBucket_ObjStore_Lib */

class RGWGetBucketLogging_ObjStore_Lib : public RGWGetBucketLogging {
public:
  RGWGetBucketLogging_ObjStore_Lib() {}
  ~RGWGetBucketLogging_ObjStore_Lib() {}
}; /* RGWGetBucketLogging_ObjStore_Lib */

#endif /* CEPH_RGW_REST_LIB_H */
