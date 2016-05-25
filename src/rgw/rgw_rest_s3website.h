// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Robin H. Johnson <robin.johnson@dreamhost.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#ifndef CEPH_RGW_REST_S3WEBSITE_H
#define CEPH_RGW_REST_S3WEBSITE_H
 
#include "rgw_rest_s3.h"

class RGWHandler_REST_S3Website : public RGWHandler_REST_S3 {
protected:
  int retarget(RGWOp *op, RGWOp **new_op);
  // TODO: this should be virtual I think, and ensure that it's always
  // overridden, but that conflates that op_get/op_head are defined in this
  // class and call this; and don't need to be overridden later.
  virtual RGWOp *get_obj_op(bool get_data) { return NULL; }
  RGWOp *op_get();
  RGWOp *op_head();
  // Only allowed to use GET+HEAD
  RGWOp *op_put() { return NULL; }
  RGWOp *op_delete() { return NULL; }
  RGWOp *op_post() { return NULL; }
  RGWOp *op_copy() { return NULL; }
  RGWOp *op_options() { return NULL; }

  int serve_errordoc(int http_ret, const string &errordoc_key);
public:
  RGWHandler_REST_S3Website() : RGWHandler_REST_S3() {}
  virtual ~RGWHandler_REST_S3Website() {}
  virtual int error_handler(int err_no, string *error_content);
};

class RGWHandler_REST_Service_S3Website : public RGWHandler_REST_S3Website {
protected:
  virtual RGWOp *get_obj_op(bool get_data);
public:
  RGWHandler_REST_Service_S3Website() {}
  virtual ~RGWHandler_REST_Service_S3Website() {}
};

class RGWHandler_REST_Obj_S3Website : public RGWHandler_REST_S3Website {
protected:
  virtual RGWOp *get_obj_op(bool get_data);
public:
  RGWHandler_REST_Obj_S3Website() {}
  virtual ~RGWHandler_REST_Obj_S3Website() {}
};

/* The cross-inheritance from Obj to Bucket is deliberate!
 * S3Websites do NOT support any bucket operations
 */
class RGWHandler_REST_Bucket_S3Website : public RGWHandler_REST_S3Website {
protected:
  RGWOp *get_obj_op(bool get_data);
public:
  RGWHandler_REST_Bucket_S3Website() {}
  virtual ~RGWHandler_REST_Bucket_S3Website() {}
};

// TODO: do we actually need this?
class  RGWGetObj_ObjStore_S3Website : public RGWGetObj_ObjStore_S3
{
  friend class RGWHandler_REST_S3Website;
private:
   bool is_errordoc_request;
public:
  RGWGetObj_ObjStore_S3Website() : is_errordoc_request(false) {}
  explicit RGWGetObj_ObjStore_S3Website(bool is_errordoc_request) : is_errordoc_request(false) { this->is_errordoc_request = is_errordoc_request; }
  ~RGWGetObj_ObjStore_S3Website() {}
  int send_response_data_error();
  int send_response_data(bufferlist& bl, off_t ofs, off_t len);
  // We override RGWGetObj_ObjStore::get_params here, to allow ignoring all
  // conditional params for error pages.
  int get_params() {
      if (is_errordoc_request) {
        range_str = NULL;
        if_mod = NULL;
        if_unmod = NULL;
        if_match = NULL;
        if_nomatch = NULL;
        return 0;
      } else {
        return RGWGetObj_ObjStore_S3::get_params();
      }
  }
};
 
#endif
