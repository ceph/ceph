// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#pragma once
 
#include "rgw_rest_s3.h"

class RGWHandler_REST_S3Website : public RGWHandler_REST_S3 {
  std::string original_object_name; // object name before retarget()
  bool web_dir() const;
protected:
  int retarget(RGWOp *op, RGWOp **new_op) override;
  // TODO: this should be virtual I think, and ensure that it's always
  // overridden, but that conflates that op_get/op_head are defined in this
  // class and call this; and don't need to be overridden later.
  virtual RGWOp *get_obj_op(bool get_data) { return NULL; }
  RGWOp *op_get() override;
  RGWOp *op_head() override;
  // Only allowed to use GET+HEAD
  RGWOp *op_put() override { return NULL; }
  RGWOp *op_delete() override { return NULL; }
  RGWOp *op_post() override { return NULL; }
  RGWOp *op_copy() override { return NULL; }
  RGWOp *op_options() override { return NULL; }

  int serve_errordoc(int http_ret, const string &errordoc_key);
public:
  using RGWHandler_REST_S3::RGWHandler_REST_S3;
  ~RGWHandler_REST_S3Website() override = default;

  int init(rgw::sal::RGWRadosStore *store, req_state *s, rgw::io::BasicClient* cio) override;
  int error_handler(int err_no, string *error_content) override;
};

class RGWHandler_REST_Service_S3Website : public RGWHandler_REST_S3Website {
protected:
  RGWOp *get_obj_op(bool get_data) override;
public:
  using RGWHandler_REST_S3Website::RGWHandler_REST_S3Website;
  ~RGWHandler_REST_Service_S3Website() override = default;
};

class RGWHandler_REST_Obj_S3Website : public RGWHandler_REST_S3Website {
protected:
  RGWOp *get_obj_op(bool get_data) override;
public:
  using RGWHandler_REST_S3Website::RGWHandler_REST_S3Website;
  ~RGWHandler_REST_Obj_S3Website() override = default;
};

/* The cross-inheritance from Obj to Bucket is deliberate!
 * S3Websites do NOT support any bucket operations
 */
class RGWHandler_REST_Bucket_S3Website : public RGWHandler_REST_S3Website {
protected:
  RGWOp *get_obj_op(bool get_data) override;
public:
  using RGWHandler_REST_S3Website::RGWHandler_REST_S3Website;
  ~RGWHandler_REST_Bucket_S3Website() override = default;
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
  ~RGWGetObj_ObjStore_S3Website() override {}
  int send_response_data_error(const Span& parent_span = nullptr) override;
  int send_response_data(bufferlist& bl, off_t ofs, off_t len, const Span& parent_span = nullptr) override;
  // We override RGWGetObj_ObjStore::get_params here, to allow ignoring all
  // conditional params for error pages.
  int get_params(const Span& parent_span = nullptr) override {
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
