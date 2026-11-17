// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "rgw_rest_s3.h"

// s3 compliant bucket inventory handler factory
class RGWHandler_REST_BucketInventory_S3 : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op, optional_yield y) override {return 0;}
  int read_permissions(RGWOp* op, optional_yield y) override {return 0;}
  bool supports_quota() override {return false;}
public:
  virtual ~RGWHandler_REST_BucketInventory_S3() = default;
  // GET ?inventory&id=<id>
  static RGWOp* create_get_op();
  // GET ?inventory
  static RGWOp* create_list_op();
  static RGWOp* create_put_op();
  static RGWOp* create_delete_op();
};
