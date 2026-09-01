// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp
#pragma once

#include "rgw_op.h"
#include "rgw_inventory_s3.h"

// PUT /bucket?inventory&id=<id>
class RGWPutBucketInventory_ObjStore_S3 : public RGWOp {
  bufferlist data;
  rgw::inventory::Configuration config;
public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  int get_params(optional_yield y);
  void execute(optional_yield y) override;
  void send_response() override;
  const char* name() const override { return "put_bucket_inventory"; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_INVENTORY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

// GET /bucket?inventory&id=<id>
class RGWGetBucketInventory_ObjStore_S3 : public RGWOp {
  rgw::inventory::Configuration config;
public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  void send_response() override;
  const char* name() const override { return "get_bucket_inventory"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_INVENTORY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// DELETE /bucket?inventory&id=<id>
class RGWDeleteBucketInventory_ObjStore_S3 : public RGWOp {
public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  void send_response() override;
  const char* name() const override { return "delete_bucket_inventory"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET_INVENTORY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

// GET /bucket?inventory  (list all configs)
class RGWListBucketInventory_ObjStore_S3 : public RGWOp {
  rgw::inventory::BucketConfigurations configs;
public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  void send_response() override;
  const char* name() const override { return "list_bucket_inventory"; }
  RGWOpType get_type() override { return RGW_OP_LIST_BUCKET_INVENTORY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};
