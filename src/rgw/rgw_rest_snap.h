#pragma once

#include "rgw_rest.h"
#include "rgw_bucket_snap_types.h"

class RGWListBucketSnapshots_ObjStore_S3 : public RGWRESTOp {
public:
  RGWListBucketSnapshots_ObjStore_S3() {}

  RGWOpType get_type() override { return RGW_OP_LIST_BUCKET_SNAPSHOTS; }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;

  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_bucket_snapshots"; }
};

class RGWCreateBucketSnapshot_ObjStore_S3 : public RGWRESTOp {
  std::string snap_name;
  std::string desc;

  rgw_bucket_snap snap;
public:
  RGWCreateBucketSnapshot_ObjStore_S3() {}

  RGWOpType get_type() override { return RGW_OP_CREATE_BUCKET_SNAPSHOT; }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;

  int get_params(optional_yield y);
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "create_bucket_snapshot"; }
};

