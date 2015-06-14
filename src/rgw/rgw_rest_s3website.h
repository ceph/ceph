#ifndef CEPH_RGW_REST_S3WEBSITE_H
#define CEPH_RGW_REST_S3WEBSITE_H
 
#include "rgw_rest_s3.h"

class RGWHandler_ObjStore_S3Website : public RGWHandler_ObjStore_S3 {
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
public:
  RGWHandler_ObjStore_S3Website() : RGWHandler_ObjStore_S3() {}
  virtual ~RGWHandler_ObjStore_S3Website() {}
};

class RGWHandler_ObjStore_Service_S3Website : public RGWHandler_ObjStore_S3Website {
protected:
  virtual RGWOp *get_obj_op(bool get_data);
public:
  RGWHandler_ObjStore_Service_S3Website() {}
  virtual ~RGWHandler_ObjStore_Service_S3Website() {}
};

class RGWHandler_ObjStore_Obj_S3Website : public RGWHandler_ObjStore_S3Website {
protected:
  virtual RGWOp *get_obj_op(bool get_data);
public:
  RGWHandler_ObjStore_Obj_S3Website() {}
  virtual ~RGWHandler_ObjStore_Obj_S3Website() {}
};

/* The cross-inheritance from Obj to Bucket is deliberate!
 * S3Websites do NOT support any bucket operations
 */
class RGWHandler_ObjStore_Bucket_S3Website : public RGWHandler_ObjStore_S3Website {
protected:
  RGWOp *get_obj_op(bool get_data);
public:
  RGWHandler_ObjStore_Bucket_S3Website() {}
  virtual ~RGWHandler_ObjStore_Bucket_S3Website() {}
};

// TODO: do we actually need this?
class  RGWGetObj_ObjStore_S3Website : public RGWGetObj_ObjStore_S3
{
public:
  RGWGetObj_ObjStore_S3Website() {}
  ~RGWGetObj_ObjStore_S3Website() {}
};
 
#endif
