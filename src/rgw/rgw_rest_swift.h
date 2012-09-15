#ifndef CEPH_RGW_REST_SWIFT_H
#define CEPH_RGW_REST_SWIFT_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_rest.h"

class RGWGetObj_REST_SWIFT : public RGWGetObj_REST {
public:
  RGWGetObj_REST_SWIFT() {}
  ~RGWGetObj_REST_SWIFT() {}

  int send_response(bufferlist& bl);
};

class RGWListBuckets_REST_SWIFT : public RGWListBuckets_REST {
  int limit_max;
  int limit;
  string marker;
public:
  RGWListBuckets_REST_SWIFT() {
    limit_max = 10000;
    limit = limit_max;
  }
  ~RGWListBuckets_REST_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWListBucket_REST_SWIFT : public RGWListBucket_REST {
  string path;
public:
  RGWListBucket_REST_SWIFT() {
    default_max = 10000;
  }
  ~RGWListBucket_REST_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWStatAccount_REST_SWIFT : public RGWStatAccount_REST {
public:
  RGWStatAccount_REST_SWIFT() {
  }
  ~RGWStatAccount_REST_SWIFT() {}

  void send_response();
};

class RGWStatBucket_REST_SWIFT : public RGWStatBucket_REST {
public:
  RGWStatBucket_REST_SWIFT() {}
  ~RGWStatBucket_REST_SWIFT() {}

  void send_response();
};

class RGWCreateBucket_REST_SWIFT : public RGWCreateBucket_REST {
public:
  RGWCreateBucket_REST_SWIFT() {}
  ~RGWCreateBucket_REST_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWDeleteBucket_REST_SWIFT : public RGWDeleteBucket_REST {
public:
  RGWDeleteBucket_REST_SWIFT() {}
  ~RGWDeleteBucket_REST_SWIFT() {}

  void send_response();
};

class RGWPutObj_REST_SWIFT : public RGWPutObj_REST {
public:
  RGWPutObj_REST_SWIFT() {}
  ~RGWPutObj_REST_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWPutMetadata_REST_SWIFT : public RGWPutMetadata_REST {
public:
  RGWPutMetadata_REST_SWIFT() {}
  ~RGWPutMetadata_REST_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWDeleteObj_REST_SWIFT : public RGWDeleteObj_REST {
public:
  RGWDeleteObj_REST_SWIFT() {}
  ~RGWDeleteObj_REST_SWIFT() {}

  void send_response();
};

class RGWCopyObj_REST_SWIFT : public RGWCopyObj_REST {
public:
  RGWCopyObj_REST_SWIFT() {}
  ~RGWCopyObj_REST_SWIFT() {}

  int init_dest_policy();
  int get_params();
  void send_response();
};

class RGWGetACLs_REST_SWIFT : public RGWGetACLs_REST {
public:
  RGWGetACLs_REST_SWIFT() {}
  ~RGWGetACLs_REST_SWIFT() {}

  void send_response() {}
};

class RGWPutACLs_REST_SWIFT : public RGWPutACLs_REST {
public:
  RGWPutACLs_REST_SWIFT() : RGWPutACLs_REST() {}
  virtual ~RGWPutACLs_REST_SWIFT() {}

  void send_response() {}
};


class RGWHandler_REST_SWIFT : public RGWHandler_REST {
protected:
  bool is_acl_op() {
    return false; // for now
  }
  bool is_obj_update_op() {
    return s->op == OP_POST;
  }

  RGWOp *get_retrieve_obj_op(bool get_data);
  RGWOp *get_retrieve_op(bool get_data);
  RGWOp *get_create_op();
  RGWOp *get_delete_op();
  RGWOp *get_post_op();
  RGWOp *get_copy_op();

public:
  RGWHandler_REST_SWIFT() : RGWHandler_REST() {}
  virtual ~RGWHandler_REST_SWIFT() {}

  int init(RGWRados *store, struct req_state *state, FCGX_Request *fcgx);
  int authorize();

  RGWAccessControlPolicy *alloc_policy() { return NULL; /* return new RGWAccessControlPolicy_SWIFT; */ }
  void free_policy(RGWAccessControlPolicy *policy) { delete policy; }
};

#endif
