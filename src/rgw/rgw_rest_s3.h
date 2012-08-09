#ifndef CEPH_RGW_REST_S3_H
#define CEPH_RGW_REST_S3_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_html_errors.h"
#include "rgw_acl_s3.h"

#define RGW_AUTH_GRACE_MINS 15

void rgw_get_errno_s3(struct rgw_html_errors *e, int err_no);

class RGWGetObj_REST_S3 : public RGWGetObj_REST
{
public:
  RGWGetObj_REST_S3() {}
  ~RGWGetObj_REST_S3() {}

  int send_response(bufferlist& bl);
};

class RGWListBuckets_REST_S3 : public RGWListBuckets_REST {
public:
  RGWListBuckets_REST_S3() {}
  ~RGWListBuckets_REST_S3() {}

  int get_params() { return 0; }
  void send_response();
};

class RGWListBucket_REST_S3 : public RGWListBucket_REST {
public:
  RGWListBucket_REST_S3() {
    default_max = 1000;
  }
  ~RGWListBucket_REST_S3() {}

  int get_params();
  void send_response();
};

class RGWStatBucket_REST_S3 : public RGWStatBucket_REST {
public:
  RGWStatBucket_REST_S3() {}
  ~RGWStatBucket_REST_S3() {}

  void send_response();
};

class RGWCreateBucket_REST_S3 : public RGWCreateBucket_REST {
public:
  RGWCreateBucket_REST_S3() {}
  ~RGWCreateBucket_REST_S3() {}

  int get_params();
  void send_response();
};

class RGWDeleteBucket_REST_S3 : public RGWDeleteBucket_REST {
public:
  RGWDeleteBucket_REST_S3() {}
  ~RGWDeleteBucket_REST_S3() {}

  void send_response();
};

class RGWPutObj_REST_S3 : public RGWPutObj_REST {
public:
  RGWPutObj_REST_S3() {}
  ~RGWPutObj_REST_S3() {}

  int get_params();
  void send_response();
};

class RGWDeleteObj_REST_S3 : public RGWDeleteObj_REST {
public:
  RGWDeleteObj_REST_S3() {}
  ~RGWDeleteObj_REST_S3() {}

  void send_response();
};

class RGWCopyObj_REST_S3 : public RGWCopyObj_REST {
public:
  RGWCopyObj_REST_S3() {}
  ~RGWCopyObj_REST_S3() {}

  int init_dest_policy();
  int get_params();
  void send_response();
};

class RGWGetACLs_REST_S3 : public RGWGetACLs_REST {
public:
  RGWGetACLs_REST_S3() {}
  ~RGWGetACLs_REST_S3() {}

  void send_response();
};

class RGWPutACLs_REST_S3 : public RGWPutACLs_REST {
public:
  RGWPutACLs_REST_S3() {}
  ~RGWPutACLs_REST_S3() {}

  int get_canned_policy(ACLOwner& owner, stringstream& ss);
  void send_response();
};


class RGWInitMultipart_REST_S3 : public RGWInitMultipart_REST {
public:
  RGWInitMultipart_REST_S3() {}
  ~RGWInitMultipart_REST_S3() {}

  int get_params();
  void send_response();
};

class RGWCompleteMultipart_REST_S3 : public RGWCompleteMultipart_REST {
public:
  RGWCompleteMultipart_REST_S3() {}
  ~RGWCompleteMultipart_REST_S3() {}

  void send_response();
};

class RGWAbortMultipart_REST_S3 : public RGWAbortMultipart_REST {
public:
  RGWAbortMultipart_REST_S3() {}
  ~RGWAbortMultipart_REST_S3() {}

  void send_response();
};

class RGWListMultipart_REST_S3 : public RGWListMultipart_REST {
public:
  RGWListMultipart_REST_S3() {}
  ~RGWListMultipart_REST_S3() {}

  void send_response();
};

class RGWListBucketMultiparts_REST_S3 : public RGWListBucketMultiparts_REST {
public:
  RGWListBucketMultiparts_REST_S3() {
    default_max = 1000;
  }
  ~RGWListBucketMultiparts_REST_S3() {}

  void send_response();
};

class RGWDeleteMultiObj_REST_S3 : public RGWDeleteMultiObj_REST {
public:
  RGWDeleteMultiObj_REST_S3() {}
  ~RGWDeleteMultiObj_REST_S3() {}

  void send_status();
  void begin_response();
  void send_partial_response(pair<string,int>& result);
  void end_response();
};


class RGWHandler_REST_S3 : public RGWHandler_REST {
protected:
  bool is_acl_op() {
    return s->args.exists("acl");
  }
  bool is_obj_update_op() {
    return is_acl_op();
  }
  RGWOp *get_retrieve_obj_op(bool get_data);
  RGWOp *get_retrieve_op(bool get_data);
  RGWOp *get_create_op();
  RGWOp *get_delete_op();
  RGWOp *get_post_op();
  RGWOp *get_copy_op() { return NULL; }

public:
  RGWHandler_REST_S3() : RGWHandler_REST() {}
  virtual ~RGWHandler_REST_S3() {}

  virtual int init(RGWRados *store, struct req_state *state, FCGX_Request *fcgx);
  int authorize();
};

#endif
