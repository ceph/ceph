#ifndef CEPH_RGW_REST_S3_H
#define CEPH_RGW_REST_S3_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_html_errors.h"
#include "rgw_acl_s3.h"

#define RGW_AUTH_GRACE_MINS 15

void rgw_get_errno_s3(struct rgw_html_errors *e, int err_no);

class RGWGetObj_ObjStore_S3 : public RGWGetObj_ObjStore
{
public:
  RGWGetObj_ObjStore_S3() {}
  ~RGWGetObj_ObjStore_S3() {}

  int send_response(bufferlist& bl);
};

class RGWListBuckets_ObjStore_S3 : public RGWListBuckets_ObjStore {
public:
  RGWListBuckets_ObjStore_S3() {}
  ~RGWListBuckets_ObjStore_S3() {}

  int get_params() { return 0; }
  void send_response();
};

class RGWListBucket_ObjStore_S3 : public RGWListBucket_ObjStore {
public:
  RGWListBucket_ObjStore_S3() {
    default_max = 1000;
  }
  ~RGWListBucket_ObjStore_S3() {}

  int get_params();
  void send_response();
};

class RGWStatBucket_ObjStore_S3 : public RGWStatBucket_ObjStore {
public:
  RGWStatBucket_ObjStore_S3() {}
  ~RGWStatBucket_ObjStore_S3() {}

  void send_response();
};

class RGWCreateBucket_ObjStore_S3 : public RGWCreateBucket_ObjStore {
public:
  RGWCreateBucket_ObjStore_S3() {}
  ~RGWCreateBucket_ObjStore_S3() {}

  int get_params();
  void send_response();
};

class RGWDeleteBucket_ObjStore_S3 : public RGWDeleteBucket_ObjStore {
public:
  RGWDeleteBucket_ObjStore_S3() {}
  ~RGWDeleteBucket_ObjStore_S3() {}

  void send_response();
};

class RGWPutObj_ObjStore_S3 : public RGWPutObj_ObjStore {
public:
  RGWPutObj_ObjStore_S3() {}
  ~RGWPutObj_ObjStore_S3() {}

  int get_params();
  void send_response();
};

class RGWDeleteObj_ObjStore_S3 : public RGWDeleteObj_ObjStore {
public:
  RGWDeleteObj_ObjStore_S3() {}
  ~RGWDeleteObj_ObjStore_S3() {}

  void send_response();
};

class RGWCopyObj_ObjStore_S3 : public RGWCopyObj_ObjStore {
public:
  RGWCopyObj_ObjStore_S3() {}
  ~RGWCopyObj_ObjStore_S3() {}

  int init_dest_policy();
  int get_params();
  void send_response();
};

class RGWGetACLs_ObjStore_S3 : public RGWGetACLs_ObjStore {
public:
  RGWGetACLs_ObjStore_S3() {}
  ~RGWGetACLs_ObjStore_S3() {}

  void send_response();
};

class RGWPutACLs_ObjStore_S3 : public RGWPutACLs_ObjStore {
public:
  RGWPutACLs_ObjStore_S3() {}
  ~RGWPutACLs_ObjStore_S3() {}

  int get_canned_policy(ACLOwner& owner, stringstream& ss);
  void send_response();
};


class RGWInitMultipart_ObjStore_S3 : public RGWInitMultipart_ObjStore {
public:
  RGWInitMultipart_ObjStore_S3() {}
  ~RGWInitMultipart_ObjStore_S3() {}

  int get_params();
  void send_response();
};

class RGWCompleteMultipart_ObjStore_S3 : public RGWCompleteMultipart_ObjStore {
public:
  RGWCompleteMultipart_ObjStore_S3() {}
  ~RGWCompleteMultipart_ObjStore_S3() {}

  void send_response();
};

class RGWAbortMultipart_ObjStore_S3 : public RGWAbortMultipart_ObjStore {
public:
  RGWAbortMultipart_ObjStore_S3() {}
  ~RGWAbortMultipart_ObjStore_S3() {}

  void send_response();
};

class RGWListMultipart_ObjStore_S3 : public RGWListMultipart_ObjStore {
public:
  RGWListMultipart_ObjStore_S3() {}
  ~RGWListMultipart_ObjStore_S3() {}

  void send_response();
};

class RGWListBucketMultiparts_ObjStore_S3 : public RGWListBucketMultiparts_ObjStore {
public:
  RGWListBucketMultiparts_ObjStore_S3() {
    default_max = 1000;
  }
  ~RGWListBucketMultiparts_ObjStore_S3() {}

  void send_response();
};

class RGWDeleteMultiObj_ObjStore_S3 : public RGWDeleteMultiObj_ObjStore {
public:
  RGWDeleteMultiObj_ObjStore_S3() {}
  ~RGWDeleteMultiObj_ObjStore_S3() {}

  void send_status();
  void begin_response();
  void send_partial_response(pair<string,int>& result);
  void end_response();
};


class RGWHandler_ObjStore_S3 : public RGWHandler_ObjStore {
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

  int init_from_header(struct req_state *s);
public:
  RGWHandler_ObjStore_S3() : RGWHandler_ObjStore() {}
  virtual ~RGWHandler_ObjStore_S3() {}

  bool filter_request(struct req_state *state) { return true; }
  int validate_bucket_name(const string& bucket);

  virtual int init(struct req_state *state, RGWClientIO *cio);
  int authorize();
};

#endif
