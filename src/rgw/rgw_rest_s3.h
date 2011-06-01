#ifndef CEPH_RGW_REST_S3_H
#define CEPH_RGW_REST_S3_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"

class RGWGetObj_REST_S3 : public RGWGetObj_REST
{
public:
  RGWGetObj_REST_S3() {}
  ~RGWGetObj_REST_S3() {}

  int send_response(void *handle);
};

class RGWListBuckets_REST_S3 : public RGWListBuckets_REST {
public:
  RGWListBuckets_REST_S3() {}
  ~RGWListBuckets_REST_S3() {}

  void send_response();
};

class RGWListBucket_REST_S3 : public RGWListBucket_REST {
public:
  RGWListBucket_REST_S3() {
    limit_opt_name ="max-keys";
    default_max = 1000;
  }
  ~RGWListBucket_REST_S3() {}

  void send_response();
};

class RGWCreateBucket_REST_S3 : public RGWCreateBucket_REST {
public:
  RGWCreateBucket_REST_S3() {}
  ~RGWCreateBucket_REST_S3() {}

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

  void send_response();
};


class RGWInitMultipart_REST_S3 : public RGWInitMultipart_REST {
public:
  RGWInitMultipart_REST_S3() {}
  ~RGWInitMultipart_REST_S3() {}

  void send_response();
};

class RGWCompleteMultipart_REST_S3 : public RGWCompleteMultipart_REST {
public:
  RGWCompleteMultipart_REST_S3() {}
  ~RGWCompleteMultipart_REST_S3() {}

  void send_response();
};

class RGWListMultipart_REST_S3 : public RGWListMultipart_REST {
public:
  RGWListMultipart_REST_S3() {}
  ~RGWListMultipart_REST_S3() {}

  void send_response();
};

class RGWHandler_REST_S3 : public RGWHandler_REST {
  RGWGetObj_REST_S3 get_obj_op;
  RGWListBuckets_REST_S3 list_buckets_op;
  RGWListBucket_REST_S3 list_bucket_op;
  RGWCreateBucket_REST_S3 create_bucket_op;
  RGWDeleteBucket_REST_S3 delete_bucket_op;
  RGWPutObj_REST_S3 put_obj_op;
  RGWDeleteObj_REST_S3 delete_obj_op;
  RGWCopyObj_REST_S3 copy_obj_op;
  RGWGetACLs_REST_S3 get_acls_op;
  RGWPutACLs_REST_S3 put_acls_op;
  RGWInitMultipart_REST_S3 init_multipart;
  RGWCompleteMultipart_REST_S3 complete_multipart;
  RGWListMultipart_REST_S3 list_multipart;

protected:

  RGWOp *get_retrieve_obj_op(struct req_state *s, bool get_data);
  RGWOp *get_retrieve_op(struct req_state *s, bool get_data);
  RGWOp *get_create_op(struct req_state *s);
  RGWOp *get_delete_op(struct req_state *s);
  RGWOp *get_post_op(struct req_state *s);

  bool expect100cont;

public:
  RGWHandler_REST_S3() : RGWHandler_REST() {}
  virtual ~RGWHandler_REST_S3() {}

  bool authorize(struct req_state *s);
};

#endif
