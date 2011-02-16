#ifndef CEPH_RGW_REST_OS_H
#define CEPH_RGW_REST_OS_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_rest.h"

class RGWGetObj_REST_OS : public RGWGetObj_REST {
  bool sent_header;
public:
  RGWGetObj_REST_OS() {}
  ~RGWGetObj_REST_OS() {}

  int send_response(void *handle) { return 0; }
};

class RGWListBuckets_REST_OS : public RGWListBuckets_REST {
public:
  RGWListBuckets_REST_OS() {}
  ~RGWListBuckets_REST_OS() {}

  void send_response();
};

class RGWListBucket_REST_OS : public RGWListBucket_REST {
public:
  RGWListBucket_REST_OS() {}
  ~RGWListBucket_REST_OS() {}

  void send_response() {}
};

class RGWCreateBucket_REST_OS : public RGWCreateBucket_REST {
public:
  RGWCreateBucket_REST_OS() {}
  ~RGWCreateBucket_REST_OS() {}

  void send_response() {}
};

class RGWDeleteBucket_REST_OS : public RGWDeleteBucket_REST {
public:
  RGWDeleteBucket_REST_OS() {}
  ~RGWDeleteBucket_REST_OS() {}

  void send_response() {}
};

class RGWPutObj_REST_OS : public RGWPutObj_REST {
public:
  RGWPutObj_REST_OS() {}
  ~RGWPutObj_REST_OS() {}

  void send_response() {}
};

class RGWDeleteObj_REST_OS : public RGWDeleteObj_REST {
public:
  RGWDeleteObj_REST_OS() {}
  ~RGWDeleteObj_REST_OS() {}

  void send_response() {}
};

class RGWCopyObj_REST_OS : public RGWCopyObj_REST {
public:
  RGWCopyObj_REST_OS() {}
  ~RGWCopyObj_REST_OS() {}

  void send_response() {}
};

class RGWGetACLs_REST_OS : public RGWGetACLs_REST {
public:
  RGWGetACLs_REST_OS() {}
  ~RGWGetACLs_REST_OS() {}

  void send_response() {}
};

class RGWPutACLs_REST_OS : public RGWPutACLs_REST {
public:
  RGWPutACLs_REST_OS() : RGWPutACLs_REST() {}
  virtual ~RGWPutACLs_REST_OS() {}

  void send_response() {}
};


class RGWHandler_REST_OS : public RGWHandler_REST {
  RGWGetObj_REST_OS get_obj_op;
  RGWListBuckets_REST_OS list_buckets_op;
  RGWListBucket_REST_OS list_bucket_op;
  RGWCreateBucket_REST_OS create_bucket_op;
  RGWDeleteBucket_REST_OS delete_bucket_op;
  RGWPutObj_REST_OS put_obj_op;
  RGWDeleteObj_REST_OS delete_obj_op;
  RGWCopyObj_REST_OS copy_obj_op;
  RGWGetACLs_REST_OS get_acls_op;
  RGWPutACLs_REST_OS put_acls_op;

protected:

  RGWOp *get_retrieve_obj_op(struct req_state *s, bool get_data);
  RGWOp *get_retrieve_op(struct req_state *s, bool get_data);
  RGWOp *get_create_op(struct req_state *s);
  RGWOp *get_delete_op(struct req_state *s);

public:
  RGWHandler_REST_OS() : RGWHandler_REST() {}
  virtual ~RGWHandler_REST_OS() {}

  bool authorize(struct req_state *s);
};

#endif
