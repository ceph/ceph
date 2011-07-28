#ifndef CEPH_RGW_REST_OS_H
#define CEPH_RGW_REST_OS_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_rest.h"

class RGWGetObj_REST_OS : public RGWGetObj_REST {
public:
  RGWGetObj_REST_OS() {}
  ~RGWGetObj_REST_OS() {}

  int send_response(void *handle);
};

class RGWListBuckets_REST_OS : public RGWListBuckets_REST {
public:
  RGWListBuckets_REST_OS() {}
  ~RGWListBuckets_REST_OS() {}

  void send_response();
};

class RGWListBucket_REST_OS : public RGWListBucket_REST {
public:
  RGWListBucket_REST_OS() {
    limit_opt_name = "limit";
    default_max = 10000;
  }
  ~RGWListBucket_REST_OS() {}

  void send_response();
};

class RGWStatBucket_REST_OS : public RGWStatBucket_REST {
public:
  RGWStatBucket_REST_OS() {}
  ~RGWStatBucket_REST_OS() {}

  void send_response();
};

class RGWCreateBucket_REST_OS : public RGWCreateBucket_REST {
public:
  RGWCreateBucket_REST_OS() {}
  ~RGWCreateBucket_REST_OS() {}

  void send_response();
};

class RGWDeleteBucket_REST_OS : public RGWDeleteBucket_REST {
public:
  RGWDeleteBucket_REST_OS() {}
  ~RGWDeleteBucket_REST_OS() {}

  void send_response();
};

class RGWPutObj_REST_OS : public RGWPutObj_REST {
public:
  RGWPutObj_REST_OS() {}
  ~RGWPutObj_REST_OS() {}

  void send_response();
};

class RGWDeleteObj_REST_OS : public RGWDeleteObj_REST {
public:
  RGWDeleteObj_REST_OS() {}
  ~RGWDeleteObj_REST_OS() {}

  void send_response();
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
protected:

  RGWOp *get_retrieve_obj_op(bool get_data);
  RGWOp *get_retrieve_op(bool get_data);
  RGWOp *get_create_op();
  RGWOp *get_delete_op();
  RGWOp *get_post_op() { return NULL; }

public:
  RGWHandler_REST_OS() : RGWHandler_REST() {}
  virtual ~RGWHandler_REST_OS() {}

  int authorize();
};

#endif
