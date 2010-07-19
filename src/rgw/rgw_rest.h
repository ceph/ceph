#ifndef CEPH_RGW_REST_H
#define CEPH_RGW_REST_H

#include "rgw_op.h"

class RGWGetObj_REST : public RGWGetObj
{
  bool sent_header;
public:
  RGWGetObj_REST() {}
  ~RGWGetObj_REST() {}

  virtual void init(struct req_state *s) {
    RGWGetObj::init(s);
    sent_header = false;
  }

  int get_params();
  int send_response(void *handle);
};

class RGWListBuckets_REST : public RGWListBuckets {
public:
  RGWListBuckets_REST() {}
  ~RGWListBuckets_REST() {}

  void send_response();
};

class RGWListBucket_REST : public RGWListBucket {
public:
  RGWListBucket_REST() {}
  ~RGWListBucket_REST() {}

  void send_response();
};

class RGWCreateBucket_REST : public RGWCreateBucket {
public:
  RGWCreateBucket_REST() {}
  ~RGWCreateBucket_REST() {}

  void send_response();
};

class RGWDeleteBucket_REST : public RGWDeleteBucket {
public:
  RGWDeleteBucket_REST() {}
  ~RGWDeleteBucket_REST() {}

  void send_response();
};

class RGWPutObj_REST : public RGWPutObj
{
public:
  RGWPutObj_REST() {}
  ~RGWPutObj_REST() {}

  int get_params();
  void send_response();
};

class RGWDeleteObj_REST : public RGWDeleteObj {
public:
  RGWDeleteObj_REST() {}
  ~RGWDeleteObj_REST() {}

  void send_response();
};

class RGWCopyObj_REST : public RGWCopyObj {
public:
  RGWCopyObj_REST() {}
  ~RGWCopyObj_REST() {}

  int get_params();
  void send_response();
};

class RGWGetACLs_REST : public RGWGetACLs {
public:
  RGWGetACLs_REST() {}
  ~RGWGetACLs_REST() {}

  void send_response();
};

class RGWPutACLs_REST : public RGWPutACLs {
public:
  RGWPutACLs_REST() {}
  ~RGWPutACLs_REST() {}

  int get_params();
  void send_response();
};


class RGWHandler_REST : public RGWHandler {
  RGWGetObj_REST get_obj_op;
  RGWListBuckets_REST list_buckets_op;
  RGWListBucket_REST list_bucket_op;
  RGWCreateBucket_REST create_bucket_op;
  RGWDeleteBucket_REST delete_bucket_op;
  RGWPutObj_REST put_obj_op;
  RGWDeleteObj_REST delete_obj_op;
  RGWCopyObj_REST copy_obj_op;
  RGWGetACLs_REST get_acls_op;
  RGWPutACLs_REST put_acls_op;

  RGWOp *get_retrieve_obj_op(struct req_state *s, bool get_data);
  RGWOp *get_retrieve_op(struct req_state *s, bool get_data);
  RGWOp *get_create_op(struct req_state *s);
  RGWOp *get_delete_op(struct req_state *s);

protected:
  void provider_init_state();
public:
  RGWHandler_REST() : RGWHandler() {}
  ~RGWHandler_REST() {}
  RGWOp *get_op();
  int read_permissions();
};

extern void dump_errno(struct req_state *s, int err, struct rgw_err *rgwerr = NULL);
extern void end_header(struct req_state *s, const char *content_type = NULL);
extern void dump_start_xml(struct req_state *s);
extern void list_all_buckets_start(struct req_state *s);
extern void dump_owner(struct req_state *s, string& id, string& name);
extern void open_section(struct req_state *s, const char *name);
extern void close_section(struct req_state *s, const char *name);
extern void dump_bucket(struct req_state *s, RGWObjEnt& obj);
extern void abort_early(struct req_state *s, int err);
extern void list_all_buckets_end(struct req_state *s);
extern void dump_value(struct req_state *s, const char *name, const char *fmt, ...);
extern void dump_time(struct req_state *s, const char *name, time_t *t);

#endif
