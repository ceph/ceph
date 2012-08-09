#ifndef CEPH_RGW_REST_H
#define CEPH_RGW_REST_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"

extern void rgw_flush_formatter_and_reset(struct req_state *s,
					 ceph::Formatter *formatter);

extern void rgw_flush_formatter(struct req_state *s,
                                         ceph::Formatter *formatter);

class RGWGetObj_REST : public RGWGetObj
{
protected:
  bool sent_header;
public:
  RGWGetObj_REST() {}

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWGetObj::init(store, s, h);
    sent_header = false;
  }

  int get_params();
};

class RGWListBuckets_REST : public RGWListBuckets {
public:
  RGWListBuckets_REST() {}
  ~RGWListBuckets_REST() {}
};

class RGWListBucket_REST : public RGWListBucket {
public:
  RGWListBucket_REST() {}
  ~RGWListBucket_REST() {}
};

class RGWStatAccount_REST : public RGWStatAccount {
public:
  RGWStatAccount_REST() {}
  ~RGWStatAccount_REST() {}
};

class RGWStatBucket_REST : public RGWStatBucket {
public:
  RGWStatBucket_REST() {}
  ~RGWStatBucket_REST() {}
};

class RGWCreateBucket_REST : public RGWCreateBucket {
public:
  RGWCreateBucket_REST() {}
  ~RGWCreateBucket_REST() {}
};

class RGWDeleteBucket_REST : public RGWDeleteBucket {
public:
  RGWDeleteBucket_REST() {}
  ~RGWDeleteBucket_REST() {}
};

class RGWPutObj_REST : public RGWPutObj
{
public:
  RGWPutObj_REST() {}
  ~RGWPutObj_REST() {}

  virtual int verify_params();
  virtual int get_params();
  int get_data(bufferlist& bl);
};

class RGWPutMetadata_REST : public RGWPutMetadata
{
public:
  RGWPutMetadata_REST() {}
  ~RGWPutMetadata_REST() {}
};

class RGWDeleteObj_REST : public RGWDeleteObj {
public:
  RGWDeleteObj_REST() {}
  ~RGWDeleteObj_REST() {}
};

class RGWCopyObj_REST : public RGWCopyObj {
public:
  RGWCopyObj_REST() {}
  ~RGWCopyObj_REST() {}
};

class RGWGetACLs_REST : public RGWGetACLs {
public:
  RGWGetACLs_REST() {}
  ~RGWGetACLs_REST() {}
};

class RGWPutACLs_REST : public RGWPutACLs {
public:
  RGWPutACLs_REST() {}
  ~RGWPutACLs_REST() {}

  int get_params();
};

class RGWInitMultipart_REST : public RGWInitMultipart {
public:
  RGWInitMultipart_REST() {}
  ~RGWInitMultipart_REST() {}

  virtual int get_params();
};

class RGWCompleteMultipart_REST : public RGWCompleteMultipart {
public:
  RGWCompleteMultipart_REST() {}
  ~RGWCompleteMultipart_REST() {}

  int get_params();
};

class RGWAbortMultipart_REST : public RGWAbortMultipart {
public:
  RGWAbortMultipart_REST() {}
  ~RGWAbortMultipart_REST() {}
};

class RGWListMultipart_REST : public RGWListMultipart {
public:
  RGWListMultipart_REST() {}
  ~RGWListMultipart_REST() {}

  int get_params();
};

class RGWListBucketMultiparts_REST : public RGWListBucketMultiparts {
public:
  RGWListBucketMultiparts_REST() {}
  ~RGWListBucketMultiparts_REST() {}

  int get_params();
};

class RGWDeleteMultiObj_REST : public RGWDeleteMultiObj {
public:
  RGWDeleteMultiObj_REST() {}
  ~RGWDeleteMultiObj_REST() {}

  int get_params();
};

class RGWHandler_REST : public RGWHandler {
protected:
  virtual bool is_acl_op() = 0;
  virtual bool is_obj_update_op() = 0;

  virtual RGWOp *get_retrieve_obj_op(bool get_data) = 0;
  virtual RGWOp *get_retrieve_op(bool get_data) = 0;
  virtual RGWOp *get_create_op() = 0;
  virtual RGWOp *get_delete_op() = 0;
  virtual RGWOp *get_post_op() = 0;
  virtual RGWOp *get_copy_op() = 0;

public:
  int read_permissions(RGWOp *op);
  RGWOp *get_op();
  void put_op(RGWOp *op);

  static int preprocess(struct req_state *s, FCGX_Request *fcgx);
  virtual int authorize() = 0;
};

class RGWHandler_REST_SWIFT;
class RGWHandler_SWIFT_Auth;
class RGWHandler_REST_S3;

class RGWRESTMgr {
  RGWHandler_REST_SWIFT *m_os_handler;
  RGWHandler_SWIFT_Auth *m_os_auth_handler;
  RGWHandler_REST_S3 *m_s3_handler;

public:
  RGWRESTMgr();
  ~RGWRESTMgr();
  RGWHandler *get_handler(RGWRados *store, struct req_state *s, FCGX_Request *fcgx,
			  int *init_error);
};

extern void set_req_state_err(struct req_state *s, int err_no);
extern void dump_errno(struct req_state *s);
extern void dump_errno(struct req_state *s, int ret);
extern void end_header(struct req_state *s, const char *content_type = NULL);
extern void dump_start(struct req_state *s);
extern void list_all_buckets_start(struct req_state *s);
extern void dump_owner(struct req_state *s, string& id, string& name, const char *section = NULL);
extern void dump_content_length(struct req_state *s, size_t len);
extern void dump_etag(struct req_state *s, const char *etag);
extern void dump_last_modified(struct req_state *s, time_t t);
extern void abort_early(struct req_state *s, int err);
extern void dump_range(struct req_state *s, uint64_t ofs, uint64_t end, uint64_t total_size);
extern void dump_continue(struct req_state *s);
extern void list_all_buckets_end(struct req_state *s);
extern void dump_time(struct req_state *s, const char *name, time_t *t);

#endif
