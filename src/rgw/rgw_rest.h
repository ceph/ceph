#ifndef CEPH_RGW_REST_H
#define CEPH_RGW_REST_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"

extern void rgw_flush_formatter_and_reset(struct req_state *s,
					 ceph::Formatter *formatter);

extern void rgw_flush_formatter(struct req_state *s,
                                         ceph::Formatter *formatter);

class RGWClientIO;

class RGWGetObj_ObjStore : public RGWGetObj
{
protected:
  bool sent_header;
public:
  RGWGetObj_ObjStore() : sent_header(false) {}

  virtual void init(struct req_state *s, RGWHandler *h) {
    RGWGetObj::init(s, h);
    sent_header = false;
  }

  int get_params();
};

class RGWListBuckets_ObjStore : public RGWListBuckets {
public:
  RGWListBuckets_ObjStore() {}
  ~RGWListBuckets_ObjStore() {}
};

class RGWListBucket_ObjStore : public RGWListBucket {
public:
  RGWListBucket_ObjStore() {}
  ~RGWListBucket_ObjStore() {}
};

class RGWStatAccount_ObjStore : public RGWStatAccount {
public:
  RGWStatAccount_ObjStore() {}
  ~RGWStatAccount_ObjStore() {}
};

class RGWStatBucket_ObjStore : public RGWStatBucket {
public:
  RGWStatBucket_ObjStore() {}
  ~RGWStatBucket_ObjStore() {}
};

class RGWCreateBucket_ObjStore : public RGWCreateBucket {
public:
  RGWCreateBucket_ObjStore() {}
  ~RGWCreateBucket_ObjStore() {}
};

class RGWDeleteBucket_ObjStore : public RGWDeleteBucket {
public:
  RGWDeleteBucket_ObjStore() {}
  ~RGWDeleteBucket_ObjStore() {}
};

class RGWPutObj_ObjStore : public RGWPutObj
{
public:
  RGWPutObj_ObjStore() {}
  ~RGWPutObj_ObjStore() {}

  virtual int verify_params();
  virtual int get_params();
  int get_data(bufferlist& bl);
};

class RGWPutMetadata_ObjStore : public RGWPutMetadata
{
public:
  RGWPutMetadata_ObjStore() {}
  ~RGWPutMetadata_ObjStore() {}
};

class RGWDeleteObj_ObjStore : public RGWDeleteObj {
public:
  RGWDeleteObj_ObjStore() {}
  ~RGWDeleteObj_ObjStore() {}
};

class RGWCopyObj_ObjStore : public RGWCopyObj {
public:
  RGWCopyObj_ObjStore() {}
  ~RGWCopyObj_ObjStore() {}
};

class RGWGetACLs_ObjStore : public RGWGetACLs {
public:
  RGWGetACLs_ObjStore() {}
  ~RGWGetACLs_ObjStore() {}
};

class RGWPutACLs_ObjStore : public RGWPutACLs {
public:
  RGWPutACLs_ObjStore() {}
  ~RGWPutACLs_ObjStore() {}

  int get_params();
};

class RGWInitMultipart_ObjStore : public RGWInitMultipart {
public:
  RGWInitMultipart_ObjStore() {}
  ~RGWInitMultipart_ObjStore() {}

  virtual int get_params();
};

class RGWCompleteMultipart_ObjStore : public RGWCompleteMultipart {
public:
  RGWCompleteMultipart_ObjStore() {}
  ~RGWCompleteMultipart_ObjStore() {}

  int get_params();
};

class RGWAbortMultipart_ObjStore : public RGWAbortMultipart {
public:
  RGWAbortMultipart_ObjStore() {}
  ~RGWAbortMultipart_ObjStore() {}
};

class RGWListMultipart_ObjStore : public RGWListMultipart {
public:
  RGWListMultipart_ObjStore() {}
  ~RGWListMultipart_ObjStore() {}

  int get_params();
};

class RGWListBucketMultiparts_ObjStore : public RGWListBucketMultiparts {
public:
  RGWListBucketMultiparts_ObjStore() {}
  ~RGWListBucketMultiparts_ObjStore() {}

  int get_params();
};

class RGWDeleteMultiObj_ObjStore : public RGWDeleteMultiObj {
public:
  RGWDeleteMultiObj_ObjStore() {}
  ~RGWDeleteMultiObj_ObjStore() {}

  int get_params();
};

class RGWHandler_ObjStore : public RGWHandler {
protected:
  virtual bool is_acl_op() = 0;
  virtual bool is_obj_update_op() = 0;

  virtual RGWOp *op_get() = 0;
  virtual RGWOp *op_put() = 0;
  virtual RGWOp *op_delete() = 0;
  virtual RGWOp *op_head() = 0;
  virtual RGWOp *op_post() = 0;
  virtual RGWOp *op_copy() = 0;

  virtual int validate_bucket_name(const string& bucket);
  virtual int validate_object_name(const string& object);
public:
  RGWHandler_ObjStore() {}
  virtual ~RGWHandler_ObjStore() {}
  int read_permissions(RGWOp *op);

  static int preprocess(struct req_state *s, RGWClientIO *cio);
  virtual bool filter_request(struct req_state *s) = 0;
  virtual int authorize() = 0;
};

class RGWHandler_ObjStore_SWIFT;
class RGWHandler_SWIFT_Auth;
class RGWHandler_ObjStore_S3;

class RGWRESTMgr {
  vector<RGWHandler *> protocol_handlers;

public:
  RGWRESTMgr();
  ~RGWRESTMgr();
  RGWHandler *get_handler(struct req_state *s, RGWClientIO *cio,
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
