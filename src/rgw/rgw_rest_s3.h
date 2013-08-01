#ifndef CEPH_RGW_REST_S3_H
#define CEPH_RGW_REST_S3_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_http_errors.h"
#include "rgw_acl_s3.h"
#include "rgw_policy_s3.h"

#define RGW_AUTH_GRACE_MINS 15

void rgw_get_errno_s3(struct rgw_http_errors *e, int err_no);

class RGWGetObj_ObjStore_S3 : public RGWGetObj_ObjStore
{
public:
  RGWGetObj_ObjStore_S3() {}
  ~RGWGetObj_ObjStore_S3() {}

  int send_response_data(bufferlist& bl, off_t ofs, off_t len);
};

class RGWListBuckets_ObjStore_S3 : public RGWListBuckets_ObjStore {
public:
  RGWListBuckets_ObjStore_S3() {}
  ~RGWListBuckets_ObjStore_S3() {}

  int get_params() {
    limit = 0; /* no limit */
    return 0;
  }
  virtual void send_response_begin(bool has_buckets);
  virtual void send_response_data(RGWUserBuckets& buckets);
  virtual void send_response_end();
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

class RGWGetBucketLogging_ObjStore_S3 : public RGWGetBucketLogging {
public:
  RGWGetBucketLogging_ObjStore_S3() {}
  ~RGWGetBucketLogging_ObjStore_S3() {}

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

struct post_part_field {
  string val;
  map<string, string> params;
};

struct post_form_part {
  string name;
  string content_type;
  map<string, struct post_part_field, ltstr_nocase> fields;
  bufferlist data;
};

class RGWPostObj_ObjStore_S3 : public RGWPostObj_ObjStore {
  string boundary;
  string filename;
  bufferlist in_data;
  map<string, post_form_part, const ltstr_nocase> parts;  
  RGWPolicyEnv env;
  RGWPolicy post_policy;
  string err_msg;

  int read_with_boundary(bufferlist& bl, uint64_t max, bool check_eol,
                         bool *reached_boundary,
			 bool *done);

  int read_line(bufferlist& bl, uint64_t max,
                bool *reached_boundary, bool *done);

  int read_data(bufferlist& bl, uint64_t max, bool *reached_boundary, bool *done);

  int read_form_part_header(struct post_form_part *part,
                            bool *done);
  bool part_str(const string& name, string *val);
  bool part_bl(const string& name, bufferlist *pbl);

  int get_policy();
  void rebuild_key(string& key);
public:
  RGWPostObj_ObjStore_S3() {}
  ~RGWPostObj_ObjStore_S3() {}

  int get_params();
  int complete_get_params();
  void send_response();
  int get_data(bufferlist& bl);
};

class RGWDeleteObj_ObjStore_S3 : public RGWDeleteObj_ObjStore {
public:
  RGWDeleteObj_ObjStore_S3() {}
  ~RGWDeleteObj_ObjStore_S3() {}

  void send_response();
};

class RGWCopyObj_ObjStore_S3 : public RGWCopyObj_ObjStore {
  bool sent_header;
public:
  RGWCopyObj_ObjStore_S3() : sent_header(false) {}
  ~RGWCopyObj_ObjStore_S3() {}

  int init_dest_policy();
  int get_params();
  void send_partial_response(off_t ofs);
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

  int get_policy_from_state(RGWRados *store, struct req_state *s, stringstream& ss);
  void send_response();
};

class RGWGetCORS_ObjStore_S3 : public RGWGetCORS_ObjStore {
public:
  RGWGetCORS_ObjStore_S3() {}
  ~RGWGetCORS_ObjStore_S3() {}

  void send_response();
};

class RGWPutCORS_ObjStore_S3 : public RGWPutCORS_ObjStore {
public:
  RGWPutCORS_ObjStore_S3() {}
  ~RGWPutCORS_ObjStore_S3() {}

  int get_params();
  void send_response();
};

class RGWDeleteCORS_ObjStore_S3 : public RGWDeleteCORS_ObjStore {
public:
  RGWDeleteCORS_ObjStore_S3() {}
  ~RGWDeleteCORS_ObjStore_S3() {}

  void send_response();
};

class RGWOptionsCORS_ObjStore_S3 : public RGWOptionsCORS_ObjStore {
public:
  RGWOptionsCORS_ObjStore_S3() {}
  ~RGWOptionsCORS_ObjStore_S3() {}

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

class RGW_Auth_S3 {
public:
  static int authorize(RGWRados *store, struct req_state *s);
};

class RGWHandler_Auth_S3 : public RGWHandler_ObjStore {
  friend class RGWRESTMgr_S3;
public:
  RGWHandler_Auth_S3() : RGWHandler_ObjStore() {}
  virtual ~RGWHandler_Auth_S3() {}

  virtual int validate_bucket_name(const string& bucket) {
    return 0;
  }

  virtual int validate_object_name(const string& bucket) { return 0; }

  virtual int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  virtual int authorize() {
    return RGW_Auth_S3::authorize(store, s);
  }
};

class RGWHandler_ObjStore_S3 : public RGWHandler_ObjStore {
  friend class RGWRESTMgr_S3;
public:
  static int init_from_header(struct req_state *s, int default_formatter, bool configurable_format);

  RGWHandler_ObjStore_S3() : RGWHandler_ObjStore() {}
  virtual ~RGWHandler_ObjStore_S3() {}

  int validate_bucket_name(const string& bucket, bool relaxed_names);

  virtual int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  virtual int authorize() {
    return RGW_Auth_S3::authorize(store, s);
  }
};

class RGWHandler_ObjStore_Service_S3 : public RGWHandler_ObjStore_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_head();
public:
  RGWHandler_ObjStore_Service_S3() {}
  virtual ~RGWHandler_ObjStore_Service_S3() {}
};

class RGWHandler_ObjStore_Bucket_S3 : public RGWHandler_ObjStore_S3 {
protected:
  bool is_acl_op() {
    return s->info.args.exists("acl");
  }
  bool is_cors_op() {
      return s->info.args.exists("cors");
  }
  bool is_obj_update_op() {
    return is_acl_op() || is_cors_op();
  }
  RGWOp *get_obj_op(bool get_data);

  RGWOp *op_get();
  RGWOp *op_head();
  RGWOp *op_put();
  RGWOp *op_delete();
  RGWOp *op_post();
  RGWOp *op_options();
public:
  RGWHandler_ObjStore_Bucket_S3() {}
  virtual ~RGWHandler_ObjStore_Bucket_S3() {}
};

class RGWHandler_ObjStore_Obj_S3 : public RGWHandler_ObjStore_S3 {
protected:
  bool is_acl_op() {
    return s->info.args.exists("acl");
  }
  bool is_cors_op() {
      return s->info.args.exists("cors");
  }
  bool is_obj_update_op() {
    return is_acl_op();
  }
  RGWOp *get_obj_op(bool get_data);

  RGWOp *op_get();
  RGWOp *op_head();
  RGWOp *op_put();
  RGWOp *op_delete();
  RGWOp *op_post();
  RGWOp *op_options();
public:
  RGWHandler_ObjStore_Obj_S3() {}
  virtual ~RGWHandler_ObjStore_Obj_S3() {}
};

class RGWRESTMgr_S3 : public RGWRESTMgr {
public:
  RGWRESTMgr_S3() {}
  virtual ~RGWRESTMgr_S3() {}

  virtual RGWRESTMgr *get_resource_mgr(struct req_state *s, const string& uri) {
    return this;
  }
  virtual RGWHandler *get_handler(struct req_state *s);
};


#endif
