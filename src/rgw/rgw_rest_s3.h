// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_S3_H

#define CEPH_RGW_REST_S3_H
#define TIME_BUF_SIZE 128

#include <mutex>

#include "rgw_op.h"
#include "rgw_http_errors.h"
#include "rgw_acl_s3.h"
#include "rgw_policy_s3.h"
#include "rgw_keystone.h"
#include "rgw_rest_conn.h"
#include "rgw_ldap.h"

#define RGW_AUTH_GRACE_MINS 15

void rgw_get_errno_s3(struct rgw_http_errors *e, int err_no);

class RGWGetObj_ObjStore_S3 : public RGWGetObj_ObjStore
{
public:
  RGWGetObj_ObjStore_S3() {}
  ~RGWGetObj_ObjStore_S3() {}

  int send_response_data_error();
  int send_response_data(bufferlist& bl, off_t ofs, off_t len);
};

class RGWListBuckets_ObjStore_S3 : public RGWListBuckets_ObjStore {
public:
  RGWListBuckets_ObjStore_S3() {}
  ~RGWListBuckets_ObjStore_S3() {}

  int get_params() {
    limit = -1; /* no limit */
    return 0;
  }
  virtual void send_response_begin(bool has_buckets);
  virtual void send_response_data(RGWUserBuckets& buckets);
  virtual void send_response_end();
};

class RGWGetUsage_ObjStore_S3 : public RGWGetUsage_ObjStore {
public:
  RGWGetUsage_ObjStore_S3() {}
  ~RGWGetUsage_ObjStore_S3() {}

  int get_params() ;
  virtual void send_response();
};

class RGWListBucket_ObjStore_S3 : public RGWListBucket_ObjStore {
  bool objs_container;
public:
  RGWListBucket_ObjStore_S3() : objs_container(false) {
    default_max = 1000;
  }
  ~RGWListBucket_ObjStore_S3() {}

  int get_params();
  void send_response();
  void send_versioned_response();
};

class RGWGetBucketLogging_ObjStore_S3 : public RGWGetBucketLogging {
public:
  RGWGetBucketLogging_ObjStore_S3() {}
  ~RGWGetBucketLogging_ObjStore_S3() {}

  void send_response();
};

class RGWGetBucketLocation_ObjStore_S3 : public RGWGetBucketLocation {
public:
  RGWGetBucketLocation_ObjStore_S3() {}
  ~RGWGetBucketLocation_ObjStore_S3() {}

  void send_response();
};

class RGWGetBucketVersioning_ObjStore_S3 : public RGWGetBucketVersioning {
public:
  RGWGetBucketVersioning_ObjStore_S3() {}
  ~RGWGetBucketVersioning_ObjStore_S3() {}

  void send_response();
};

class RGWSetBucketVersioning_ObjStore_S3 : public RGWSetBucketVersioning {
public:
  RGWSetBucketVersioning_ObjStore_S3() {}
  ~RGWSetBucketVersioning_ObjStore_S3() {}

  int get_params();
  void send_response();
};

class RGWGetBucketWebsite_ObjStore_S3 : public RGWGetBucketWebsite {
public:
  RGWGetBucketWebsite_ObjStore_S3() {}
  ~RGWGetBucketWebsite_ObjStore_S3() {}

  void send_response();
};

class RGWSetBucketWebsite_ObjStore_S3 : public RGWSetBucketWebsite {
public:
  RGWSetBucketWebsite_ObjStore_S3() {}
  ~RGWSetBucketWebsite_ObjStore_S3() {}

  int get_params();
  void send_response();
};

class RGWDeleteBucketWebsite_ObjStore_S3 : public RGWDeleteBucketWebsite {
public:
  RGWDeleteBucketWebsite_ObjStore_S3() {}
  ~RGWDeleteBucketWebsite_ObjStore_S3() {}

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
  int get_data(bufferlist& bl);
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

  int get_params();
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
  int get_params();
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

class RGWGetRequestPayment_ObjStore_S3 : public RGWGetRequestPayment {
public:
  RGWGetRequestPayment_ObjStore_S3() {}
  ~RGWGetRequestPayment_ObjStore_S3() {}

  void send_response();
};

class RGWSetRequestPayment_ObjStore_S3 : public RGWSetRequestPayment {
public:
  RGWSetRequestPayment_ObjStore_S3() {}
  ~RGWSetRequestPayment_ObjStore_S3() {}

  int get_params();
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

  int get_params();
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

  int get_params();
  void send_status();
  void begin_response();
  void send_partial_response(rgw_obj_key& key, bool delete_marker,
                             const string& marker_version_id, int ret);
  void end_response();
};

class RGW_Auth_S3_Keystone_ValidateToken : public RGWHTTPClient {
private:
  bufferlist rx_buffer;
  bufferlist tx_buffer;
  bufferlist::iterator tx_buffer_it;
  vector<string> accepted_roles;

public:
  KeystoneToken response;

private:
  void set_tx_buffer(const string& d) {
    tx_buffer.clear();
    tx_buffer.append(d);
    tx_buffer_it = tx_buffer.begin();
    set_send_length(tx_buffer.length());
  }

public:
  explicit RGW_Auth_S3_Keystone_ValidateToken(CephContext *_cct)
      : RGWHTTPClient(_cct) {
    get_str_vec(cct->_conf->rgw_keystone_accepted_roles, accepted_roles);
  }

  int receive_header(void *ptr, size_t len) {
    return 0;
  }
  int receive_data(void *ptr, size_t len) {
    rx_buffer.append((char *)ptr, len);
    return 0;
  }

  int send_data(void *ptr, size_t len) {
    if (!tx_buffer_it.get_remaining())
      return 0; // nothing left to send

    int l = MIN(tx_buffer_it.get_remaining(), len);
    memcpy(ptr, tx_buffer_it.get_current_ptr().c_str(), l);
    try {
      tx_buffer_it.advance(l);
    } catch (buffer::end_of_buffer &e) {
      assert(0);
    }

    return l;
  }

  int validate_s3token(const string& auth_id, const string& auth_token, const string& auth_sign);

};

class RGW_Auth_S3 {
private:
  static std::mutex mtx;
  static rgw::LDAPHelper* ldh;

  static int authorize_v2(RGWRados *store, struct req_state *s);
  static int authorize_v4(RGWRados *store, struct req_state *s);
  static int authorize_v4_complete(RGWRados *store, struct req_state *s,
				  const string& request_payload,
				  bool unsigned_payload);
public:
  static int authorize(RGWRados *store, struct req_state *s);
  static int authorize_aws4_auth_complete(RGWRados *store, struct req_state *s);

  static inline void init(RGWRados* store) {
    if (! ldh) {
      std::lock_guard<std::mutex> lck(mtx);
      if (! ldh) {
	init_impl(store);
      }
    }
  }

  static inline rgw::LDAPHelper* get_ldap_ctx(RGWRados* store) {
    init(store);
    return ldh;
  }

  static void init_impl(RGWRados* store);
};

class RGWHandler_Auth_S3 : public RGWHandler_REST {
  friend class RGWRESTMgr_S3;
public:
  RGWHandler_Auth_S3() : RGWHandler_REST() {}
  virtual ~RGWHandler_Auth_S3() {}

  virtual int validate_bucket_name(const string& bucket) {
    return 0;
  }

  virtual int validate_object_name(const string& bucket) { return 0; }

  virtual int init(RGWRados *store, struct req_state *s, RGWClientIO *cio);
  virtual int authorize() {
    return RGW_Auth_S3::authorize(store, s);
  }
  int postauth_init() { return 0; }
};

class RGWHandler_REST_S3 : public RGWHandler_REST {
  friend class RGWRESTMgr_S3;
public:
  static int init_from_header(struct req_state *s, int default_formatter, bool configurable_format);

  RGWHandler_REST_S3() : RGWHandler_REST() {}
  virtual ~RGWHandler_REST_S3() {}

  int get_errordoc(const string& errordoc_key, string* error_content);  

  virtual int init(RGWRados *store, struct req_state *s, RGWClientIO *cio);
  virtual int authorize() {
    return RGW_Auth_S3::authorize(store, s);
  }
  int postauth_init();
  virtual int retarget(RGWOp *op, RGWOp **new_op) {
    *new_op = op;
    return 0;
  }
};

class RGWHandler_REST_Service_S3 : public RGWHandler_REST_S3 {
protected:
    bool is_usage_op() {
    return s->info.args.exists("usage");
  }
  RGWOp *op_get();
  RGWOp *op_head();
public:
  RGWHandler_REST_Service_S3() {}
  virtual ~RGWHandler_REST_Service_S3() {}
};

class RGWHandler_REST_Bucket_S3 : public RGWHandler_REST_S3 {
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
  bool is_request_payment_op() {
    return s->info.args.exists("requestPayment");
  }
  RGWOp *get_obj_op(bool get_data);

  RGWOp *op_get();
  RGWOp *op_head();
  RGWOp *op_put();
  RGWOp *op_delete();
  RGWOp *op_post();
  RGWOp *op_options();
public:
  RGWHandler_REST_Bucket_S3() {}
  virtual ~RGWHandler_REST_Bucket_S3() {}
};

class RGWHandler_REST_Obj_S3 : public RGWHandler_REST_S3 {
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
  RGWHandler_REST_Obj_S3() {}
  virtual ~RGWHandler_REST_Obj_S3() {}
};

class RGWRESTMgr_S3 : public RGWRESTMgr {
private:
  bool enable_s3website;
public:
  explicit RGWRESTMgr_S3(bool _enable_s3website = false)
    : enable_s3website(_enable_s3website)
    {}

  virtual ~RGWRESTMgr_S3() {}

  virtual RGWHandler_REST *get_handler(struct req_state *s);
};

class RGWHandler_REST_Obj_S3Website;

static inline bool looks_like_ip_address(const char *bucket)
{
  int num_periods = 0;
  bool expect_period = false;
  for (const char *b = bucket; *b; ++b) {
    if (*b == '.') {
      if (!expect_period)
	return false;
      ++num_periods;
      if (num_periods > 3)
	return false;
      expect_period = false;
    }
    else if (isdigit(*b)) {
      expect_period = true;
    }
    else {
      return false;
    }
  }
  return (num_periods == 3);
}

static inline bool valid_s3_object_name(const string& name) {
  if (name.size() > 1024) {
    return false;
  }
  if (check_utf8(name.c_str(), name.size())) {
    return false;
  }
  return true;
}

static inline int valid_s3_bucket_name(const string& name, bool relaxed=false)
{
  // This function enforces Amazon's spec for bucket names.
  // (The requirements, not the recommendations.)
  int len = name.size();
  if (len < 3) {
    // Name too short
    return -ERR_INVALID_BUCKET_NAME;
  } else if (len > 255) {
    // Name too long
    return -ERR_INVALID_BUCKET_NAME;
  }

  // bucket names must start with a number, letter, or underscore
  if (!(isalpha(name[0]) || isdigit(name[0]))) {
    if (!relaxed)
      return -ERR_INVALID_BUCKET_NAME;
    else if (!(name[0] == '_' || name[0] == '.' || name[0] == '-'))
      return -ERR_INVALID_BUCKET_NAME;
  }

  for (const char *s = name.c_str(); *s; ++s) {
    char c = *s;
    if (isdigit(c) || (c == '.'))
      continue;
    if (isalpha(c))
      continue;
    if ((c == '-') || (c == '_'))
      continue;
    // Invalid character
    return -ERR_INVALID_BUCKET_NAME;
  }

  if (looks_like_ip_address(name.c_str()))
    return -ERR_INVALID_BUCKET_NAME;

  return 0;
}

#endif /* CEPH_RGW_REST_S3_H */
