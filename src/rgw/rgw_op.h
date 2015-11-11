// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/**
 * All operations via the rados gateway are carried out by
 * small classes known as RGWOps. This class contains a req_state
 * and each possible command is a subclass of this with a defined
 * execute() method that does whatever the subclass name implies.
 * These subclasses must be further subclassed (by interface type)
 * to provide additional virtual methods such as send_response or get_params.
 */
#ifndef CEPH_RGW_OP_H
#define CEPH_RGW_OP_H

#include <limits.h>

#include <string>
#include <set>
#include <map>

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_acl.h"
#include "rgw_cors.h"
#include "rgw_quota.h"

using namespace std;

struct req_state;
class RGWHandler;

enum RGWOpType {
  RGW_OP_UNKNOWN = 0,
  RGW_OP_GET_OBJ,
  RGW_OP_LIST_BUCKETS,
  RGW_OP_STAT_ACCOUNT,
  RGW_OP_LIST_BUCKET,
  RGW_OP_GET_BUCKET_LOGGING,
  RGW_OP_GET_BUCKET_VERSIONING,
  RGW_OP_SET_BUCKET_VERSIONING,
  RGW_OP_STAT_BUCKET,
  RGW_OP_CREATE_BUCKET,
  RGW_OP_DELETE_BUCKET,
  RGW_OP_PUT_OBJ,
  RGW_OP_POST_OBJ,
  RGW_OP_PUT_METADATA,
  RGW_OP_SET_TEMPURL,
  RGW_OP_DELETE_OBJ,
  RGW_OP_COPY_OBJ,
  RGW_OP_GET_ACLS,
  RGW_OP_PUT_ACLS,
  RGW_OP_GET_CORS,
  RGW_OP_PUT_CORS,
  RGW_OP_DELETE_CORS,
  RGW_OP_OPTIONS_CORS,
  RGW_OP_INIT_MULTIPART,
  RGW_OP_COMPLETE_MULTIPART,
  RGW_OP_ABORT_MULTIPART,
  RGW_OP_LIST_MULTIPART,
  RGW_OP_LIST_BUCKET_MULTIPARTS,
  RGW_OP_DELETE_MULTI_OBJ,
};

/**
 * Provide the base class for all ops.
 */
class RGWOp {
protected:
  struct req_state *s;
  RGWHandler *dialect_handler;
  RGWRados *store;
  RGWCORSConfiguration bucket_cors;
  bool cors_exist;
  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  virtual int init_quota();
public:
  RGWOp() : s(NULL), dialect_handler(NULL), store(NULL), cors_exist(false) {}
  virtual ~RGWOp() {}

  virtual int init_processing() {
    int ret = init_quota();
    if (ret < 0)
      return ret;

    return 0;
  }

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *dialect_handler) {
    this->store = store;
    this->s = s;
    this->dialect_handler = dialect_handler;
  }
  int read_bucket_cors();
  bool generate_cors_headers(string& origin, string& method, string& headers, string& exp_headers, unsigned *max_age);

  virtual int verify_params() { return 0; }
  virtual bool prefetch_data() { return false; }
  virtual int verify_permission() = 0;
  virtual int verify_op_mask();
  virtual void pre_exec() {}
  virtual void execute() = 0;
  virtual void send_response() {}
  virtual void complete() {
    send_response();
  }
  virtual const string name() = 0;
  virtual RGWOpType get_type() { return RGW_OP_UNKNOWN; }

  virtual uint32_t op_mask() { return 0; }
};

class RGWGetObj : public RGWOp {
protected:
  const char *range_str;
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  off_t ofs;
  uint64_t total_len;
  off_t start;
  off_t end;
  time_t mod_time;
  time_t lastmod;
  time_t unmod_time;
  time_t *mod_ptr;
  time_t *unmod_ptr;
  map<string, bufferlist> attrs;
  int ret;
  bool get_data;
  bool partial_content;
  rgw_obj obj;
  utime_t gc_invalidate_time;

  int init_common();
public:
  RGWGetObj() {
    range_str = NULL;
    if_mod = NULL;
    if_unmod = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    start = 0;
    ofs = 0;
    total_len = 0;
    end = -1;
    mod_time = 0;
    lastmod = 0;
    unmod_time = 0;
    mod_ptr = NULL;
    unmod_ptr = NULL;
    get_data = false;
    partial_content = false;
    ret = 0;
 }

  virtual bool prefetch_data() { return get_data; }

  void set_get_data(bool get_data) {
    this->get_data = get_data;
  }
  int verify_permission();
  void pre_exec();
  void execute();
  int read_user_manifest_part(rgw_bucket& bucket, RGWObjEnt& ent, RGWAccessControlPolicy *bucket_policy, off_t start_ofs, off_t end_ofs);
  int handle_user_manifest(const char *prefix);

  int get_data_cb(bufferlist& bl, off_t ofs, off_t len);

  virtual int get_params() = 0;
  virtual int send_response_data(bufferlist& bl, off_t ofs, off_t len) = 0;

  virtual const string name() { return "get_obj"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_OBJ; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

#define RGW_LIST_BUCKETS_LIMIT_MAX 10000

class RGWListBuckets : public RGWOp {
protected:
  int ret;
  bool sent_data;
  string marker;
  uint64_t limit;
  uint64_t limit_max;

public:
  RGWListBuckets() : ret(0), sent_data(false) {
    limit = limit_max = RGW_LIST_BUCKETS_LIMIT_MAX;
  }

  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response_begin(bool has_buckets) = 0;
  virtual void send_response_data(RGWUserBuckets& buckets) = 0;
  virtual void send_response_end() = 0;
  virtual void send_response() {}

  virtual bool should_get_stats() { return false; }

  virtual const string name() { return "list_buckets"; }
  virtual RGWOpType get_type() { return RGW_OP_LIST_BUCKETS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWStatAccount : public RGWOp {
protected:
  int ret;
  uint32_t buckets_count;
  uint64_t buckets_objcount;
  uint64_t buckets_size;
  uint64_t buckets_size_rounded;

public:
  RGWStatAccount() {
    ret = 0;
    buckets_count = 0;
    buckets_objcount = 0;
    buckets_size = 0;
    buckets_size_rounded = 0;
  }

  int verify_permission();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "stat_account"; }
  virtual RGWOpType get_type() { return RGW_OP_STAT_ACCOUNT; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWListBucket : public RGWOp {
protected:
  RGWBucketEnt bucket;
  string prefix;
  rgw_obj_key marker; 
  rgw_obj_key next_marker; 
  string max_keys;
  string delimiter;
  string encoding_type;
  bool list_versions;
  int max;
  int ret;
  vector<RGWObjEnt> objs;
  map<string, bool> common_prefixes;

  int default_max;
  bool is_truncated;

  int parse_max_keys();

public:
  RGWListBucket() : list_versions(false), max(0), ret(0),
                    default_max(0), is_truncated(false) {}
  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "list_bucket"; }
  virtual RGWOpType get_type() { return RGW_OP_LIST_BUCKET; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
  virtual bool need_container_stats() { return false; }
};

class RGWGetBucketLogging : public RGWOp {
public:
  RGWGetBucketLogging() {}
  int verify_permission();
  void execute() {}

  virtual void send_response() = 0;
  virtual const string name() { return "get_bucket_logging"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_BUCKET_LOGGING; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWGetBucketLocation : public RGWOp {
public:
  RGWGetBucketLocation() {}
  ~RGWGetBucketLocation() {}
  int verify_permission();
  void execute() {}

  virtual void send_response() = 0;
  virtual const string name() { return "get_bucket_location"; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWGetBucketVersioning : public RGWOp {
protected:
  bool versioned;
  bool versioning_enabled;
public:
  RGWGetBucketVersioning() : versioned(false), versioning_enabled(false) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "get_bucket_versioning"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_BUCKET_VERSIONING; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWSetBucketVersioning : public RGWOp {
protected:
  bool enable_versioning;
  int ret;
public:
  RGWSetBucketVersioning() : enable_versioning(false), ret(0) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() { return 0; }

  virtual void send_response() = 0;
  virtual const string name() { return "set_bucket_versioning"; }
  virtual RGWOpType get_type() { return RGW_OP_SET_BUCKET_VERSIONING; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWStatBucket : public RGWOp {
protected:
  int ret;
  RGWBucketEnt bucket;

public:
  RGWStatBucket() : ret(0) {}
  ~RGWStatBucket() {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "stat_bucket"; }
  virtual RGWOpType get_type() { return RGW_OP_STAT_BUCKET; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWCreateBucket : public RGWOp {
protected:
  int ret;
  RGWAccessControlPolicy policy;
  string location_constraint;
  string placement_rule;
  RGWBucketInfo info;
  obj_version ep_objv;
  bool has_cors;
  RGWCORSConfiguration cors_config;

  bufferlist in_data;

public:
  RGWCreateBucket() : ret(0), has_cors(false) {}

  int verify_permission();
  void pre_exec();
  void execute();
  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }
  virtual int get_params() { return 0; }
  virtual void send_response() = 0;
  virtual const string name() { return "create_bucket"; }
  virtual RGWOpType get_type() { return RGW_OP_CREATE_BUCKET; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteBucket : public RGWOp {
protected:
  int ret;

  RGWObjVersionTracker objv_tracker;

public:
  RGWDeleteBucket() : ret(0) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "delete_bucket"; }
  virtual RGWOpType get_type() { return RGW_OP_DELETE_BUCKET; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_DELETE; }
};

class RGWPutObj : public RGWOp {

  friend class RGWPutObjProcessor;

protected:
  int ret;
  off_t ofs;
  const char *supplied_md5_b64;
  const char *supplied_etag;
  const char *if_match;
  const char *if_nomatch;
  string etag;
  bool chunked_upload;
  RGWAccessControlPolicy policy;
  const char *obj_manifest;
  time_t mtime;

  MD5 *user_manifest_parts_hash;

  uint64_t olh_epoch;
  string version_id;

public:
  RGWPutObj() {
    ret = 0;
    ofs = 0;
    supplied_md5_b64 = NULL;
    supplied_etag = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    chunked_upload = false;
    obj_manifest = NULL;
    mtime = 0;
    user_manifest_parts_hash = NULL;
    olh_epoch = 0;
  }

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }

  RGWPutObjProcessor *select_processor(RGWObjectCtx& obj_ctx, bool *is_multipart);
  void dispose_processor(RGWPutObjProcessor *processor);

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual int get_data(bufferlist& bl) = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "put_obj"; }
  virtual RGWOpType get_type() { return RGW_OP_PUT_OBJ; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWPostObj : public RGWOp {

  friend class RGWPutObjProcessor;

protected:
  off_t min_len;
  off_t max_len;
  int ret;
  int len;
  off_t ofs;
  const char *supplied_md5_b64;
  const char *supplied_etag;
  string etag;
  string boundary;
  bool data_pending;
  string content_type;
  RGWAccessControlPolicy policy;
  map<string, bufferlist> attrs;

public:
  RGWPostObj() : min_len(0), max_len(LLONG_MAX), ret(0), len(0), ofs(0),
		 supplied_md5_b64(NULL), supplied_etag(NULL),
		 data_pending(false) {}

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }

  int verify_permission();
  void pre_exec();
  void execute();

  RGWPutObjProcessor *select_processor(RGWObjectCtx& obj_ctx);
  void dispose_processor(RGWPutObjProcessor *processor);

  virtual int get_params() = 0;
  virtual int get_data(bufferlist& bl) = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "post_obj"; }
  virtual RGWOpType get_type() { return RGW_OP_POST_OBJ; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWPutMetadata : public RGWOp {
protected:
  int ret;
  set<string> rmattr_names;
  bool has_policy, has_cors;
  RGWAccessControlPolicy policy;
  RGWCORSConfiguration cors_config;
  string placement_rule;

public:
  RGWPutMetadata() {
    has_cors = false;
    has_policy = false;
    ret = 0;
  }

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }
  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "put_obj_metadata"; }
  virtual RGWOpType get_type() { return RGW_OP_PUT_METADATA; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWSetTempUrl : public RGWOp {
protected:
  int ret;
  map<int, string> temp_url_keys;
public:
  RGWSetTempUrl() : ret(0) {}

  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "set_temp_url"; }
  virtual RGWOpType get_type() { return RGW_OP_SET_TEMPURL; }
};

class RGWDeleteObj : public RGWOp {
protected:
  int ret;
  bool delete_marker;
  string version_id;

public:
  RGWDeleteObj() : ret(0), delete_marker(false) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "delete_obj"; }
  virtual RGWOpType get_type() { return RGW_OP_DELETE_OBJ; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_DELETE; }
};

class RGWCopyObj : public RGWOp {
protected:
  RGWAccessControlPolicy dest_policy;
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  off_t ofs;
  off_t len;
  off_t end;
  time_t mod_time;
  time_t unmod_time;
  time_t *mod_ptr;
  time_t *unmod_ptr;
  int ret;
  map<string, bufferlist> attrs;
  string src_bucket_name;
  rgw_bucket src_bucket;
  rgw_obj_key src_object;
  string dest_bucket_name;
  rgw_bucket dest_bucket;
  string dest_object;
  time_t src_mtime;
  time_t mtime;
  RGWRados::AttrsMod attrs_mod;
  RGWBucketInfo src_bucket_info;
  RGWBucketInfo dest_bucket_info;
  string source_zone;
  string client_id;
  string op_id;
  string etag;

  off_t last_ofs;

  string version_id;
  uint64_t olh_epoch;


  int init_common();

public:
  RGWCopyObj() {
    if_mod = NULL;
    if_unmod = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    ofs = 0;
    len = 0;
    end = -1;
    mod_time = 0;
    unmod_time = 0;
    mod_ptr = NULL;
    unmod_ptr = NULL;
    ret = 0;
    src_mtime = 0;
    mtime = 0;
    attrs_mod = RGWRados::ATTRSMOD_NONE;
    last_ofs = 0;
    olh_epoch = 0;
  }

  static bool parse_copy_location(const string& src, string& bucket_name, rgw_obj_key& object);

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    dest_policy.set_ctx(s->cct);
  }
  int verify_permission();
  void pre_exec();
  void execute();
  void progress_cb(off_t ofs);

  virtual int init_dest_policy() { return 0; }
  virtual int get_params() = 0;
  virtual void send_partial_response(off_t ofs) {}
  virtual void send_response() = 0;
  virtual const string name() { return "copy_obj"; }
  virtual RGWOpType get_type() { return RGW_OP_COPY_OBJ; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWGetACLs : public RGWOp {
protected:
  int ret;
  string acls;

public:
  RGWGetACLs() : ret(0) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "get_acls"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_ACLS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWPutACLs : public RGWOp {
protected:
  int ret;
  size_t len;
  char *data;
  ACLOwner owner;

public:
  RGWPutACLs() {
    ret = 0;
    len = 0;
    data = NULL;
  }
  virtual ~RGWPutACLs() {
    free(data);
  }

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_policy_from_state(RGWRados *store, struct req_state *s, stringstream& ss) { return 0; }
  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "put_acls"; }
  virtual RGWOpType get_type() { return RGW_OP_PUT_ACLS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWGetCORS : public RGWOp {
protected:
  int ret;

public:
  RGWGetCORS() : ret(0) {}

  int verify_permission();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "get_cors"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_CORS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWPutCORS : public RGWOp {
protected:
  int ret;
  bufferlist cors_bl;

public:
  RGWPutCORS() {
    ret = 0;
  }
  virtual ~RGWPutCORS() { }

  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "put_cors"; }
  virtual RGWOpType get_type() { return RGW_OP_PUT_CORS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteCORS : public RGWOp {
protected:
  int ret;

public:
  RGWDeleteCORS() : ret(0) {}

  int verify_permission();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "delete_cors"; }
  virtual RGWOpType get_type() { return RGW_OP_DELETE_CORS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWOptionsCORS : public RGWOp {
protected:
  int ret;
  RGWCORSRule *rule;
  const char *origin, *req_hdrs, *req_meth;

public:
  RGWOptionsCORS() : ret(0), rule(NULL), origin(NULL),
                     req_hdrs(NULL), req_meth(NULL) {
  }

  int verify_permission() {return 0;}
  int validate_cors_request(RGWCORSConfiguration *cc);
  void execute();
  void get_response_params(string& allowed_hdrs, string& exp_hdrs, unsigned *max_age);
  virtual void send_response() = 0;
  virtual const string name() { return "options_cors"; }
  virtual RGWOpType get_type() { return RGW_OP_OPTIONS_CORS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWInitMultipart : public RGWOp {
protected:
  int ret;
  string upload_id;
  RGWAccessControlPolicy policy;

public:
  RGWInitMultipart() {
    ret = 0;
  }

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }
  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "init_multipart"; }
  virtual RGWOpType get_type() { return RGW_OP_INIT_MULTIPART; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWCompleteMultipart : public RGWOp {
protected:
  int ret;
  string upload_id;
  string etag;
  char *data;
  int len;

public:
  RGWCompleteMultipart() {
    ret = 0;
    data = NULL;
    len = 0;
  }
  virtual ~RGWCompleteMultipart() {
    free(data);
  }

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "complete_multipart"; }
  virtual RGWOpType get_type() { return RGW_OP_COMPLETE_MULTIPART; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWAbortMultipart : public RGWOp {
protected:
  int ret;

public:
  RGWAbortMultipart() : ret(0) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "abort_multipart"; }
  virtual RGWOpType get_type() { return RGW_OP_ABORT_MULTIPART; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_DELETE; }
};

class RGWListMultipart : public RGWOp {
protected:
  int ret;
  string upload_id;
  map<uint32_t, RGWUploadPartInfo> parts;
  int max_parts;
  int marker;
  RGWAccessControlPolicy policy;
  bool truncated;

public:
  RGWListMultipart() {
    ret = 0;
    max_parts = 1000;
    marker = 0;
    truncated = false;
  }

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy = RGWAccessControlPolicy(s->cct);
  }
  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "list_multipart"; }
  virtual RGWOpType get_type() { return RGW_OP_LIST_MULTIPART; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

#define MP_META_SUFFIX ".meta"

class RGWMPObj {
  string oid;
  string prefix;
  string meta;
  string upload_id;
public:
  RGWMPObj() {}
  RGWMPObj(const string& _oid, const string& _upload_id) {
    init(_oid, _upload_id, _upload_id);
  }
  void init(const string& _oid, const string& _upload_id) {
    init(_oid, _upload_id, _upload_id);
  }
  void init(const string& _oid, const string& _upload_id, const string& part_unique_str) {
    if (_oid.empty()) {
      clear();
      return;
    }
    oid = _oid;
    upload_id = _upload_id;
    prefix = oid + ".";
    meta = prefix + upload_id + MP_META_SUFFIX;
    prefix.append(part_unique_str);
  }
  string& get_meta() { return meta; }
  string get_part(int num) {
    char buf[16];
    snprintf(buf, 16, ".%d", num);
    string s = prefix;
    s.append(buf);
    return s;
  }
  string get_part(string& part) {
    string s = prefix;
    s.append(".");
    s.append(part);
    return s;
  }
  string& get_upload_id() {
    return upload_id;
  }
  string& get_key() {
    return oid;
  }
  bool from_meta(string& meta) {
    int end_pos = meta.rfind('.'); // search for ".meta"
    if (end_pos < 0)
      return false;
    int mid_pos = meta.rfind('.', end_pos - 1); // <key>.<upload_id>
    if (mid_pos < 0)
      return false;
    oid = meta.substr(0, mid_pos);
    upload_id = meta.substr(mid_pos + 1, end_pos - mid_pos - 1);
    init(oid, upload_id, upload_id);
    return true;
  }
  void clear() {
    oid = "";
    prefix = "";
    meta = "";
    upload_id = "";
  }
};

struct RGWMultipartUploadEntry {
  RGWObjEnt obj;
  RGWMPObj mp;
};

class RGWListBucketMultiparts : public RGWOp {
protected:
  string prefix;
  RGWMPObj marker; 
  RGWMultipartUploadEntry next_marker; 
  int max_uploads;
  string delimiter;
  int ret;
  vector<RGWMultipartUploadEntry> uploads;
  map<string, bool> common_prefixes;
  bool is_truncated;
  int default_max;

public:
  RGWListBucketMultiparts() {
    max_uploads = 0;
    ret = 0;
    is_truncated = false;
    default_max = 0;
  }

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    max_uploads = default_max;
  }

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "list_bucket_multiparts"; }
  virtual RGWOpType get_type() { return RGW_OP_LIST_BUCKET_MULTIPARTS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWDeleteMultiObj : public RGWOp {
protected:
  int ret;
  int max_to_delete;
  size_t len;
  char *data;
  string bucket_name;
  rgw_bucket bucket;
  bool quiet;
  bool status_dumped;

public:
  RGWDeleteMultiObj() {
    ret = 0;
    max_to_delete = 1000;
    len = 0;
    data = NULL;
    quiet = false;
    status_dumped = false;
  }
  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual void send_status() = 0;
  virtual void begin_response() = 0;
  virtual void send_partial_response(rgw_obj_key& key, bool delete_marker,
                                     const string& marker_version_id, int ret) = 0;
  virtual void end_response() = 0;
  virtual const string name() { return "multi_object_delete"; }
  virtual RGWOpType get_type() { return RGW_OP_DELETE_MULTI_OBJ; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_DELETE; }
};


class RGWHandler {
protected:
  RGWRados *store;
  struct req_state *s;

  int do_read_permissions(RGWOp *op, bool only_bucket);

  virtual RGWOp *op_get() { return NULL; }
  virtual RGWOp *op_put() { return NULL; }
  virtual RGWOp *op_delete() { return NULL; }
  virtual RGWOp *op_head() { return NULL; }
  virtual RGWOp *op_post() { return NULL; }
  virtual RGWOp *op_copy() { return NULL; }
  virtual RGWOp *op_options() { return NULL; }
public:
  RGWHandler() : store(NULL), s(NULL) {}
  virtual ~RGWHandler();
  virtual int init(RGWRados *store, struct req_state *_s, RGWClientIO *cio);

  virtual RGWOp *get_op(RGWRados *store);
  virtual void put_op(RGWOp *op);
  virtual int read_permissions(RGWOp *op) = 0;
  virtual int authorize() = 0;
};

#endif
