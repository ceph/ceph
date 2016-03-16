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

#include <memory>
#include <string>
#include <set>
#include <map>

#include "common/armor.h"
#include "common/mime.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/utf8.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_acl.h"
#include "rgw_cors.h"
#include "rgw_quota.h"

#include "include/assert.h"

using namespace std;

struct req_state;
class RGWHandler;

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
  int op_ret;

  int do_aws4_auth_completion();

  virtual int init_quota();
public:
RGWOp() : s(nullptr), dialect_handler(nullptr), store(nullptr),
    cors_exist(false), op_ret(0) {}

  virtual ~RGWOp() {}

  int get_ret() const { return op_ret; }

  virtual int init_processing() {
    op_ret = init_quota();
    if (op_ret < 0)
      return op_ret;

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

  virtual int error_handler(int err_no, string *error_content);
};

class RGWGetObj : public RGWOp {
protected:
  const char *range_str;
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  uint32_t mod_zone_id;
  uint64_t mod_pg_ver;
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
  bool get_data;
  bool partial_content;
  bool range_parsed;
  bool skip_manifest;
  rgw_obj obj;
  utime_t gc_invalidate_time;
  bool is_slo;
  string lo_etag;

  int init_common();
public:
  RGWGetObj() {
    range_str = NULL;
    if_mod = NULL;
    if_unmod = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    mod_zone_id = 0;
    mod_pg_ver = 0;
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
    range_parsed = false;
    skip_manifest = false;
    is_slo = false;
 }

  bool prefetch_data();

  void set_get_data(bool get_data) {
    this->get_data = get_data;
  }
  int verify_permission();
  void pre_exec();
  void execute();
  int read_user_manifest_part(rgw_bucket& bucket,
                              const RGWObjEnt& ent,
                              RGWAccessControlPolicy *bucket_policy,
                              off_t start_ofs,
                              off_t end_ofs);
  int handle_user_manifest(const char *prefix);
  int handle_slo_manifest(bufferlist& bl);

  int get_data_cb(bufferlist& bl, off_t ofs, off_t len);

  virtual int get_params() = 0;
  virtual int send_response_data_error() = 0;
  virtual int send_response_data(bufferlist& bl, off_t ofs, off_t len) = 0;

  virtual const string name() { return "get_obj"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_OBJ; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
  virtual bool need_object_expiration() { return false; }
};

class RGWGetObj_CB : public RGWGetDataCB
{
  RGWGetObj *op;
public:
  explicit RGWGetObj_CB(RGWGetObj *_op) : op(_op) {}
  virtual ~RGWGetObj_CB() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
    return op->get_data_cb(bl, bl_ofs, bl_len);
  }
};

class RGWBulkDelete : public RGWOp {
public:
  struct acct_path_t {
    std::string bucket_name;
    rgw_obj_key obj_key;
  };

  struct fail_desc_t {
    int err;
    acct_path_t path;
  };

  class Deleter {
  protected:
    unsigned int num_deleted;
    unsigned int num_unfound;
    std::list<fail_desc_t> failures;

    RGWRados * const store;
    req_state * const s;

  public:
    Deleter(RGWRados * const str, req_state * const s)
      : num_deleted(0),
        num_unfound(0),
        store(str),
        s(s) {
    }

    unsigned int get_num_deleted() const {
      return num_deleted;
    }

    unsigned int get_num_unfound() const {
      return num_unfound;
    }

    const std::list<fail_desc_t> get_failures() const {
      return failures;
    }

    bool verify_permission(RGWBucketInfo& binfo,
                           map<string, bufferlist>& battrs,
                           ACLOwner& bucket_owner /* out */);
    bool delete_single(const acct_path_t& path);
    bool delete_chunk(const std::list<acct_path_t>& paths);
  };
  /* End of Deleter subclass */

  static const size_t MAX_CHUNK_ENTRIES = 1024;

protected:
  std::unique_ptr<Deleter> deleter;

public:
  RGWBulkDelete()
    : deleter(nullptr) {
  }

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_data(std::list<acct_path_t>& items,
                       bool * is_truncated) = 0;
  virtual void send_response() = 0;

  virtual const string name() { return "bulk_delete"; }
  virtual RGWOpType get_type() { return RGW_OP_BULK_DELETE; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_DELETE; }
};

inline ostream& operator<<(ostream& out, const RGWBulkDelete::acct_path_t &o) {
  return out << o.bucket_name << "/" << o.obj_key;
}

#define RGW_LIST_BUCKETS_LIMIT_MAX 10000

class RGWListBuckets : public RGWOp {
protected:
  bool sent_data;
  string marker;
  string end_marker;
  int64_t limit;
  uint64_t limit_max;
  uint32_t buckets_count;
  uint64_t buckets_objcount;
  uint64_t buckets_size;
  uint64_t buckets_size_rounded;
  map<string, bufferlist> attrs;
  bool is_truncated;

  virtual uint64_t get_default_max() const {
    return 1000;
  }

public:
  RGWListBuckets() : sent_data(false) {
    limit = limit_max = RGW_LIST_BUCKETS_LIMIT_MAX;
    buckets_count = 0;
    buckets_objcount = 0;
    buckets_size = 0;
    buckets_size_rounded = 0;
  }

  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response_begin(bool has_buckets) = 0;
  virtual void send_response_data(RGWUserBuckets& buckets) = 0;
  virtual void send_response_end() = 0;
  virtual void send_response() {}

  virtual bool should_get_stats() { return false; }
  virtual bool supports_account_metadata() { return false; }

  virtual const string name() { return "list_buckets"; }
  virtual RGWOpType get_type() { return RGW_OP_LIST_BUCKETS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWGetUsage : public RGWOp {
protected:
  bool sent_data;
  string start_date;
  string end_date;
  int show_log_entries;
  int show_log_sum;
  map<string, bool> categories;
  map<rgw_user_bucket, rgw_usage_log_entry> usage;
  map<string, rgw_usage_log_entry> summary_map;
  cls_user_header header;
public:
  RGWGetUsage() : sent_data(false), show_log_entries(true), show_log_sum(true){
  }

  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() {}

  virtual bool should_get_stats() { return false; }

  virtual const string name() { return "get_usage"; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWStatAccount : public RGWOp {
protected:
  uint32_t buckets_count;
  uint64_t buckets_objcount;
  uint64_t buckets_size;
  uint64_t buckets_size_rounded;

public:
  RGWStatAccount() {
    buckets_count = 0;
    buckets_objcount = 0;
    buckets_size = 0;
    buckets_size_rounded = 0;
  }

  int verify_permission();
  virtual void execute();

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
  rgw_obj_key end_marker;
  string max_keys;
  string delimiter;
  string encoding_type;
  bool list_versions;
  int max;
  vector<RGWObjEnt> objs;
  map<string, bool> common_prefixes;

  int default_max;
  bool is_truncated;

  int shard_id;

  int parse_max_keys();

public:
  RGWListBucket() : list_versions(false), max(0),
                    default_max(0), is_truncated(false), shard_id(-1) {}
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
  void execute() { }

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
  void execute() { }

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
public:
  RGWSetBucketVersioning() : enable_versioning(false) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() { return 0; }

  virtual void send_response() = 0;
  virtual const string name() { return "set_bucket_versioning"; }
  virtual RGWOpType get_type() { return RGW_OP_SET_BUCKET_VERSIONING; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWGetBucketWebsite : public RGWOp {
public:
  RGWGetBucketWebsite() {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "get_bucket_website"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_BUCKET_WEBSITE; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWSetBucketWebsite : public RGWOp {
protected:
  RGWBucketWebsiteConf website_conf;
public:
  RGWSetBucketWebsite() {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() { return 0; }

  virtual void send_response() = 0;
  virtual const string name() { return "set_bucket_website"; }
  virtual RGWOpType get_type() { return RGW_OP_SET_BUCKET_WEBSITE; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteBucketWebsite : public RGWOp {
public:
  RGWDeleteBucketWebsite() {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "delete_bucket_website"; }
  virtual RGWOpType get_type() { return RGW_OP_SET_BUCKET_WEBSITE; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWStatBucket : public RGWOp {
protected:
  RGWBucketEnt bucket;

public:
  RGWStatBucket() {}
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
  RGWAccessControlPolicy policy;
  string location_constraint;
  string placement_rule;
  RGWBucketInfo info;
  obj_version ep_objv;
  bool has_cors;
  RGWCORSConfiguration cors_config;

  bufferlist in_data;

public:
  RGWCreateBucket() : has_cors(false) {}

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
  RGWObjVersionTracker objv_tracker;

public:
  RGWDeleteBucket() {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "delete_bucket"; }
  virtual RGWOpType get_type() { return RGW_OP_DELETE_BUCKET; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_DELETE; }
};

struct rgw_slo_entry {
  string path;
  string etag;
  uint64_t size_bytes;

  rgw_slo_entry() : size_bytes(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(path, bl);
    ::encode(etag, bl);
    ::encode(size_bytes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     ::decode(path, bl);
     ::decode(etag, bl);
     ::decode(size_bytes, bl);
     DECODE_FINISH(bl);
  }

  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_slo_entry)

struct RGWSLOInfo {
  vector<rgw_slo_entry> entries;
  uint64_t total_size;

  /* in memory only */
  char *raw_data;
  int raw_data_len;

  RGWSLOInfo() : raw_data(NULL), raw_data_len(0) {}
  ~RGWSLOInfo() {
    free(raw_data);
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ::encode(total_size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     ::decode(entries, bl);
     ::decode(total_size, bl);
     DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(RGWSLOInfo)

class RGWPutObj : public RGWOp {

  friend class RGWPutObjProcessor;

protected:
  off_t ofs;
  const char *supplied_md5_b64;
  const char *supplied_etag;
  const char *if_match;
  const char *if_nomatch;
  string etag;
  bool chunked_upload;
  RGWAccessControlPolicy policy;
  const char *dlo_manifest;
  RGWSLOInfo *slo_info;

  time_t mtime;
  uint64_t olh_epoch;
  string version_id;

  time_t delete_at;

public:
  RGWPutObj() : ofs(0),
                supplied_md5_b64(NULL),
                supplied_etag(NULL),
                if_match(NULL),
                if_nomatch(NULL),
                chunked_upload(0),
                dlo_manifest(NULL),
                slo_info(NULL),
                mtime(0),
                olh_epoch(0),
                delete_at(0) {}

  ~RGWPutObj() {
    delete slo_info;
  }

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }

  virtual RGWPutObjProcessor *select_processor(RGWObjectCtx& obj_ctx, bool *is_multipart);
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
  time_t delete_at;

public:
  RGWPostObj() : min_len(0), max_len(LLONG_MAX), len(0), ofs(0),
		 supplied_md5_b64(NULL), supplied_etag(NULL),
		 data_pending(false), delete_at(0) {}

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

class RGWPutMetadataAccount : public RGWOp {
protected:
  set<string> rmattr_names;
  RGWAccessControlPolicy policy;

public:
  RGWPutMetadataAccount() {}

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }
  int verify_permission();
  void pre_exec() { }
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual void filter_out_temp_url(map<string, bufferlist>& add_attrs,
                                   const set<string>& rmattr_names,
                                   map<int, string>& temp_url_keys);
  virtual int handle_temp_url_update(const map<int, string>& temp_url_keys);
  virtual const string name() { return "put_account_metadata"; }
  virtual RGWOpType get_type() { return RGW_OP_PUT_METADATA_ACCOUNT; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWPutMetadataBucket : public RGWOp {
protected:
  set<string> rmattr_names;
  bool has_policy, has_cors;
  RGWAccessControlPolicy policy;
  RGWCORSConfiguration cors_config;
  string placement_rule;

public:
  RGWPutMetadataBucket()
    : has_policy(false), has_cors(false)
  {}

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }
  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const string name() { return "put_bucket_metadata"; }
  virtual RGWOpType get_type() { return RGW_OP_PUT_METADATA_BUCKET; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWPutMetadataObject : public RGWOp {
protected:
  RGWAccessControlPolicy policy;
  string placement_rule;
  time_t delete_at;
  const char *dlo_manifest;

public:
  RGWPutMetadataObject()
    : delete_at(0),
      dlo_manifest(NULL)
  {}

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
  virtual RGWOpType get_type() { return RGW_OP_PUT_METADATA_OBJECT; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
  virtual bool need_object_expiration() { return false; }
};

class RGWDeleteObj : public RGWOp {
protected:
  bool delete_marker;
  bool multipart_delete;
  string version_id;
  time_t unmod_since; /* if unmodified since */
  bool no_precondition_error;
  std::unique_ptr<RGWBulkDelete::Deleter> deleter;

public:
  RGWDeleteObj()
    : delete_marker(false),
      multipart_delete(false),
      unmod_since(0),
      no_precondition_error(false),
      deleter(nullptr) {
  }

  int verify_permission();
  void pre_exec();
  void execute();
  int handle_slo_manifest(bufferlist& bl);

  virtual int get_params() { return 0; }
  virtual void send_response() = 0;
  virtual const string name() { return "delete_obj"; }
  virtual RGWOpType get_type() { return RGW_OP_DELETE_OBJ; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_DELETE; }
  virtual bool need_object_expiration() { return false; }
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
  map<string, bufferlist> attrs;
  string src_tenant_name, src_bucket_name;
  rgw_bucket src_bucket;
  rgw_obj_key src_object;
  string dest_tenant_name, dest_bucket_name;
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

  time_t delete_at;
  bool copy_if_newer;

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
    src_mtime = 0;
    mtime = 0;
    attrs_mod = RGWRados::ATTRSMOD_NONE;
    last_ofs = 0;
    olh_epoch = 0;
    delete_at = 0;
    copy_if_newer = false;
  }

  static bool parse_copy_location(const string& src,
                                  string& bucket_name,
                                  rgw_obj_key& object);

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
  string acls;

public:
  RGWGetACLs() {}

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
  size_t len;
  char *data;
  ACLOwner owner;

public:
  RGWPutACLs() {
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

public:
  RGWGetCORS() {}

  int verify_permission();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "get_cors"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_CORS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWPutCORS : public RGWOp {
protected:
  bufferlist cors_bl;

public:
  RGWPutCORS() {}
  virtual ~RGWPutCORS() {}

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

public:
  RGWDeleteCORS() {}

  int verify_permission();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "delete_cors"; }
  virtual RGWOpType get_type() { return RGW_OP_DELETE_CORS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWOptionsCORS : public RGWOp {
protected:
  RGWCORSRule *rule;
  const char *origin, *req_hdrs, *req_meth;

public:
  RGWOptionsCORS() : rule(NULL), origin(NULL),
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

class RGWGetRequestPayment : public RGWOp {
protected:
  bool requester_pays;

public:
  RGWGetRequestPayment() : requester_pays(0) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "get_request_payment"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_REQUEST_PAYMENT; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

class RGWSetRequestPayment : public RGWOp {
protected:
  bool requester_pays;
public:
 RGWSetRequestPayment() : requester_pays(false) {}

  int verify_permission();
  void pre_exec();
  void execute();

  virtual int get_params() { return 0; }

  virtual void send_response() = 0;
  virtual const string name() { return "set_request_payment"; }
  virtual RGWOpType get_type() { return RGW_OP_SET_REQUEST_PAYMENT; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

class RGWInitMultipart : public RGWOp {
protected:
  string upload_id;
  RGWAccessControlPolicy policy;

public:
  RGWInitMultipart() {}

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
  string upload_id;
  string etag;
  char *data;
  int len;

public:
  RGWCompleteMultipart() {
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
public:
  RGWAbortMultipart() {}

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
  string upload_id;
  map<uint32_t, RGWUploadPartInfo> parts;
  int max_parts;
  int marker;
  RGWAccessControlPolicy policy;
  bool truncated;

public:
  RGWListMultipart() {
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
  vector<RGWMultipartUploadEntry> uploads;
  map<string, bool> common_prefixes;
  bool is_truncated;
  int default_max;

public:
  RGWListBucketMultiparts() {
    max_uploads = 0;
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
  int max_to_delete;
  size_t len;
  char *data;
  rgw_bucket bucket;
  bool quiet;
  bool status_dumped;

public:
  RGWDeleteMultiObj() {
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

  int do_init_permissions();
  int do_read_permissions(RGWOp *op, bool only_bucket);

public:
  RGWHandler() : store(NULL), s(NULL) {}
  virtual ~RGWHandler();

  virtual int init(RGWRados* store, struct req_state* _s, RGWClientIO* cio);

  virtual int init_permissions(RGWOp *op) {
    return 0;
  }

  virtual int retarget(RGWOp *op, RGWOp **new_op) {
    *new_op = op;
    return 0;
  }

  virtual int read_permissions(RGWOp *op) = 0;
  virtual int authorize() = 0;
  virtual int postauth_init() = 0;
  virtual int error_handler(int err_no, string *error_content);
};

extern int rgw_build_bucket_policies(RGWRados* store, struct req_state* s);

static inline int put_data_and_throttle(RGWPutObjProcessor *processor,
					bufferlist& data, off_t ofs,
					MD5 *hash, bool need_to_wait)
{
  bool again;

  do {
    void *handle;

    int ret = processor->handle_data(data, ofs, hash, &handle, &again);
    if (ret < 0)
      return ret;

    ret = processor->throttle_data(handle, need_to_wait);
    if (ret < 0)
      return ret;

    need_to_wait = false; /* the need to wait only applies to the first
			   * iteration */
  } while (again);

  return 0;
} /* put_data_and_throttle */

static inline int get_system_versioning_params(req_state *s,
					      uint64_t *olh_epoch,
					      string *version_id)
{
  if (!s->system_request) {
    return 0;
  }

  if (olh_epoch) {
    string epoch_str = s->info.args.get(RGW_SYS_PARAM_PREFIX "versioned-epoch");
    if (!epoch_str.empty()) {
      string err;
      *olh_epoch = strict_strtol(epoch_str.c_str(), 10, &err);
      if (!err.empty()) {
        lsubdout(s->cct, rgw, 0) << "failed to parse versioned-epoch param"
				 << dendl;
        return -EINVAL;
      }
    }
  }

  if (version_id) {
    *version_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "version-id");
  }

  return 0;
} /* get_system_versioning_params */

static inline void format_xattr(std::string &xattr)
{
  /* If the extended attribute is not valid UTF-8, we encode it using
   * quoted-printable encoding.
   */
  if ((check_utf8(xattr.c_str(), xattr.length()) != 0) ||
      (check_for_control_characters(xattr.c_str(), xattr.length()) != 0)) {
    static const char MIME_PREFIX_STR[] = "=?UTF-8?Q?";
    static const int MIME_PREFIX_LEN = sizeof(MIME_PREFIX_STR) - 1;
    static const char MIME_SUFFIX_STR[] = "?=";
    static const int MIME_SUFFIX_LEN = sizeof(MIME_SUFFIX_STR) - 1;
    int mlen = mime_encode_as_qp(xattr.c_str(), NULL, 0);
    char *mime = new char[MIME_PREFIX_LEN + mlen + MIME_SUFFIX_LEN + 1];
    strcpy(mime, MIME_PREFIX_STR);
    mime_encode_as_qp(xattr.c_str(), mime + MIME_PREFIX_LEN, mlen);
    strcpy(mime + MIME_PREFIX_LEN + (mlen - 1), MIME_SUFFIX_STR);
    xattr.assign(mime);
    delete [] mime;
  }
} /* format_xattr */

/**
 * Get the HTTP request metadata out of the req_state as a
 * map(<attr_name, attr_contents>, where attr_name is RGW_ATTR_PREFIX.HTTP_NAME)
 * s: The request state
 * attrs: will be filled up with attrs mapped as <attr_name, attr_contents>
 *
 */
static inline void rgw_get_request_metadata(CephContext *cct,
					    struct req_info& info,
					    map<string, bufferlist>& attrs,
					    const bool allow_empty_attrs = true)
{
  map<string, string>::iterator iter;
  for (iter = info.x_meta_map.begin(); iter != info.x_meta_map.end(); ++iter) {
    const string &name(iter->first);
    string &xattr(iter->second);

    if (allow_empty_attrs || !xattr.empty()) {
      lsubdout(cct, rgw, 10) << "x>> " << name << ":" << xattr << dendl;
      format_xattr(xattr);
      string attr_name(RGW_ATTR_PREFIX);
      attr_name.append(name);
      map<string, bufferlist>::value_type v(attr_name, bufferlist());
      std::pair < map<string, bufferlist>::iterator, bool >
	rval(attrs.insert(v));
      bufferlist& bl(rval.first->second);
      bl.append(xattr.c_str(), xattr.size() + 1);
    }
  }
} /* rgw_get_request_metadata */

static inline void encode_delete_at_attr(time_t delete_at,
					map<string, bufferlist>& attrs)
{
  if (delete_at == 0) {
    return;
  }

  bufferlist delatbl;
  ::encode(utime_t(delete_at, 0), delatbl);
  attrs[RGW_ATTR_DELETE_AT] = delatbl;
} /* encode_delete_at_attr */

static inline int encode_dlo_manifest_attr(const char * const dlo_manifest,
					  map<string, bufferlist>& attrs)
{
  string dm = dlo_manifest;

  if (dm.find('/') == string::npos) {
    return -EINVAL;
  }

  bufferlist manifest_bl;
  manifest_bl.append(dlo_manifest, strlen(dlo_manifest) + 1);
  attrs[RGW_ATTR_USER_MANIFEST] = manifest_bl;

  return 0;
} /* encode_dlo_manifest_attr */

static inline void complete_etag(MD5& hash, string *etag)
{
  char etag_buf[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char etag_buf_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];

  hash.Final((byte *)etag_buf);
  buf_to_hex((const unsigned char *)etag_buf, CEPH_CRYPTO_MD5_DIGESTSIZE,
	    etag_buf_str);

  *etag = etag_buf_str;
} /* complete_etag */

#endif /* CEPH_RGW_OP_H */
