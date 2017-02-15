// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_H
#define CEPH_RGW_REST_H

#define TIME_BUF_SIZE 128

#include "common/sstring.hh"
#include "common/ceph_json.h"
#include "include/assert.h" /* needed because of common/ceph_json.h */
#include "rgw_op.h"
#include "rgw_formats.h"
#include "rgw_client_io.h"

extern std::map<std::string, std::string> rgw_to_http_attrs;

extern string camelcase_dash_http_attr(const string& orig);
extern string lowercase_dash_http_attr(const string& orig);

extern void rgw_rest_init(CephContext *cct, RGWRados *store, RGWZoneGroup& zone_group);

extern void rgw_flush_formatter_and_reset(struct req_state *s,
					 ceph::Formatter *formatter);

extern void rgw_flush_formatter(struct req_state *s,
				ceph::Formatter *formatter);

extern int rgw_rest_read_all_input(struct req_state *s, char **data, int *plen,
				   int max_len);

/* type conversions to work around lack of req_state type
 * hierarchy matching (e.g.) REST backends (may be replaced w/dynamic
 * typed req_state) */
static inline RGWStreamIO* STREAM_IO(struct req_state* s) {
  return static_cast<RGWStreamIO*>(s->cio);
}

template <class T>
int rgw_rest_get_json_input(CephContext *cct, req_state *s, T& out,
			    int max_len, bool *empty)
{
  int rv, data_len;
  char *data;

  if (empty)
    *empty = false;

  if ((rv = rgw_rest_read_all_input(s, &data, &data_len, max_len)) < 0) {
    return rv;
  }

  if (!data_len) {
    if (empty) {
      *empty = true;
    }

    return -EINVAL;
  }

  JSONParser parser;

  if (!parser.parse(data, data_len)) {
    free(data);
    return -EINVAL;
  }

  free(data);

  try {
      decode_json_obj(out, &parser);
  } catch (JSONDecoder::err& e) {
      return -EINVAL;
  }

  return 0;
}

template <class T>
int rgw_rest_get_json_input_keep_data(CephContext *cct, req_state *s, T& out, int max_len, char **pdata, int *len)
{
  int rv, data_len;
  char *data;

  if ((rv = rgw_rest_read_all_input(s, &data, &data_len, max_len)) < 0) {
    return rv;
  }

  if (!data_len) {
    return -EINVAL;
  }

  *len = data_len;

  JSONParser parser;

  if (!parser.parse(data, data_len)) {
    free(data);
    return -EINVAL;
  }

  try {
      decode_json_obj(out, &parser);
  } catch (JSONDecoder::err& e) {
      return -EINVAL;
  }

  *pdata = data;
  return 0;
}

class RESTArgs {
public:
  static int get_string(struct req_state *s, const string& name,
			const string& def_val, string *val,
			bool *existed = NULL);
  static int get_uint64(struct req_state *s, const string& name,
			uint64_t def_val, uint64_t *val, bool *existed = NULL);
  static int get_int64(struct req_state *s, const string& name,
		       int64_t def_val, int64_t *val, bool *existed = NULL);
  static int get_uint32(struct req_state *s, const string& name,
			uint32_t def_val, uint32_t *val, bool *existed = NULL);
  static int get_int32(struct req_state *s, const string& name,
		       int32_t def_val, int32_t *val, bool *existed = NULL);
  static int get_time(struct req_state *s, const string& name,
		      const utime_t& def_val, utime_t *val,
		      bool *existed = NULL);
  static int get_epoch(struct req_state *s, const string& name,
		       uint64_t def_val, uint64_t *epoch,
		       bool *existed = NULL);
  static int get_bool(struct req_state *s, const string& name, bool def_val,
		      bool *val, bool *existed = NULL);
};

class RGWRESTFlusher : public RGWFormatterFlusher {
  struct req_state *s;
  RGWOp *op;
protected:
  virtual void do_flush();
  virtual void do_start(int ret);
public:
  RGWRESTFlusher(struct req_state *_s, RGWOp *_op) :
    RGWFormatterFlusher(_s->formatter), s(_s), op(_op) {}
  RGWRESTFlusher() : RGWFormatterFlusher(NULL), s(NULL), op(NULL) {}

  void init(struct req_state *_s, RGWOp *_op) {
    s = _s;
    op = _op;
    set_formatter(s->formatter);
  }
};

class RGWStreamIO;

class RGWGetObj_ObjStore : public RGWGetObj
{
protected:
  bool sent_header;
public:
  RGWGetObj_ObjStore() : sent_header(false) {}

  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *h) {
    RGWGetObj::init(store, s, h);
    sent_header = false;
  }

  int get_params();
};

class RGWListBuckets_ObjStore : public RGWListBuckets {
public:
  RGWListBuckets_ObjStore() {}
  ~RGWListBuckets_ObjStore() {}
};

class RGWGetUsage_ObjStore : public RGWGetUsage {
public:
  RGWGetUsage_ObjStore() {}
  ~RGWGetUsage_ObjStore() {}
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
  virtual int get_data(bufferlist& bl);

  int get_padding_last_aws4_chunk_encoded(bufferlist &bl, uint64_t chunk_size);
};

class RGWPostObj_ObjStore : public RGWPostObj
{
public:
  RGWPostObj_ObjStore() {}
  ~RGWPostObj_ObjStore() {}

  virtual int verify_params();
};

class RGWPutMetadataAccount_ObjStore : public RGWPutMetadataAccount
{
public:
  RGWPutMetadataAccount_ObjStore() {}
  ~RGWPutMetadataAccount_ObjStore() {}
};

class RGWPutMetadataBucket_ObjStore : public RGWPutMetadataBucket
{
public:
  RGWPutMetadataBucket_ObjStore() {}
  ~RGWPutMetadataBucket_ObjStore() {}
};

class RGWPutMetadataObject_ObjStore : public RGWPutMetadataObject
{
public:
  RGWPutMetadataObject_ObjStore() {}
  ~RGWPutMetadataObject_ObjStore() {}
};

class RGWDeleteObj_ObjStore : public RGWDeleteObj {
public:
  RGWDeleteObj_ObjStore() {}
  ~RGWDeleteObj_ObjStore() {}
};

class  RGWGetCrossDomainPolicy_ObjStore : public RGWGetCrossDomainPolicy {
public:
  RGWGetCrossDomainPolicy_ObjStore() = default;
  ~RGWGetCrossDomainPolicy_ObjStore() = default;
};

class  RGWGetHealthCheck_ObjStore : public RGWGetHealthCheck {
public:
  RGWGetHealthCheck_ObjStore() = default;
  ~RGWGetHealthCheck_ObjStore() = default;
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

  virtual int get_params();
};

class RGWGetCORS_ObjStore : public RGWGetCORS {
public:
  RGWGetCORS_ObjStore() {}
  ~RGWGetCORS_ObjStore() {}
};

class RGWPutCORS_ObjStore : public RGWPutCORS {
public:
  RGWPutCORS_ObjStore() {}
  ~RGWPutCORS_ObjStore() {}
};

class RGWDeleteCORS_ObjStore : public RGWDeleteCORS {
public:
  RGWDeleteCORS_ObjStore() {}
  ~RGWDeleteCORS_ObjStore() {}
};

class RGWOptionsCORS_ObjStore : public RGWOptionsCORS {
public:
  RGWOptionsCORS_ObjStore() {}
  ~RGWOptionsCORS_ObjStore() {}
};

class RGWInitMultipart_ObjStore : public RGWInitMultipart {
public:
  RGWInitMultipart_ObjStore() {}
  ~RGWInitMultipart_ObjStore() {}
};

class RGWCompleteMultipart_ObjStore : public RGWCompleteMultipart {
public:
  RGWCompleteMultipart_ObjStore() {}
  ~RGWCompleteMultipart_ObjStore() {}

  virtual int get_params();
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

class RGWBulkDelete_ObjStore : public RGWBulkDelete {
public:
  RGWBulkDelete_ObjStore() {}
  ~RGWBulkDelete_ObjStore() {}
};

class RGWDeleteMultiObj_ObjStore : public RGWDeleteMultiObj {
public:
  RGWDeleteMultiObj_ObjStore() {}
  ~RGWDeleteMultiObj_ObjStore() {}

  virtual int get_params();
};

class RGWInfo_ObjStore : public RGWInfo {
public:
    RGWInfo_ObjStore() = default;
    ~RGWInfo_ObjStore() = default;
};

class RGWRESTOp : public RGWOp {
protected:
  int http_ret;
  RGWRESTFlusher flusher;
public:
  RGWRESTOp() : http_ret(0) {}
  virtual void init(RGWRados *store, struct req_state *s,
		    RGWHandler *dialect_handler) {
    RGWOp::init(store, s, dialect_handler);
    flusher.init(s, this);
  }
  virtual void send_response();
  virtual int check_caps(RGWUserCaps& caps)
    { return -EPERM; } /* should to be implemented! */
  virtual int verify_permission();
};

class RGWHandler_REST : public RGWHandler {
protected:

  virtual bool is_obj_update_op() { return false; }
  virtual RGWOp *op_get() { return NULL; }
  virtual RGWOp *op_put() { return NULL; }
  virtual RGWOp *op_delete() { return NULL; }
  virtual RGWOp *op_head() { return NULL; }
  virtual RGWOp *op_post() { return NULL; }
  virtual RGWOp *op_copy() { return NULL; }
  virtual RGWOp *op_options() { return NULL; }

  static int allocate_formatter(struct req_state *s, int default_formatter,
				bool configurable);
public:
  static constexpr int MAX_BUCKET_NAME_LEN = 255;
  static constexpr int MAX_OBJ_NAME_LEN = 1024;

  RGWHandler_REST() {}
  virtual ~RGWHandler_REST() {}

  static int validate_tenant_name(const string& bucket);
  static int validate_bucket_name(const string& bucket);
  static int validate_object_name(const string& object);

  int init_permissions(RGWOp* op);
  int read_permissions(RGWOp* op);

  virtual RGWOp* get_op(RGWRados* store);
  virtual void put_op(RGWOp* op);

  virtual int retarget(RGWOp* op, RGWOp** new_op) {
    *new_op = op;
    return 0;
  }

  virtual int authorize() = 0;
  // virtual int postauth_init(struct req_init_state *t) = 0;
};

class RGWHandler_REST_SWIFT;
class RGWHandler_SWIFT_Auth;
class RGWHandler_REST_S3;


class RGWRESTMgr {
  bool should_log;

protected:
  map<string, RGWRESTMgr *> resource_mgrs;
  multimap<size_t, string> resources_by_size;
  RGWRESTMgr *default_mgr;

public:
  RGWRESTMgr() : should_log(false), default_mgr(NULL) {}
  virtual ~RGWRESTMgr();

  void register_resource(string resource, RGWRESTMgr *mgr);
  void register_default_mgr(RGWRESTMgr *mgr);

  virtual RGWRESTMgr *get_resource_mgr(struct req_state *s, const string& uri,
				       string *out_uri);

  virtual RGWRESTMgr* get_resource_mgr_as_default(struct req_state* s,
                                                  const std::string& uri,
                                                  std::string* our_uri) {
    return this;
  }

  virtual RGWHandler_REST *get_handler(struct req_state *s) { return NULL; }
  virtual void put_handler(RGWHandler_REST *handler) { delete handler; }

  void set_logging(bool _should_log) { should_log = _should_log; }
  bool get_logging() { return should_log; }
};

class RGWLibIO;

class RGWREST {
  using x_header = basic_sstring<char, uint16_t, 32>;
  std::set<x_header> x_headers;
  RGWRESTMgr mgr;

  static int preprocess(struct req_state *s, RGWClientIO *sio);
public:
  RGWREST() {}
  RGWHandler_REST *get_handler(RGWRados *store, struct req_state *s,
			      RGWStreamIO *sio,
			      RGWRESTMgr **pmgr, int *init_error);
#if 0
  RGWHandler *get_handler(RGWRados *store, struct req_state *s,
			  RGWLibIO *io, RGWRESTMgr **pmgr,
			  int *init_error);
#endif

  void put_handler(RGWHandler_REST *handler) {
    mgr.put_handler(handler);
  }

  void register_resource(string resource, RGWRESTMgr *m,
			 bool register_empty = false) {
    if (!register_empty && resource.empty())
      return;

    mgr.register_resource(resource, m);
  }

  void register_default_mgr(RGWRESTMgr *m) {
    mgr.register_default_mgr(m);
  }

  void register_x_headers(const std::string& headers);

  bool log_x_headers(void) {
    return (x_headers.size() > 0);
  }

  bool log_x_header(const std::string& header) {
    return (x_headers.find(header) != x_headers.end());
  }
};

static constexpr int64_t NO_CONTENT_LENGTH = -1;

extern void set_req_state_err(struct rgw_err &err, int err_no, int prot_flags);
extern void set_req_state_err(struct req_state *s, int err_no);
extern void dump_errno(int http_ret, string& out);
extern void dump_errno(const struct rgw_err &err, string& out);
extern void dump_errno(struct req_state *s);
extern void dump_errno(struct req_state *s, int http_ret);
extern void end_header(struct req_state *s,
                       RGWOp* op = nullptr,
                       const char *content_type = nullptr,
                       const int64_t proposed_content_length =
		       NO_CONTENT_LENGTH,
		       bool force_content_type = false,
		       bool force_no_error = false);
extern void dump_start(struct req_state *s);
extern void list_all_buckets_start(struct req_state *s);
extern void dump_owner(struct req_state *s, rgw_user& id, string& name,
		       const char *section = NULL);
extern void dump_string_header(struct req_state *s, const char *name,
			       const char *val);
extern void dump_content_length(struct req_state *s, uint64_t len);
extern void dump_etag(struct req_state *s, const char *etag);
extern void dump_epoch_header(struct req_state *s, const char *name, real_time t);
extern void dump_time_header(struct req_state *s, const char *name, real_time t);
extern void dump_last_modified(struct req_state *s, real_time t);
extern void abort_early(struct req_state* s, RGWOp* op, int err,
			RGWHandler* handler);
extern void dump_range(struct req_state* s, uint64_t ofs, uint64_t end,
		       uint64_t total_size);
extern void dump_continue(struct req_state *s);
extern void list_all_buckets_end(struct req_state *s);
extern void dump_time(struct req_state *s, const char *name, real_time *t);
extern void dump_bucket_from_state(struct req_state *s);
extern void dump_uri_from_state(struct req_state *s);
extern void dump_redirect(struct req_state *s, const string& redirect);
extern void dump_pair(struct req_state *s, const char *key, const char *value);
extern bool is_valid_url(const char *url);
extern void dump_access_control(struct req_state *s, const char *origin,
				const char *meth,
				const char *hdr, const char *exp_hdr,
				uint32_t max_age);
extern void dump_access_control(req_state *s, RGWOp *op);

#endif /* CEPH_RGW_REST_H */
