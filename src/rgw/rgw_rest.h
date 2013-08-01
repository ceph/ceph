#ifndef CEPH_RGW_REST_H
#define CEPH_RGW_REST_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_formats.h"


extern std::map<std::string, std::string> rgw_to_http_attrs;

extern void rgw_rest_init(CephContext *cct);

extern void rgw_flush_formatter_and_reset(struct req_state *s,
					 ceph::Formatter *formatter);

extern void rgw_flush_formatter(struct req_state *s,
                                         ceph::Formatter *formatter);

extern int rgw_rest_read_all_input(struct req_state *s, char **data, int *plen, int max_len);

class RESTArgs {
public:
  static int get_string(struct req_state *s, const string& name, const string& def_val, string *val, bool *existed = NULL);
  static int get_uint64(struct req_state *s, const string& name, uint64_t def_val, uint64_t *val, bool *existed = NULL);
  static int get_int64(struct req_state *s, const string& name, int64_t def_val, int64_t *val, bool *existed = NULL);
  static int get_uint32(struct req_state *s, const string& name, uint32_t def_val, uint32_t *val, bool *existed = NULL);
  static int get_int32(struct req_state *s, const string& name, int32_t def_val, int32_t *val, bool *existed = NULL);
  static int get_time(struct req_state *s, const string& name, const utime_t& def_val, utime_t *val, bool *existed = NULL);
  static int get_epoch(struct req_state *s, const string& name, uint64_t def_val, uint64_t *epoch, bool *existed = NULL);
  static int get_bool(struct req_state *s, const string& name, bool def_val, bool *val, bool *existed = NULL);
};


class RGWRESTFlusher : public RGWFormatterFlusher {
  struct req_state *s;
protected:
  virtual void do_flush();
  virtual void do_start(int ret);
public:
  RGWRESTFlusher(struct req_state *_s) : RGWFormatterFlusher(_s->formatter), s(_s) {}
  RGWRESTFlusher() : RGWFormatterFlusher(NULL), s(NULL) {}

  void init(struct req_state *_s) {
    s = _s;
    set_formatter(s->formatter);
  }
};

class RGWClientIO;

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

class RGWPostObj_ObjStore : public RGWPostObj
{
public:
  RGWPostObj_ObjStore() {}
  ~RGWPostObj_ObjStore() {}

  virtual int verify_params();
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

class RGWRESTOp : public RGWOp {
protected:
  int http_ret;
  RGWRESTFlusher flusher;
public:
  RGWRESTOp() : http_ret(0) {}
  virtual void init(RGWRados *store, struct req_state *s, RGWHandler *dialect_handler) {
    RGWOp::init(store, s, dialect_handler);
    flusher.init(s);
  }
  virtual void send_response();
  virtual int check_caps(RGWUserCaps& caps) { return -EPERM; } /* should to be implemented! */
  virtual int verify_permission();
};

class RGWHandler_ObjStore : public RGWHandler {
protected:
  virtual bool is_obj_update_op() { return false; }
  virtual RGWOp *op_get() { return NULL; }
  virtual RGWOp *op_put() { return NULL; }
  virtual RGWOp *op_delete() { return NULL; }
  virtual RGWOp *op_head() { return NULL; }
  virtual RGWOp *op_post() { return NULL; }
  virtual RGWOp *op_copy() { return NULL; }
  virtual RGWOp *op_options() { return NULL; }

  virtual int validate_bucket_name(const string& bucket);
  virtual int validate_object_name(const string& object);

  static int allocate_formatter(struct req_state *s, int default_formatter, bool configurable);
public:
  RGWHandler_ObjStore() {}
  virtual ~RGWHandler_ObjStore() {}
  int read_permissions(RGWOp *op);

  virtual int authorize() = 0;
};

class RGWHandler_ObjStore_SWIFT;
class RGWHandler_SWIFT_Auth;
class RGWHandler_ObjStore_S3;

class RGWRESTMgr {
protected:
  map<string, RGWRESTMgr *> resource_mgrs;
  multimap<size_t, string> resources_by_size;
  RGWRESTMgr *default_mgr;

public:
  RGWRESTMgr() : default_mgr(NULL) {}
  virtual ~RGWRESTMgr();

  void register_resource(string resource, RGWRESTMgr *mgr);
  void register_default_mgr(RGWRESTMgr *mgr);

  virtual RGWRESTMgr *get_resource_mgr(struct req_state *s, const string& uri, string *out_uri);
  virtual RGWHandler *get_handler(struct req_state *s) { return NULL; }
  virtual void put_handler(RGWHandler *handler) { delete handler; }
};

class RGWREST {
  RGWRESTMgr mgr;

  static int preprocess(struct req_state *s, RGWClientIO *cio);
public:
  RGWREST() {}
  RGWHandler *get_handler(RGWRados *store, struct req_state *s, RGWClientIO *cio,
			  int *init_error);
  void put_handler(RGWHandler *handler) {
    mgr.put_handler(handler);
  }

  void register_resource(string resource, RGWRESTMgr *m, bool register_empty = false) {
    if (!register_empty && resource.empty())
      return;

    mgr.register_resource(resource, m);
  }
  void register_default_mgr(RGWRESTMgr *m) {
    mgr.register_default_mgr(m);
  }
};

extern void set_req_state_err(struct req_state *s, int err_no);
extern void dump_errno(struct req_state *s);
extern void dump_errno(struct req_state *s, int ret);
extern void end_header(struct req_state *s, const char *content_type = NULL);
extern void dump_start(struct req_state *s);
extern void list_all_buckets_start(struct req_state *s);
extern void dump_owner(struct req_state *s, string& id, string& name, const char *section = NULL);
extern void dump_content_length(struct req_state *s, uint64_t len);
extern void dump_etag(struct req_state *s, const char *etag);
extern void dump_epoch_header(struct req_state *s, const char *name, time_t t);
extern void dump_last_modified(struct req_state *s, time_t t);
extern void abort_early(struct req_state *s, int err);
extern void dump_range(struct req_state *s, uint64_t ofs, uint64_t end, uint64_t total_size);
extern void dump_continue(struct req_state *s);
extern void list_all_buckets_end(struct req_state *s);
extern void dump_time(struct req_state *s, const char *name, time_t *t);
extern void dump_bucket_from_state(struct req_state *s);
extern void dump_object_from_state(struct req_state *s);
extern void dump_uri_from_state(struct req_state *s);
extern void dump_redirect(struct req_state *s, const string& redirect);
extern void dump_pair(struct req_state *s, const char *key, const char *value);
extern bool is_valid_url(const char *url);
extern void dump_access_control(struct req_state *s, const char *origin, const char *meth,
                         const char *hdr, const char *exp_hdr, uint32_t max_age);


#endif
