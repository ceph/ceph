// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_SWIFT_H
#define CEPH_RGW_REST_SWIFT_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_rest.h"

class RGWGetObj_ObjStore_SWIFT : public RGWGetObj_ObjStore {
public:
  RGWGetObj_ObjStore_SWIFT() {}
  ~RGWGetObj_ObjStore_SWIFT() {}

  int get_params();
  int send_response_data_error();
  int send_response_data(bufferlist& bl, off_t ofs, off_t len);
  bool need_object_expiration() { return true; }
};

class RGWListBuckets_ObjStore_SWIFT : public RGWListBuckets_ObjStore {
  bool need_stats;
  std::string prefix;

  uint64_t get_default_max() const override {
    return 0;
  }
public:
  RGWListBuckets_ObjStore_SWIFT() : need_stats(true) {}
  ~RGWListBuckets_ObjStore_SWIFT() {}

  int get_params();
  void send_response_begin(bool has_buckets);
  void send_response_data(RGWUserBuckets& buckets);
  void send_response_end();

  bool should_get_stats() { return need_stats; }
  bool supports_account_metadata() { return true; }
};

class RGWListBucket_ObjStore_SWIFT : public RGWListBucket_ObjStore {
  string path;
public:
  RGWListBucket_ObjStore_SWIFT() {
    default_max = 10000;
  }
  ~RGWListBucket_ObjStore_SWIFT() {}

  int get_params();
  void send_response();
  bool need_container_stats() { return true; }
};

class RGWStatAccount_ObjStore_SWIFT : public RGWStatAccount_ObjStore {
  map<string, bufferlist> attrs;
public:
  RGWStatAccount_ObjStore_SWIFT() {
  }
  ~RGWStatAccount_ObjStore_SWIFT() {}

  void execute();
  void send_response();
};

class RGWStatBucket_ObjStore_SWIFT : public RGWStatBucket_ObjStore {
public:
  RGWStatBucket_ObjStore_SWIFT() {}
  ~RGWStatBucket_ObjStore_SWIFT() {}

  void send_response();
};

class RGWCreateBucket_ObjStore_SWIFT : public RGWCreateBucket_ObjStore {
protected:
  bool need_metadata_upload() const override { return true; }
public:
  RGWCreateBucket_ObjStore_SWIFT() {}
  ~RGWCreateBucket_ObjStore_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWDeleteBucket_ObjStore_SWIFT : public RGWDeleteBucket_ObjStore {
public:
  RGWDeleteBucket_ObjStore_SWIFT() {}
  ~RGWDeleteBucket_ObjStore_SWIFT() {}

  void send_response();
};

class RGWPutObj_ObjStore_SWIFT : public RGWPutObj_ObjStore {
  string lo_etag;
public:
  RGWPutObj_ObjStore_SWIFT() {}
  ~RGWPutObj_ObjStore_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWPutMetadataAccount_ObjStore_SWIFT : public RGWPutMetadataAccount_ObjStore {
public:
  RGWPutMetadataAccount_ObjStore_SWIFT() {}
  ~RGWPutMetadataAccount_ObjStore_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWPutMetadataBucket_ObjStore_SWIFT : public RGWPutMetadataBucket_ObjStore {
public:
  RGWPutMetadataBucket_ObjStore_SWIFT() {}
  ~RGWPutMetadataBucket_ObjStore_SWIFT() {}

  int get_params();
  void send_response();
};

class RGWPutMetadataObject_ObjStore_SWIFT : public RGWPutMetadataObject_ObjStore {
public:
  RGWPutMetadataObject_ObjStore_SWIFT() {}
  ~RGWPutMetadataObject_ObjStore_SWIFT() {}

  int get_params();
  void send_response();
  bool need_object_expiration() { return true; }
};

class RGWDeleteObj_ObjStore_SWIFT : public RGWDeleteObj_ObjStore {
public:
  RGWDeleteObj_ObjStore_SWIFT() {}
  ~RGWDeleteObj_ObjStore_SWIFT() {}

  int get_params();
  bool need_object_expiration() { return true; }
  void send_response();
};

class RGWCopyObj_ObjStore_SWIFT : public RGWCopyObj_ObjStore {
  bool sent_header;
protected:
  void dump_copy_info();
public:
  RGWCopyObj_ObjStore_SWIFT() : sent_header(false) {}
  ~RGWCopyObj_ObjStore_SWIFT() {}

  int init_dest_policy();
  int get_params();
  void send_response();
  void send_partial_response(off_t ofs);
};

class RGWGetACLs_ObjStore_SWIFT : public RGWGetACLs_ObjStore {
public:
  RGWGetACLs_ObjStore_SWIFT() {}
  ~RGWGetACLs_ObjStore_SWIFT() {}

  void send_response() {}
};

class RGWPutACLs_ObjStore_SWIFT : public RGWPutACLs_ObjStore {
public:
  RGWPutACLs_ObjStore_SWIFT() : RGWPutACLs_ObjStore() {}
  virtual ~RGWPutACLs_ObjStore_SWIFT() {}

  void send_response() {}
};

class RGWOptionsCORS_ObjStore_SWIFT : public RGWOptionsCORS_ObjStore {
public:
  RGWOptionsCORS_ObjStore_SWIFT() {}
  ~RGWOptionsCORS_ObjStore_SWIFT() {}

  void send_response();
};

class RGWBulkDelete_ObjStore_SWIFT : public RGWBulkDelete_ObjStore {
public:
  RGWBulkDelete_ObjStore_SWIFT() {}
  ~RGWBulkDelete_ObjStore_SWIFT() {}

  int get_data(std::list<RGWBulkDelete::acct_path_t>& items,
               bool * is_truncated);
  void send_response();
};

class RGWInfo_ObjStore_SWIFT : public RGWInfo_ObjStore {
protected:
  struct info
  {
    bool is_admin_info;
    function<void (Formatter&, const md_config_t&, RGWRados&)> list_data;
  };

  static const vector<pair<string, struct info>> swift_info;
public:
  RGWInfo_ObjStore_SWIFT() {}
  ~RGWInfo_ObjStore_SWIFT() {}

  void execute() override;
  void send_response() override;
  static void list_swift_data(Formatter& formatter, const md_config_t& config, RGWRados& store);
  static void list_tempurl_data(Formatter& formatter, const md_config_t& config, RGWRados& store);
  static void list_slo_data(Formatter& formatter, const md_config_t& config, RGWRados& store);
  static bool is_expired(const std::string& expires, CephContext* cct);
};

class RGWHandler_REST_SWIFT : public RGWHandler_REST {
  friend class RGWRESTMgr_SWIFT;
  friend class RGWRESTMgr_SWIFT_Info;
protected:
  virtual bool is_acl_op() {
    return false;
  }

  static int init_from_header(struct req_state *s);
public:
  RGWHandler_REST_SWIFT() {}
  virtual ~RGWHandler_REST_SWIFT() {}

  static int validate_bucket_name(const string& bucket);

  int init(RGWRados *store, struct req_state *s, RGWClientIO *cio);
  int authorize();
  int postauth_init();

  RGWAccessControlPolicy *alloc_policy() { return NULL; /* return new RGWAccessControlPolicy_SWIFT; */ }
  void free_policy(RGWAccessControlPolicy *policy) { delete policy; }
};

class RGWHandler_REST_Service_SWIFT : public RGWHandler_REST_SWIFT {
protected:
  RGWOp *op_get();
  RGWOp *op_head();
  RGWOp *op_post();
  RGWOp *op_delete();
public:
  RGWHandler_REST_Service_SWIFT() {}
  virtual ~RGWHandler_REST_Service_SWIFT() {}
};

class RGWHandler_REST_Bucket_SWIFT : public RGWHandler_REST_SWIFT {
protected:
  bool is_obj_update_op() {
    return s->op == OP_POST;
  }

  RGWOp *get_obj_op(bool get_data);
  RGWOp *op_get();
  RGWOp *op_head();
  RGWOp *op_put();
  RGWOp *op_delete();
  RGWOp *op_post();
  RGWOp *op_options();
public:
  RGWHandler_REST_Bucket_SWIFT() {}
  virtual ~RGWHandler_REST_Bucket_SWIFT() {}
};

class RGWHandler_REST_Obj_SWIFT : public RGWHandler_REST_SWIFT {
protected:
  bool is_obj_update_op() {
    return s->op == OP_POST;
  }

  RGWOp *get_obj_op(bool get_data);
  RGWOp *op_get();
  RGWOp *op_head();
  RGWOp *op_put();
  RGWOp *op_delete();
  RGWOp *op_post();
  RGWOp *op_copy();
  RGWOp *op_options();

public:
  RGWHandler_REST_Obj_SWIFT() {}
  virtual ~RGWHandler_REST_Obj_SWIFT() {}
};

class RGWRESTMgr_SWIFT : public RGWRESTMgr {
public:
  RGWRESTMgr_SWIFT() {}
  virtual ~RGWRESTMgr_SWIFT() {}

  virtual RGWHandler_REST *get_handler(struct req_state *s);

  RGWRESTMgr* get_resource_mgr_as_default(struct req_state* s,
                                          const std::string& uri,
                                          std::string* out_uri) override {
    return this->get_resource_mgr(s, uri, out_uri);
  }
};


class  RGWGetCrossDomainPolicy_ObjStore_SWIFT
  : public RGWGetCrossDomainPolicy_ObjStore {
public:
  RGWGetCrossDomainPolicy_ObjStore_SWIFT() = default;
  ~RGWGetCrossDomainPolicy_ObjStore_SWIFT() = default;

  void send_response() override;
};

class  RGWGetHealthCheck_ObjStore_SWIFT
  : public RGWGetHealthCheck_ObjStore {
public:
  RGWGetHealthCheck_ObjStore_SWIFT() = default;
  ~RGWGetHealthCheck_ObjStore_SWIFT() = default;

  void send_response() override;
};

class RGWHandler_SWIFT_CrossDomain : public RGWHandler_REST {
public:
  RGWHandler_SWIFT_CrossDomain() = default;
  ~RGWHandler_SWIFT_CrossDomain() = default;

  RGWOp *op_get() override {
    return new RGWGetCrossDomainPolicy_ObjStore_SWIFT();
  }

  int init(RGWRados* const store,
           struct req_state* const state,
           RGWClientIO* const cio) override {
    state->dialect = "swift";
    state->formatter = new JSONFormatter;
    state->format = RGW_FORMAT_JSON;

    return RGWHandler::init(store, state, cio);
  }

  int authorize() override {
    return 0;
  }

  int postauth_init() override {
    return 0;
  }

  int read_permissions(RGWOp *) override {
    return 0;
  }

  virtual RGWAccessControlPolicy *alloc_policy() { return nullptr; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

class RGWRESTMgr_SWIFT_CrossDomain : public RGWRESTMgr {
public:
  RGWRESTMgr_SWIFT_CrossDomain() = default;
  ~RGWRESTMgr_SWIFT_CrossDomain() = default;

  RGWRESTMgr *get_resource_mgr(struct req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(struct req_state* const s) override {
    s->prot_flags |= RGW_REST_SWIFT;
    return new RGWHandler_SWIFT_CrossDomain;
  }
};


class RGWHandler_SWIFT_HealthCheck : public RGWHandler_REST {
public:
  RGWHandler_SWIFT_HealthCheck() = default;
  ~RGWHandler_SWIFT_HealthCheck() = default;

  RGWOp *op_get() override {
    return new RGWGetHealthCheck_ObjStore_SWIFT();
  }

  int init(RGWRados* const store,
           struct req_state* const state,
           RGWClientIO* const cio) override {
    state->dialect = "swift";
    state->formatter = new JSONFormatter;
    state->format = RGW_FORMAT_JSON;

    return RGWHandler::init(store, state, cio);
  }

  int authorize() override {
    return 0;
  }

  int postauth_init() override {
    return 0;
  }

  int read_permissions(RGWOp *) override {
    return 0;
  }

  virtual RGWAccessControlPolicy *alloc_policy() { return nullptr; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

class RGWRESTMgr_SWIFT_HealthCheck : public RGWRESTMgr {
public:
  RGWRESTMgr_SWIFT_HealthCheck() = default;
  ~RGWRESTMgr_SWIFT_HealthCheck() = default;

  RGWRESTMgr *get_resource_mgr(struct req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(struct req_state* const s) override {
    s->prot_flags |= RGW_REST_SWIFT;
    return new RGWHandler_SWIFT_HealthCheck;
  }
};


class RGWHandler_REST_SWIFT_Info : public RGWHandler_REST_SWIFT {
public:
  RGWHandler_REST_SWIFT_Info() = default;
  ~RGWHandler_REST_SWIFT_Info() = default;

  RGWOp *op_get() override {
    return new RGWInfo_ObjStore_SWIFT();
  }

  int init(RGWRados* const store,
           struct req_state* const state,
           RGWClientIO* const cio) override {
    state->dialect = "swift";
    state->formatter = new JSONFormatter;
    state->format = RGW_FORMAT_JSON;

    return RGWHandler::init(store, state, cio);
  }

  int authorize() override {
    return 0;
  }

  int postauth_init() override {
    return 0;
  }

  int read_permissions(RGWOp *) override {
    return 0;
  }
};

class RGWRESTMgr_SWIFT_Info : public RGWRESTMgr {
public:
  RGWRESTMgr_SWIFT_Info() = default;
  virtual ~RGWRESTMgr_SWIFT_Info() = default;
  virtual RGWHandler_REST *get_handler(struct req_state *s) override;
};

#endif
