// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once
#define TIME_BUF_SIZE 128

#include <string_view>

#include <boost/optional.hpp>
#include <boost/utility/typed_in_place_factory.hpp>

#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_swift_auth.h"
#include "rgw_http_errors.h"


class RGWGetObj_ObjStore_SWIFT : public RGWGetObj_ObjStore {
  int custom_http_ret = 0;
public:
  RGWGetObj_ObjStore_SWIFT() {}
  ~RGWGetObj_ObjStore_SWIFT() override {}

  int verify_permission() override;
  int get_params() override;
  int send_response_data_error() override;
  int send_response_data(bufferlist& bl, off_t ofs, off_t len) override;

  void set_custom_http_response(const int http_ret) {
    custom_http_ret = http_ret;
  }

  bool need_object_expiration() override {
    return true;
  }
};

class RGWListBuckets_ObjStore_SWIFT : public RGWListBuckets_ObjStore {
  bool need_stats;
  bool wants_reversed;
  std::string prefix;
  std::vector<rgw::sal::RGWBucketList> reverse_buffer;

  uint64_t get_default_max() const override {
    return 0;
  }

public:
  RGWListBuckets_ObjStore_SWIFT()
    : need_stats(true),
      wants_reversed(false) {
  }
  ~RGWListBuckets_ObjStore_SWIFT() override {}

  int get_params() override;
  void handle_listing_chunk(rgw::sal::RGWBucketList&& buckets) override;
  void send_response_begin(bool has_buckets) override;
  void send_response_data(rgw::sal::RGWBucketList& buckets) override;
  void send_response_data_reversed(rgw::sal::RGWBucketList& buckets);
  void dump_bucket_entry(const rgw::sal::RGWBucket& obj);
  void send_response_end() override;

  bool should_get_stats() override { return need_stats; }
  bool supports_account_metadata() override { return true; }
};

class RGWListBucket_ObjStore_SWIFT : public RGWListBucket_ObjStore {
  string path;
public:
  RGWListBucket_ObjStore_SWIFT() {
    default_max = 10000;
  }
  ~RGWListBucket_ObjStore_SWIFT() override {}

  int get_params() override;
  void send_response() override;
  bool need_container_stats() override { return true; }
};

class RGWStatAccount_ObjStore_SWIFT : public RGWStatAccount_ObjStore {
  map<string, bufferlist> attrs;
public:
  RGWStatAccount_ObjStore_SWIFT() {
  }
  ~RGWStatAccount_ObjStore_SWIFT() override {}

  void execute() override;
  void send_response() override;
};

class RGWStatBucket_ObjStore_SWIFT : public RGWStatBucket_ObjStore {
public:
  RGWStatBucket_ObjStore_SWIFT() {}
  ~RGWStatBucket_ObjStore_SWIFT() override {}

  void send_response() override;
};

class RGWCreateBucket_ObjStore_SWIFT : public RGWCreateBucket_ObjStore {
protected:
  bool need_metadata_upload() const override { return true; }
public:
  RGWCreateBucket_ObjStore_SWIFT() {}
  ~RGWCreateBucket_ObjStore_SWIFT() override {}

  int get_params() override;
  void send_response() override;
};

class RGWDeleteBucket_ObjStore_SWIFT : public RGWDeleteBucket_ObjStore {
public:
  RGWDeleteBucket_ObjStore_SWIFT() {}
  ~RGWDeleteBucket_ObjStore_SWIFT() override {}

  void send_response() override;
};

class RGWPutObj_ObjStore_SWIFT : public RGWPutObj_ObjStore {
  string lo_etag;
public:
  RGWPutObj_ObjStore_SWIFT() {}
  ~RGWPutObj_ObjStore_SWIFT() override {}

  int update_slo_segment_size(rgw_slo_entry& entry);

  int verify_permission() override;
  int get_params() override;
  void send_response() override;
};

class RGWPutMetadataAccount_ObjStore_SWIFT : public RGWPutMetadataAccount_ObjStore {
public:
  RGWPutMetadataAccount_ObjStore_SWIFT() {}
  ~RGWPutMetadataAccount_ObjStore_SWIFT() override {}

  int get_params() override;
  void send_response() override;
};

class RGWPutMetadataBucket_ObjStore_SWIFT : public RGWPutMetadataBucket_ObjStore {
public:
  RGWPutMetadataBucket_ObjStore_SWIFT() {}
  ~RGWPutMetadataBucket_ObjStore_SWIFT() override {}

  int get_params() override;
  void send_response() override;
};

class RGWPutMetadataObject_ObjStore_SWIFT : public RGWPutMetadataObject_ObjStore {
public:
  RGWPutMetadataObject_ObjStore_SWIFT() {}
  ~RGWPutMetadataObject_ObjStore_SWIFT() override {}

  int get_params() override;
  void send_response() override;
  bool need_object_expiration() override { return true; }
};

class RGWDeleteObj_ObjStore_SWIFT : public RGWDeleteObj_ObjStore {
public:
  RGWDeleteObj_ObjStore_SWIFT() {}
  ~RGWDeleteObj_ObjStore_SWIFT() override {}

  int verify_permission() override;
  int get_params() override;
  bool need_object_expiration() override { return true; }
  void send_response() override;
};

class RGWCopyObj_ObjStore_SWIFT : public RGWCopyObj_ObjStore {
  bool sent_header;
protected:
  void dump_copy_info();
public:
  RGWCopyObj_ObjStore_SWIFT() : sent_header(false) {}
  ~RGWCopyObj_ObjStore_SWIFT() override {}

  int init_dest_policy() override;
  int get_params() override;
  void send_response() override;
  void send_partial_response(off_t ofs) override;
};

class RGWGetACLs_ObjStore_SWIFT : public RGWGetACLs_ObjStore {
public:
  RGWGetACLs_ObjStore_SWIFT() {}
  ~RGWGetACLs_ObjStore_SWIFT() override {}

  void send_response() override {}
};

class RGWPutACLs_ObjStore_SWIFT : public RGWPutACLs_ObjStore {
public:
  RGWPutACLs_ObjStore_SWIFT() : RGWPutACLs_ObjStore() {}
  ~RGWPutACLs_ObjStore_SWIFT() override {}

  void send_response() override {}
};

class RGWOptionsCORS_ObjStore_SWIFT : public RGWOptionsCORS_ObjStore {
public:
  RGWOptionsCORS_ObjStore_SWIFT() {}
  ~RGWOptionsCORS_ObjStore_SWIFT() override {}

  void send_response() override;
};

class RGWBulkDelete_ObjStore_SWIFT : public RGWBulkDelete_ObjStore {
public:
  RGWBulkDelete_ObjStore_SWIFT() {}
  ~RGWBulkDelete_ObjStore_SWIFT() override {}

  int get_data(std::list<RGWBulkDelete::acct_path_t>& items,
               bool * is_truncated) override;
  void send_response() override;
};

class RGWBulkUploadOp_ObjStore_SWIFT : public RGWBulkUploadOp_ObjStore {
  size_t conlen;
  size_t curpos;

public:
  RGWBulkUploadOp_ObjStore_SWIFT()
    : conlen(0),
      curpos(0) {
  }
  ~RGWBulkUploadOp_ObjStore_SWIFT() = default;

  std::unique_ptr<StreamGetter> create_stream() override;
  void send_response() override;
};

class RGWInfo_ObjStore_SWIFT : public RGWInfo_ObjStore {
protected:
  struct info
  {
    bool is_admin_info;
    function<void (Formatter&, const ConfigProxy&, RGWRados&)> list_data;
  };

  static const vector<pair<string, struct info>> swift_info;
public:
  RGWInfo_ObjStore_SWIFT() {}
  ~RGWInfo_ObjStore_SWIFT() override {}

  void execute() override;
  void send_response() override;
  static void list_swift_data(Formatter& formatter, const ConfigProxy& config, RGWRados& store);
  static void list_tempauth_data(Formatter& formatter, const ConfigProxy& config, RGWRados& store);
  static void list_tempurl_data(Formatter& formatter, const ConfigProxy& config, RGWRados& store);
  static void list_slo_data(Formatter& formatter, const ConfigProxy& config, RGWRados& store);
  static bool is_expired(const std::string& expires, const DoutPrefixProvider* dpp);
};


class RGWFormPost : public RGWPostObj_ObjStore {
  std::string get_current_filename() const override;
  std::string get_current_content_type() const override;
  std::size_t get_max_file_size() /*const*/;
  bool is_next_file_to_upload() override;
  bool is_integral();
  bool is_non_expired();
  void get_owner_info(const req_state* s,
                      RGWUserInfo& owner_info) const;

  parts_collection_t ctrl_parts;
  boost::optional<post_form_part> current_data_part;
  std::string prefix;
  bool stream_done = false;

  class SignatureHelper;
public:
  RGWFormPost() = default;
  ~RGWFormPost() = default;

  void init(rgw::sal::RGWRadosStore* store,
            req_state* s,
            RGWHandler* dialect_handler) override;

  int get_params() override;
  int get_data(ceph::bufferlist& bl, bool& again) override;
  void send_response() override;

  static bool is_formpost_req(req_state* const s);
};

class RGWFormPost::SignatureHelper
{
private:
  static constexpr uint32_t output_size =
    CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2 + 1;

  unsigned char dest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE]; // 20
  char dest_str[output_size];

public:
  SignatureHelper() = default;

  const char* calc(const std::string& key,
                   const std::string_view& path_info,
                   const std::string_view& redirect,
                   const std::string_view& max_file_size,
                   const std::string_view& max_file_count,
                   const std::string_view& expires) {
    using ceph::crypto::HMACSHA1;
    using UCHARPTR = const unsigned char*;

    HMACSHA1 hmac((UCHARPTR) key.data(), key.size());

    hmac.Update((UCHARPTR) path_info.data(), path_info.size());
    hmac.Update((UCHARPTR) "\n", 1);

    hmac.Update((UCHARPTR) redirect.data(), redirect.size());
    hmac.Update((UCHARPTR) "\n", 1);

    hmac.Update((UCHARPTR) max_file_size.data(), max_file_size.size());
    hmac.Update((UCHARPTR) "\n", 1);

    hmac.Update((UCHARPTR) max_file_count.data(), max_file_count.size());
    hmac.Update((UCHARPTR) "\n", 1);

    hmac.Update((UCHARPTR) expires.data(), expires.size());

    hmac.Final(dest);

    buf_to_hex((UCHARPTR) dest, sizeof(dest), dest_str);

    return dest_str;
  }

  const char* get_signature() const {
    return dest_str;
  }

  bool is_equal_to(const std::string& rhs) const {
    /* never allow out-of-range exception */
    if (rhs.size() < (output_size - 1)) {
      return false;
    }
    return rhs.compare(0 /* pos */,  output_size, dest_str) == 0;
  }

}; /* RGWFormPost::SignatureHelper */


class RGWSwiftWebsiteHandler {
  rgw::sal::RGWRadosStore* const store;
  req_state* const s;
  RGWHandler_REST* const handler;

  bool is_web_mode() const;
  bool can_be_website_req() const;
  bool is_web_dir() const;
  bool is_index_present(const std::string& index) const;

  int serve_errordoc(int http_ret, std::string error_doc);

  RGWOp* get_ws_redirect_op();
  RGWOp* get_ws_index_op();
  RGWOp* get_ws_listing_op();
public:
  RGWSwiftWebsiteHandler(rgw::sal::RGWRadosStore* const store,
                         req_state* const s,
                         RGWHandler_REST* const handler)
    : store(store),
      s(s),
      handler(handler) {
  }

  int error_handler(const int err_no,
                    std::string* const error_content);
  int retarget_bucket(RGWOp* op, RGWOp** new_op);
  int retarget_object(RGWOp* op, RGWOp** new_op);
};


class RGWHandler_REST_SWIFT : public RGWHandler_REST {
  friend class RGWRESTMgr_SWIFT;
  friend class RGWRESTMgr_SWIFT_Info;
protected:
  const rgw::auth::Strategy& auth_strategy;

  virtual bool is_acl_op() const {
    return false;
  }

  static int init_from_header(struct req_state* s,
                              const std::string& frontend_prefix);
public:
  explicit RGWHandler_REST_SWIFT(const rgw::auth::Strategy& auth_strategy)
    : auth_strategy(auth_strategy) {
  }
  ~RGWHandler_REST_SWIFT() override = default;

  int validate_bucket_name(const string& bucket);

  int init(rgw::sal::RGWRadosStore *store, struct req_state *s, rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider *dpp) override;
  int postauth_init() override;

  RGWAccessControlPolicy *alloc_policy() { return nullptr; /* return new RGWAccessControlPolicy_SWIFT; */ }
  void free_policy(RGWAccessControlPolicy *policy) { delete policy; }
};

class RGWHandler_REST_Service_SWIFT : public RGWHandler_REST_SWIFT {
protected:
  RGWOp *op_get() override;
  RGWOp *op_head() override;
  RGWOp *op_put() override;
  RGWOp *op_post() override;
  RGWOp *op_delete() override;
public:
  using RGWHandler_REST_SWIFT::RGWHandler_REST_SWIFT;
  ~RGWHandler_REST_Service_SWIFT() override = default;
};

class RGWHandler_REST_Bucket_SWIFT : public RGWHandler_REST_SWIFT {
  /* We need the boost::optional here only because of handler's late
   * initialization (see the init() method). */
  boost::optional<RGWSwiftWebsiteHandler> website_handler;
protected:
  bool is_obj_update_op() const override {
    return s->op == OP_POST;
  }

  RGWOp *get_obj_op(bool get_data);
  RGWOp *op_get() override;
  RGWOp *op_head() override;
  RGWOp *op_put() override;
  RGWOp *op_delete() override;
  RGWOp *op_post() override;
  RGWOp *op_options() override;
public:
  using RGWHandler_REST_SWIFT::RGWHandler_REST_SWIFT;
  ~RGWHandler_REST_Bucket_SWIFT() override = default;

  int error_handler(int err_no, std::string *error_content) override {
    return website_handler->error_handler(err_no, error_content);
  }

  int retarget(RGWOp* op, RGWOp** new_op) override {
    return website_handler->retarget_bucket(op, new_op);
  }

  int init(rgw::sal::RGWRadosStore* const store,
           struct req_state* const s,
           rgw::io::BasicClient* const cio) override {
    website_handler = boost::in_place<RGWSwiftWebsiteHandler>(store, s, this);
    return RGWHandler_REST_SWIFT::init(store, s, cio);
  }
};

class RGWHandler_REST_Obj_SWIFT : public RGWHandler_REST_SWIFT {
  /* We need the boost::optional here only because of handler's late
   * initialization (see the init() method). */
  boost::optional<RGWSwiftWebsiteHandler> website_handler;
protected:
  bool is_obj_update_op() const override {
    return s->op == OP_POST;
  }

  RGWOp *get_obj_op(bool get_data);
  RGWOp *op_get() override;
  RGWOp *op_head() override;
  RGWOp *op_put() override;
  RGWOp *op_delete() override;
  RGWOp *op_post() override;
  RGWOp *op_copy() override;
  RGWOp *op_options() override;

public:
  using RGWHandler_REST_SWIFT::RGWHandler_REST_SWIFT;
  ~RGWHandler_REST_Obj_SWIFT() override = default;

  int error_handler(int err_no, std::string *error_content) override {
    return website_handler->error_handler(err_no, error_content);
  }

  int retarget(RGWOp* op, RGWOp** new_op) override {
    return website_handler->retarget_object(op, new_op);
  }

  int init(rgw::sal::RGWRadosStore* const store,
           struct req_state* const s,
           rgw::io::BasicClient* const cio) override {
    website_handler = boost::in_place<RGWSwiftWebsiteHandler>(store, s, this);
    return RGWHandler_REST_SWIFT::init(store, s, cio);
  }
};

class RGWRESTMgr_SWIFT : public RGWRESTMgr {
protected:
  RGWRESTMgr* get_resource_mgr_as_default(struct req_state* const s,
                                          const std::string& uri,
                                          std::string* const out_uri) override {
    return this->get_resource_mgr(s, uri, out_uri);
  }

public:
  RGWRESTMgr_SWIFT() = default;
  ~RGWRESTMgr_SWIFT() override = default;

  RGWHandler_REST *get_handler(struct req_state *s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefix) override;
};


class  RGWGetCrossDomainPolicy_ObjStore_SWIFT
  : public RGWGetCrossDomainPolicy_ObjStore {
public:
  RGWGetCrossDomainPolicy_ObjStore_SWIFT() = default;
  ~RGWGetCrossDomainPolicy_ObjStore_SWIFT() override = default;

  void send_response() override;
};

class  RGWGetHealthCheck_ObjStore_SWIFT
  : public RGWGetHealthCheck_ObjStore {
public:
  RGWGetHealthCheck_ObjStore_SWIFT() = default;
  ~RGWGetHealthCheck_ObjStore_SWIFT() override = default;

  void send_response() override;
};

class RGWHandler_SWIFT_CrossDomain : public RGWHandler_REST {
public:
  RGWHandler_SWIFT_CrossDomain() = default;
  ~RGWHandler_SWIFT_CrossDomain() override = default;

  RGWOp *op_get() override {
    return new RGWGetCrossDomainPolicy_ObjStore_SWIFT();
  }

  int init(rgw::sal::RGWRadosStore* const store,
           struct req_state* const state,
           rgw::io::BasicClient* const cio) override {
    state->dialect = "swift";
    state->formatter = new JSONFormatter;
    state->format = RGW_FORMAT_JSON;

    return RGWHandler::init(store, state, cio);
  }

  int authorize(const DoutPrefixProvider *dpp) override {
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
protected:
  RGWRESTMgr *get_resource_mgr(struct req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

public:
  RGWRESTMgr_SWIFT_CrossDomain() = default;
  ~RGWRESTMgr_SWIFT_CrossDomain() override = default;

  RGWHandler_REST* get_handler(struct req_state* const s,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override {
    s->prot_flags |= RGW_REST_SWIFT;
    return new RGWHandler_SWIFT_CrossDomain;
  }
};


class RGWHandler_SWIFT_HealthCheck : public RGWHandler_REST {
public:
  RGWHandler_SWIFT_HealthCheck() = default;
  ~RGWHandler_SWIFT_HealthCheck() override = default;

  RGWOp *op_get() override {
    return new RGWGetHealthCheck_ObjStore_SWIFT();
  }

  int init(rgw::sal::RGWRadosStore* const store,
           struct req_state* const state,
           rgw::io::BasicClient* const cio) override {
    state->dialect = "swift";
    state->formatter = new JSONFormatter;
    state->format = RGW_FORMAT_JSON;

    return RGWHandler::init(store, state, cio);
  }

  int authorize(const DoutPrefixProvider *dpp) override {
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
protected:
  RGWRESTMgr *get_resource_mgr(struct req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

public:
  RGWRESTMgr_SWIFT_HealthCheck() = default;
  ~RGWRESTMgr_SWIFT_HealthCheck() override = default;

  RGWHandler_REST* get_handler(struct req_state* const s,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override {
    s->prot_flags |= RGW_REST_SWIFT;
    return new RGWHandler_SWIFT_HealthCheck;
  }
};


class RGWHandler_REST_SWIFT_Info : public RGWHandler_REST_SWIFT {
public:
  using RGWHandler_REST_SWIFT::RGWHandler_REST_SWIFT;
  ~RGWHandler_REST_SWIFT_Info() override = default;

  RGWOp *op_get() override {
    return new RGWInfo_ObjStore_SWIFT();
  }

  int init(rgw::sal::RGWRadosStore* const store,
           struct req_state* const state,
           rgw::io::BasicClient* const cio) override {
    state->dialect = "swift";
    state->formatter = new JSONFormatter;
    state->format = RGW_FORMAT_JSON;

    return RGWHandler::init(store, state, cio);
  }

  int authorize(const DoutPrefixProvider *dpp) override {
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
  ~RGWRESTMgr_SWIFT_Info() override = default;

  RGWHandler_REST *get_handler(struct req_state* s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefix) override;
};
