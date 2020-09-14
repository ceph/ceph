// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#define TIME_BUF_SIZE 128

#include <mutex>
#include <string_view>

#include <boost/container/static_vector.hpp>
#include <boost/crc.hpp> 

#include "common/sstring.hh"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_http_errors.h"
#include "rgw_acl_s3.h"
#include "rgw_policy_s3.h"
#include "rgw_lc_s3.h"
#include "rgw_keystone.h"
#include "rgw_rest_conn.h"
#include "rgw_ldap.h"

#include "rgw_token.h"
#include "include/ceph_assert.h"

#include "rgw_auth.h"
#include "rgw_auth_filters.h"
#include "rgw_sts.h"

struct rgw_http_error {
  int http_ret;
  const char *s3_code;
};

void rgw_get_errno_s3(struct rgw_http_error *e, int err_no);

class RGWGetObj_ObjStore_S3 : public RGWGetObj_ObjStore
{
protected:
  // Serving a custom error page from an object is really a 200 response with
  // just the status line altered.
  int custom_http_ret = 0;
  std::map<std::string, std::string> crypt_http_responses;
  int override_range_hdr(const rgw::auth::StrategyRegistry& auth_registry);
public:
  RGWGetObj_ObjStore_S3() {}
  ~RGWGetObj_ObjStore_S3() override {}

  int verify_requester(const rgw::auth::StrategyRegistry& auth_registry) override;
  int get_params() override;
  int send_response_data_error() override;
  int send_response_data(bufferlist& bl, off_t ofs, off_t len) override;
  void set_custom_http_response(int http_ret) { custom_http_ret = http_ret; }
  int get_decrypt_filter(std::unique_ptr<RGWGetObj_Filter>* filter,
                         RGWGetObj_Filter* cb,
                         bufferlist* manifest_bl) override;
};

class RGWGetObjTags_ObjStore_S3 : public RGWGetObjTags_ObjStore
{
public:
  RGWGetObjTags_ObjStore_S3() {}
  ~RGWGetObjTags_ObjStore_S3() {}

  void send_response_data(bufferlist &bl) override;
};

class RGWPutObjTags_ObjStore_S3 : public RGWPutObjTags_ObjStore
{
public:
  RGWPutObjTags_ObjStore_S3() {}
  ~RGWPutObjTags_ObjStore_S3() {}

  int get_params() override;
  void send_response() override;
};

class RGWDeleteObjTags_ObjStore_S3 : public RGWDeleteObjTags
{
public:
  ~RGWDeleteObjTags_ObjStore_S3() override {}
  void send_response() override;
};

class RGWGetBucketTags_ObjStore_S3 : public RGWGetBucketTags_ObjStore
{
  bufferlist tags_bl;
public:
  void send_response_data(bufferlist &bl) override;
};

class RGWPutBucketTags_ObjStore_S3 : public RGWPutBucketTags_ObjStore
{
public:
  int get_params() override;
  void send_response() override;
};

class RGWDeleteBucketTags_ObjStore_S3 : public RGWDeleteBucketTags
{
public:
  void send_response() override;
};

class RGWGetBucketReplication_ObjStore_S3 : public RGWGetBucketReplication_ObjStore
{
public:
  void send_response_data() override;
};

class RGWPutBucketReplication_ObjStore_S3 : public RGWPutBucketReplication_ObjStore
{
public:
  int get_params() override;
  void send_response() override;
};

class RGWDeleteBucketReplication_ObjStore_S3 : public RGWDeleteBucketReplication_ObjStore
{
protected:
  void update_sync_policy(rgw_sync_policy_info *policy) override;
public:
  void send_response() override;
};

class RGWListBuckets_ObjStore_S3 : public RGWListBuckets_ObjStore {
public:
  RGWListBuckets_ObjStore_S3() {}
  ~RGWListBuckets_ObjStore_S3() override {}

  int get_params() override {
    limit = -1; /* no limit */
    return 0;
  }
  void send_response_begin(bool has_buckets) override;
  void send_response_data(rgw::sal::RGWBucketList& buckets) override;
  void send_response_end() override;
};

class RGWGetUsage_ObjStore_S3 : public RGWGetUsage_ObjStore {
public:
  RGWGetUsage_ObjStore_S3() {}
  ~RGWGetUsage_ObjStore_S3() override {}

  int get_params() override ;
  void send_response() override;
};

class RGWListBucket_ObjStore_S3 : public RGWListBucket_ObjStore {
protected:
  bool objs_container;
  bool encode_key {false};
  int get_common_params();
  void send_common_response();
  void send_common_versioned_response();
  public:
  RGWListBucket_ObjStore_S3() : objs_container(false) {
    default_max = 1000;
  }
  ~RGWListBucket_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
  void send_versioned_response();
};

class RGWListBucket_ObjStore_S3v2 : public RGWListBucket_ObjStore_S3 {
  bool fetchOwner;
  bool start_after_exist;
  bool continuation_token_exist;
  string startAfter;
  string continuation_token;
public:
  RGWListBucket_ObjStore_S3v2() :  fetchOwner(false) {
  }
  ~RGWListBucket_ObjStore_S3v2() override {}

  int get_params() override;
  void send_response() override;
  void send_versioned_response();
};

class RGWGetBucketLogging_ObjStore_S3 : public RGWGetBucketLogging {
public:
  RGWGetBucketLogging_ObjStore_S3() {}
  ~RGWGetBucketLogging_ObjStore_S3() override {}

  void send_response() override;
};

class RGWGetBucketLocation_ObjStore_S3 : public RGWGetBucketLocation {
public:
  RGWGetBucketLocation_ObjStore_S3() {}
  ~RGWGetBucketLocation_ObjStore_S3() override {}

  void send_response() override;
};

class RGWGetBucketVersioning_ObjStore_S3 : public RGWGetBucketVersioning {
public:
  RGWGetBucketVersioning_ObjStore_S3() {}
  ~RGWGetBucketVersioning_ObjStore_S3() override {}

  void send_response() override;
};

class RGWSetBucketVersioning_ObjStore_S3 : public RGWSetBucketVersioning {
public:
  RGWSetBucketVersioning_ObjStore_S3() {}
  ~RGWSetBucketVersioning_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
};

class RGWGetBucketWebsite_ObjStore_S3 : public RGWGetBucketWebsite {
public:
  RGWGetBucketWebsite_ObjStore_S3() {}
  ~RGWGetBucketWebsite_ObjStore_S3() override {}

  void send_response() override;
};

class RGWSetBucketWebsite_ObjStore_S3 : public RGWSetBucketWebsite {
public:
  RGWSetBucketWebsite_ObjStore_S3() {}
  ~RGWSetBucketWebsite_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
};

class RGWDeleteBucketWebsite_ObjStore_S3 : public RGWDeleteBucketWebsite {
public:
  RGWDeleteBucketWebsite_ObjStore_S3() {}
  ~RGWDeleteBucketWebsite_ObjStore_S3() override {}

  void send_response() override;
};

class RGWStatBucket_ObjStore_S3 : public RGWStatBucket_ObjStore {
public:
  RGWStatBucket_ObjStore_S3() {}
  ~RGWStatBucket_ObjStore_S3() override {}

  void send_response() override;
};

class RGWCreateBucket_ObjStore_S3 : public RGWCreateBucket_ObjStore {
public:
  RGWCreateBucket_ObjStore_S3() {}
  ~RGWCreateBucket_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
};

class RGWDeleteBucket_ObjStore_S3 : public RGWDeleteBucket_ObjStore {
public:
  RGWDeleteBucket_ObjStore_S3() {}
  ~RGWDeleteBucket_ObjStore_S3() override {}

  void send_response() override;
};

class RGWPutObj_ObjStore_S3 : public RGWPutObj_ObjStore {
private:
  std::map<std::string, std::string> crypt_http_responses;

public:
  RGWPutObj_ObjStore_S3() {}
  ~RGWPutObj_ObjStore_S3() override {}

  int get_params() override;
  int get_data(bufferlist& bl) override;
  void send_response() override;

  int get_encrypt_filter(std::unique_ptr<rgw::putobj::DataProcessor> *filter,
                         rgw::putobj::DataProcessor *cb) override;
  int get_decrypt_filter(std::unique_ptr<RGWGetObj_Filter>* filter,
                         RGWGetObj_Filter* cb,
                         map<string, bufferlist>& attrs,
                         bufferlist* manifest_bl) override;
};

class RGWPostObj_ObjStore_S3 : public RGWPostObj_ObjStore {
  parts_collection_t parts;
  std::string filename;
  std::string content_type;
  RGWPolicyEnv env;
  RGWPolicy post_policy;
  map<string, string> crypt_http_responses;

  const rgw::auth::StrategyRegistry* auth_registry_ptr = nullptr;

  int get_policy();
  int get_tags();
  void rebuild_key(rgw::sal::RGWObject* obj);

  std::string get_current_filename() const override;
  std::string get_current_content_type() const override;

public:
  RGWPostObj_ObjStore_S3() {}
  ~RGWPostObj_ObjStore_S3() override {}

  int verify_requester(const rgw::auth::StrategyRegistry& auth_registry) override {
    auth_registry_ptr = &auth_registry;
    return RGWPostObj_ObjStore::verify_requester(auth_registry);
  }

  int get_params() override;
  int complete_get_params();

  void send_response() override;
  int get_data(ceph::bufferlist& bl, bool& again) override;
  int get_encrypt_filter(std::unique_ptr<rgw::putobj::DataProcessor> *filter,
                         rgw::putobj::DataProcessor *cb) override;
};

class RGWDeleteObj_ObjStore_S3 : public RGWDeleteObj_ObjStore {
public:
  RGWDeleteObj_ObjStore_S3() {}
  ~RGWDeleteObj_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
};

class RGWCopyObj_ObjStore_S3 : public RGWCopyObj_ObjStore {
  bool sent_header;
public:
  RGWCopyObj_ObjStore_S3() : sent_header(false) {}
  ~RGWCopyObj_ObjStore_S3() override {}

  int init_dest_policy() override;
  int get_params() override;
  int check_storage_class(const rgw_placement_rule& src_placement);
  void send_partial_response(off_t ofs) override;
  void send_response() override;
};

class RGWGetACLs_ObjStore_S3 : public RGWGetACLs_ObjStore {
public:
  RGWGetACLs_ObjStore_S3() {}
  ~RGWGetACLs_ObjStore_S3() override {}

  void send_response() override;
};

class RGWPutACLs_ObjStore_S3 : public RGWPutACLs_ObjStore {
public:
  RGWPutACLs_ObjStore_S3() {}
  ~RGWPutACLs_ObjStore_S3() override {}

  int get_policy_from_state(rgw::sal::RGWRadosStore *store, struct req_state *s, stringstream& ss) override;
  void send_response() override;
  int get_params() override;
};

class RGWGetLC_ObjStore_S3 : public RGWGetLC_ObjStore {
protected:
  RGWLifecycleConfiguration_S3 config;
public:
  RGWGetLC_ObjStore_S3() {}
  ~RGWGetLC_ObjStore_S3() override {}
  void execute() override;

 void send_response() override;
};

class RGWPutLC_ObjStore_S3 : public RGWPutLC_ObjStore {
public:
  RGWPutLC_ObjStore_S3() {}
  ~RGWPutLC_ObjStore_S3() override {}
  
 void send_response() override;
};

class RGWDeleteLC_ObjStore_S3 : public RGWDeleteLC_ObjStore {
public:
  RGWDeleteLC_ObjStore_S3() {}
  ~RGWDeleteLC_ObjStore_S3() override {}
  
 void send_response() override;
};

class RGWGetCORS_ObjStore_S3 : public RGWGetCORS_ObjStore {
public:
  RGWGetCORS_ObjStore_S3() {}
  ~RGWGetCORS_ObjStore_S3() override {}

  void send_response() override;
};

class RGWPutCORS_ObjStore_S3 : public RGWPutCORS_ObjStore {
public:
  RGWPutCORS_ObjStore_S3() {}
  ~RGWPutCORS_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
};

class RGWDeleteCORS_ObjStore_S3 : public RGWDeleteCORS_ObjStore {
public:
  RGWDeleteCORS_ObjStore_S3() {}
  ~RGWDeleteCORS_ObjStore_S3() override {}

  void send_response() override;
};

class RGWOptionsCORS_ObjStore_S3 : public RGWOptionsCORS_ObjStore {
public:
  RGWOptionsCORS_ObjStore_S3() {}
  ~RGWOptionsCORS_ObjStore_S3() override {}

  void send_response() override;
};

class RGWGetRequestPayment_ObjStore_S3 : public RGWGetRequestPayment {
public:
  RGWGetRequestPayment_ObjStore_S3() {}
  ~RGWGetRequestPayment_ObjStore_S3() override {}

  void send_response() override;
};

class RGWSetRequestPayment_ObjStore_S3 : public RGWSetRequestPayment {
public:
  RGWSetRequestPayment_ObjStore_S3() {}
  ~RGWSetRequestPayment_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
};

class RGWInitMultipart_ObjStore_S3 : public RGWInitMultipart_ObjStore {
private:
  std::map<std::string, std::string> crypt_http_responses;
public:
  RGWInitMultipart_ObjStore_S3() {}
  ~RGWInitMultipart_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
  int prepare_encryption(map<string, bufferlist>& attrs) override;
};

class RGWCompleteMultipart_ObjStore_S3 : public RGWCompleteMultipart_ObjStore {
public:
  RGWCompleteMultipart_ObjStore_S3() {}
  ~RGWCompleteMultipart_ObjStore_S3() override {}

  int get_params() override;
  void send_response() override;
};

class RGWAbortMultipart_ObjStore_S3 : public RGWAbortMultipart_ObjStore {
public:
  RGWAbortMultipart_ObjStore_S3() {}
  ~RGWAbortMultipart_ObjStore_S3() override {}

  void send_response() override;
};

class RGWListMultipart_ObjStore_S3 : public RGWListMultipart_ObjStore {
public:
  RGWListMultipart_ObjStore_S3() {}
  ~RGWListMultipart_ObjStore_S3() override {}

  void send_response() override;
};

class RGWListBucketMultiparts_ObjStore_S3 : public RGWListBucketMultiparts_ObjStore {
public:
  RGWListBucketMultiparts_ObjStore_S3() {
    default_max = 1000;
  }
  ~RGWListBucketMultiparts_ObjStore_S3() override {}

  void send_response() override;
};

class RGWDeleteMultiObj_ObjStore_S3 : public RGWDeleteMultiObj_ObjStore {
public:
  RGWDeleteMultiObj_ObjStore_S3() {}
  ~RGWDeleteMultiObj_ObjStore_S3() override {}

  int get_params() override;
  void send_status() override;
  void begin_response() override;
  void send_partial_response(rgw_obj_key& key, bool delete_marker,
                             const string& marker_version_id, int ret) override;
  void end_response() override;
};

class RGWPutBucketObjectLock_ObjStore_S3 : public RGWPutBucketObjectLock_ObjStore {
public:
  RGWPutBucketObjectLock_ObjStore_S3() {}
  ~RGWPutBucketObjectLock_ObjStore_S3() override {}
  void send_response() override;
};

class RGWGetBucketObjectLock_ObjStore_S3 : public RGWGetBucketObjectLock_ObjStore {
public:
  RGWGetBucketObjectLock_ObjStore_S3() {}
  ~RGWGetBucketObjectLock_ObjStore_S3() {}
  void send_response() override;
};

class RGWPutObjRetention_ObjStore_S3 : public RGWPutObjRetention_ObjStore {
public:
  RGWPutObjRetention_ObjStore_S3() {}
  ~RGWPutObjRetention_ObjStore_S3() {}
  int get_params() override;
  void send_response() override;
};

class RGWGetObjRetention_ObjStore_S3 : public RGWGetObjRetention_ObjStore {
public:
  RGWGetObjRetention_ObjStore_S3() {}
  ~RGWGetObjRetention_ObjStore_S3() {}
  void send_response() override;
};

class RGWPutObjLegalHold_ObjStore_S3 : public RGWPutObjLegalHold_ObjStore {
public:
  RGWPutObjLegalHold_ObjStore_S3() {}
  ~RGWPutObjLegalHold_ObjStore_S3() {}
  void send_response() override;
};

class RGWGetObjLegalHold_ObjStore_S3 : public RGWGetObjLegalHold_ObjStore {
public:
  RGWGetObjLegalHold_ObjStore_S3() {}
  ~RGWGetObjLegalHold_ObjStore_S3() {}
  void send_response() override;
};

class RGWGetObjLayout_ObjStore_S3 : public RGWGetObjLayout {
public:
  RGWGetObjLayout_ObjStore_S3() {}
  ~RGWGetObjLayout_ObjStore_S3() {}

  void send_response() override;
};

class RGWConfigBucketMetaSearch_ObjStore_S3 : public RGWConfigBucketMetaSearch {
public:
  RGWConfigBucketMetaSearch_ObjStore_S3() {}
  ~RGWConfigBucketMetaSearch_ObjStore_S3() {}

  int get_params() override;
  void send_response() override;
};

class RGWGetBucketMetaSearch_ObjStore_S3 : public RGWGetBucketMetaSearch {
public:
  RGWGetBucketMetaSearch_ObjStore_S3() {}
  ~RGWGetBucketMetaSearch_ObjStore_S3() {}

  void send_response() override;
};

class RGWDelBucketMetaSearch_ObjStore_S3 : public RGWDelBucketMetaSearch {
public:
  RGWDelBucketMetaSearch_ObjStore_S3() {}
  ~RGWDelBucketMetaSearch_ObjStore_S3() {}

  void send_response() override;
};

class RGWGetBucketPolicyStatus_ObjStore_S3 : public RGWGetBucketPolicyStatus {
public:
  void send_response() override;
};

class RGWPutBucketPublicAccessBlock_ObjStore_S3 : public RGWPutBucketPublicAccessBlock {
public:
  void send_response() override;
};

class RGWGetBucketPublicAccessBlock_ObjStore_S3 : public RGWGetBucketPublicAccessBlock {
public:
  void send_response() override;
};

class RGW_Auth_S3 {
public:
  static int authorize(const DoutPrefixProvider *dpp,
                       rgw::sal::RGWRadosStore *store,
                       const rgw::auth::StrategyRegistry& auth_registry,
                       struct req_state *s);
};

class RGWHandler_Auth_S3 : public RGWHandler_REST {
  friend class RGWRESTMgr_S3;

  const rgw::auth::StrategyRegistry& auth_registry;

public:
  explicit RGWHandler_Auth_S3(const rgw::auth::StrategyRegistry& auth_registry)
    : RGWHandler_REST(),
      auth_registry(auth_registry) {
  }
  ~RGWHandler_Auth_S3() override = default;

  static int validate_bucket_name(const string& bucket);
  static int validate_object_name(const string& bucket);

  int init(rgw::sal::RGWRadosStore *store,
           struct req_state *s,
           rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider *dpp) override {
    return RGW_Auth_S3::authorize(dpp, store, auth_registry, s);
  }
  int postauth_init() override { return 0; }
};

class RGWHandler_REST_S3 : public RGWHandler_REST {
  friend class RGWRESTMgr_S3;
protected:
  const rgw::auth::StrategyRegistry& auth_registry;
public:
  static int init_from_header(rgw::sal::RGWRadosStore *store, struct req_state *s, int default_formatter, bool configurable_format);

  explicit RGWHandler_REST_S3(const rgw::auth::StrategyRegistry& auth_registry)
    : RGWHandler_REST(),
      auth_registry(auth_registry) {
    }
  ~RGWHandler_REST_S3() override = default;

  int init(rgw::sal::RGWRadosStore *store,
           struct req_state *s,
           rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider *dpp) override;
  int postauth_init() override;
};

class RGWHandler_REST_Service_S3 : public RGWHandler_REST_S3 {
protected:
  const bool isSTSEnabled;
  const bool isIAMEnabled;
  const bool isPSEnabled;
  bool is_usage_op() const {
    return s->info.args.exists("usage");
  }
  RGWOp *op_get() override;
  RGWOp *op_head() override;
  RGWOp *op_post() override;
public:
   RGWHandler_REST_Service_S3(const rgw::auth::StrategyRegistry& auth_registry,
                              bool _isSTSEnabled, bool _isIAMEnabled, bool _isPSEnabled) :
      RGWHandler_REST_S3(auth_registry), isSTSEnabled(_isSTSEnabled), isIAMEnabled(_isIAMEnabled), isPSEnabled(_isPSEnabled) {}
  ~RGWHandler_REST_Service_S3() override = default;
};

class RGWHandler_REST_Bucket_S3 : public RGWHandler_REST_S3 {
  const bool enable_pubsub;
protected:
  bool is_acl_op() const {
    return s->info.args.exists("acl");
  }
  bool is_cors_op() const {
      return s->info.args.exists("cors");
  }
  bool is_lc_op() const {
      return s->info.args.exists("lifecycle");
  }
  bool is_obj_update_op() const override {
    return is_acl_op() || is_cors_op();
  }
  bool is_tagging_op() const {
    return s->info.args.exists("tagging");
  }
  bool is_request_payment_op() const {
    return s->info.args.exists("requestPayment");
  }
  bool is_policy_op() const {
    return s->info.args.exists("policy");
  }
  bool is_object_lock_op() const {
    return s->info.args.exists("object-lock");
  }
  bool is_notification_op() const {
    if (enable_pubsub) {
        return s->info.args.exists("notification");
    }
    return false;
  }
  bool is_replication_op() const {
    return s->info.args.exists("replication");
  }
  bool is_policy_status_op() {
    return s->info.args.exists("policyStatus");
  }
  bool is_block_public_access_op() {
    return s->info.args.exists("publicAccessBlock");
  }

  RGWOp *get_obj_op(bool get_data) const;
  RGWOp *op_get() override;
  RGWOp *op_head() override;
  RGWOp *op_put() override;
  RGWOp *op_delete() override;
  RGWOp *op_post() override;
  RGWOp *op_options() override;
public:
  RGWHandler_REST_Bucket_S3(const rgw::auth::StrategyRegistry& auth_registry, bool _enable_pubsub) :
      RGWHandler_REST_S3(auth_registry), enable_pubsub(_enable_pubsub) {}
  ~RGWHandler_REST_Bucket_S3() override = default;
};

class RGWHandler_REST_Obj_S3 : public RGWHandler_REST_S3 {
protected:
  bool is_acl_op() const {
    return s->info.args.exists("acl");
  }
  bool is_tagging_op() const {
    return s->info.args.exists("tagging");
  }
  bool is_obj_retention_op() const {
    return s->info.args.exists("retention");
  }
  bool is_obj_legal_hold_op() const {
    return s->info.args.exists("legal-hold");
  }

  bool is_select_op() const {
    return s->info.args.exists("select-type");
  }

  bool is_obj_update_op() const override {
    return is_acl_op() || is_tagging_op() || is_obj_retention_op() || is_obj_legal_hold_op() || is_select_op();
  }
  RGWOp *get_obj_op(bool get_data);

  RGWOp *op_get() override;
  RGWOp *op_head() override;
  RGWOp *op_put() override;
  RGWOp *op_delete() override;
  RGWOp *op_post() override;
  RGWOp *op_options() override;
public:
  using RGWHandler_REST_S3::RGWHandler_REST_S3;
  ~RGWHandler_REST_Obj_S3() override = default;
};

class RGWRESTMgr_S3 : public RGWRESTMgr {
private:
  const bool enable_s3website;
  const bool enable_sts;
  const bool enable_iam;
  const bool enable_pubsub;
public:
  explicit RGWRESTMgr_S3(bool _enable_s3website=false, bool _enable_sts=false, bool _enable_iam=false, bool _enable_pubsub=false)
    : enable_s3website(_enable_s3website),
      enable_sts(_enable_sts),
      enable_iam(_enable_iam),
      enable_pubsub(_enable_pubsub) {
  }

  ~RGWRESTMgr_S3() override = default;

  RGWHandler_REST *get_handler(rgw::sal::RGWRadosStore *store,
			       struct req_state* s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefix) override;
};

class RGWHandler_REST_Obj_S3Website;

static inline bool looks_like_ip_address(const char *bucket)
{
  struct in6_addr a;
  if (inet_pton(AF_INET6, bucket, static_cast<void*>(&a)) == 1) {
    return true;
  }
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

static inline int valid_s3_object_name(const string& name) {
  if (name.size() > 1024) {
    return -ERR_INVALID_OBJECT_NAME;
  }
  if (check_utf8(name.c_str(), name.size())) {
    return -ERR_INVALID_OBJECT_NAME;
  }
  return 0;
}

static inline int valid_s3_bucket_name(const string& name, bool relaxed=false)
{
  // This function enforces Amazon's spec for bucket names.
  // (The requirements, not the recommendations.)
  int len = name.size();
  int max = (relaxed ? 255 : 63);

  if (len < 3) {
    // Name too short
    return -ERR_INVALID_BUCKET_NAME;
  } else if (len > max) {
    // Name too long
    return -ERR_INVALID_BUCKET_NAME;
  }

  // bucket names must start with a number or letter
  if (!(isalpha(name[0]) || isdigit(name[0]))) {
    if (!relaxed)
      return -ERR_INVALID_BUCKET_NAME;
    else if (!(name[0] == '_' || name[0] == '.' || name[0] == '-'))
      return -ERR_INVALID_BUCKET_NAME;
  }

  // bucket names must end with a number or letter
  if (!(isalpha(name[len-1]) || isdigit(name[len-1])))
    if (!relaxed)
      return -ERR_INVALID_BUCKET_NAME;

  for (const char *s = name.c_str(); *s; ++s) {
    char c = *s;
    if (isdigit(c))
      continue;

    if (isalpha(c)) {
      // name cannot contain uppercase letters
      if (relaxed || islower(c))
	continue;
    }

    if (c == '_')
      // name cannot contain underscore
      if (relaxed)
	continue;

    if (c == '-')
      continue;

    if (c == '.') {
      if (!relaxed && s && *s) {
	// name cannot have consecutive periods or dashes
	// adjacent to periods
	// ensure s is neither the first nor the last character
	char p = *(s-1);
	char n = *(s+1);
	if ((p != '-') && (n != '.') && (n != '-'))
	  continue;
      } else {
	continue;
      }
    }

    // Invalid character
    return -ERR_INVALID_BUCKET_NAME;
  }

  if (looks_like_ip_address(name.c_str()))
    return -ERR_INVALID_BUCKET_NAME;

  return 0;
}

namespace s3selectEngine
{
class s3select;
class csv_object;
}

class RGWSelectObj_ObjStore_S3 : public RGWGetObj_ObjStore_S3
{

private:
  std::unique_ptr<s3selectEngine::s3select> s3select_syntax;
  std::string m_s3select_query;
  std::string m_result;
  std::unique_ptr<s3selectEngine::csv_object> m_s3_csv_object;
  std::string m_column_delimiter;
  std::string m_quot;
  std::string m_row_delimiter;
  std::string m_compression_type;
  std::string m_escape_char;
  std::unique_ptr<char[]>  m_buff_header;
  std::string m_header_info;
  std::string m_sql_query;

public:
  unsigned int chunk_number;

  enum header_name_En
  {
    EVENT_TYPE,
    CONTENT_TYPE,
    MESSAGE_TYPE
  };
  static const char* header_name_str[3];

  enum header_value_En
  {
    RECORDS,
    OCTET_STREAM,
    EVENT
  };
  static const char* header_value_str[3];

  RGWSelectObj_ObjStore_S3();
  virtual ~RGWSelectObj_ObjStore_S3();

  virtual int send_response_data(bufferlist& bl, off_t ofs, off_t len) override;

  virtual int get_params() override;

private:
  void encode_short(char* buff, uint16_t s, int& i);

  void encode_int(char* buff, u_int32_t s, int& i);

  int create_header_records(char* buff);

  std::unique_ptr<boost::crc_32_type> crc32;

  int create_message(char* buff, u_int32_t result_len, u_int32_t header_len);

  int run_s3select(const char* query, const char* input, size_t input_length);

  int extract_by_tag(std::string tag_name, std::string& result);

  void convert_escape_seq(std::string& esc);

  int handle_aws_cli_parameters(std::string& sql_query);
};


namespace rgw::auth::s3 {

class AWSEngine : public rgw::auth::Engine {
public:
  class VersionAbstractor {
    static constexpr size_t DIGEST_SIZE_V2 = CEPH_CRYPTO_HMACSHA1_DIGESTSIZE;
    static constexpr size_t DIGEST_SIZE_V4 = CEPH_CRYPTO_HMACSHA256_DIGESTSIZE;

    /* Knowing the signature max size allows us to employ the sstring, and thus
     * avoid dynamic allocations. The multiplier comes from representing digest
     * in the base64-encoded form. */
    static constexpr size_t SIGNATURE_MAX_SIZE = \
      std::max(DIGEST_SIZE_V2, DIGEST_SIZE_V4) * 2 + sizeof('\0');

  public:
    virtual ~VersionAbstractor() {};

    using access_key_id_t = std::string_view;
    using client_signature_t = std::string_view;
    using session_token_t = std::string_view;
    using server_signature_t = basic_sstring<char, uint16_t, SIGNATURE_MAX_SIZE>;
    using string_to_sign_t = std::string;

    /* Transformation for crafting the AWS signature at server side which is
     * used later to compare with the user-provided one. The methodology for
     * doing that depends on AWS auth version. */
    using signature_factory_t = \
      std::function<server_signature_t(CephContext* cct,
                                       const std::string& secret_key,
                                       const string_to_sign_t& string_to_sign)>;

    /* Return an instance of Completer for verifying the payload's fingerprint
     * if necessary. Otherwise caller gets nullptr. Caller may provide secret
     * key */
    using completer_factory_t = \
      std::function<rgw::auth::Completer::cmplptr_t(
        const boost::optional<std::string>& secret_key)>;

    struct auth_data_t {
      access_key_id_t access_key_id;
      client_signature_t client_signature;
      session_token_t session_token;
      string_to_sign_t string_to_sign;
      signature_factory_t signature_factory;
      completer_factory_t completer_factory;
    };

    virtual auth_data_t get_auth_data(const req_state* s) const = 0;
  };

protected:
  CephContext* cct;
  const VersionAbstractor& ver_abstractor;

  AWSEngine(CephContext* const cct, const VersionAbstractor& ver_abstractor)
    : cct(cct),
      ver_abstractor(ver_abstractor) {
  }

  using result_t = rgw::auth::Engine::result_t;
  using string_to_sign_t = VersionAbstractor::string_to_sign_t;
  using signature_factory_t = VersionAbstractor::signature_factory_t;
  using completer_factory_t = VersionAbstractor::completer_factory_t;

  /* TODO(rzarzynski): clean up. We've too many input parameter hee. Also
   * the signature get_auth_data() of VersionAbstractor is too complicated.
   * Replace these thing with a simple, dedicated structure. */
  virtual result_t authenticate(const DoutPrefixProvider* dpp,
                                const std::string_view& access_key_id,
                                const std::string_view& signature,
                                const std::string_view& session_token,
                                const string_to_sign_t& string_to_sign,
                                const signature_factory_t& signature_factory,
                                const completer_factory_t& completer_factory,
                                const req_state* s) const = 0;

public:
  result_t authenticate(const DoutPrefixProvider* dpp, const req_state* const s) const final;
};


class AWSGeneralAbstractor : public AWSEngine::VersionAbstractor {
  CephContext* const cct;

  virtual boost::optional<std::string>
  get_v4_canonical_headers(const req_info& info,
                           const std::string_view& signedheaders,
                           const bool using_qs) const;

  auth_data_t get_auth_data_v2(const req_state* s) const;
  auth_data_t get_auth_data_v4(const req_state* s, const bool using_qs) const;

public:
  explicit AWSGeneralAbstractor(CephContext* const cct)
    : cct(cct) {
  }

  auth_data_t get_auth_data(const req_state* s) const override;
};

class AWSGeneralBoto2Abstractor : public AWSGeneralAbstractor {
  boost::optional<std::string>
  get_v4_canonical_headers(const req_info& info,
                           const std::string_view& signedheaders,
                           const bool using_qs) const override;

public:
  using AWSGeneralAbstractor::AWSGeneralAbstractor;
};

class AWSBrowserUploadAbstractor : public AWSEngine::VersionAbstractor {
  static std::string to_string(ceph::bufferlist bl) {
    return std::string(bl.c_str(),
                       static_cast<std::string::size_type>(bl.length()));
  }

  auth_data_t get_auth_data_v2(const req_state* s) const;
  auth_data_t get_auth_data_v4(const req_state* s) const;

public:
  explicit AWSBrowserUploadAbstractor(CephContext*) {
  }

  auth_data_t get_auth_data(const req_state* s) const override;
};


class LDAPEngine : public AWSEngine {
  static rgw::LDAPHelper* ldh;
  static std::mutex mtx;

  static void init(CephContext* const cct);

  using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;
  using auth_info_t = rgw::auth::RemoteApplier::AuthInfo;
  using result_t = rgw::auth::Engine::result_t;

protected:
  RGWCtl* const ctl;
  const rgw::auth::RemoteApplier::Factory* const apl_factory;

  acl_strategy_t get_acl_strategy() const;
  auth_info_t get_creds_info(const rgw::RGWToken& token) const noexcept;

  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string_view& access_key_id,
                        const std::string_view& signature,
                        const std::string_view& session_token,
                        const string_to_sign_t& string_to_sign,
                        const signature_factory_t&,
                        const completer_factory_t& completer_factory,
                        const req_state* s) const override;
public:
  LDAPEngine(CephContext* const cct,
             RGWCtl* const ctl,
             const VersionAbstractor& ver_abstractor,
             const rgw::auth::RemoteApplier::Factory* const apl_factory)
    : AWSEngine(cct, ver_abstractor),
      ctl(ctl),
      apl_factory(apl_factory) {
    init(cct);
  }

  using AWSEngine::authenticate;

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::LDAPEngine";
  }

  static bool valid();
  static void shutdown();
};

class LocalEngine : public AWSEngine {
  RGWCtl* const ctl;
  const rgw::auth::LocalApplier::Factory* const apl_factory;

  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string_view& access_key_id,
                        const std::string_view& signature,
                        const std::string_view& session_token,
                        const string_to_sign_t& string_to_sign,
                        const signature_factory_t& signature_factory,
                        const completer_factory_t& completer_factory,
                        const req_state* s) const override;
public:
  LocalEngine(CephContext* const cct,
              RGWCtl* const ctl,
              const VersionAbstractor& ver_abstractor,
              const rgw::auth::LocalApplier::Factory* const apl_factory)
    : AWSEngine(cct, ver_abstractor),
      ctl(ctl),
      apl_factory(apl_factory) {
  }

  using AWSEngine::authenticate;

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::LocalEngine";
  }
};

class STSEngine : public AWSEngine {
  RGWCtl* const ctl;
  const rgw::auth::LocalApplier::Factory* const local_apl_factory;
  const rgw::auth::RemoteApplier::Factory* const remote_apl_factory;
  const rgw::auth::RoleApplier::Factory* const role_apl_factory;

  using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;
  using auth_info_t = rgw::auth::RemoteApplier::AuthInfo;

  acl_strategy_t get_acl_strategy() const { return nullptr; };
  auth_info_t get_creds_info(const STS::SessionToken& token) const noexcept;

  int get_session_token(const DoutPrefixProvider* dpp, const std::string_view& session_token,
                        STS::SessionToken& token) const;

  result_t authenticate(const DoutPrefixProvider* dpp, 
                        const std::string_view& access_key_id,
                        const std::string_view& signature,
                        const std::string_view& session_token,
                        const string_to_sign_t& string_to_sign,
                        const signature_factory_t& signature_factory,
                        const completer_factory_t& completer_factory,
                        const req_state* s) const override;
public:
  STSEngine(CephContext* const cct,
              RGWCtl* const ctl,
              const VersionAbstractor& ver_abstractor,
              const rgw::auth::LocalApplier::Factory* const local_apl_factory,
              const rgw::auth::RemoteApplier::Factory* const remote_apl_factory,
              const rgw::auth::RoleApplier::Factory* const role_apl_factory)
    : AWSEngine(cct, ver_abstractor),
      ctl(ctl),
      local_apl_factory(local_apl_factory),
      remote_apl_factory(remote_apl_factory),
      role_apl_factory(role_apl_factory) {
  }

  using AWSEngine::authenticate;

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::STSEngine";
  }
};

class S3AnonymousEngine : public rgw::auth::AnonymousEngine {
  bool is_applicable(const req_state* s) const noexcept override;

public:
  /* Let's reuse the parent class' constructor. */
  using rgw::auth::AnonymousEngine::AnonymousEngine;

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::S3AnonymousEngine";
  }
};


} // namespace rgw::auth::s3
