
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal.h"
#include "rgw_rados.h"
#include "rgw_notify.h"
#include "rgw_oidc_provider.h"
#include "rgw_role.h"
#include "rgw_multi.h"
#include "rgw_putobj_processor.h"
#include "services/svc_tier_rados.h"
#include "cls/lock/cls_lock_client.h"

namespace rgw { namespace sal {

class RadosStore;
class RadosMultipartUpload;

class RadosCompletions : public Completions {
  public:
    std::list<librados::AioCompletion*> handles;
    RadosCompletions() {}
    ~RadosCompletions() = default;
    virtual int drain() override;
};

class RadosUser : public User {
  private:
    RadosStore* store;

  public:
    RadosUser(RadosStore *_st, const rgw_user& _u) : User(_u), store(_st) { }
    RadosUser(RadosStore *_st, const RGWUserInfo& _i) : User(_i), store(_st) { }
    RadosUser(RadosStore *_st) : store(_st) { }
    RadosUser(RadosUser& _o) = default;
    RadosUser() {}

    virtual std::unique_ptr<User> clone() override {
      return std::unique_ptr<User>(new RadosUser(*this));
    }
    int list_buckets(const DoutPrefixProvider* dpp, const std::string& marker, const std::string& end_marker,
		     uint64_t max, bool need_stats, BucketList& buckets,
		     optional_yield y) override;
    virtual int create_bucket(const DoutPrefixProvider* dpp,
                            const rgw_bucket& b,
                            const std::string& zonegroup_id,
                            rgw_placement_rule& placement_rule,
                            std::string& swift_ver_location,
                            const RGWQuotaInfo * pquota_info,
                            const RGWAccessControlPolicy& policy,
			    Attrs& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
			    bool exclusive,
			    bool obj_lock_enabled,
			    bool* existed,
			    req_info& req_info,
			    std::unique_ptr<Bucket>* bucket,
			    optional_yield y) override;
    virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) override;
    virtual int read_stats(const DoutPrefixProvider *dpp,
                           optional_yield y, RGWStorageStats* stats,
			   ceph::real_time* last_stats_sync = nullptr,
			   ceph::real_time* last_stats_update = nullptr) override;
    virtual int read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb) override;
    virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;

    virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) override;
    virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;

    friend class RadosBucket;
};

class RadosObject : public Object {
  private:
    RadosStore* store;
    RGWAccessControlPolicy acls;

  public:

    struct RadosReadOp : public ReadOp {
    private:
      RadosObject* source;
      RGWObjectCtx* rctx;
      RGWRados::Object op_target;
      RGWRados::Object::Read parent_op;

    public:
      RadosReadOp(RadosObject *_source, RGWObjectCtx *_rctx);

      virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
      virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp) override;
      virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y) override;
      virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) override;
    };

    struct RadosDeleteOp : public DeleteOp {
    private:
      RadosObject* source;
      RGWObjectCtx* rctx;
      RGWRados::Object op_target;
      RGWRados::Object::Delete parent_op;

    public:
      RadosDeleteOp(RadosObject* _source, RGWObjectCtx* _rctx);

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
    };

    RadosObject() = default;

    RadosObject(RadosStore *_st, const rgw_obj_key& _k)
      : Object(_k),
	store(_st),
        acls() {
    }
    RadosObject(RadosStore *_st, const rgw_obj_key& _k, Bucket* _b)
      : Object(_k, _b),
	store(_st),
        acls() {
    }
    RadosObject(RadosObject& _o) = default;

    virtual ~RadosObject();

    virtual int delete_object(const DoutPrefixProvider* dpp, RGWObjectCtx* obj_ctx,
			      optional_yield y, bool prevent_versioning) override;
    virtual int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
			       bool keep_index_consistent, optional_yield y) override;
    virtual int copy_object(RGWObjectCtx& obj_ctx, User* user,
               req_info* info, const rgw_zone_id& source_zone,
               rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
               rgw::sal::Bucket* src_bucket,
               const rgw_placement_rule& dest_placement,
               ceph::real_time* src_mtime, ceph::real_time* mtime,
               const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
               bool high_precision_time,
               const char* if_match, const char* if_nomatch,
               AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
               RGWObjCategory category, uint64_t olh_epoch,
	       boost::optional<ceph::real_time> delete_at,
               std::string* version_id, std::string* tag, std::string* etag,
               void (*progress_cb)(off_t, void *), void* progress_data,
               const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
    virtual int set_acl(const RGWAccessControlPolicy& acl) override { acls = acl; return 0; }
    virtual void set_atomic(RGWObjectCtx* rctx) const override;
    virtual void set_prefetch_data(RGWObjectCtx* rctx) override;
    virtual void set_compressed(RGWObjectCtx* rctx) override;

    virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, RGWObjState **state, optional_yield y, bool follow_olh = true) override;
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Attrs* setattrs, Attrs* delattrs, optional_yield y, rgw_obj* target_obj = NULL) override;
    virtual int get_obj_attrs(RGWObjectCtx* rctx, optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) override;
    virtual int modify_obj_attrs(RGWObjectCtx* rctx, const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, const char* attr_name, optional_yield y) override;
    virtual bool is_expired() override;
    virtual void gen_rand_obj_instance_name() override;
    void get_raw_obj(rgw_raw_obj* raw_obj);
    virtual std::unique_ptr<Object> clone() override {
      return std::unique_ptr<Object>(new RadosObject(*this));
    }
    virtual MPSerializer* get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name) override;
    virtual int transition(RGWObjectCtx& rctx,
			   Bucket* bucket,
			   const rgw_placement_rule& placement_rule,
			   const real_time& mtime,
			   uint64_t olh_epoch,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) override;
    virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
    virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f, RGWObjectCtx* obj_ctx) override;

    /* Swift versioning */
    virtual int swift_versioning_restore(RGWObjectCtx* obj_ctx,
					 bool& restored,
					 const DoutPrefixProvider* dpp) override;
    virtual int swift_versioning_copy(RGWObjectCtx* obj_ctx,
				      const DoutPrefixProvider* dpp,
				      optional_yield y) override;

    /* OPs */
    virtual std::unique_ptr<ReadOp> get_read_op(RGWObjectCtx *) override;
    virtual std::unique_ptr<DeleteOp> get_delete_op(RGWObjectCtx*) override;

    /* OMAP */
    virtual int omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
			      std::map<std::string, bufferlist> *m,
			      bool* pmore, optional_yield y) override;
    virtual int omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m,
			     optional_yield y) override;
    virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
			      const std::set<std::string>& keys,
			      Attrs* vals) override;
    virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
				    bool must_exist, optional_yield y) override;

    /* Internal to RadosStore */
    int get_max_chunk_size(const DoutPrefixProvider* dpp,
			   rgw_placement_rule placement_rule,
			   uint64_t* max_chunk_size,
			   uint64_t* alignment = nullptr);
    void get_max_aligned_size(uint64_t size, uint64_t alignment, uint64_t* max_size);
    void raw_obj_to_obj(const rgw_raw_obj& raw_obj);

  private:
    int read_attrs(const DoutPrefixProvider* dpp, RGWRados::Object::Read &read_op, optional_yield y, rgw_obj* target_obj = nullptr);
};

class RadosBucket : public Bucket {
  private:
    RadosStore* store;
    RGWAccessControlPolicy acls;

  public:
    RadosBucket(RadosStore *_st)
      : store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, User* _u)
      : Bucket(_u),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const rgw_bucket& _b)
      : Bucket(_b),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const RGWBucketEnt& _e)
      : Bucket(_e),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const RGWBucketInfo& _i)
      : Bucket(_i),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const rgw_bucket& _b, User* _u)
      : Bucket(_b, _u),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const RGWBucketEnt& _e, User* _u)
      : Bucket(_e, _u),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const RGWBucketInfo& _i, User* _u)
      : Bucket(_i, _u),
	store(_st),
        acls() {
    }

    virtual ~RadosBucket();

    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
    virtual int list(const DoutPrefixProvider* dpp, ListParams&, int, ListResults&, optional_yield y) override;
    virtual int remove_bucket(const DoutPrefixProvider* dpp, bool delete_children, bool forward_to_master, req_info* req_info, optional_yield y) override;
    virtual int remove_bucket_bypass_gc(int concurrent_max, bool
					keep_index_consistent,
					optional_yield y, const
					DoutPrefixProvider *dpp) override;
    virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
    virtual int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl, optional_yield y) override;
    virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y, bool get_stats = false) override;
    virtual int read_stats(const DoutPrefixProvider *dpp, int shard_id,
				 std::string* bucket_ver, std::string* master_ver,
				 std::map<RGWObjCategory, RGWStorageStats>& stats,
				 std::string* max_marker = nullptr,
				 bool* syncstopped = nullptr) override;
    virtual int read_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB* ctx) override;
    virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int update_container_stats(const DoutPrefixProvider* dpp) override;
    virtual int check_bucket_shards(const DoutPrefixProvider* dpp) override;
    virtual int chown(const DoutPrefixProvider* dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker = nullptr) override;
    virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime) override;
    virtual bool is_owner(User* user) override;
    virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
    virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& attrs, optional_yield y) override;
    virtual int try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime) override;
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;
    virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) override;
    virtual int check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
    virtual int rebuild_index(const DoutPrefixProvider *dpp) override;
    virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) override;
    virtual int purge_instance(const DoutPrefixProvider* dpp) override;
    virtual std::unique_ptr<Bucket> clone() override {
      return std::make_unique<RadosBucket>(*this);
    }
    virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) override;
    virtual int list_multiparts(const DoutPrefixProvider *dpp,
				const std::string& prefix,
				std::string& marker,
				const std::string& delim,
				const int& max_uploads,
				std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				std::map<std::string, bool> *common_prefixes,
				bool *is_truncated) override;
    virtual int abort_multiparts(const DoutPrefixProvider* dpp,
				 CephContext* cct) override;

  private:
    int link(const DoutPrefixProvider* dpp, User* new_user, optional_yield y, bool update_entrypoint = true, RGWObjVersionTracker* objv = nullptr);
    int unlink(const DoutPrefixProvider* dpp, User* new_user, optional_yield y, bool update_entrypoint = true);
    friend class RadosUser;
};

class RadosZone : public Zone {
  protected:
    RadosStore* store;
  public:
    RadosZone(RadosStore* _store) : store(_store) {}
    ~RadosZone() = default;

    virtual const RGWZoneGroup& get_zonegroup() override;
    virtual int get_zonegroup(const std::string& id, RGWZoneGroup& zonegroup) override;
    virtual const RGWZoneParams& get_params() override;
    virtual const rgw_zone_id& get_id() override;
    virtual const RGWRealm& get_realm() override;
    virtual const std::string& get_name() const override;
    virtual bool is_writeable() override;
    virtual bool get_redirect_endpoint(std::string* endpoint) override;
    virtual bool has_zonegroup_api(const std::string& api) const override;
    virtual const std::string& get_current_period_id() override;
};

class RadosStore : public Store {
  private:
    RGWRados* rados;
    RGWUserCtl* user_ctl;
    std::string luarocks_path;
    RadosZone zone;

  public:
    RadosStore()
      : rados(nullptr), zone(this) {
      }
    ~RadosStore() {
      delete rados;
    }

    virtual const char* get_name() const override {
			return "rados";
	}

    virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
    virtual std::string get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y) override;
    virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) override;
    virtual int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) override;
    virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) override;
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
    virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
    virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
    virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string&name, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
    virtual bool is_meta_master() override;
    virtual int forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version* objv,
					  bufferlist& in_data, JSONParser* jp, req_info& info,
					  optional_yield y) override;
    virtual Zone* get_zone() { return &zone; }
    virtual std::string zone_unique_id(uint64_t unique_num) override;
    virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
    virtual int cluster_stat(RGWClusterStat& stats) override;
    virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
    virtual std::unique_ptr<Completions> get_completions(void) override;

    // op variant
    virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s, rgw::notify::EventType event_type, const std::string* object_name=nullptr) override;

    // non-op variant (e.g., rgwlc)
    virtual std::unique_ptr<Notification> get_notification(const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, RGWObjectCtx* rctx, rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant, std::string& _req_id, optional_yield y) override;
    virtual RGWLC* get_rgwlc(void) override { return rados->get_lc(); }
    virtual RGWCoroutinesManagerRegistry* get_cr_registry() override { return rados->get_cr_registry(); }

    virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info) override;
    virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) override;
    virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
				const std::map<std::string, std::string>& meta) override;
    virtual void get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota) override;
    virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit) override;
    virtual int set_buckets_enabled(const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets, bool enabled) override;
    virtual uint64_t get_new_req_id() override { return rados->get_new_req_id(); }
    virtual int get_sync_policy_handler(const DoutPrefixProvider* dpp,
					std::optional<rgw_zone_id> zone,
					std::optional<rgw_bucket> bucket,
					RGWBucketSyncPolicyHandlerRef* phandler,
					optional_yield y) override;
    virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
    virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) override { rados->wakeup_meta_sync_shards(shard_ids); }
    virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, std::map<int, std::set<std::string> >& shard_ids) override { rados->wakeup_data_sync_shards(dpp, source_zone, shard_ids); }
    virtual int clear_usage(const DoutPrefixProvider *dpp) override { return rados->clear_usage(dpp); }
    virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool* is_truncated,
			       RGWUsageIter& usage_iter,
			       std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
    virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;
    virtual int get_config_key_val(std::string name, bufferlist* bl) override;
    virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle) override;
    virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, std::list<std::string>& keys, bool* truncated) override;
    virtual void meta_list_keys_complete(void* handle) override;
    virtual std::string meta_get_marker(void* handle) override;
    virtual int list_users(const DoutPrefixProvider* dpp, const std::string& metadata_key,
                          std::string& marker, int max_entries, void *&handle,
                          bool* truncated, std::list<std::string>& users) override;
    virtual int meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y) override;
    virtual const RGWSyncModuleInstanceRef& get_sync_module() { return rados->get_sync_module(); }
    virtual std::string get_host_id() { return rados->host_id; }
    virtual std::unique_ptr<LuaScriptManager> get_lua_script_manager() override;
    virtual std::unique_ptr<RGWRole> get_role(std::string name,
					      std::string tenant,
					      std::string path="",
					      std::string trust_policy="",
					      std::string max_session_duration_str="",
                std::multimap<std::string,std::string> tags={}) override;
    virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
    virtual int get_roles(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  const std::string& path_prefix,
			  const std::string& tenant,
			  std::vector<std::unique_ptr<RGWRole>>& roles) override;
    virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;
    virtual int get_oidc_providers(const DoutPrefixProvider *dpp,
				   const std::string& tenant,
				   std::vector<std::unique_ptr<RGWOIDCProvider>>& providers) override;
    virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) override;
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;

    virtual void finalize(void) override;

    virtual CephContext* ctx(void) override { return rados->ctx(); }

    virtual const std::string& get_luarocks_path() const override {
      return luarocks_path;
    }

    virtual void set_luarocks_path(const std::string& path) override {
      luarocks_path = path;
    }

    /* Unique to RadosStore */
    int get_obj_head_ioctx(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
			   librados::IoCtx* ioctx);
    int delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj);
    int delete_raw_obj_aio(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, Completions* aio);
    void get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj);
    int get_raw_chunk_size(const DoutPrefixProvider* dpp, const rgw_raw_obj& obj, uint64_t* chunk_size);

    void setRados(RGWRados * st) { rados = st; }
    RGWRados* getRados(void) { return rados; }

    RGWServices* svc() { return &rados->svc; }
    const RGWServices* svc() const { return &rados->svc; }
    RGWCtl* ctl() { return &rados->ctl; }
    const RGWCtl* ctl() const { return &rados->ctl; }

    void setUserCtl(RGWUserCtl *_ctl) { user_ctl = _ctl; }
};

class RadosMultipartPart : public MultipartPart {
protected:
  RGWUploadPartInfo info;

public:
  RadosMultipartPart() = default;
  virtual ~RadosMultipartPart() = default;

  virtual uint32_t get_num() { return info.num; }
  virtual uint64_t get_size() { return info.accounted_size; }
  virtual const std::string& get_etag() { return info.etag; }
  virtual ceph::real_time& get_mtime() { return info.modified; }

  /* For RadosStore code */
  RGWObjManifest& get_manifest() { return info.manifest; }

  friend class RadosMultipartUpload;
};

class RadosMultipartUpload : public MultipartUpload {
  RadosStore* store;
  RGWMPObj mp_obj;
  ACLOwner owner;
  ceph::real_time mtime;
  rgw_placement_rule placement;
  RGWObjManifest manifest;

public:
  RadosMultipartUpload(RadosStore* _store, Bucket* _bucket, const std::string& oid,
                       std::optional<std::string> upload_id, ACLOwner owner,
                       ceph::real_time _mtime)
      : MultipartUpload(_bucket), store(_store), mp_obj(oid, upload_id),
        owner(owner), mtime(_mtime) {}
  virtual ~RadosMultipartUpload() = default;

  virtual const std::string& get_meta() const override { return mp_obj.get_meta(); }
  virtual const std::string& get_key() const override { return mp_obj.get_key(); }
  virtual const std::string& get_upload_id() const override { return mp_obj.get_upload_id(); }
  virtual const ACLOwner& get_owner() const override { return owner; }
  virtual ceph::real_time& get_mtime() override { return mtime; }
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() override;
  virtual int init(const DoutPrefixProvider* dpp, optional_yield y, RGWObjectCtx* obj_ctx, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) override;
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated,
			 bool assume_unsorted = false) override;
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct,
		    RGWObjectCtx* obj_ctx) override;
  virtual int complete(const DoutPrefixProvider* dpp,
		       optional_yield y, CephContext* cct,
		       std::map<int, std::string>& part_etags,
		       std::list<rgw_obj_index_key>& remove_objs,
		       uint64_t& accounted_size, bool& compressed,
		       RGWCompressionInfo& cs_info, off_t& ofs,
		       std::string& tag, ACLOwner& owner,
		       uint64_t olh_epoch,
		       rgw::sal::Object* target_obj,
		       RGWObjectCtx* obj_ctx) override;
  virtual int get_info(const DoutPrefixProvider *dpp, optional_yield y, RGWObjectCtx* obj_ctx, rgw_placement_rule** rule, rgw::sal::Attrs* attrs = nullptr) override;
  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  std::unique_ptr<rgw::sal::Object> _head_obj,
			  const rgw_user& owner, RGWObjectCtx& obj_ctx,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) override;
};

class MPRadosSerializer : public MPSerializer {
  librados::IoCtx ioctx;
  rados::cls::lock::Lock lock;
  librados::ObjectWriteOperation op;

public:
  MPRadosSerializer(const DoutPrefixProvider *dpp, RadosStore* store, RadosObject* obj, const std::string& lock_name);

  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override;
  virtual int unlock() override {
    return lock.unlock(&ioctx, oid);
  }
};

class LCRadosSerializer : public LCSerializer {
  librados::IoCtx* ioctx;
  rados::cls::lock::Lock lock;
  const std::string oid;

public:
  LCRadosSerializer(RadosStore* store, const std::string& oid, const std::string& lock_name, const std::string& cookie);

  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override;
  virtual int unlock() override {
    return lock.unlock(ioctx, oid);
  }
};

class RadosLifecycle : public Lifecycle {
  RadosStore* store;

public:
  RadosLifecycle(RadosStore* _st) : store(_st) {}

  virtual int get_entry(const std::string& oid, const std::string& marker, LCEntry& entry) override;
  virtual int get_next_entry(const std::string& oid, std::string& marker, LCEntry& entry) override;
  virtual int set_entry(const std::string& oid, const LCEntry& entry) override;
  virtual int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries, std::vector<LCEntry>& entries) override;
  virtual int rm_entry(const std::string& oid, const LCEntry& entry) override;
  virtual int get_head(const std::string& oid, LCHead& head) override;
  virtual int put_head(const std::string& oid, const LCHead& head) override;
  virtual LCSerializer* get_serializer(const std::string& lock_name, const std::string& oid, const std::string& cookie) override;
};

class RadosNotification : public Notification {
  RadosStore* store;
  /* XXX it feels incorrect to me that rgw::notify::reservation_t is
   * currently RADOS-specific; instead, I think notification types such as
   * reservation_t should be generally visible, whereas the internal
   * notification behavior should be made portable (e.g., notification
   * to non-RADOS message sinks) */
  rgw::notify::reservation_t res;

  public:
    RadosNotification(const DoutPrefixProvider* _dpp, RadosStore* _store, Object* _obj, Object* _src_obj, req_state* _s, rgw::notify::EventType _type, const std::string* object_name=nullptr) :
      Notification(_obj, _src_obj, _type), store(_store), res(_dpp, _store, _s, _obj, _src_obj, object_name) { }

    RadosNotification(const DoutPrefixProvider* _dpp, RadosStore* _store, Object* _obj, Object* _src_obj, RGWObjectCtx* rctx, rgw::notify::EventType _type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant, std::string& _req_id, optional_yield y) :
      Notification(_obj, _src_obj, _type), store(_store), res(_dpp, _store, rctx, _obj, _src_obj, _bucket, _user_id, _user_tenant, _req_id, y) {}

    ~RadosNotification() = default;

    rgw::notify::reservation_t& get_reservation(void) {
      return res;
    }

    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) override;
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const std::string& etag, const std::string& version) override;
};

class RadosAtomicWriter : public Writer {
protected:
  rgw::sal::RadosStore* store;
  std::unique_ptr<Aio> aio;
  rgw::putobj::AtomicObjectProcessor processor;

public:
  RadosAtomicWriter(const DoutPrefixProvider *dpp,
		    optional_yield y,
		    std::unique_ptr<rgw::sal::Object> _head_obj,
		    RadosStore* _store, std::unique_ptr<Aio> _aio,
		    const rgw_user& owner, RGWObjectCtx& obj_ctx,
		    const rgw_placement_rule *ptail_placement_rule,
		    uint64_t olh_epoch,
		    const std::string& unique_tag) :
			Writer(dpp, y),
			store(_store),
			aio(std::move(_aio)),
			processor(&*aio, store,
				  ptail_placement_rule, owner, obj_ctx,
				  std::move(_head_obj), olh_epoch, unique_tag,
				  dpp, y)
  {}
  ~RadosAtomicWriter() = default;

  // prepare to start processing object data
  virtual int prepare(optional_yield y) override;

  // Process a bufferlist
  virtual int process(bufferlist&& data, uint64_t offset) override;

  // complete the operation and make its result visible to clients
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) override;
};

class RadosAppendWriter : public Writer {
protected:
  rgw::sal::RadosStore* store;
  std::unique_ptr<Aio> aio;
  rgw::putobj::AppendObjectProcessor processor;

public:
  RadosAppendWriter(const DoutPrefixProvider *dpp,
		    optional_yield y,
		    std::unique_ptr<rgw::sal::Object> _head_obj,
		    RadosStore* _store, std::unique_ptr<Aio> _aio,
		    const rgw_user& owner, RGWObjectCtx& obj_ctx,
		    const rgw_placement_rule *ptail_placement_rule,
		    const std::string& unique_tag,
		    uint64_t position,
		    uint64_t *cur_accounted_size) :
				  Writer(dpp, y),
				  store(_store),
				  aio(std::move(_aio)),
				  processor(&*aio, store,
					    ptail_placement_rule, owner, obj_ctx,
					    std::move(_head_obj), unique_tag, position,
					    cur_accounted_size, dpp, y)
  {}
  ~RadosAppendWriter() = default;

  // prepare to start processing object data
  virtual int prepare(optional_yield y) override;

  // Process a bufferlist
  virtual int process(bufferlist&& data, uint64_t offset) override;

  // complete the operation and make its result visible to clients
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) override;
};

class RadosMultipartWriter : public Writer {
protected:
  rgw::sal::RadosStore* store;
  std::unique_ptr<Aio> aio;
  rgw::putobj::MultipartObjectProcessor processor;

public:
  RadosMultipartWriter(const DoutPrefixProvider *dpp,
		       optional_yield y, MultipartUpload* upload,
		       std::unique_ptr<rgw::sal::Object> _head_obj,
		       RadosStore* _store, std::unique_ptr<Aio> _aio,
		       const rgw_user& owner, RGWObjectCtx& obj_ctx,
		       const rgw_placement_rule *ptail_placement_rule,
		       uint64_t part_num, const std::string& part_num_str) :
				  Writer(dpp, y),
				  store(_store),
				  aio(std::move(_aio)),
				  processor(&*aio, store,
					    ptail_placement_rule, owner, obj_ctx,
					    std::move(_head_obj), upload->get_upload_id(),
					    part_num, part_num_str, dpp, y)
  {}
  ~RadosMultipartWriter() = default;

  // prepare to start processing object data
  virtual int prepare(optional_yield y) override;

  // Process a bufferlist
  virtual int process(bufferlist&& data, uint64_t offset) override;

  // complete the operation and make its result visible to clients
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) override;
};

class RadosLuaScriptManager : public LuaScriptManager {
  RadosStore* store;
  rgw_pool pool;

public:
  RadosLuaScriptManager(RadosStore* _s) : store(_s)
  {
    pool = store->get_zone()->get_params().log_pool;
  }
  virtual ~RadosLuaScriptManager() = default;

  virtual int get(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) override;
  virtual int put(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) override;
  virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) override;
};

class RadosOIDCProvider : public RGWOIDCProvider {
  RadosStore* store;
public:
  RadosOIDCProvider(RadosStore* _store) : store(_store) {}
  ~RadosOIDCProvider() = default;

  virtual int store_url(const DoutPrefixProvider *dpp, const std::string& url, bool exclusive, optional_yield y) override;
  virtual int read_url(const DoutPrefixProvider *dpp, const std::string& url, const std::string& tenant) override;
  virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) override;
  void encode(bufferlist& bl) const {
    RGWOIDCProvider::encode(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    RGWOIDCProvider::decode(bl);
  }
};

class RadosRole : public RGWRole {
  RadosStore* store;
public:
  RadosRole(RadosStore* _store, std::string name,
          std::string tenant,
          std::string path,
          std::string trust_policy,
          std::string max_session_duration,
          std::multimap<std::string,std::string> tags) : RGWRole(name, tenant, path, trust_policy, max_session_duration, tags), store(_store) {}
  RadosRole(RadosStore* _store, std::string id) : RGWRole(id), store(_store) {}
  ~RadosRole() = default;

  virtual int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) override;
  virtual int store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) override;
  virtual int store_path(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) override;
  virtual int read_id(const DoutPrefixProvider *dpp, const std::string& role_name, const std::string& tenant, std::string& role_id, optional_yield y) override;
  virtual int read_name(const DoutPrefixProvider *dpp, optional_yield y) override;
  virtual int read_info(const DoutPrefixProvider *dpp, optional_yield y) override;
  virtual int create(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) override;
  virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) override;
};

} } // namespace rgw::sal

WRITE_CLASS_ENCODER(rgw::sal::RadosOIDCProvider)
