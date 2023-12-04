// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#include "rgw_sal_store.h"
#include "rgw_rados.h"
#include "rgw_notify.h"
#include "rgw_oidc_provider.h"
#include "rgw_role.h"
#include "rgw_multi.h"
#include "rgw_putobj_processor.h"
#include "services/svc_tier_rados.h"
#include "cls/lock/cls_lock_client.h"

namespace rgw { namespace sal {

class RadosMultipartUpload;

class RadosCompletions : public Completions {
  public:
    std::list<librados::AioCompletion*> handles;
    RadosCompletions() {}
    ~RadosCompletions() = default;
    virtual int drain() override;
};

class RadosPlacementTier: public StorePlacementTier {
  RadosStore* store;
  RGWZoneGroupPlacementTier tier;
public:
  RadosPlacementTier(RadosStore* _store, const RGWZoneGroupPlacementTier& _tier) : store(_store), tier(_tier) {}
  virtual ~RadosPlacementTier() = default;

  virtual const std::string& get_tier_type() { return tier.tier_type; }
  virtual const std::string& get_storage_class() { return tier.storage_class; }
  virtual bool retain_head_object() { return tier.retain_head_object; }
  RGWZoneGroupPlacementTier& get_rt() { return tier; }
};

class RadosZoneGroup : public StoreZoneGroup {
  RadosStore* store;
  const RGWZoneGroup group;
  std::string empty;
public:
  RadosZoneGroup(RadosStore* _store, const RGWZoneGroup& _group) : store(_store), group(_group) {}
  virtual ~RadosZoneGroup() = default;

  virtual const std::string& get_id() const override { return group.get_id(); };
  virtual const std::string& get_name() const override { return group.get_name(); };
  virtual int equals(const std::string& other_zonegroup) const override {
    return group.equals(other_zonegroup);
  };
  /** Get the endpoint from zonegroup, or from master zone if not set */
  virtual const std::string& get_endpoint() const override;
  virtual bool placement_target_exists(std::string& target) const override;
  virtual bool is_master_zonegroup() const override {
    return group.is_master_zonegroup();
  };
  virtual const std::string& get_api_name() const override { return group.api_name; };
  virtual void get_placement_target_names(std::set<std::string>& names) const override;
  virtual const std::string& get_default_placement_name() const override {
    return group.default_placement.name; };
  virtual int get_hostnames(std::list<std::string>& names) const override {
    names = group.hostnames;
    return 0;
  };
  virtual int get_s3website_hostnames(std::list<std::string>& names) const override {
    names = group.hostnames_s3website;
    return 0;
  };
  virtual int get_zone_count() const override {
    return group.zones.size();
  }
  virtual int get_placement_tier(const rgw_placement_rule& rule, std::unique_ptr<PlacementTier>* tier);
  virtual int get_zone_by_id(const std::string& id, std::unique_ptr<Zone>* zone) override;
  virtual int get_zone_by_name(const std::string& name, std::unique_ptr<Zone>* zone) override;
  virtual int list_zones(std::list<std::string>& zone_ids) override;
  bool supports(std::string_view feature) const override {
    return group.supports(feature);
  }
  virtual std::unique_ptr<ZoneGroup> clone() override {
    return std::make_unique<RadosZoneGroup>(store, group);
  }
  const RGWZoneGroup& get_group() const { return group; }
};

class RadosZone : public StoreZone {
  protected:
    RadosStore* store;
    std::unique_ptr<ZoneGroup> group;
    RGWZone rgw_zone;
    bool local_zone{false};
  public:
    RadosZone(RadosStore* _store, std::unique_ptr<ZoneGroup> _zg) : store(_store), group(std::move(_zg)), local_zone(true) {}
    RadosZone(RadosStore* _store, std::unique_ptr<ZoneGroup> _zg, RGWZone& z) : store(_store), group(std::move(_zg)), rgw_zone(z) {}
    ~RadosZone() = default;

    virtual std::unique_ptr<Zone> clone() override;
    virtual ZoneGroup& get_zonegroup() override { return *(group.get()); }
    virtual const std::string& get_id() override;
    virtual const std::string& get_name() const override;
    virtual bool is_writeable() override;
    virtual bool get_redirect_endpoint(std::string* endpoint) override;
    virtual bool has_zonegroup_api(const std::string& api) const override;
    virtual const std::string& get_current_period_id() override;
    virtual const RGWAccessKey& get_system_key() override;
    virtual const std::string& get_realm_name() override;
    virtual const std::string& get_realm_id() override;
    virtual const std::string_view get_tier_type() override;
    virtual RGWBucketSyncPolicyHandlerRef get_sync_policy_handler() override;
};

class RadosStore : public StoreDriver {
  private:
    RGWRados* rados;
    RGWUserCtl* user_ctl;
    std::unique_ptr<RadosZone> zone;
    std::string topics_oid(const std::string& tenant) const;

  public:
    RadosStore()
      : rados(nullptr) {
      }
    ~RadosStore() {
      delete rados;
    }

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    virtual const std::string get_name() const override {
      return "rados";
    }
    virtual std::string get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y) override;
    virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
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
    virtual int forward_iam_request_to_master(const DoutPrefixProvider *dpp, const RGWAccessKey& key, obj_version* objv,
					     bufferlist& in_data,
					     RGWXMLDecoder::XMLParser* parser, req_info& info,
					     optional_yield y) override;
    virtual Zone* get_zone() { return zone.get(); }
    virtual std::string zone_unique_id(uint64_t unique_num) override;
    virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
    virtual int get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup) override;
    virtual int list_all_zones(const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids) override;
    virtual int cluster_stat(RGWClusterStat& stats) override;
    virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
    virtual std::unique_ptr<Completions> get_completions(void) override;
    virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s, rgw::notify::EventType event_type, optional_yield y, const std::string* object_name=nullptr) override;
    virtual std::unique_ptr<Notification> get_notification(
    const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, 
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant,
    std::string& _req_id, optional_yield y) override;
    int read_topics(const std::string& tenant, rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) override;
    int write_topics(const std::string& tenant, const rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
	optional_yield y, const DoutPrefixProvider *dpp) override;
    int remove_topics(const std::string& tenant, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) override;
    virtual RGWLC* get_rgwlc(void) override { return rados->get_lc(); }
    virtual RGWCoroutinesManagerRegistry* get_cr_registry() override { return rados->get_cr_registry(); }

    virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info) override;
    virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) override;
    virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
				const std::map<std::string, std::string>& meta) override;
    virtual void get_quota(RGWQuota& quota) override;
    virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit) override;
    virtual int set_buckets_enabled(const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets, bool enabled) override;
    virtual int get_sync_policy_handler(const DoutPrefixProvider* dpp,
					std::optional<rgw_zone_id> zone,
					std::optional<rgw_bucket> bucket,
					RGWBucketSyncPolicyHandlerRef* phandler,
					optional_yield y) override;
    virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
    virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) override { rados->wakeup_meta_sync_shards(shard_ids); }
    virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids) override { rados->wakeup_data_sync_shards(dpp, source_zone, shard_ids); }
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
    virtual int meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y) override;
    virtual const RGWSyncModuleInstanceRef& get_sync_module() { return rados->get_sync_module(); }
    virtual std::string get_host_id() { return rados->host_id; }
    virtual std::unique_ptr<LuaManager> get_lua_manager() override;
    virtual std::unique_ptr<RGWRole> get_role(std::string name,
					      std::string tenant,
					      std::string path="",
					      std::string trust_policy="",
					      std::string max_session_duration_str="",
                std::multimap<std::string,std::string> tags={}) override;
    virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
    virtual std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) override;
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
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) override;
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;
    virtual const std::string& get_compression_type(const rgw_placement_rule& rule) override;
    virtual bool valid_placement(const rgw_placement_rule& rule) override;

    virtual void finalize(void) override;

    virtual CephContext* ctx(void) override { return rados->ctx(); }

    virtual void register_admin_apis(RGWRESTMgr* mgr) override;

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

class RadosUser : public StoreUser {
  private:
    RadosStore* store;

  public:
    RadosUser(RadosStore *_st, const rgw_user& _u) : StoreUser(_u), store(_st) { }
    RadosUser(RadosStore *_st, const RGWUserInfo& _i) : StoreUser(_i), store(_st) { }
    RadosUser(RadosStore *_st) : store(_st) { }
    RadosUser(RadosUser& _o) = default;

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
    virtual int verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider* dpp, optional_yield y) override;

    friend class RadosBucket;
};

class RadosObject : public StoreObject {
  private:
    RadosStore* store;
    RGWAccessControlPolicy acls;
    RGWObjManifest *manifest{nullptr};
    RGWObjectCtx* rados_ctx;
    bool rados_ctx_owned;

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

      /*
       * Both `read` and `iterate` read up through index `end`
       * *inclusive*. The number of bytes that could be returned is
       * `end - ofs + 1`.
       */
      virtual int read(int64_t ofs, int64_t end,
		       bufferlist& bl, optional_yield y,
		       const DoutPrefixProvider* dpp) override;
      virtual int iterate(const DoutPrefixProvider* dpp,
			  int64_t ofs, int64_t end,
			  RGWGetDataCB* cb, optional_yield y) override;

        virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) override;
    };

    struct RadosDeleteOp : public DeleteOp {
    private:
      RadosObject* source;
      RGWRados::Object op_target;
      RGWRados::Object::Delete parent_op;

    public:
      RadosDeleteOp(RadosObject* _source);

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, bool log_op = true) override;
    };

    RadosObject(RadosStore *_st, const rgw_obj_key& _k)
      : StoreObject(_k),
	store(_st),
        acls(),
	rados_ctx(new RGWObjectCtx(dynamic_cast<Driver*>(store))),
	rados_ctx_owned(true) {
    }
    RadosObject(RadosStore *_st, const rgw_obj_key& _k, Bucket* _b)
      : StoreObject(_k, _b),
	store(_st),
        acls(),
	rados_ctx(new RGWObjectCtx(dynamic_cast<Driver*>(store))) ,
	rados_ctx_owned(true) {
    }
    RadosObject(RadosObject& _o) : StoreObject(_o) {
      store = _o.store;
      acls = _o.acls;
      manifest = _o.manifest;
      rados_ctx = _o.rados_ctx;
      rados_ctx_owned = false;
    }

    virtual ~RadosObject();

    virtual void invalidate() override {
      StoreObject::invalidate();
      rados_ctx->invalidate(get_obj());
    }
    virtual int delete_object(const DoutPrefixProvider* dpp,
			      optional_yield y, bool prevent_versioning) override;
    virtual int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
			       bool keep_index_consistent, optional_yield y) override;
    virtual int copy_object(User* user,
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
    virtual void set_atomic() override {
      rados_ctx->set_atomic(state.obj);
      StoreObject::set_atomic();
    }
    virtual void set_prefetch_data() override {
      rados_ctx->set_prefetch_data(state.obj);
      StoreObject::set_prefetch_data();
    }
    virtual void set_compressed() override {
      rados_ctx->set_compressed(state.obj);
      StoreObject::set_compressed();
    }

    virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state, optional_yield y, bool follow_olh = true) override;
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y) override;
    virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) override;
    virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y) override;
    virtual bool is_expired() override;
    virtual void gen_rand_obj_instance_name() override;
    void get_raw_obj(rgw_raw_obj* raw_obj);
    virtual std::unique_ptr<Object> clone() override {
      return std::unique_ptr<Object>(new RadosObject(*this));
    }
    virtual std::unique_ptr<MPSerializer> get_serializer(const DoutPrefixProvider *dpp,
							 const std::string& lock_name) override;
    virtual int transition(Bucket* bucket,
			   const rgw_placement_rule& placement_rule,
			   const real_time& mtime,
			   uint64_t olh_epoch,
			   const DoutPrefixProvider* dpp,
			   optional_yield y,
                           bool log_op = true) override;
    virtual int transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) override;
    virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
    virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f) override;

    /* Swift versioning */
    virtual int swift_versioning_restore(bool& restored,
					 const DoutPrefixProvider* dpp) override;
    virtual int swift_versioning_copy(const DoutPrefixProvider* dpp,
				      optional_yield y) override;

    /* OPs */
    virtual std::unique_ptr<ReadOp> get_read_op() override;
    virtual std::unique_ptr<DeleteOp> get_delete_op() override;

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
    virtual int chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y) override;

    /* Internal to RadosStore */
    int get_max_chunk_size(const DoutPrefixProvider* dpp,
			   rgw_placement_rule placement_rule,
			   uint64_t* max_chunk_size,
			   uint64_t* alignment = nullptr);
    void get_max_aligned_size(uint64_t size, uint64_t alignment, uint64_t* max_size);
    void raw_obj_to_obj(const rgw_raw_obj& raw_obj);
    int write_cloud_tier(const DoutPrefixProvider* dpp,
			   optional_yield y,
			   uint64_t olh_epoch,
			   rgw::sal::PlacementTier* tier,
			   bool is_multipart_upload,
			   rgw_placement_rule& target_placement,
			   Object* head_obj);
    RGWObjManifest* get_manifest() { return manifest; }
    RGWObjectCtx& get_ctx() { return *rados_ctx; }

  private:
    int read_attrs(const DoutPrefixProvider* dpp, RGWRados::Object::Read &read_op, optional_yield y, rgw_obj* target_obj = nullptr);
};

class RadosBucket : public StoreBucket {
  private:
    RadosStore* store;
    RGWAccessControlPolicy acls;
    std::string topics_oid() const;

  public:
    RadosBucket(RadosStore *_st)
      : store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, User* _u)
      : StoreBucket(_u),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const rgw_bucket& _b)
      : StoreBucket(_b),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const RGWBucketEnt& _e)
      : StoreBucket(_e),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const RGWBucketInfo& _i)
      : StoreBucket(_i),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const rgw_bucket& _b, User* _u)
      : StoreBucket(_b, _u),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const RGWBucketEnt& _e, User* _u)
      : StoreBucket(_e, _u),
	store(_st),
        acls() {
    }

    RadosBucket(RadosStore *_st, const RGWBucketInfo& _i, User* _u)
      : StoreBucket(_i, _u),
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
    virtual int read_stats(const DoutPrefixProvider *dpp,
                           const bucket_index_layout_generation& idx_layout,
                           int shard_id, std::string* bucket_ver, std::string* master_ver,
                           std::map<RGWObjCategory, RGWStorageStats>& stats,
                           std::string* max_marker = nullptr,
                           bool* syncstopped = nullptr) override;
    virtual int read_stats_async(const DoutPrefixProvider *dpp,
                                 const bucket_index_layout_generation& idx_layout,
                                 int shard_id, RGWGetBucketStats_CB* ctx) override;
    virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int update_container_stats(const DoutPrefixProvider* dpp) override;
    virtual int check_bucket_shards(const DoutPrefixProvider* dpp) override;
    virtual int chown(const DoutPrefixProvider* dpp, User& new_user, optional_yield y) override;
    virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime) override;
    virtual bool is_owner(User* user) override;
    virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
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
    int read_topics(rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker, 
        optional_yield y, const DoutPrefixProvider *dpp) override;
    int write_topics(const rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker, 
        optional_yield y, const DoutPrefixProvider *dpp) override;
    int remove_topics(RGWObjVersionTracker* objv_tracker, 
        optional_yield y, const DoutPrefixProvider *dpp) override;

  private:
    int link(const DoutPrefixProvider* dpp, User* new_user, optional_yield y, bool update_entrypoint = true, RGWObjVersionTracker* objv = nullptr);
    int unlink(const DoutPrefixProvider* dpp, User* new_user, optional_yield y, bool update_entrypoint = true);
    friend class RadosUser;
};

class RadosMultipartPart : public StoreMultipartPart {
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
  const std::set<std::string>& get_past_prefixes() const { return info.past_prefixes; }

  friend class RadosMultipartUpload;
};

class RadosMultipartUpload : public StoreMultipartUpload {
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
      : StoreMultipartUpload(_bucket), store(_store), mp_obj(oid, upload_id),
        owner(owner), mtime(_mtime) {}
  virtual ~RadosMultipartUpload() = default;

  virtual const std::string& get_meta() const override { return mp_obj.get_meta(); }
  virtual const std::string& get_key() const override { return mp_obj.get_key(); }
  virtual const std::string& get_upload_id() const override { return mp_obj.get_upload_id(); }
  virtual const ACLOwner& get_owner() const override { return owner; }
  virtual ceph::real_time& get_mtime() override { return mtime; }
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() override;
  virtual int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) override;
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated,
			 bool assume_unsorted = false) override;
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct, bool log_op = true) override;
  virtual int complete(const DoutPrefixProvider* dpp,
		       optional_yield y, CephContext* cct,
		       std::map<int, std::string>& part_etags,
		       std::list<rgw_obj_index_key>& remove_objs,
		       uint64_t& accounted_size, bool& compressed,
		       RGWCompressionInfo& cs_info, off_t& ofs,
		       std::string& tag, ACLOwner& owner,
		       uint64_t olh_epoch,
		       rgw::sal::Object* target_obj) override;
  virtual int get_info(const DoutPrefixProvider *dpp, optional_yield y, rgw_placement_rule** rule, rgw::sal::Attrs* attrs = nullptr) override;
  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  rgw::sal::Object* obj,
			  const rgw_user& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) override;
protected:
  int cleanup_part_history(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           RadosMultipartPart* part,
                           std::list<rgw_obj_index_key>& remove_objs);
};

class MPRadosSerializer : public StoreMPSerializer {
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

class LCRadosSerializer : public StoreLCSerializer {
  librados::IoCtx* ioctx;
  rados::cls::lock::Lock lock;

public:
  LCRadosSerializer(RadosStore* store, const std::string& oid, const std::string& lock_name, const std::string& cookie);

  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override;
  virtual int unlock() override {
    return lock.unlock(ioctx, oid);
  }
};

class RadosLifecycle : public StoreLifecycle {
  RadosStore* store;

public:
  RadosLifecycle(RadosStore* _st) : store(_st) {}

  using StoreLifecycle::get_entry;
  virtual int get_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) override;
  virtual int get_next_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) override;
  virtual int set_entry(const std::string& oid, LCEntry& entry) override;
  virtual int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries,
			   std::vector<std::unique_ptr<LCEntry>>& entries) override;
  virtual int rm_entry(const std::string& oid, LCEntry& entry) override;
  virtual int get_head(const std::string& oid, std::unique_ptr<LCHead>* head) override;
  virtual int put_head(const std::string& oid, LCHead& head) override;
  virtual std::unique_ptr<LCSerializer> get_serializer(const std::string& lock_name,
						       const std::string& oid,
						       const std::string& cookie) override;
};

class RadosNotification : public StoreNotification {
  RadosStore* store;
  /* XXX it feels incorrect to me that rgw::notify::reservation_t is
   * currently RADOS-specific; instead, I think notification types such as
   * reservation_t should be generally visible, whereas the internal
   * notification behavior should be made portable (e.g., notification
   * to non-RADOS message sinks) */
  rgw::notify::reservation_t res;

  public:
    RadosNotification(const DoutPrefixProvider* _dpp, RadosStore* _store, Object* _obj, Object* _src_obj, req_state* _s, rgw::notify::EventType _type, optional_yield y, const std::string* object_name) :
      StoreNotification(_obj, _src_obj, _type), store(_store), res(_dpp, _store, _s, _obj, _src_obj, object_name, y) { }

    RadosNotification(const DoutPrefixProvider* _dpp, RadosStore* _store, Object* _obj, Object* _src_obj, rgw::notify::EventType _type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant, std::string& _req_id, optional_yield y) :
      StoreNotification(_obj, _src_obj, _type), store(_store), res(_dpp, _store, _obj, _src_obj, _bucket, _user_id, _user_tenant, _req_id, y) {}

    ~RadosNotification() = default;

    rgw::notify::reservation_t& get_reservation(void) {
      return res;
    }

    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) override;
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const std::string& etag, const std::string& version) override;
};

class RadosAtomicWriter : public StoreWriter {
protected:
  rgw::sal::RadosStore* store;
  std::unique_ptr<Aio> aio;
  RGWObjectCtx& obj_ctx;
  rgw::putobj::AtomicObjectProcessor processor;

public:
  RadosAtomicWriter(const DoutPrefixProvider *dpp,
		    optional_yield y,
		    RGWBucketInfo& bucket_info,
		    RGWObjectCtx& obj_ctx,
		    const rgw_obj& obj,
		    RadosStore* _store, std::unique_ptr<Aio> _aio,
		    const rgw_user& owner,
		    const rgw_placement_rule *ptail_placement_rule,
		    uint64_t olh_epoch,
		    const std::string& unique_tag) :
			StoreWriter(dpp, y),
			store(_store),
			aio(std::move(_aio)),
			obj_ctx(obj_ctx),
			processor(&*aio, store->getRados(), bucket_info,
				  ptail_placement_rule, owner, obj_ctx,
				  obj, olh_epoch, unique_tag,
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
                       optional_yield y,
                       bool log_op = true) override;
};

class RadosAppendWriter : public StoreWriter {
protected:
  rgw::sal::RadosStore* store;
  std::unique_ptr<Aio> aio;
  RGWObjectCtx& obj_ctx;
  rgw::putobj::AppendObjectProcessor processor;

public:
  RadosAppendWriter(const DoutPrefixProvider *dpp,
		    optional_yield y,
		    RGWBucketInfo& bucket_info,
		    RGWObjectCtx& obj_ctx,
		    const rgw_obj& obj,
		    RadosStore* _store, std::unique_ptr<Aio> _aio,
		    const rgw_user& owner,
		    const rgw_placement_rule *ptail_placement_rule,
		    const std::string& unique_tag,
		    uint64_t position,
		    uint64_t *cur_accounted_size) :
			StoreWriter(dpp, y),
			store(_store),
			aio(std::move(_aio)),
			obj_ctx(obj_ctx),
			processor(&*aio, store->getRados(), bucket_info,
				  ptail_placement_rule, owner, obj_ctx,
				  obj, unique_tag, position,
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
                       optional_yield y,
                       bool log_op = true) override;
};

class RadosMultipartWriter : public StoreWriter {
protected:
  rgw::sal::RadosStore* store;
  std::unique_ptr<Aio> aio;
  RGWObjectCtx& obj_ctx;
  rgw::putobj::MultipartObjectProcessor processor;

public:
  RadosMultipartWriter(const DoutPrefixProvider *dpp,
		       optional_yield y, const std::string& upload_id,
		       RGWBucketInfo& bucket_info,
		       RGWObjectCtx& obj_ctx,
		       const rgw_obj& obj,
		       RadosStore* _store, std::unique_ptr<Aio> _aio,
		       const rgw_user& owner,
		       const rgw_placement_rule *ptail_placement_rule,
		       uint64_t part_num, const std::string& part_num_str) :
			StoreWriter(dpp, y),
			store(_store),
			aio(std::move(_aio)),
			obj_ctx(obj_ctx),
			processor(&*aio, store->getRados(), bucket_info,
				  ptail_placement_rule, owner, obj_ctx,
				  obj, upload_id,
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
                       optional_yield y,
                       bool log_op = true) override;
};

class RadosLuaManager : public StoreLuaManager {
  RadosStore* const store;
  rgw_pool pool;

public:
  RadosLuaManager(RadosStore* _s);
  virtual ~RadosLuaManager() = default;

  virtual int get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script);
  virtual int put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script);
  virtual int del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key);
  virtual int add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name);
  virtual int remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name);
  virtual int list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages);
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
  RadosRole(RadosStore* _store, const RGWRoleInfo& info) : RGWRole(info), store(_store) {}
  RadosRole(RadosStore* _store) : store(_store) {}
  ~RadosRole() = default;

  virtual int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) override;
  virtual int store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) override;
  virtual int store_path(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) override;
  virtual int read_id(const DoutPrefixProvider *dpp, const std::string& role_name, const std::string& tenant, std::string& role_id, optional_yield y) override;
  virtual int read_name(const DoutPrefixProvider *dpp, optional_yield y) override;
  virtual int read_info(const DoutPrefixProvider *dpp, optional_yield y) override;
  virtual int create(const DoutPrefixProvider *dpp, bool exclusive, const std::string& role_id, optional_yield y) override;
  virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) override;
};
}} // namespace rgw::sal

WRITE_CLASS_ENCODER(rgw::sal::RadosOIDCProvider)
