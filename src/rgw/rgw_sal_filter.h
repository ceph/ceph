// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal.h"

namespace rgw { namespace sal {

class FilterStore : public Store {
  public:
    FilterStore() {}
    virtual ~FilterStore() = default;

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) = 0;
    virtual const std::string get_name() const = 0;
    virtual std::string get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y) = 0;
    virtual std::unique_ptr<User> get_user(const rgw_user& u) = 0;
    virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) = 0;
    virtual int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) = 0;
    virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) = 0;
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) = 0;
    virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) = 0;
    virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) = 0;
    virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y) = 0;
    virtual bool is_meta_master() = 0;
    virtual int forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version* objv,
					  bufferlist& in_data, JSONParser* jp, req_info& info,
					  optional_yield y) = 0;
    virtual Zone* get_zone() = 0;
    virtual std::string zone_unique_id(uint64_t unique_num) = 0;
    virtual std::string zone_unique_trans_id(const uint64_t unique_num) = 0;
    virtual int cluster_stat(RGWClusterStat& stats) = 0;
    virtual std::unique_ptr<Lifecycle> get_lifecycle(void) = 0;
    virtual std::unique_ptr<Completions> get_completions(void) = 0;

    virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s,
        rgw::notify::EventType event_type, const std::string* object_name=nullptr) = 0;
    virtual std::unique_ptr<Notification> get_notification(
    const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, 
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant,
    std::string& _req_id, optional_yield y) = 0;

    virtual RGWLC* get_rgwlc(void) = 0;
    virtual RGWCoroutinesManagerRegistry* get_cr_registry() = 0;

    virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info) = 0;
    virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) = 0;
    virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
					const std::map<std::string, std::string>& meta) = 0;
    virtual void get_quota(RGWQuota& quota) = 0;
    virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit) = 0;
    virtual int set_buckets_enabled(const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets, bool enabled) = 0;
    virtual uint64_t get_new_req_id() = 0;
    virtual int get_sync_policy_handler(const DoutPrefixProvider* dpp,
					std::optional<rgw_zone_id> zone,
					std::optional<rgw_bucket> bucket,
					RGWBucketSyncPolicyHandlerRef* phandler,
					optional_yield y) = 0;
    virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) = 0;
    virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) = 0;
    virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids) = 0;
    virtual int clear_usage(const DoutPrefixProvider *dpp) = 0;
    virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool* is_truncated,
			       RGWUsageIter& usage_iter,
			       std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;
    virtual int get_config_key_val(std::string name, bufferlist* bl) = 0;
    virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle) = 0;
    virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, std::list<std::string>& keys, bool* truncated) = 0;
    virtual void meta_list_keys_complete(void* handle) = 0;
    virtual std::string meta_get_marker(void* handle) = 0;
    virtual int meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y) = 0;
    virtual const RGWSyncModuleInstanceRef& get_sync_module() = 0;
    virtual std::string get_host_id() = 0;
    virtual std::unique_ptr<LuaScriptManager> get_lua_script_manager() = 0;
    virtual std::unique_ptr<RGWRole> get_role(std::string name,
					      std::string tenant,
					      std::string path="",
					      std::string trust_policy="",
					      std::string max_session_duration_str="",
                std::multimap<std::string,std::string> tags={}) = 0;
    virtual std::unique_ptr<RGWRole> get_role(std::string id) = 0;
    virtual std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) = 0;
    virtual int get_roles(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  const std::string& path_prefix,
			  const std::string& tenant,
			  std::vector<std::unique_ptr<RGWRole>>& roles) = 0;
    virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() = 0;
    virtual int get_oidc_providers(const DoutPrefixProvider *dpp,
				   const std::string& tenant,
				   std::vector<std::unique_ptr<RGWOIDCProvider>>& providers) = 0;
    virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) = 0;
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) = 0;

    virtual const std::string& get_compression_type(const rgw_placement_rule& rule) = 0;
    virtual bool valid_placement(const rgw_placement_rule& rule) = 0;

    virtual void finalize(void) = 0;

    virtual CephContext* ctx(void) = 0;

    virtual const std::string& get_luarocks_path() const = 0;
    virtual void set_luarocks_path(const std::string& path) = 0;
};

class FilterUser : public User {
  public:
    FilterUser() {}
    virtual ~FilterUser() = default;

    virtual std::unique_ptr<User> clone() = 0;
    virtual int list_buckets(const DoutPrefixProvider* dpp,
			     const std::string& marker, const std::string& end_marker,
			     uint64_t max, bool need_stats, BucketList& buckets,
			     optional_yield y) = 0;
    virtual int create_bucket(const DoutPrefixProvider* dpp,
                            const rgw_bucket& b,
                            const std::string& zonegroup_id,
                            rgw_placement_rule& placement_rule,
                            std::string& swift_ver_location,
                            const RGWQuotaInfo* pquota_info,
                            const RGWAccessControlPolicy& policy,
			    Attrs& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
			    bool exclusive,
			    bool obj_lock_enabled,
			    bool* existed,
			    req_info& req_info,
			    std::unique_ptr<Bucket>* bucket,
			    optional_yield y) = 0;

    virtual std::string& get_display_name() = 0;
    virtual const std::string& get_tenant() = 0;
    virtual void set_tenant(std::string& _t) = 0;
    virtual const std::string& get_ns() = 0;
    virtual void set_ns(std::string& _ns) = 0;
    virtual void clear_ns() = 0;
    virtual const rgw_user& get_id() const = 0;
    virtual uint32_t get_type() const = 0;
    virtual int32_t get_max_buckets() const = 0;
    virtual const RGWUserCaps& get_caps() const = 0;
    virtual RGWObjVersionTracker& get_version_tracker() = 0;
    virtual Attrs& get_attrs() = 0;
    virtual void set_attrs(Attrs& _attrs) = 0;
    virtual bool empty() = 0;
    virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) = 0;
    virtual int read_stats(const DoutPrefixProvider *dpp,
			   const bucket_index_layout_generation& idx_layout,
			   int shard_id, std::string* bucket_ver, std::string* master_ver,
			   std::map<RGWObjCategory, RGWStorageStats>& stats,
			   std::string* max_marker = nullptr,
			   bool* syncstopped = nullptr) = 0;
    virtual int read_stats_async(const DoutPrefixProvider *dpp,
				 const bucket_index_layout_generation& idx_layout,
				 int shard_id, RGWGetBucketStats_CB* ctx) = 0;
    virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) = 0;
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			   uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;

    virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) = 0;
    virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) = 0;

    RGWUserInfo& get_info() = 0;
    virtual void print(std::ostream& out) const = 0;
};

class FilterBucket : public Bucket {
  public:

    Bucket() = default;
    virtual ~Bucket() = default;

    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) = 0;
    virtual int list(const DoutPrefixProvider* dpp, ListParams&, int, ListResults&, optional_yield y) = 0;
    virtual Attrs& get_attrs(void) = 0;
    virtual int set_attrs(Attrs a) = 0;
    virtual int remove_bucket(const DoutPrefixProvider* dpp, bool delete_children, bool forward_to_master, req_info* req_info, optional_yield y) = 0;
    virtual int remove_bucket_bypass_gc(int concurrent_max, bool
					keep_index_consistent,
					optional_yield y, const
					DoutPrefixProvider *dpp) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl, optional_yield y) = 0;

    virtual void set_owner(rgw::sal::User* _owner) = 0;
    virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y, bool get_stats = false) = 0;
    virtual int read_stats(const DoutPrefixProvider *dpp, int shard_id,
				 std::string* bucket_ver, std::string* master_ver,
				 std::map<RGWObjCategory, RGWStorageStats>& stats,
				 std::string* max_marker = nullptr,
				 bool* syncstopped = nullptr) = 0;
    virtual int read_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB* ctx) = 0;
    virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y) = 0;
    virtual int update_container_stats(const DoutPrefixProvider* dpp) = 0;
    virtual int check_bucket_shards(const DoutPrefixProvider* dpp) = 0;
    virtual int chown(const DoutPrefixProvider* dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker = nullptr) = 0;
    virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime) = 0;
    virtual bool is_owner(User* user) = 0;
    virtual User* get_owner(void) = 0;
    virtual ACLOwner get_acl_owner(void) = 0;
    virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) = 0;
    virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) = 0;
    virtual int try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime) = 0;
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;
    virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) = 0;
    virtual int check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) = 0;
    virtual int rebuild_index(const DoutPrefixProvider *dpp) = 0;
    virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) = 0;
    virtual int purge_instance(const DoutPrefixProvider* dpp) = 0;
    virtual bool empty() const = 0;
    virtual const std::string& get_name() const = 0;
    virtual const std::string& get_tenant() const = 0;
    virtual const std::string& get_marker() const = 0;
    virtual const std::string& get_bucket_id() const = 0;
    virtual size_t get_size() const = 0;
    virtual size_t get_size_rounded() const = 0;
    virtual uint64_t get_count() const = 0;
    virtual rgw_placement_rule& get_placement_rule() = 0;
    virtual ceph::real_time& get_creation_time() = 0;
    virtual ceph::real_time& get_modification_time() = 0;
    virtual obj_version& get_version() = 0;
    virtual void set_version(obj_version &ver) = 0;
    virtual bool versioned() = 0;
    virtual bool versioning_enabled() = 0;

    virtual std::unique_ptr<Bucket> clone() = 0;

    virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) = 0;
    virtual int list_multiparts(const DoutPrefixProvider *dpp,
				const std::string& prefix,
				std::string& marker,
				const std::string& delim,
				const int& max_uploads,
				std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				std::map<std::string, bool> *common_prefixes,
				bool *is_truncated) = 0;
    virtual int abort_multiparts(const DoutPrefixProvider* dpp,
				 CephContext* cct) = 0;

    virtual rgw_bucket& get_key() = 0;
    virtual RGWBucketInfo& get_info() = 0;

    virtual void print(std::ostream& out) const = 0;

    virtual bool operator==(const Bucket& b) const = 0;
    virtual bool operator!=(const Bucket& b) const = 0;

    friend class BucketList;
};

class FilterObject : public Object {
  public:

    struct FilterOp : ReadOp {
      virtual ~FilterOp() = default;

      virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) = 0;
      virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp) = 0;
      virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y) = 0;
      virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) = 0;
    };

    struct FilterOp : DeleteOp {
      virtual ~FilterOp() = default;

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    };

    FilterObject() {}
    virtual ~FilterObject() = default;

    virtual int delete_object(const DoutPrefixProvider* dpp,
			      optional_yield y,
			      bool prevent_versioning = false) = 0;
    virtual int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
			       bool keep_index_consistent, optional_yield y) = 0;
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
               const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(const RGWAccessControlPolicy& acl) = 0;
    virtual void set_atomic() = 0;
    virtual bool is_atomic() = 0;
    virtual void set_prefetch_data() = 0;
    virtual bool is_prefetch_data() = 0;
    virtual void set_compressed() = 0;
    virtual bool is_compressed() = 0;
    virtual void invalidate() = 0;
    virtual bool empty() const = 0;
    virtual const std::string &get_name() const = 0;

    virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state, optional_yield y, bool follow_olh = true) = 0;
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y) = 0;
    virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) = 0;
    virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) = 0;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y) = 0;
    virtual bool is_expired() = 0;
    virtual void gen_rand_obj_instance_name() = 0;
    virtual MPSerializer* get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name) = 0;
    virtual int transition(Bucket* bucket,
			   const rgw_placement_rule& placement_rule,
			   const real_time& mtime,
			   uint64_t olh_epoch,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) = 0;
    virtual int transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) = 0;
    virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) = 0;
    virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f) = 0;

    virtual Attrs& get_attrs(void) = 0;
    virtual const Attrs& get_attrs(void) const = 0;
    virtual int set_attrs(Attrs a) = 0;
    virtual bool has_attrs(void) = 0;
    virtual ceph::real_time get_mtime(void) const = 0;
    virtual uint64_t get_obj_size(void) const = 0;
    virtual Bucket* get_bucket(void) const = 0;
    virtual void set_bucket(Bucket* b) = 0;
    virtual std::string get_hash_source(void) = 0;
    virtual void set_hash_source(std::string s) = 0;
    virtual std::string get_oid(void) const = 0;
    virtual bool get_delete_marker(void) = 0;
    virtual bool get_in_extra_data(void) = 0;
    virtual void set_in_extra_data(bool i) = 0;
    int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
    virtual void set_obj_size(uint64_t s) = 0;
    virtual void set_name(const std::string& n) = 0;
    virtual void set_key(const rgw_obj_key& k) = 0;
    virtual rgw_obj get_obj(void) const = 0;

    virtual int swift_versioning_restore(bool& restored,   /* out */
					 const DoutPrefixProvider* dpp) = 0;
    virtual int swift_versioning_copy(const DoutPrefixProvider* dpp,
				      optional_yield y) = 0;

    virtual std::unique_ptr<ReadOp> get_read_op() = 0;
    virtual std::unique_ptr<DeleteOp> get_delete_op() = 0;

    virtual int omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
			      std::map<std::string, bufferlist>* m,
			      bool* pmore, optional_yield y) = 0;
    virtual int omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist>* m,
			     optional_yield y) = 0;
    virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
			      const std::set<std::string>& keys,
			      Attrs* vals) = 0;
    virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
				    bool must_exist, optional_yield y) = 0;

    virtual std::unique_ptr<Object> clone() = 0;

    virtual rgw_obj_key& get_key() = 0;
    virtual void set_instance(const std::string &i) = 0;
    virtual const std::string &get_instance() const = 0;
    virtual bool have_instance(void) = 0;
    virtual void clear_instance() = 0;

    virtual void print(std::ostream& out) const = 0;
};

class FilterMultipartPart : public MultipartPart {
public:
  FilterMultipartPart() = default;
  virtual ~FilterMultipartPart() = default;

  virtual uint32_t get_num() = 0;
  virtual uint64_t get_size() = 0;
  virtual const std::string& get_etag() = 0;
  virtual ceph::real_time& get_mtime() = 0;
};

class FilterMultipartUpload : public MultipartUpload {
public:
  FilterMultipartUpload() = default;
  virtual ~FilterMultipartUpload() = default;

  virtual const std::string& get_meta() const = 0;
  virtual const std::string& get_key() const = 0;
  virtual const std::string& get_upload_id() const = 0;
  virtual const ACLOwner& get_owner() const = 0;
  virtual ceph::real_time& get_mtime() = 0;

  virtual std::map<uint32_t, std::unique_ptr<MultipartPart>>& get_parts() = 0;

  virtual const jspan_context& get_trace() = 0;

  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() = 0;

  virtual int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) = 0;
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated,
			 bool assume_unsorted = false) = 0;
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct) = 0;
  virtual int complete(const DoutPrefixProvider* dpp,
		       optional_yield y, CephContext* cct,
		       std::map<int, std::string>& part_etags,
		       std::list<rgw_obj_index_key>& remove_objs,
		       uint64_t& accounted_size, bool& compressed,
		       RGWCompressionInfo& cs_info, off_t& ofs,
		       std::string& tag, ACLOwner& owner,
		       uint64_t olh_epoch,
		       rgw::sal::Object* target_obj) = 0;

  virtual int get_info(const DoutPrefixProvider *dpp, optional_yield y, rgw_placement_rule** rule, rgw::sal::Attrs* attrs = nullptr) = 0;

  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  std::unique_ptr<rgw::sal::Object> _head_obj,
			  const rgw_user& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) = 0;
  virtual void print(std::ostream& out) const = 0;
};

class FilterMPSerializer : public MPSerializer {
public:
  FilterMPSerializer() = default;
  virtual ~FilterMPSerializer() = default;

  virtual void clear_locked() = 0;
};

class FilterLCSerializer : public LCSerializer {
public:
  FilterLCSerializer() {}
  virtual ~FilterLCSerializer() = default;
};

class FilterLifecycle : public Lifecycle {
public:
  struct FilterLCHead : LCHead {
    FilterLCHead() = default;
    virtual ~FilterLCHead() = default;

    virtual time_t& get_start_date() = 0;
    virtual void set_start_date(time_t) = 0;
    virtual std::string& get_marker() = 0;
    virtual void set_marker(const std::string&) = 0;
    virtual time_t& get_shard_rollover_date() = 0;
    virtual void set_shard_rollover_date(time_t) = 0;
  };

  struct FilterLCEntry : LCEntry {
    FilterLCEntry() = default;
    virtual ~FilterLCEntry() = default;

    virtual std::string& get_bucket() = 0;
    virtual void set_bucket(const std::string&) = 0;
    virtual std::string& get_oid() = 0;
    virtual void set_oid(const std::string&) = 0;
    virtual uint64_t get_start_time() = 0;
    virtual void set_start_time(uint64_t) = 0;
    virtual uint32_t get_status() = 0;
    virtual void set_status(uint32_t) = 0;
  };

  FilterLifecycle() = default;
  virtual ~FilterLifecycle() = default;

  virtual std::unique_ptr<LCEntry> get_entry() = 0;
  virtual int get_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) = 0;
  virtual int get_next_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) = 0;
  virtual int set_entry(const std::string& oid, LCEntry& entry) = 0;
  virtual int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries,
			   std::vector<std::unique_ptr<LCEntry>>& entries) = 0;
  virtual int rm_entry(const std::string& oid, LCEntry& entry) = 0;
  virtual int get_head(const std::string& oid, std::unique_ptr<LCHead>* head) = 0;
  virtual int put_head(const std::string& oid, LCHead& head) = 0;
  virtual LCSerializer* get_serializer(const std::string& lock_name, const std::string& oid, const std::string& cookie) = 0;
};

class FilterNotification : public Notification {
protected:
  Object* obj;
  Object* src_obj;
  rgw::notify::EventType event_type;

  public:
    FilterNotification(Object* _obj, Object* _src_obj, rgw::notify::EventType _type)
      : obj(_obj), src_obj(_src_obj), event_type(_type)
    {}

    virtual ~FilterNotification() = default;

    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) = 0;
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const std::string& etag, const std::string& version) = 0;
};

class FilterWriter : public Writer {
public:
  FilterWriter() {}
  virtual ~FilterWriter() = default;

  virtual int prepare(optional_yield y) = 0;
  virtual int process(bufferlist&& data, uint64_t offset) = 0;
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) = 0;
};

class FilterPlacementTier : public PlacementTier {
public:
  virtual ~FilterPlacementTier() = default;

  virtual const std::string& get_tier_type() = 0;
  virtual const std::string& get_storage_class() = 0;
  virtual bool retain_head_object() = 0;
};

class FilterZoneGroup : public ZoneGroup {
public:
  virtual ~FilterZoneGroup() = default;
  virtual const std::string& get_id() const = 0;
  virtual const std::string& get_name() const = 0;
  virtual int equals(const std::string& other_zonegroup) const = 0;
  virtual const std::string& get_endpoint() const = 0;
  virtual bool placement_target_exists(std::string& target) const = 0;
  virtual bool is_master_zonegroup() const = 0;
  virtual const std::string& get_api_name() const = 0;
  virtual int get_placement_target_names(std::set<std::string>& names) const = 0;
  virtual const std::string& get_default_placement_name() const = 0;
  virtual int get_hostnames(std::list<std::string>& names) const = 0;
  virtual int get_s3website_hostnames(std::list<std::string>& names) const = 0;
  virtual int get_zone_count() const = 0;
  virtual int get_placement_tier(const rgw_placement_rule& rule, std::unique_ptr<PlacementTier>* tier) = 0;
};

class FilterZone : public Zone {
  public:
    virtual ~FilterZone() = default;

    virtual ZoneGroup& get_zonegroup() = 0;
    virtual int get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup) = 0;
    virtual const rgw_zone_id& get_id() = 0;
    virtual const std::string& get_name() const = 0;
    virtual bool is_writeable() = 0;
    virtual bool get_redirect_endpoint(std::string* endpoint) = 0;
    virtual bool has_zonegroup_api(const std::string& api) const = 0;
    virtual const std::string& get_current_period_id() = 0;
    virtual const RGWAccessKey& get_system_key() = 0;
    virtual const std::string& get_realm_name() = 0;
    virtual const std::string& get_realm_id() = 0;
};

class FilterLuaScriptManager : public LuaScriptManager {
public:
  virtual ~FilterLuaScriptManager() = default;

  virtual int get(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) = 0;
  virtual int put(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) = 0;
  virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) = 0;
};

} } // namespace rgw::sal
