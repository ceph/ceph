// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal.h"
#include "rgw_oidc_provider.h"
#include "rgw_role.h"

#include "store/dbstore/common/dbstore.h"
#include "store/dbstore/dbstore_mgr.h"

namespace rgw { namespace sal {

  class DBStore;

class DBNotification : public Notification {
protected:
  Object* obj;
  rgw::notify::EventType event_type;

  public:
    DBNotification(Object* _obj, rgw::notify::EventType _type) : Notification(_obj, _type), obj(_obj), event_type(_type) {}
    ~DBNotification() = default;

    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) override { return 0;}
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const std::string& etag, const std::string& version) override { return 0; }
};

  class DBUser : public User {
    private:
      DBStore *store;

    public:
      DBUser(DBStore *_st, const rgw_user& _u) : User(_u), store(_st) { }
      DBUser(DBStore *_st, const RGWUserInfo& _i) : User(_i), store(_st) { }
      DBUser(DBStore *_st) : store(_st) { }
      DBUser(DBUser& _o) = default;
      DBUser() {}

      virtual std::unique_ptr<User> clone() override {
        return std::unique_ptr<User>(new DBUser(*this));
      }
      int list_buckets(const DoutPrefixProvider *dpp, const std::string& marker, const std::string& end_marker,
          uint64_t max, bool need_stats, BucketList& buckets, optional_yield y) override;
      virtual Bucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time) override;
      virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual int read_stats(const DoutPrefixProvider *dpp,
          optional_yield y, RGWStorageStats* stats,
          ceph::real_time *last_stats_sync = nullptr,
          ceph::real_time *last_stats_update = nullptr) override;
      virtual int read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb) override;
      virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
          bool* is_truncated, RGWUsageIter& usage_iter,
          map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;

      /* Placeholders */
      virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) override;
      virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;

      friend class DBBucket;
  };

  class DBBucket : public Bucket {
    private:
      DBStore *store;
      RGWAccessControlPolicy acls;

    public:
      DBBucket(DBStore *_st)
        : store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, User* _u)
        : Bucket(_u),
        store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, const rgw_bucket& _b)
        : Bucket(_b),
        store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, const RGWBucketEnt& _e)
        : Bucket(_e),
        store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, const RGWBucketInfo& _i)
        : Bucket(_i),
        store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, const rgw_bucket& _b, User* _u)
        : Bucket(_b, _u),
        store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, const RGWBucketEnt& _e, User* _u)
        : Bucket(_e, _u),
        store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, const RGWBucketInfo& _i, User* _u)
        : Bucket(_i, _u),
        store(_st),
        acls() {
        }

      ~DBBucket() { }

      virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
      virtual int list(const DoutPrefixProvider *dpp, ListParams&, int, ListResults&, optional_yield y) override;
      virtual int remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, std::string prefix, std::string delimiter, bool forward_to_master, req_info* req_info, optional_yield y) override;
      virtual int remove_bucket_bypass_gc(int concurrent_max, bool
					keep_index_consistent,
					optional_yield y, const
					DoutPrefixProvider *dpp) override;
      virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
      virtual int set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy& acl, optional_yield y) override;
      virtual int get_bucket_info(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int get_bucket_stats(const DoutPrefixProvider *dpp, int shard_id,
          std::string *bucket_ver, std::string *master_ver,
          std::map<RGWObjCategory, RGWStorageStats>& stats,
          std::string *max_marker = nullptr,
          bool *syncstopped = nullptr) override;
      virtual int get_bucket_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB* ctx) override;
      virtual int read_bucket_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int update_container_stats(const DoutPrefixProvider *dpp) override;
      virtual int check_bucket_shards(const DoutPrefixProvider *dpp) override;
      virtual int chown(const DoutPrefixProvider *dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker = nullptr) override;
      virtual int put_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time mtime) override;
      virtual int remove_metadata(const DoutPrefixProvider* dpp, RGWObjVersionTracker* objv, optional_yield y) override;
      virtual bool is_owner(User* user) override;
      virtual int check_empty(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
      virtual int merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& attrs, optional_yield y) override;
      virtual int try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime) override;
      virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
          bool *is_truncated, RGWUsageIter& usage_iter,
          map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;
      virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) override;
      virtual int check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
      virtual int rebuild_index(const DoutPrefixProvider *dpp) override;
      virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) override;
      virtual int purge_instance(const DoutPrefixProvider *dpp) override;
      virtual std::unique_ptr<Bucket> clone() override {
        return std::make_unique<DBBucket>(*this);
      }
      virtual int list_multiparts(const DoutPrefixProvider *dpp,
				const string& prefix,
				string& marker,
				const string& delim,
				const int& max_uploads,
				vector<std::unique_ptr<MultipartUpload>>& uploads,
				map<string, bool> *common_prefixes,
				bool *is_truncated) override;
      virtual int abort_multiparts(const DoutPrefixProvider *dpp,
				 CephContext *cct,
				 string& prefix, string& delim) override;

      friend class DBStore;
  };

  class DBZone : public Zone {
    protected:
      DBStore* store;
      RGWRealm *realm{nullptr};
      RGWZoneGroup *zonegroup{nullptr};
      RGWZone *zone_public_config{nullptr}; /* external zone params, e.g., entrypoints, log flags, etc. */  
      RGWZoneParams *zone_params{nullptr}; /* internal zone params, e.g., rados pools */
      RGWPeriod *current_period{nullptr};
      rgw_zone_id cur_zone_id;

    public:
      DBZone(DBStore* _store) : store(_store) {
        realm = new RGWRealm();
        zonegroup = new RGWZoneGroup();
        zone_public_config = new RGWZone();
        zone_params = new RGWZoneParams();
        current_period = new RGWPeriod();
        cur_zone_id = rgw_zone_id(zone_params->get_id());

        // XXX: only default and STANDARD supported for now
        RGWZonePlacementInfo info;
        RGWZoneStorageClasses sc;
        sc.set_storage_class("STANDARD", nullptr, nullptr);
        info.storage_classes = sc;
        zone_params->placement_pools["default"] = info;
      }
      ~DBZone() = default;

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

  class DBLuaScriptManager : public LuaScriptManager {
    DBStore* store;

    public:
    DBLuaScriptManager(DBStore* _s) : store(_s)
    {
    }
    virtual ~DBLuaScriptManager() = default;

    virtual int get(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) override { return -ENOENT; }
    virtual int put(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) override { return -ENOENT; }
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) override { return -ENOENT; }
  };

  class DBOIDCProvider : public RGWOIDCProvider {
    DBStore* store;
    public:
    DBOIDCProvider(DBStore* _store) : store(_store) {}
    ~DBOIDCProvider() = default;

    virtual int store_url(const DoutPrefixProvider *dpp, const std::string& url, bool exclusive, optional_yield y) override { return 0; }
    virtual int read_url(const DoutPrefixProvider *dpp, const std::string& url, const std::string& tenant) override { return 0; }
    virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) override { return 0;}

    void encode(bufferlist& bl) const {
      RGWOIDCProvider::encode(bl);
    }
    void decode(bufferlist::const_iterator& bl) {
      RGWOIDCProvider::decode(bl);
    }
  };

  class DBObject : public Object {
    private:
      DBStore* store;
      RGWAccessControlPolicy acls;
      /* XXX: to be removed. Till Dan's patch comes, a placeholder
       * for RGWObjState
       */
      RGWObjState* state;

    public:
      struct DBReadOp : public ReadOp {
        private:
          DBObject* source;
          RGWObjectCtx* rctx;
          DB::Object op_target;
          DB::Object::Read parent_op;

        public:
          DBReadOp(DBObject *_source, RGWObjectCtx *_rctx);

          virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
          virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp) override;
          virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y) override;
          virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) override; 
      };

      struct DBDeleteOp : public DeleteOp {
        private:
          DBObject* source;
          RGWObjectCtx* rctx;
          DB::Object op_target;
          DB::Object::Delete parent_op;

        public:
          DBDeleteOp(DBObject* _source, RGWObjectCtx* _rctx);

          virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
      };

      DBObject() = default;

      DBObject(DBStore *_st, const rgw_obj_key& _k)
        : Object(_k),
        store(_st),
        acls() {
        }
      DBObject(DBStore *_st, const rgw_obj_key& _k, Bucket* _b)
        : Object(_k, _b),
        store(_st),
        acls() {
        }
      DBObject(DBObject& _o) = default;

      virtual int delete_object(const DoutPrefixProvider* dpp,
          RGWObjectCtx* obj_ctx,
          optional_yield y,
          bool prevent_versioning = false) override;
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
      virtual int copy_obj_data(RGWObjectCtx& rctx, Bucket* dest_bucket, Object* dest_obj, uint16_t olh_epoch, std::string* petag, const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual bool is_expired() override;
      virtual void gen_rand_obj_instance_name() override;
      virtual std::unique_ptr<Object> clone() override {
        return std::unique_ptr<Object>(new DBObject(*this));
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
      virtual int get_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f, RGWObjectCtx* obj_ctx) override;

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
    private:
      int read_attrs(const DoutPrefixProvider* dpp, DB::Object::Read &read_op, optional_yield y, rgw_obj* target_obj = nullptr);
  };

  class DBAtomicWriter : public Writer {
    protected:
    rgw::sal::DBStore* store;
    const rgw_user& owner;
	const rgw_placement_rule *ptail_placement_rule;
	uint64_t olh_epoch;
	const std::string& unique_tag;
    DBObject obj;
    DB::Object op_target;
    DB::Object::Write parent_op;
    uint64_t total_data_size = 0; /* for total data being uploaded */
    bufferlist head_data;
    bufferlist tail_part_data;
    uint64_t tail_part_offset;
    uint64_t tail_part_size = 0; /* corresponds to each tail part being
                                  written to dbstore */

    public:
    DBAtomicWriter(const DoutPrefixProvider *dpp,
	    	    optional_yield y,
		        std::unique_ptr<rgw::sal::Object> _head_obj,
		        DBStore* _store,
    		    const rgw_user& _owner, RGWObjectCtx& obj_ctx,
	    	    const rgw_placement_rule *_ptail_placement_rule,
		        uint64_t _olh_epoch,
		        const std::string& _unique_tag);
    ~DBAtomicWriter() = default;

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

  class DBStore : public Store {
    private:
      /* DBStoreManager is used in case multiple
       * connections are needed one for each tenant.
       */
      DBStoreManager *dbsm;
      /* default db (single connection). If needed
       * multiple db handles (for eg., one for each tenant),
       * use dbsm->getDB(tenant) */
      DB *db;
      string luarocks_path;
      DBZone zone;
      RGWSyncModuleInstanceRef sync_module;

    public:
      DBStore(): dbsm(nullptr), zone(this) {}
      ~DBStore() { delete dbsm; }

      virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
      virtual int get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) override;
      virtual int get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) override;
      virtual int get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) override;
      virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
      virtual int get_bucket(const DoutPrefixProvider *dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
      virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
      virtual int get_bucket(const DoutPrefixProvider *dpp, User* u, const std::string& tenant, const std::string&name, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
      virtual int create_bucket(const DoutPrefixProvider* dpp,
          User* u, const rgw_bucket& b,
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
          optional_yield y) override;
      virtual bool is_meta_master() override;
      virtual int forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version* objv,
          bufferlist& in_data, JSONParser *jp, req_info& info,
          optional_yield y) override;
      virtual int defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, Bucket* bucket, Object* obj,
          optional_yield y) override;
      virtual Zone* get_zone() { return &zone; }
      virtual std::string zone_unique_id(uint64_t unique_num) override;
      virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
      virtual int cluster_stat(RGWClusterStat& stats) override;
      virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
      virtual std::unique_ptr<Completions> get_completions(void) override;
      virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, struct req_state* s, 
          rgw::notify::EventType event_type, const std::string* object_name=nullptr) override;
      virtual RGWLC* get_rgwlc(void) override { return NULL; }
      virtual RGWCoroutinesManagerRegistry* get_cr_registry() override { return NULL; }

      virtual int log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info) override;
      virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) override;
      virtual int register_to_service_map(const DoutPrefixProvider *dpp, const string& daemon_type,
          const map<string, string>& meta) override;
      virtual void get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota) override;
      virtual int set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled) override;
      virtual uint64_t get_new_req_id() override { return 0; }
      virtual int get_sync_policy_handler(const DoutPrefixProvider *dpp,
          std::optional<rgw_zone_id> zone,
          std::optional<rgw_bucket> bucket,
          RGWBucketSyncPolicyHandlerRef *phandler,
          optional_yield y) override;
      virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
      virtual void wakeup_meta_sync_shards(set<int>& shard_ids) override { return; }
      virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, map<int, set<string> >& shard_ids) override { return; }
      virtual int clear_usage(const DoutPrefixProvider *dpp) override { return 0; }
      virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
          uint32_t max_entries, bool *is_truncated,
          RGWUsageIter& usage_iter,
          map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;
      virtual int get_config_key_val(std::string name, bufferlist* bl) override;
      virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle) override;
      virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, list<std::string>& keys, bool* truncated) override;
      virtual void meta_list_keys_complete(void* handle) override;
      virtual std::string meta_get_marker(void *handle) override;
      virtual int meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y) override;

      virtual const RGWSyncModuleInstanceRef& get_sync_module() { return sync_module; }
      virtual std::string get_host_id() { return ""; }

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
          vector<std::unique_ptr<RGWRole>>& roles) override;
      virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;
      virtual int get_oidc_providers(const DoutPrefixProvider *dpp,
          const std::string& tenant,
          vector<std::unique_ptr<RGWOIDCProvider>>& providers) override;
      virtual std::unique_ptr<MultipartUpload> get_multipart_upload(Bucket* bucket, const std::string& oid, std::optional<std::string> upload_id, ceph::real_time mtime) override;
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

      virtual CephContext *ctx(void) override {
        return db->ctx();
      }

      virtual const std::string& get_luarocks_path() const override {
        return luarocks_path;
      }

      virtual void set_luarocks_path(const std::string& path) override {
        luarocks_path = path;
      }

      /* Unique to DBStore */
      void setDBStoreManager(DBStoreManager *stm) { dbsm = stm; }
      DBStoreManager *getDBStoreManager(void) { return dbsm; }

      void setDB(DB * st) { db = st; }
      DB *getDB(void) { return db; }

      DB *getDB(string tenant) { return dbsm->getDB(tenant, false); }
  };

} } // namespace rgw::sal
