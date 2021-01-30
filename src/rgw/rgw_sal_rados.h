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

#include "rgw_sal.h"
#include "rgw_rados.h"
#include "cls/lock/cls_lock_client.h"

namespace rgw { namespace sal {

class RGWRadosStore;

class RGWRadosUser : public RGWUser {
  private:
    RGWRadosStore *store;

  public:
    RGWRadosUser(RGWRadosStore *_st, const rgw_user& _u) : RGWUser(_u), store(_st) { }
    RGWRadosUser(RGWRadosStore *_st, const RGWUserInfo& _i) : RGWUser(_i), store(_st) { }
    RGWRadosUser(RGWRadosStore *_st) : store(_st) { }
    RGWRadosUser() {}

    int list_buckets(const DoutPrefixProvider *dpp, const std::string& marker, const std::string& end_marker,
		     uint64_t max, bool need_stats, RGWBucketList& buckets,
		     optional_yield y) override;
    RGWBucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time);

    /* Placeholders */
    virtual int load_by_id(const DoutPrefixProvider *dpp, optional_yield y);

    friend class RGWRadosBucket;
};

class RGWRadosObject : public RGWObject {
  private:
    RGWRadosStore *store;
    RGWAccessControlPolicy acls;

  public:

    struct RadosReadOp : public ReadOp {
    private:
      RGWRadosObject* source;
      RGWObjectCtx* rctx;
      RGWRados::Object op_target;
      RGWRados::Object::Read parent_op;

    public:
      RadosReadOp(RGWRadosObject *_source, RGWObjectCtx *_rctx);

      virtual int prepare(optional_yield y, const DoutPrefixProvider *dpp) override;
      virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider *dpp) override;
      virtual int iterate(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end, RGWGetDataCB *cb, optional_yield y) override;
      virtual int get_manifest(const DoutPrefixProvider *dpp, RGWObjManifest **pmanifest, optional_yield y) override;
      virtual int get_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist& dest, optional_yield y) override;
    };

    struct RadosWriteOp : public WriteOp {
    private:
      RGWRadosObject* source;
      RGWObjectCtx* rctx;
      RGWRados::Object op_target;
      RGWRados::Object::Write parent_op;

    public:
      RadosWriteOp(RGWRadosObject* _source, RGWObjectCtx* _rctx);

      virtual int prepare(optional_yield y) override;
      virtual int write_meta(const DoutPrefixProvider *dpp, uint64_t size, uint64_t accounted_size, optional_yield y) override;
      //virtual int write_data(const char *data, uint64_t ofs, uint64_t len, bool exclusive) override;
    };

    RGWRadosObject() = default;

    RGWRadosObject(RGWRadosStore *_st, const rgw_obj_key& _k)
      : RGWObject(_k),
	store(_st),
        acls() {
    }
    RGWRadosObject(RGWRadosStore *_st, const rgw_obj_key& _k, RGWBucket* _b)
      : RGWObject(_k, _b),
	store(_st),
        acls() {
    }
    RGWRadosObject(RGWRadosObject& _o) = default;

    int read(off_t offset, off_t length, std::iostream& stream) { return length; }
    int write(off_t offset, off_t length, std::iostream& stream) { return length; }
    virtual int delete_object(const DoutPrefixProvider *dpp, RGWObjectCtx* obj_ctx, ACLOwner obj_owner,
			      ACLOwner bucket_owner, ceph::real_time unmod_since,
			      bool high_precision_time, uint64_t epoch,
			      std::string& version_id,optional_yield y) override;
    virtual int copy_object(RGWObjectCtx& obj_ctx, RGWUser* user,
               req_info *info, const rgw_zone_id& source_zone,
               rgw::sal::RGWObject* dest_object, rgw::sal::RGWBucket* dest_bucket,
               rgw::sal::RGWBucket* src_bucket,
               const rgw_placement_rule& dest_placement,
               ceph::real_time *src_mtime, ceph::real_time *mtime,
               const ceph::real_time *mod_ptr, const ceph::real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match, const char *if_nomatch,
               AttrsMod attrs_mod, bool copy_if_newer, RGWAttrs& attrs,
               RGWObjCategory category, uint64_t olh_epoch,
	       boost::optional<ceph::real_time> delete_at,
               string *version_id, string *tag, string *etag,
               void (*progress_cb)(off_t, void *), void *progress_data,
               const DoutPrefixProvider *dpp, optional_yield y) override;
    RGWAccessControlPolicy& get_acl(void) { return acls; }
    int set_acl(const RGWAccessControlPolicy& acl) { acls = acl; return 0; }
    virtual void set_atomic(RGWObjectCtx *rctx) const;
    virtual void set_prefetch_data(RGWObjectCtx *rctx);

    virtual int get_obj_state(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, RGWBucket& bucket, RGWObjState **state, optional_yield y, bool follow_olh = true) override;
    virtual int set_obj_attrs(const DoutPrefixProvider *dpp, RGWObjectCtx* rctx, RGWAttrs* setattrs, RGWAttrs* delattrs, optional_yield y, rgw_obj* target_obj = NULL) override;
    virtual int get_obj_attrs(RGWObjectCtx *rctx, optional_yield y, const DoutPrefixProvider *dpp, rgw_obj* target_obj = NULL) override;
    virtual int modify_obj_attrs(RGWObjectCtx *rctx, const char *attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider *dpp) override;
    virtual int delete_obj_attrs(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, const char *attr_name, optional_yield y) override;
    virtual int copy_obj_data(RGWObjectCtx& rctx, RGWBucket* dest_bucket, RGWObject* dest_obj, uint16_t olh_epoch, std::string* petag, const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual bool is_expired() override;
    virtual void gen_rand_obj_instance_name() override;
    virtual void raw_obj_to_obj(const rgw_raw_obj& raw_obj) override;
    virtual void get_raw_obj(rgw_raw_obj* raw_obj) override;
    virtual std::unique_ptr<RGWObject> clone() {
      return std::unique_ptr<RGWObject>(new RGWRadosObject(*this));
    }
    virtual MPSerializer* get_serializer(const std::string& lock_name) override;
    virtual int transition(RGWObjectCtx& rctx,
			   RGWBucket* bucket,
			   const rgw_placement_rule& placement_rule,
			   const real_time& mtime,
			   uint64_t olh_epoch,
			   const DoutPrefixProvider *dpp,
			   optional_yield y) override;
    virtual int get_max_chunk_size(const DoutPrefixProvider *dpp, 
                                   rgw_placement_rule placement_rule,
				   uint64_t *max_chunk_size,
				   uint64_t *alignment = nullptr) override;
    virtual void get_max_aligned_size(uint64_t size, uint64_t alignment, uint64_t *max_size) override;
    virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;

    /* Swift versioning */
    virtual int swift_versioning_restore(RGWObjectCtx* obj_ctx,
					 bool& restored,
					 const DoutPrefixProvider *dpp) override;
    virtual int swift_versioning_copy(RGWObjectCtx* obj_ctx,
				      const DoutPrefixProvider *dpp,
				      optional_yield y) override;

    /* OPs */
    virtual std::unique_ptr<ReadOp> get_read_op(RGWObjectCtx *) override;
    virtual std::unique_ptr<WriteOp> get_write_op(RGWObjectCtx *) override;

    /* OMAP */
    virtual int omap_get_vals_by_keys(const std::string& oid,
			      const std::set<std::string>& keys,
			      RGWAttrs *vals) override;
    virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
				    bool must_exist, optional_yield y) override;

  private:
    int read_attrs(RGWRados::Object::Read &read_op, optional_yield y, const DoutPrefixProvider *dpp, rgw_obj *target_obj = nullptr);
};

class RGWRadosBucket : public RGWBucket {
  private:
    RGWRadosStore *store;
    RGWAccessControlPolicy acls;

  public:
    RGWRadosBucket(RGWRadosStore *_st)
      : store(_st),
        acls() {
    }

    RGWRadosBucket(RGWRadosStore *_st, const rgw_bucket& _b)
      : RGWBucket(_b),
	store(_st),
        acls() {
    }

    RGWRadosBucket(RGWRadosStore *_st, const RGWBucketEnt& _e)
      : RGWBucket(_e),
	store(_st),
        acls() {
    }

    RGWRadosBucket(RGWRadosStore *_st, const RGWBucketInfo& _i)
      : RGWBucket(_i),
	store(_st),
        acls() {
    }

    RGWRadosBucket(RGWRadosStore *_st, const rgw_bucket& _b, RGWUser* _u)
      : RGWBucket(_b, _u),
	store(_st),
        acls() {
    }

    RGWRadosBucket(RGWRadosStore *_st, const RGWBucketEnt& _e, RGWUser* _u)
      : RGWBucket(_e, _u),
	store(_st),
        acls() {
    }

    RGWRadosBucket(RGWRadosStore *_st, const RGWBucketInfo& _i, RGWUser* _u)
      : RGWBucket(_i, _u),
	store(_st),
        acls() {
    }

    ~RGWRadosBucket() { }

    virtual int load_by_name(const DoutPrefixProvider *dpp, const std::string& tenant, const std::string& bucket_name, const std::string bucket_instance_id, RGWSysObjectCtx *rctx, optional_yield y) override;
    virtual std::unique_ptr<RGWObject> get_object(const rgw_obj_key& k) override;
    RGWBucketList* list(void) { return new RGWBucketList(); }
    virtual int list(const DoutPrefixProvider *dpp, ListParams&, int, ListResults&, optional_yield y) override;
    RGWObject* create_object(const rgw_obj_key& key /* Attributes */) override;
    virtual int remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, std::string prefix, std::string delimiter, bool forward_to_master, req_info* req_info, optional_yield y) override;
    RGWAccessControlPolicy& get_acl(void) { return acls; }
    virtual int set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy& acl, optional_yield y) override;
    virtual int get_bucket_info(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int get_bucket_stats(RGWBucketInfo& bucket_info, int shard_id,
				 std::string *bucket_ver, std::string *master_ver,
				 std::map<RGWObjCategory, RGWStorageStats>& stats,
				 std::string *max_marker = nullptr,
				 bool *syncstopped = nullptr) override;
    virtual int read_bucket_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int sync_user_stats(optional_yield y) override;
    virtual int update_container_stats(const DoutPrefixProvider *dpp) override;
    virtual int check_bucket_shards(const DoutPrefixProvider *dpp) override;
    virtual int link(const DoutPrefixProvider *dpp, RGWUser* new_user, optional_yield y) override;
    virtual int unlink(RGWUser* new_user, optional_yield y) override;
    virtual int chown(RGWUser* new_user, RGWUser* old_user, optional_yield y, const DoutPrefixProvider *dpp) override;
    virtual int put_instance_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time mtime) override;
    virtual bool is_owner(RGWUser* user) override;
    virtual int check_empty(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int check_quota(RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
    virtual int set_instance_attrs(const DoutPrefixProvider *dpp, RGWAttrs& attrs, optional_yield y) override;
    virtual int try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime) override;
    virtual int read_usage(uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool *is_truncated, RGWUsageIter& usage_iter,
			   map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
    virtual std::unique_ptr<RGWBucket> clone() {
      return std::make_unique<RGWRadosBucket>(*this);
    }

    friend class RGWRadosStore;
};

class RGWRadosStore : public RGWStore {
  private:
    RGWRados *rados;
    RGWUserCtl *user_ctl;
    std::string luarocks_path;

  public:
    RGWRadosStore()
      : rados(nullptr) {
      }
    ~RGWRadosStore() {
      delete rados;
    }

    virtual std::unique_ptr<RGWUser> get_user(const rgw_user& u);
    virtual std::unique_ptr<RGWObject> get_object(const rgw_obj_key& k) override;
    virtual int get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const rgw_bucket& b, std::unique_ptr<RGWBucket>* bucket, optional_yield y) override;
    virtual int get_bucket(RGWUser* u, const RGWBucketInfo& i, std::unique_ptr<RGWBucket>* bucket) override;
    virtual int get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const std::string& tenant, const std::string&name, std::unique_ptr<RGWBucket>* bucket, optional_yield y) override;
    virtual int create_bucket(const DoutPrefixProvider *dpp, 
                            RGWUser& u, const rgw_bucket& b,
                            const std::string& zonegroup_id,
                            rgw_placement_rule& placement_rule,
                            std::string& swift_ver_location,
                            const RGWQuotaInfo * pquota_info,
                            const RGWAccessControlPolicy& policy,
			    RGWAttrs& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
			    bool exclusive,
			    bool obj_lock_enabled,
			    bool *existed,
			    req_info& req_info,
			    std::unique_ptr<RGWBucket>* bucket,
			    optional_yield y);
    virtual RGWBucketList* list_buckets(void) { return new RGWBucketList(); }
    virtual bool is_meta_master() override;
    virtual int forward_request_to_master(RGWUser* user, obj_version *objv,
					  bufferlist& in_data, JSONParser *jp, req_info& info,
					  optional_yield y) override;
    virtual int defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, RGWBucket* bucket, RGWObject* obj,
			 optional_yield y) override;
    virtual const RGWZoneGroup& get_zonegroup() override;
    virtual int get_zonegroup(const string& id, RGWZoneGroup& zonegroup) override;
    virtual int cluster_stat(RGWClusterStat& stats) override;
    virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
    virtual RGWLC* get_rgwlc(void) { return rados->get_lc(); }
    virtual int delete_raw_obj(const rgw_raw_obj& obj) override;
    virtual void get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj) override;
    virtual int get_raw_chunk_size(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, uint64_t* chunk_size) override;

    void setRados(RGWRados * st) { rados = st; }
    RGWRados *getRados(void) { return rados; }

    RGWServices *svc() { return &rados->svc; }
    const RGWServices *svc() const { return &rados->svc; }
    RGWCtl *ctl() { return &rados->ctl; }
    const RGWCtl *ctl() const { return &rados->ctl; }

    void setUserCtl(RGWUserCtl *_ctl) { user_ctl = _ctl; }

    void finalize(void) override;

    virtual CephContext *ctx(void) { return rados->ctx(); }


    int get_obj_head_ioctx(const RGWBucketInfo& bucket_info, const rgw_obj& obj,
			   librados::IoCtx *ioctx);

    const std::string& get_luarocks_path() const override {
      return luarocks_path;
    }

    void set_luarocks_path(const std::string& path) override {
      luarocks_path = path;
    }
};

class MPRadosSerializer : public MPSerializer {
  librados::IoCtx ioctx;
  rados::cls::lock::Lock lock;
  librados::ObjectWriteOperation op;

public:
  MPRadosSerializer(RGWRadosStore* store, RGWRadosObject* obj, const std::string& lock_name);

  virtual int try_lock(utime_t dur, optional_yield y) override;
  int unlock() {
    return lock.unlock(&ioctx, oid);
  }
};

class LCRadosSerializer : public LCSerializer {
  librados::IoCtx* ioctx;
  rados::cls::lock::Lock lock;
  const std::string oid;

public:
  LCRadosSerializer(RGWRadosStore* store, const std::string& oid, const std::string& lock_name, const std::string& cookie);

  virtual int try_lock(utime_t dur, optional_yield y) override;
  int unlock() {
    return lock.unlock(ioctx, oid);
  }
};

class RadosLifecycle : public Lifecycle {
  RGWRadosStore* store;

public:
  RadosLifecycle(RGWRadosStore* _st) : store(_st) {}

  virtual int get_entry(const string& oid, const std::string& marker, LCEntry& entry) override;
  virtual int get_next_entry(const string& oid, std::string& marker, LCEntry& entry) override;
  virtual int set_entry(const string& oid, const LCEntry& entry) override;
  virtual int list_entries(const string& oid, const string& marker,
			   uint32_t max_entries, vector<LCEntry>& entries) override;
  virtual int rm_entry(const string& oid, const LCEntry& entry) override;
  virtual int get_head(const string& oid, LCHead& head) override;
  virtual int put_head(const string& oid, const LCHead& head) override;
  virtual LCSerializer* get_serializer(const std::string& lock_name, const std::string& oid, const std::string& cookie) override;
};

} } // namespace rgw::sal

class RGWStoreManager {
public:
  RGWStoreManager() {}
  static rgw::sal::RGWRadosStore *get_storage(const DoutPrefixProvider *dpp, CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads,
			       bool run_sync_thread, bool run_reshard_thread, bool use_cache = true) {
    rgw::sal::RGWRadosStore *store = init_storage_provider(dpp, cct, use_gc_thread, use_lc_thread,
	quota_threads, run_sync_thread, run_reshard_thread, use_cache);
    return store;
  }
  static rgw::sal::RGWRadosStore *get_raw_storage(const DoutPrefixProvider *dpp, CephContext *cct) {
    rgw::sal::RGWRadosStore *rados = init_raw_storage_provider(dpp, cct);
    return rados;
  }
  static rgw::sal::RGWRadosStore *init_storage_provider(const DoutPrefixProvider *dpp, CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_metadata_cache);
  static rgw::sal::RGWRadosStore *init_raw_storage_provider(const DoutPrefixProvider *dpp, CephContext *cct);
  static void close_storage(rgw::sal::RGWRadosStore *store);

};
