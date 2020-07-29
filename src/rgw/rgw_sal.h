// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_rados.h"
#include "rgw_user.h"

namespace rgw { namespace sal {

#define RGW_SAL_VERSION 1

class RGWUser;
class RGWBucket;
class RGWObject;
class RGWBucketList;

struct RGWAttrs {
  std::map<std::string, ceph::buffer::list> attrs;

  RGWAttrs() {}
  RGWAttrs(const std::map<std::string, ceph::buffer::list>&& _a) : attrs(std::move(_a)) {}
  RGWAttrs(const std::map<std::string, ceph::buffer::list>& _a) : attrs(_a) {}

  void emplace(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */
  }
  std::map<std::string, bufferlist>::iterator find(const std::string& key) {
    return attrs.find(key);
  }
};

class RGWStore : public DoutPrefixProvider {
  public:
    RGWStore() {}
    virtual ~RGWStore() = default;

    virtual std::unique_ptr<RGWUser> get_user(const rgw_user& u) = 0;
    virtual std::unique_ptr<RGWObject> get_object(const rgw_obj_key& k) = 0;
    virtual int get_bucket(RGWUser* u, const rgw_bucket& b, std::unique_ptr<RGWBucket>* bucket) = 0;
    virtual int get_bucket(RGWUser* u, const RGWBucketInfo& i, std::unique_ptr<RGWBucket>* bucket) = 0;
    virtual int get_bucket(RGWUser* u, const std::string& tenant, const std::string&name, std::unique_ptr<RGWBucket>* bucket) = 0;
    virtual int create_bucket(RGWUser& u, const rgw_bucket& b,
                            const string& zonegroup_id,
                            const rgw_placement_rule& placement_rule,
                            const string& swift_ver_location,
                            const RGWQuotaInfo * pquota_info,
			    map<std::string, bufferlist>& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
			    bool exclusive,
			    bool obj_lock_enabled,
			    bool *existed,
			    req_info& req_info,
			    std::unique_ptr<RGWBucket>* bucket) = 0;
    virtual RGWBucketList* list_buckets(void) = 0;

    virtual void finalize(void)=0;

    virtual CephContext *ctx(void)=0;
};

class RGWUser {
  protected:
    RGWUserInfo info;

  public:
    RGWUser() : info() {}
    RGWUser(const rgw_user& _u) : info() { info.user_id = _u; }
    RGWUser(const RGWUserInfo& _i) : info(_i) {}
    virtual ~RGWUser() = default;

    virtual int list_buckets(const string& marker, const string& end_marker,
			     uint64_t max, bool need_stats, RGWBucketList& buckets) = 0;
    virtual RGWBucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time) = 0;
    friend class RGWBucket;
    virtual std::string& get_display_name() { return info.display_name; }

    std::string& get_tenant() { return info.user_id.tenant; }
    const rgw_user& get_id() const { return info.user_id; }
    uint32_t get_type() const { return info.type; }
    int32_t get_max_buckets() const { return info.max_buckets; }
    const RGWUserCaps& get_caps() const { return info.caps; }

    /* Placeholders */
    virtual int load_by_id(optional_yield y) = 0;

    /* dang temporary; will be removed when User is complete */
    rgw_user& get_user() { return info.user_id; }
    RGWUserInfo& get_info() { return info; }
};

class RGWBucket {
  protected:
    RGWBucketEnt ent;
    RGWBucketInfo info;
    RGWUser* owner;
    RGWAttrs attrs;
    obj_version bucket_version;
    ceph::real_time mtime;

  public:
    RGWBucket() : ent(), info(), owner(nullptr), attrs(), bucket_version() {}
    RGWBucket(const rgw_bucket& _b) :
      ent(), info(), owner(nullptr), attrs(), bucket_version() { ent.bucket = _b; info.bucket = _b; }
    RGWBucket(const RGWBucketEnt& _e) :
      ent(_e), info(), owner(nullptr), attrs(), bucket_version() { info.bucket = ent.bucket; info.placement_rule = ent.placement_rule; }
    RGWBucket(const RGWBucketInfo& _i) :
      ent(), info(_i), owner(nullptr), attrs(), bucket_version() {ent.bucket = info.bucket; ent.placement_rule = info.placement_rule; }
    RGWBucket(const rgw_bucket& _b, RGWUser* _u) :
      ent(), info(), owner(_u), attrs(), bucket_version() { ent.bucket = _b; info.bucket = _b; }
    RGWBucket(const RGWBucketEnt& _e, RGWUser* _u) :
      ent(_e), info(), owner(_u), attrs(), bucket_version() { info.bucket = ent.bucket; info.placement_rule = ent.placement_rule; }
    RGWBucket(const RGWBucketInfo& _i, RGWUser* _u) :
      ent(), info(_i), owner(_u), attrs(), bucket_version() { ent.bucket = info.bucket;  ent.placement_rule = info.placement_rule;}
    virtual ~RGWBucket() = default;

    virtual int load_by_name(const std::string& tenant, const std::string& bucket_name, const std::string bucket_instance_id, RGWSysObjectCtx *rctx, optional_yield y) = 0;
    virtual std::unique_ptr<RGWObject> get_object(const rgw_obj_key& key) = 0;
    virtual RGWBucketList* list(void) = 0;
    virtual RGWObject* create_object(const rgw_obj_key& key /* Attributes */) = 0;
    virtual RGWAttrs& get_attrs(void) { return attrs; }
    virtual int set_attrs(RGWAttrs a) { attrs = a; return 0; }
    virtual int remove_bucket(bool delete_children, std::string prefix, std::string delimiter, optional_yield y) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(RGWAccessControlPolicy& acl, optional_yield y) = 0;
    virtual int get_bucket_info(optional_yield y) = 0;
    virtual int get_bucket_stats(RGWBucketInfo& bucket_info, int shard_id,
				 std::string *bucket_ver, std::string *master_ver,
				 std::map<RGWObjCategory, RGWStorageStats>& stats,
				 std::string *max_marker = nullptr,
				 bool *syncstopped = nullptr) = 0;
    virtual int read_bucket_stats(optional_yield y) = 0;
    virtual int sync_user_stats() = 0;
    virtual int update_container_stats(void) = 0;
    virtual int check_bucket_shards(void) = 0;
    virtual int link(RGWUser* new_user, optional_yield y) = 0;
    virtual int unlink(RGWUser* new_user, optional_yield y) = 0;
    virtual int chown(RGWUser* new_user, RGWUser* old_user, optional_yield y) = 0;
    virtual int put_instance_info(bool exclusive, ceph::real_time mtime) = 0;
    virtual bool is_owner(RGWUser* user) = 0;
    virtual int check_empty(optional_yield y) = 0;
    virtual int check_quota(RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size) = 0;

    bool empty() const { return info.bucket.name.empty(); }
    const std::string& get_name() const { return info.bucket.name; }
    const std::string& get_tenant() const { return info.bucket.tenant; }
    const std::string& get_marker() const { return info.bucket.marker; }
    const std::string& get_bucket_id() const { return info.bucket.bucket_id; }
    size_t get_size() const { return ent.size; }
    size_t get_size_rounded() const { return ent.size_rounded; }
    uint64_t get_count() const { return ent.count; }
    rgw_placement_rule& get_placement_rule() { return info.placement_rule; }
    ceph::real_time& get_creation_time() { return ent.creation_time; }
    ceph::real_time& get_modification_time() { return mtime; }
    obj_version& get_version() { return bucket_version; }
    void set_version(obj_version &ver) { bucket_version = ver; }
    std::string get_key() { return info.bucket.get_key(); }
    bool versioned() { return info.versioned(); }
    bool versioning_enabled() { return info.versioning_enabled(); }

    void convert(cls_user_bucket_entry *b) const {
      ent.convert(b);
    }

    static bool empty(RGWBucket* b) { return (!b || b->empty()); }
    virtual std::unique_ptr<RGWBucket> clone() = 0;

    /* dang - This is temporary, until the API is completed */
    rgw_bucket& get_bi() { return info.bucket; }
    RGWBucketInfo& get_info() { return info; }

    friend inline ostream& operator<<(ostream& out, const RGWBucket& b) {
      out << b.info.bucket;
      return out;
    }

    friend inline ostream& operator<<(ostream& out, const RGWBucket* b) {
      if (!b)
	out << "<NULL>";
      else
	out << b->info.bucket;
      return out;
    }

    friend inline ostream& operator<<(ostream& out, const std::unique_ptr<RGWBucket>& p) {
      out << p.get();
      return out;
    }


    friend class RGWBucketList;
  protected:
    virtual void set_ent(RGWBucketEnt& _ent) { ent = _ent; info.bucket = ent.bucket; info.placement_rule = ent.placement_rule; }
};


class RGWBucketList {
  std::map<std::string, std::unique_ptr<RGWBucket>> buckets;
  bool truncated;

public:
  RGWBucketList() : buckets(), truncated(false) {}
  RGWBucketList(RGWBucketList&& _bl) :
    buckets(std::move(_bl.buckets)),
    truncated(_bl.truncated)
    { }
  RGWBucketList& operator=(const RGWBucketList&) = delete;
  RGWBucketList& operator=(RGWBucketList&& _bl) {
    for (auto& ent : _bl.buckets) {
      buckets.emplace(ent.first, std::move(ent.second));
    }
    truncated = _bl.truncated;
    return *this;
  };

  map<string, std::unique_ptr<RGWBucket>>& get_buckets() { return buckets; }
  bool is_truncated(void) const { return truncated; }
  void set_truncated(bool trunc) { truncated = trunc; }
  void add(std::unique_ptr<RGWBucket> bucket) {
    buckets.emplace(bucket->info.bucket.name, std::move(bucket));
  }
  size_t count() const { return buckets.size(); }
  void clear(void) {
    buckets.clear();
    truncated = false;
  }
};

class RGWObject {
  protected:
    rgw_obj_key key;
    RGWBucket* bucket;
    std::string index_hash_source;
    uint64_t obj_size;
    ceph::real_time mtime;

  public:
    RGWObject() : key(), bucket(nullptr), index_hash_source(), obj_size(), mtime() {}
    RGWObject(const rgw_obj_key& _k) : key(_k), bucket(), index_hash_source(), obj_size(), mtime() {}
    RGWObject(const rgw_obj_key& _k, RGWBucket* _b) : key(_k), bucket(_b), index_hash_source(), obj_size(), mtime() {}
    RGWObject(const RGWObject& _o) = default;

    virtual ~RGWObject() = default;

    virtual int read(off_t offset, off_t length, std::iostream& stream) = 0;
    virtual int write(off_t offset, off_t length, std::iostream& stream) = 0;
    virtual RGWAttrs& get_attrs(void) = 0;
    virtual int set_attrs(RGWAttrs& attrs) = 0;
    virtual int delete_object(void) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(const RGWAccessControlPolicy& acl) = 0;
    virtual void set_atomic(RGWObjectCtx *rctx) const = 0;
    virtual void set_prefetch_data(RGWObjectCtx *rctx) = 0;

    bool empty() const { return key.empty(); }
    const std::string &get_name() const { return key.name; }

    virtual int get_obj_state(RGWObjectCtx *rctx, RGWBucket& bucket, RGWObjState **state, optional_yield y, bool follow_olh = false) = 0;
    virtual int get_obj_attrs(RGWObjectCtx *rctx, optional_yield y, rgw_obj *target_obj = nullptr) = 0;
    virtual int modify_obj_attrs(RGWObjectCtx *rctx, const char *attr_name, bufferlist& attr_val, optional_yield y) = 0;
    virtual int delete_obj_attrs(RGWObjectCtx *rctx, const char *attr_name, optional_yield y) = 0;
    virtual int copy_obj_data(RGWObjectCtx& rctx, RGWBucket* dest_bucket, RGWObject* dest_obj, uint16_t olh_epoch, std::string* petag, const DoutPrefixProvider *dpp, optional_yield y) = 0;

    ceph::real_time get_mtime(void) const { return mtime; }
    uint64_t get_obj_size(void) const { return obj_size; }
    RGWBucket* get_bucket(void) const { return bucket; }
    void set_bucket(RGWBucket* b) { bucket = b; }
    std::string get_hash_source(void) { return index_hash_source; }
    void set_hash_source(std::string s) { index_hash_source = s; }
    std::string get_oid(void) const { return key.get_oid(); }

    static bool empty(RGWObject* o) { return (!o || o->empty()); }
    virtual std::unique_ptr<RGWObject> clone() = 0;

    /* dang - Not sure if we want this, but it simplifies things a lot */
    void set_obj_size(uint64_t s) { obj_size = s; }
    virtual void set_name(const string& n) { key = n; }
    virtual void set_key(const rgw_obj_key& k) { key = k; }
    virtual rgw_obj get_obj(void) const { return rgw_obj(bucket->get_bi(), key); }
    virtual void gen_rand_obj_instance_name() = 0;

    /* dang - This is temporary, until the API is completed */
    rgw_obj_key& get_key() { return key; }
    void set_instance(const std::string &i) { key.set_instance(i); }
    const std::string &get_instance() const { return key.instance; }
    bool have_instance(void) { return key.have_instance(); }

    friend inline ostream& operator<<(ostream& out, const RGWObject& o) {
      out << o.key;
      return out;
    }
    friend inline ostream& operator<<(ostream& out, const RGWObject* o) {
      if (!o)
	out << "<NULL>";
      else
	out << o->key;
      return out;
    }
    friend inline ostream& operator<<(ostream& out, const std::unique_ptr<RGWObject>& p) {
      out << p.get();
      return out;
    }
};


class RGWRadosStore;

class RGWRadosUser : public RGWUser {
  private:
    RGWRadosStore *store;

  public:
    RGWRadosUser(RGWRadosStore *_st, const rgw_user& _u) : RGWUser(_u), store(_st) { }
    RGWRadosUser(RGWRadosStore *_st, const RGWUserInfo& _i) : RGWUser(_i), store(_st) { }
    RGWRadosUser(RGWRadosStore *_st) : store(_st) { }
    RGWRadosUser() {}

    int list_buckets(const string& marker, const string& end_marker,
				uint64_t max, bool need_stats, RGWBucketList& buckets);
    RGWBucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time);

    /* Placeholders */
    virtual int load_by_id(optional_yield y);

    friend class RGWRadosBucket;
};

class RGWRadosObject : public RGWObject {
  private:
    RGWRadosStore *store;
    RGWAttrs attrs;
    RGWAccessControlPolicy acls;

  public:
    RGWRadosObject()
      : store(),
	attrs(),
        acls() {
    }

    RGWRadosObject(RGWRadosStore *_st, const rgw_obj_key& _k)
      : RGWObject(_k),
	store(_st),
	attrs(),
        acls() {
    }
    RGWRadosObject(RGWRadosStore *_st, const rgw_obj_key& _k, RGWBucket* _b)
      : RGWObject(_k, _b),
	store(_st),
	attrs(),
        acls() {
    }
    RGWRadosObject(const RGWRadosObject& _o) = default;

    int read(off_t offset, off_t length, std::iostream& stream) { return length; }
    int write(off_t offset, off_t length, std::iostream& stream) { return length; }
    RGWAttrs& get_attrs(void) { return attrs; }
    int set_attrs(RGWAttrs& a) { attrs = a; return 0; }
    int delete_object(void) { return 0; }
    RGWAccessControlPolicy& get_acl(void) { return acls; }
    int set_acl(const RGWAccessControlPolicy& acl) { acls = acl; return 0; }
    virtual void set_atomic(RGWObjectCtx *rctx) const;
    virtual void set_prefetch_data(RGWObjectCtx *rctx);

    virtual int get_obj_state(RGWObjectCtx *rctx, RGWBucket& bucket, RGWObjState **state, optional_yield y, bool follow_olh = true);
    virtual int get_obj_attrs(RGWObjectCtx *rctx, optional_yield y, rgw_obj *target_obj = nullptr);
    virtual int modify_obj_attrs(RGWObjectCtx *rctx, const char *attr_name, bufferlist& attr_val, optional_yield y);
    virtual int delete_obj_attrs(RGWObjectCtx *rctx, const char *attr_name, optional_yield y);
    virtual int copy_obj_data(RGWObjectCtx& rctx, RGWBucket* dest_bucket, RGWObject* dest_obj, uint16_t olh_epoch, std::string* petag, const DoutPrefixProvider *dpp, optional_yield y);
    virtual void gen_rand_obj_instance_name() override;
    virtual std::unique_ptr<RGWObject> clone() {
      return std::unique_ptr<RGWObject>(new RGWRadosObject(*this));
    }

  private:
    int read_attrs(RGWRados::Object::Read &read_op, optional_yield y, rgw_obj *target_obj = nullptr);
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

    virtual int load_by_name(const std::string& tenant, const std::string& bucket_name, const std::string bucket_instance_id, RGWSysObjectCtx *rctx, optional_yield y) override;
    virtual std::unique_ptr<RGWObject> get_object(const rgw_obj_key& k) override;
    RGWBucketList* list(void) { return new RGWBucketList(); }
    RGWObject* create_object(const rgw_obj_key& key /* Attributes */) override;
    virtual int remove_bucket(bool delete_children, std::string prefix, std::string delimiter, optional_yield y) override;
    RGWAccessControlPolicy& get_acl(void) { return acls; }
    virtual int set_acl(RGWAccessControlPolicy& acl, optional_yield y) override;
    virtual int get_bucket_info(optional_yield y) override;
    virtual int get_bucket_stats(RGWBucketInfo& bucket_info, int shard_id,
				 std::string *bucket_ver, std::string *master_ver,
				 std::map<RGWObjCategory, RGWStorageStats>& stats,
				 std::string *max_marker = nullptr,
				 bool *syncstopped = nullptr) override;
    virtual int read_bucket_stats(optional_yield y) override;
    virtual int sync_user_stats() override;
    virtual int update_container_stats(void) override;
    virtual int check_bucket_shards(void) override;
    virtual int link(RGWUser* new_user, optional_yield y) override;
    virtual int unlink(RGWUser* new_user, optional_yield y) override;
    virtual int chown(RGWUser* new_user, RGWUser* old_user, optional_yield y) override;
    virtual int put_instance_info(bool exclusive, ceph::real_time mtime) override;
    virtual bool is_owner(RGWUser* user) override;
    virtual int check_empty(optional_yield y) override;
    virtual int check_quota(RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size) override;
    virtual std::unique_ptr<RGWBucket> clone() {
      return std::unique_ptr<RGWBucket>(new RGWRadosBucket(*this));
    }

    friend class RGWRadosStore;
};

class RGWRadosStore : public RGWStore {
  private:
    RGWRados *rados;
    RGWUserCtl *user_ctl;

  public:
    RGWRadosStore()
      : rados(nullptr) {
      }
    ~RGWRadosStore() {
      delete rados;
    }

    virtual std::unique_ptr<RGWUser> get_user(const rgw_user& u);
    virtual std::unique_ptr<RGWObject> get_object(const rgw_obj_key& k) override;
    virtual int get_bucket(RGWUser* u, const rgw_bucket& b, std::unique_ptr<RGWBucket>* bucket) override;
    virtual int get_bucket(RGWUser* u, const RGWBucketInfo& i, std::unique_ptr<RGWBucket>* bucket) override;
    virtual int get_bucket(RGWUser* u, const std::string& tenant, const std::string&name, std::unique_ptr<RGWBucket>* bucket) override;
    virtual int create_bucket(RGWUser& u, const rgw_bucket& b,
                            const string& zonegroup_id,
                            const rgw_placement_rule& placement_rule,
                            const string& swift_ver_location,
                            const RGWQuotaInfo * pquota_info,
			    map<std::string, bufferlist>& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
			    bool exclusive,
			    bool obj_lock_enabled,
			    bool *existed,
			    req_info& req_info,
			    std::unique_ptr<RGWBucket>* bucket);
    virtual RGWBucketList* list_buckets(void) { return new RGWBucketList(); }

    void setRados(RGWRados * st) { rados = st; }
    RGWRados *getRados(void) { return rados; }

    RGWServices *svc() { return &rados->svc; }
    RGWCtl *ctl() { return &rados->ctl; }

    void setUserCtl(RGWUserCtl *_ctl) { user_ctl = _ctl; }

    void finalize(void) override;

    virtual CephContext *ctx(void) { return rados->ctx(); }

    // implements DoutPrefixProvider
    std::ostream& gen_prefix(std::ostream& out) const { return out << "RGWRadosStore "; }
    CephContext* get_cct() const override { return rados->ctx(); }
    unsigned get_subsys() const override { return ceph_subsys_rgw; }

  private:
    int forward_request_to_master(RGWUser* user, obj_version *objv,
				  bufferlist& in_data, JSONParser *jp, req_info& info);

};

} } // namespace rgw::sal


class RGWStoreManager {
public:
  RGWStoreManager() {}
  static rgw::sal::RGWRadosStore *get_storage(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads,
			       bool run_sync_thread, bool run_reshard_thread, bool use_cache = true) {
    rgw::sal::RGWRadosStore *store = init_storage_provider(cct, use_gc_thread, use_lc_thread,
	quota_threads, run_sync_thread, run_reshard_thread, use_cache);
    return store;
  }
  static rgw::sal::RGWRadosStore *get_raw_storage(CephContext *cct) {
    rgw::sal::RGWRadosStore *rados = init_raw_storage_provider(cct);
    return rados;
  }
  static rgw::sal::RGWRadosStore *init_storage_provider(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_metadata_cache);
  static rgw::sal::RGWRadosStore *init_raw_storage_provider(CephContext *cct);
  static void close_storage(rgw::sal::RGWRadosStore *store);

};
