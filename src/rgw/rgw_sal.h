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

typedef std::map<std::string, ceph::bufferlist> RGWAttrs;

class RGWStore {
  public:
    RGWStore() {}
    virtual ~RGWStore() = default;

    virtual RGWUser* get_user(const rgw_user& u) = 0;
    virtual int get_bucket(RGWUser& u, const rgw_bucket& b, RGWBucket** bucket) = 0;
    //virtual RGWBucket* create_bucket(RGWUser& u, const rgw_bucket& b) = 0;
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
    virtual RGWBucket* add_bucket(rgw_bucket& bucket, ceph::real_time creation_time) = 0;
    friend class RGWBucket;
    virtual std::string& get_display_name() { return info.display_name; }

    std::string& get_tenant() { return info.user_id.tenant; }
    const rgw_user& get_id() const { return info.user_id; }
    uint32_t get_type() const { return info.type; }
    int32_t get_max_buckets() const { return info.max_buckets; }
    const RGWUserCaps& get_caps() const { return info.caps; }


    /* xxx dang temporary; will be removed when User is complete */
    rgw_user& get_user() { return info.user_id; }
    RGWUserInfo& get_info() { return info; }
};

class RGWBucket {
  protected:
    RGWBucketEnt ent;
    RGWBucketInfo info;
    RGWUser *owner;
    RGWAttrs attrs;

  public:
    RGWBucket() : ent(), owner(nullptr), attrs() {}
    RGWBucket(const rgw_bucket& _b) : ent(), attrs() { ent.bucket = _b; }
    RGWBucket(const RGWBucketEnt& _e) : ent(_e), attrs() {}
    virtual ~RGWBucket() = default;

    virtual RGWObject* get_object(const rgw_obj_key& key) = 0;
    virtual RGWBucketList* list(void) = 0;
    virtual RGWObject* create_object(const rgw_obj_key& key /* Attributes */) = 0;
    virtual RGWAttrs& get_attrs(void) { return attrs; }
    virtual int set_attrs(RGWAttrs& a) { attrs = a; return 0; }
    virtual int remove_bucket(bool delete_children, optional_yield y) = 0;
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
    virtual bool is_owner(RGWUser *user) = 0;

    const std::string& get_name() const { return ent.bucket.name; }
    const std::string& get_tenant() const { return ent.bucket.tenant; }
    const std::string& get_marker() const { return ent.bucket.marker; }
    const std::string& get_bucket_id() const { return ent.bucket.bucket_id; }
    size_t get_size() const { return ent.size; }
    size_t get_size_rounded() const { return ent.size_rounded; }
    uint64_t get_count() const { return ent.count; }
    rgw_placement_rule get_placement_rule() const { return ent.placement_rule; }
    ceph::real_time& get_creation_time() { return ent.creation_time; };

    void convert(cls_user_bucket_entry *b) const {
      ent.convert(b);
    }

    /* dang - This is temporary, until the API is completed */
    rgw_bucket& get_bi() { return ent.bucket; }
    RGWBucketInfo& get_info() { return info; }

    friend inline ostream& operator<<(ostream& out, const RGWBucket& b) {
      out << b.ent.bucket;
      return out;
    }


    friend class RGWBucketList;
  protected:
    virtual void set_ent(RGWBucketEnt& _ent) { ent = _ent; }
};

class RGWBucketList {
  std::map<std::string, RGWBucket*> buckets;
  bool truncated;

public:
  RGWBucketList() : buckets(), truncated(false) {}
  RGWBucketList(RGWBucketList&&) = default;
  RGWBucketList& operator=(const RGWBucketList&) = default;
  ~RGWBucketList();

  map<string, RGWBucket*>& get_buckets() { return buckets; }
  bool is_truncated(void) const { return truncated; }
  void set_truncated(bool trunc) { truncated = trunc; }
  void add(RGWBucket* bucket) {
    buckets[bucket->ent.bucket.name] = bucket;
  }
  size_t count() const { return buckets.size(); }
  void clear() { buckets.clear(); truncated = false; }
}; // class RGWBucketList

class RGWObject {
  protected:
    rgw_obj_key key;

  public:
    RGWObject() : key() {}
    RGWObject(const rgw_obj_key& _k) : key(_k) {}
    virtual ~RGWObject() = default;

    virtual int read(off_t offset, off_t length, std::iostream& stream) = 0;
    virtual int write(off_t offset, off_t length, std::iostream& stream) = 0;
    virtual RGWAttrs& get_attrs(void) = 0;
    virtual int set_attrs(RGWAttrs& attrs) = 0;
    virtual int delete_object(void) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(const RGWAccessControlPolicy& acl) = 0;
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
    RGWBucket* add_bucket(rgw_bucket& bucket, ceph::real_time creation_time);

    /* Placeholders */
    int get_by_id(rgw_user id, optional_yield y);

    friend class RGWRadosBucket;
};

class RGWRadosObject : public RGWObject {
  private:
    RGWRadosStore *store;
    RGWAttrs attrs;
    RGWAccessControlPolicy acls;

  public:
    RGWRadosObject()
      : attrs(),
        acls() {
    }

    RGWRadosObject(RGWRadosStore *_st, const rgw_obj_key& _k)
      : RGWObject(_k),
	store(_st),
	attrs(),
        acls() {
    }

    int read(off_t offset, off_t length, std::iostream& stream) { return length; }
    int write(off_t offset, off_t length, std::iostream& stream) { return length; }
    RGWAttrs& get_attrs(void) { return attrs; }
    int set_attrs(RGWAttrs& a) { attrs = a; return 0; }
    int delete_object(void) { return 0; }
    RGWAccessControlPolicy& get_acl(void) { return acls; }
    int set_acl(const RGWAccessControlPolicy& acl) { acls = acl; return 0; }
};

class RGWRadosBucket : public RGWBucket {
  private:
    RGWRadosStore *store;
    RGWRadosObject *object;
    RGWAccessControlPolicy acls;
    RGWRadosUser user;

  public:
    RGWRadosBucket()
      : store(nullptr),
        object(nullptr),
        acls(),
	user() {
    }

    RGWRadosBucket(RGWRadosStore *_st, RGWUser& _u, const rgw_bucket& _b)
      : RGWBucket(_b),
	store(_st),
	object(nullptr),
        acls(),
	user(dynamic_cast<RGWRadosUser&>(_u)) {
    }

    RGWRadosBucket(RGWRadosStore *_st, RGWUser& _u, const RGWBucketEnt& _e)
      : RGWBucket(_e),
	store(_st),
	object(nullptr),
        acls(),
	user(dynamic_cast<RGWRadosUser&>(_u)) {
    }

    ~RGWRadosBucket() { }

    RGWObject* get_object(const rgw_obj_key& key) { return object; }
    RGWBucketList* list(void) { return new RGWBucketList(); }
    RGWObject* create_object(const rgw_obj_key& key /* Attributes */) override;
    virtual int remove_bucket(bool delete_children, optional_yield y) override;
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
    virtual bool is_owner(RGWUser *user) override;
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

    virtual RGWUser* get_user(const rgw_user& u);
    virtual int get_bucket(RGWUser& u, const rgw_bucket& b, RGWBucket** bucket) override;
    //virtual RGWBucket* create_bucket(RGWUser& u, const rgw_bucket& b);
    virtual RGWBucketList* list_buckets(void) { return new RGWBucketList(); }

    void setRados(RGWRados * st) { rados = st; }
    RGWRados *getRados(void) { return rados; }

    RGWServices *svc() { return &rados->svc; }
    RGWCtl *ctl() { return &rados->ctl; }

    void setUserCtl(RGWUserCtl *_ctl) { user_ctl = _ctl; }

    void finalize(void) override;

    virtual CephContext *ctx(void) { return rados->ctx(); }
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
