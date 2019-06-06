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
class RGWSalBucket;
class RGWObject;

typedef std::vector<RGWSalBucket> RGWBucketList;
typedef std::map<string, string> RGWAttrs;

class RGWStore {
  public:
    RGWStore() {}
    virtual ~RGWStore() = default;

    virtual RGWUser* get_user(const rgw_user &u) = 0;
    virtual RGWSalBucket* get_bucket(RGWUser &u, const cls_user_bucket &b) = 0;
    virtual RGWSalBucket* create_bucket(RGWUser &u, const cls_user_bucket &b) = 0;
    virtual RGWBucketList* list_buckets(void) = 0;

    virtual void finalize(void)=0;

    virtual CephContext *ctx(void)=0;
};

class RGWUser {
  protected:
    rgw_user user;

  public:
    RGWUser() : user() {}
    RGWUser(const rgw_user &_u) : user(_u) {}
    virtual ~RGWUser() = default;

    virtual RGWBucketList* list_buckets(void) = 0;
};

class RGWSalBucket {
  protected:
    cls_user_bucket ub;

  public:
    RGWSalBucket() : ub() {}
    RGWSalBucket(const cls_user_bucket &_b) : ub(_b) {}
    virtual ~RGWSalBucket() = default;

    virtual RGWObject* get_object(const rgw_obj_key &key) = 0;
    virtual RGWBucketList* list(void) = 0;
    virtual RGWObject* create_object(const rgw_obj_key &key /* Attributes */) = 0;
    virtual RGWAttrs& get_attrs(void) = 0;
    virtual int set_attrs(RGWAttrs &attrs) = 0;
    virtual int delete_bucket(void) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(const RGWAccessControlPolicy &acl) = 0;
};

class RGWObject {
  protected:
    rgw_obj_key key;

  public:
    RGWObject() : key() {}
    RGWObject(const rgw_obj_key &_k) : key(_k) {}
    virtual ~RGWObject() = default;

    virtual int read(off_t offset, off_t length, std::iostream &stream) = 0;
    virtual int write(off_t offset, off_t length, std::iostream &stream) = 0;
    virtual RGWAttrs& get_attrs(void) = 0;
    virtual int set_attrs(RGWAttrs &attrs) = 0;
    virtual int delete_object(void) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(const RGWAccessControlPolicy &acl) = 0;
};


class RGWRadosStore;

class RGWRadosUser : public RGWUser {
  private:
    RGWRadosStore *store;

  public:
    RGWRadosUser(RGWRadosStore *_st, const rgw_user &_u) : RGWUser(_u), store(_st) { }
    RGWRadosUser() {}

    RGWBucketList* list_buckets(void) { return new RGWBucketList(); }
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

    RGWRadosObject(RGWRadosStore *_st, const rgw_obj_key &_k)
      : RGWObject(_k),
	store(_st),
	attrs(),
        acls() {
    }

    int read(off_t offset, off_t length, std::iostream &stream) { return length; }
    int write(off_t offset, off_t length, std::iostream &stream) { return length; }
    RGWAttrs& get_attrs(void) { return attrs; }
    int set_attrs(RGWAttrs &a) { attrs = a; return 0; }
    int delete_object(void) { return 0; }
    RGWAccessControlPolicy& get_acl(void) { return acls; }
    int set_acl(const RGWAccessControlPolicy &acl) { acls = acl; return 0; }
};

class RGWRadosBucket : public RGWSalBucket {
  private:
    RGWRadosStore *store;
    RGWRadosObject *object;
    RGWAttrs attrs;
    RGWAccessControlPolicy acls;
    RGWRadosUser user;

  public:
    RGWRadosBucket()
      : object(nullptr),
        attrs(),
        acls(),
	user() {
    }

    RGWRadosBucket(RGWRadosStore *_st, RGWUser &_u, const cls_user_bucket &_b)
      : RGWSalBucket(_b),
	store(_st),
	object(nullptr),
        attrs(),
        acls(),
	user(dynamic_cast<RGWRadosUser&>(_u)) {
    }

    RGWObject* get_object(const rgw_obj_key &key) { return object; }
    RGWBucketList* list(void) { return new RGWBucketList(); }
    RGWObject* create_object(const rgw_obj_key &key /* Attributes */) override;
    RGWAttrs& get_attrs(void) { return attrs; }
    int set_attrs(RGWAttrs &a) { attrs = a; return 0; }
    int delete_bucket(void) { return 0; }
    RGWAccessControlPolicy& get_acl(void) { return acls; }
    int set_acl(const RGWAccessControlPolicy &acl) { acls = acl; return 0; }
};

class RGWRadosStore : public RGWStore {
  private:
    RGWRados *rados;
    RGWRadosUser *user;
    RGWRadosBucket *bucket;

  public:
    RGWRadosStore()
      : rados(nullptr),
        user(nullptr),
        bucket(nullptr) {
      }
    ~RGWRadosStore() {
	if (bucket)
	    delete bucket;
	if (user)
	    delete user;
	if (rados)
	    delete rados;
    }

    virtual RGWUser* get_user(const rgw_user &u);
    virtual RGWSalBucket* get_bucket(RGWUser &u, const cls_user_bucket &b) { return bucket; }
    virtual RGWSalBucket* create_bucket(RGWUser &u, const cls_user_bucket &b);
    virtual RGWBucketList* list_buckets(void) { return new RGWBucketList(); }

    void setRados(RGWRados * st) { rados = st; }
    RGWRados *getRados(void) { return rados; }

    RGWServices *svc() { return &rados->svc; }
    RGWCtl *ctl() { return &rados->ctl; }

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
