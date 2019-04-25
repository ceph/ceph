
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

#include "rgw/rgw_service.h"

#include "svc_meta_be.h"

class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_Meta;
class RGWSI_SyncModules;
class RGWMetadataHandler;

struct rgw_cache_entry_info;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_User : public RGWServiceInstance
{
  friend class User;
  friend class PutOperation;

  RGWMetadataHandler *user_meta_handler;

  struct user_info_cache_entry {
    RGWUserInfo info;
    RGWObjVersionTracker objv_tracker;
    real_time mtime;
  };

  using RGWChainedCacheImpl_user_info_cache_entry = RGWChainedCacheImpl<user_info_cache_entry>;
  unique_ptr<RGWChainedCacheImpl_user_info_cache_entry> uinfo_cache;

  int get_user_info_from_index(RGWSI_MetaBackend::Context *ctx,
                               const string& key,
                               const rgw_pool& pool,
                               RGWUserInfo *info,
                               RGWObjVersionTracker * const objv_tracker,
                               real_time * const pmtime);

  int remove_uid_index(RGWSI_MetaBackend::Context *ctx, const RGWUserInfo& user_info, RGWObjVersionTracker *objv_tracker);

  int remove_key_index(RGWSI_MetaBackend::Context *ctx, const RGWAccessKey& access_key);
  int remove_email_index(RGWSI_MetaBackend::Context *ctx, const string& email);
  int remove_swift_name_index(RGWSI_MetaBackend::Context *ctx, const string& swift_name);

  int do_start() override;
public:
  struct Svc {
    RGWSI_User *user{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
    RGWSI_Meta *meta{nullptr};
    RGWSI_MetaBackend *meta_be{nullptr};
    RGWSI_SyncModules *sync_modules{nullptr};
  } svc;

  RGWSI_User(CephContext *cct);
  ~RGWSI_User();

  void init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
	    RGWSI_SysObj_Cache *_cache_svc, RGWSI_Meta *_meta_svc,
            RGWSI_MetaBackend *_meta_be_svc,
	    RGWSI_SyncModules *_sync_modules);

  static string get_meta_key(const rgw_user& user) {
    return user.to_str();
  }

  static string get_buckets_oid(const rgw_user& user_id) {
#define RGW_BUCKETS_OID_SUFFIX ".buckets"
    return user_id.to_str() + RGW_BUCKETS_OID_SUFFIX;
  }

#if 0
  class User {
    friend class Op;

    RGWSI_User::Svc& svc;
    RGWSysObjectCtx& ctx;
    rgw_user user;
    RGWUserInfo user_info;

  public:
    User(RGWSI_User *_user_svc,
         RGWSysObjectCtx& _ctx,
         const rgw_user& _user) : svc(_user_svc->svc),
                                  ctx(_ctx), user(_user) {}

    RGWSysObjectCtx& get_ctx() {
      return ctx;
    }

    rgw_user& get_user() {
      return user;
    }

    RGWUserInfo& get_user_info() {
      return user_info;
    }

    struct GetOp {
      User& source;

      ceph::real_time *pmtime{nullptr};
      map<string, bufferlist> *pattrs{nullptr};
      RGWUserInfo *pinfo{nullptr};
      RGWObjVersionTracker *objv_tracker{nullptr};


      GetOp& set_mtime(ceph::real_time *_mtime) {
        pmtime = _mtime;
        return *this;
      }

      GetOp& set_attrs(map<string, bufferlist> *_attrs) {
        pattrs = _attrs;
        return *this;
      }

      GetOp& set_pinfo(RGWUserInfo *_pinfo) {
        pinfo = _pinfo;
        return *this;
      }

      GetOp& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      GetOp(User& _source) : source(_source) {}

      int exec();
    };

    struct SetOp {
      User& source;

      ceph::real_time mtime;
      map<string, bufferlist> *pattrs{nullptr};
      bool exclusive{false};
      RGWUserInfo *old_info{nullptr};


      SetOp& set_mtime(const ceph::real_time& _mtime) {
        mtime = _mtime;
        return *this;
      }

      SetOp& set_attrs(map<string, bufferlist> *_attrs) {
        pattrs = _attrs;
        return *this;
      }

      SetOp& set_exclusive(bool _exclusive) {
        exclusive = _exclusive;
        return *this;
      }

      SetOp& set_old_info(RGWUserInfo *_old_info) {
        old_info = _old_info;
        return *this;
      }

      SetOp(User& _source) : source(_source) {}

      int exec();
    };

    GetOp get_op() {
      return GetOp(*this);
    }

    SetOp set_op() {
      return SetOp(*this);
    }
  };

  User user(RGWSysObjectCtx& _ctx,
            const rgw_user& _user);
#endif

  /* base svc_user interfaces */

  int read_user_info(RGWSI_MetaBackend::Context *ctx,
                     RGWUserInfo *info,
                     RGWObjVersionTracker * const objv_tracker,
                     real_time * const pmtime,
                     rgw_cache_entry_info * const cache_info,
                     map<string, bufferlist> * const pattrs,
                     optional_yield y);

  int store_user_info(RGWSI_MetaBackend::Context *ctx,
                      RGWUserInfo& info,
                      RGWUserInfo *old_info,
                      RGWObjVersionTracker *objv_tracker,
                      real_time& mtime,
                      bool exclusive,
                      map<string, bufferlist> *attrs,
                      optional_yield y);

  int remove_user_info(RGWSI_MetaBackend::Context *ctx,
                       const RGWUserInfo& info,
                       RGWObjVersionTracker *objv_tracker,
                       optional_yield y);

  int get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                             const string& email, RGWUserInfo *info,
                             RGWObjVersionTracker *objv_tracker,
                             real_time *pmtime,
                             optional_yield y);
  int get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                             const string& swift_name,
                             RGWUserInfo *info,        /* out */
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             optional_yield y);
  int get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& access_key,
                                  RGWUserInfo *info,
                                  RGWObjVersionTracker* objv_tracker,
                                  real_time *pmtime,
                                  optional_yield y);
};

