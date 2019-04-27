
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
class RGWSI_MetaBackend_Handler;

struct rgw_cache_entry_info;

template <class T>
class RGWChainedCacheImpl;

/* FIXME:
 * split RGWSI_User to base class and RGWSI_User_SObj
 */

class RGWSI_User : public RGWServiceInstance
{
  friend class User;
  friend class PutOperation;

  std::unique_ptr<RGWSI_MetaBackend::Module> be_module;

  RGWSI_MetaBackend_Handler *be_handler;

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

  RGWSI_MetaBackend_Handler *get_be_handler() {
    return be_handler;
  }

  static string get_meta_key(const rgw_user& user) {
    return user.to_str();
  }

  static rgw_user user_from_meta_key(const string& key) {
    return rgw_user(key);
  }

  /* base svc_user interfaces */

  int read_user_info(RGWSI_MetaBackend::Context *ctx,
                     const rgw_user& user,
                     RGWUserInfo *info,
                     RGWObjVersionTracker * const objv_tracker,
                     real_time * const pmtime,
                     rgw_cache_entry_info * const cache_info,
                     map<string, bufferlist> * const pattrs,
                     optional_yield y);

  int store_user_info(RGWSI_MetaBackend::Context *ctx,
                      const RGWUserInfo& info,
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

