
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
#include "svc_user_rados.h"

class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_Meta;
class RGWSI_SyncModules;
class RGWSI_MetaBackend_Handler;

struct rgw_cache_entry_info;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_User_RADOS : public RGWSI_User
{
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

  rgw_raw_obj get_buckets_obj(const rgw_user& user_id) const;

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
    RGWSI_User_RADOS *user{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
    RGWSI_Meta *meta{nullptr};
    RGWSI_MetaBackend *meta_be{nullptr};
    RGWSI_SyncModules *sync_modules{nullptr};
  } svc;

  RGWSI_User_RADOS(CephContext *cct);
  ~RGWSI_User_RADOS();

  void init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
	    RGWSI_SysObj_Cache *_cache_svc, RGWSI_Meta *_meta_svc,
            RGWSI_MetaBackend *_meta_be_svc,
	    RGWSI_SyncModules *_sync_modules);

  RGWSI_MetaBackend_Handler *get_be_handler() {
    return be_handler;
  }

  int read_user_info(RGWSI_MetaBackend::Context *ctx,
                     const rgw_user& user,
                     RGWUserInfo *info,
                     RGWObjVersionTracker * const objv_tracker,
                     real_time * const pmtime,
                     rgw_cache_entry_info * const cache_info,
                     map<string, bufferlist> * const pattrs) override;

  int store_user_info(RGWSI_MetaBackend::Context *ctx,
                      const RGWUserInfo& info,
                      RGWUserInfo *old_info,
                      RGWObjVersionTracker *objv_tracker,
                      const real_time& mtime,
                      bool exclusive,
                      map<string, bufferlist> *attrs) override;

  int remove_user_info(RGWSI_MetaBackend::Context *ctx,
                       const RGWUserInfo& info,
                       RGWObjVersionTracker *objv_tracker) override;

  int get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                             const string& email, RGWUserInfo *info,
                             RGWObjVersionTracker *objv_tracker,
                             real_time *pmtime) override;
  int get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                             const string& swift_name,
                             RGWUserInfo *info,        /* out */
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime);
  int get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& access_key,
                                  RGWUserInfo *info,
                                  RGWObjVersionTracker* objv_tracker,
                                  real_time *pmtime) override;

  int add_bucket(RGWSI_MetaBackend::Context *ctx,
                 const rgw_user& user,
                 const rgw_bucket& bucket,
                 ceph::real_time creation_time);

};

