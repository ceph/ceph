
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

#include "rgw/rgw_service.h"

#include "svc_meta_be.h"
#include "svc_user.h"

class RGWSI_RADOS;
class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_Meta;
class RGWSI_SyncModules;
class RGWSI_MetaBackend_Handler;

struct rgw_cache_entry_info;

class RGWUserBuckets;

class RGWGetUserHeader_CB;
class RGWGetUserStats_CB;

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

  rgw_raw_obj get_buckets_obj(const rgw_user& user_id, bool identity_type_role = false) const;

  int get_user_info_from_index(RGWSI_MetaBackend::Context *ctx,
                               const string& key,
                               const rgw_pool& pool,
                               RGWUserInfo *info,
                               RGWObjVersionTracker * const objv_tracker,
                               real_time * const pmtime,
                               optional_yield y);

  int remove_uid_index(RGWSI_MetaBackend::Context *ctx, const RGWUserInfo& user_info, RGWObjVersionTracker *objv_tracker,
                       optional_yield y);

  int remove_key_index(RGWSI_MetaBackend::Context *ctx, const RGWAccessKey& access_key, optional_yield y);
  int remove_email_index(RGWSI_MetaBackend::Context *ctx, const string& email, optional_yield y);
  int remove_swift_name_index(RGWSI_MetaBackend::Context *ctx, const string& swift_name, optional_yield y);

  /* admin management */
  int cls_user_update_buckets(rgw_raw_obj& obj, list<cls_user_bucket_entry>& entries, bool add);
  int cls_user_add_bucket(rgw_raw_obj& obj, const cls_user_bucket_entry& entry);
  int cls_user_remove_bucket(rgw_raw_obj& obj, const cls_user_bucket& bucket);

  /* quota stats */
  int cls_user_flush_bucket_stats(rgw_raw_obj& user_obj,
                                  const RGWBucketEnt& ent);
  int cls_user_list_buckets(rgw_raw_obj& obj,
                            const string& in_marker,
                            const string& end_marker,
                            const int max_entries,
                            list<cls_user_bucket_entry>& entries,
                            string * const out_marker,
                            bool * const truncated);

  int cls_user_reset_stats(const rgw_user& user);
  int cls_user_get_header(const rgw_user& user, cls_user_header *header);
  int cls_user_get_header_async(const string& user, RGWGetUserHeader_CB *cb);

  int do_start() override;
public:
  struct Svc {
    RGWSI_User_RADOS *user{nullptr};
    RGWSI_RADOS *rados{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
    RGWSI_Meta *meta{nullptr};
    RGWSI_MetaBackend *meta_be{nullptr};
    RGWSI_SyncModules *sync_modules{nullptr};
  } svc;

  RGWSI_User_RADOS(CephContext *cct);
  ~RGWSI_User_RADOS();

  void init(RGWSI_RADOS *_rados_svc,
            RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
	    RGWSI_SysObj_Cache *_cache_svc, RGWSI_Meta *_meta_svc,
            RGWSI_MetaBackend *_meta_be_svc,
	    RGWSI_SyncModules *_sync_modules);

  RGWSI_MetaBackend_Handler *get_be_handler() override {
    return be_handler;
  }

  int read_user_info(RGWSI_MetaBackend::Context *ctx,
                     const rgw_user& user,
                     RGWUserInfo *info,
                     RGWObjVersionTracker * const objv_tracker,
                     real_time * const pmtime,
                     rgw_cache_entry_info * const cache_info,
                     map<string, bufferlist> * const pattrs,
                     optional_yield y) override;

  int store_user_info(RGWSI_MetaBackend::Context *ctx,
                      const RGWUserInfo& info,
                      RGWUserInfo *old_info,
                      RGWObjVersionTracker *objv_tracker,
                      const real_time& mtime,
                      bool exclusive,
                      map<string, bufferlist> *attrs,
                      optional_yield y) override;

  int remove_user_info(RGWSI_MetaBackend::Context *ctx,
                       const RGWUserInfo& info,
                       RGWObjVersionTracker *objv_tracker,
                       optional_yield y) override;

  int get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                             const string& email, RGWUserInfo *info,
                             RGWObjVersionTracker *objv_tracker,
                             real_time *pmtime,
                             optional_yield y) override;
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
                                  optional_yield y) override;

  /* user buckets directory */

  int add_bucket(RGWSI_MetaBackend::Context *ctx,
                 const rgw_user& user,
                 const rgw_bucket& bucket,
                 ceph::real_time creation_time,
                 bool identity_type_role = false) override;
  int remove_bucket(RGWSI_MetaBackend::Context *ctx,
                    const rgw_user& user,
                    const rgw_bucket& _bucket,
                    bool identity_type_role = false) override;
  int list_buckets(RGWSI_MetaBackend::Context *ctx,
                   const rgw_user& user,
                   const string& marker,
                   const string& end_marker,
                   uint64_t max,
                   RGWUserBuckets *buckets,
                   bool *is_truncated,
                   bool identity_type_role = false) override;

  /* quota related */
  int flush_bucket_stats(RGWSI_MetaBackend::Context *ctx,
                         const rgw_user& user,
                         const RGWBucketEnt& ent,
                         bool identity_type_role = false) override;

  int complete_flush_stats(RGWSI_MetaBackend::Context *ctx,
			   const rgw_user& user) override;

  int reset_bucket_stats(RGWSI_MetaBackend::Context *ctx,
			 const rgw_user& user) override;
  int read_stats(RGWSI_MetaBackend::Context *ctx,
		 const rgw_user& user, RGWStorageStats *stats,
		 ceph::real_time *last_stats_sync,              /* last time a full stats sync completed */
		 ceph::real_time *last_stats_update) override;  /* last time a stats update was done */

  int read_stats_async(RGWSI_MetaBackend::Context *ctx,
		       const rgw_user& user, RGWGetUserStats_CB *cb) override;
};

