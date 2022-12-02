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

#include "rgw_service.h"

#include "svc_meta_be.h"
#include "svc_user.h"

#include "driver/rados/rgw_bucket.h" // FIXME: subclass dependency

class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_Meta;
class RGWSI_SyncModules;
class RGWSI_MetaBackend_Handler;

struct rgw_cache_entry_info;

class RGWGetUserHeader_CB;

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
  std::unique_ptr<RGWChainedCacheImpl_user_info_cache_entry> uinfo_cache;

  rgw_raw_obj get_buckets_obj(const rgw_user& user_id) const;

  int get_user_info_from_index(RGWSI_MetaBackend::Context *ctx,
                               const std::string& key,
                               const rgw_pool& pool,
                               RGWUserInfo *info,
                               RGWObjVersionTracker * const objv_tracker,
                               real_time * const pmtime,
                               optional_yield y,
                               const DoutPrefixProvider *dpp);

  int remove_uid_index(RGWSI_MetaBackend::Context *ctx, const RGWUserInfo& user_info, RGWObjVersionTracker *objv_tracker,
                       optional_yield y, const DoutPrefixProvider *dpp);

  int remove_key_index(const DoutPrefixProvider *dpp, const RGWAccessKey& access_key, optional_yield y);
  int remove_email_index(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y);
  int remove_swift_name_index(const DoutPrefixProvider *dpp, const std::string& swift_name, optional_yield y);

  /* admin management */
  int cls_user_update_buckets(const DoutPrefixProvider *dpp, rgw_raw_obj& obj, std::list<cls_user_bucket_entry>& entries, bool add, optional_yield y);
  int cls_user_add_bucket(const DoutPrefixProvider *dpp, rgw_raw_obj& obj, const cls_user_bucket_entry& entry, optional_yield y);
  int cls_user_remove_bucket(const DoutPrefixProvider *dpp, rgw_raw_obj& obj, const cls_user_bucket& bucket, optional_yield y);

  /* quota stats */
  int cls_user_flush_bucket_stats(const DoutPrefixProvider *dpp, rgw_raw_obj& user_obj,
                                  const RGWBucketEnt& ent, optional_yield y);
  int cls_user_list_buckets(const DoutPrefixProvider *dpp, 
                            rgw_raw_obj& obj,
                            const std::string& in_marker,
                            const std::string& end_marker,
                            const int max_entries,
                            std::list<cls_user_bucket_entry>& entries,
                            std::string * const out_marker,
                            bool * const truncated,
                            optional_yield y);

  int cls_user_reset_stats(const DoutPrefixProvider *dpp, const rgw_user& user, optional_yield y);
  int cls_user_get_header(const DoutPrefixProvider *dpp, const rgw_user& user, cls_user_header *header, optional_yield y);
  int cls_user_get_header_async(const DoutPrefixProvider *dpp, const std::string& user, RGWGetUserHeader_CB *cb);

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;
public:
  librados::Rados* rados{nullptr};

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

  void init(librados::Rados* rados_,
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
                     std::map<std::string, bufferlist> * const pattrs,
                     optional_yield y,
                     const DoutPrefixProvider *dpp) override;

  int store_user_info(RGWSI_MetaBackend::Context *ctx,
                      const RGWUserInfo& info,
                      RGWUserInfo *old_info,
                      RGWObjVersionTracker *objv_tracker,
                      const real_time& mtime,
                      bool exclusive,
                      std::map<std::string, bufferlist> *attrs,
                      optional_yield y,
                      const DoutPrefixProvider *dpp) override;

  int remove_user_info(RGWSI_MetaBackend::Context *ctx,
                       const RGWUserInfo& info,
                       RGWObjVersionTracker *objv_tracker,
                       optional_yield y,
                       const DoutPrefixProvider *dpp) override;

  int get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                             const std::string& email, RGWUserInfo *info,
                             RGWObjVersionTracker *objv_tracker,
                             real_time *pmtime,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) override;
  int get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                             const std::string& swift_name,
                             RGWUserInfo *info,        /* out */
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) override;
  int get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& access_key,
                                  RGWUserInfo *info,
                                  RGWObjVersionTracker* objv_tracker,
                                  real_time *pmtime,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp) override;

  /* user buckets directory */

  int add_bucket(const DoutPrefixProvider *dpp, 
                 const rgw_user& user,
                 const rgw_bucket& bucket,
                 ceph::real_time creation_time,
                 optional_yield y) override;
  int remove_bucket(const DoutPrefixProvider *dpp, 
                    const rgw_user& user,
                    const rgw_bucket& _bucket,
                    optional_yield y) override;
  int list_buckets(const DoutPrefixProvider *dpp, 
                   const rgw_user& user,
                   const std::string& marker,
                   const std::string& end_marker,
                   uint64_t max,
                   RGWUserBuckets *buckets,
                   bool *is_truncated,
                   optional_yield y) override;

  /* quota related */
  int flush_bucket_stats(const DoutPrefixProvider *dpp, 
                         const rgw_user& user,
                         const RGWBucketEnt& ent, optional_yield y) override;

  int complete_flush_stats(const DoutPrefixProvider *dpp, 
			   const rgw_user& user, optional_yield y) override;

  int reset_bucket_stats(const DoutPrefixProvider *dpp, 
			 const rgw_user& user,
                         optional_yield y) override;
  int read_stats(const DoutPrefixProvider *dpp, 
                 RGWSI_MetaBackend::Context *ctx,
		 const rgw_user& user, RGWStorageStats *stats,
		 ceph::real_time *last_stats_sync,              /* last time a full stats sync completed */
		 ceph::real_time *last_stats_update,
                 optional_yield y) override;  /* last time a stats update was done */

  int read_stats_async(const DoutPrefixProvider *dpp, const rgw_user& user,
                       boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb) override;
};

