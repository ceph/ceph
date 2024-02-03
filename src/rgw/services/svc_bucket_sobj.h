
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
#include "svc_bucket_types.h"
#include "svc_bucket.h"
#include "svc_bucket_sync.h"

class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_Meta;
class RGWSI_SyncModules;

struct rgw_cache_entry_info;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_Bucket_SObj : public RGWSI_Bucket
{
  struct bucket_info_cache_entry {
    RGWBucketInfo info;
    real_time mtime;
    std::map<std::string, bufferlist> attrs;
  };

  using RGWChainedCacheImpl_bucket_info_cache_entry = RGWChainedCacheImpl<bucket_info_cache_entry>;
  std::unique_ptr<RGWChainedCacheImpl_bucket_info_cache_entry> binfo_cache;

  RGWSI_Bucket_BE_Handler ep_be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> ep_be_module;
  RGWSI_BucketInstance_BE_Handler bi_be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> bi_be_module;

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;

  int do_read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                   const std::string& key,
                                   RGWBucketInfo *info,
                                   real_time *pmtime,
                                   std::map<std::string, bufferlist> *pattrs,
                                   rgw_cache_entry_info *cache_info,
                                   boost::optional<obj_version> refresh_version,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp);

  int read_bucket_stats(const RGWBucketInfo& bucket_info,
                        RGWBucketEnt *ent,
                        optional_yield y,
                        const DoutPrefixProvider *dpp);

public:
  struct Svc {
    RGWSI_Bucket_SObj *bucket{nullptr};
    RGWSI_BucketIndex *bi{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
    RGWSI_Meta *meta{nullptr};
    RGWSI_MetaBackend *meta_be{nullptr};
    RGWSI_SyncModules *sync_modules{nullptr};
    RGWSI_Bucket_Sync *bucket_sync{nullptr};
  } svc;

  RGWSI_Bucket_SObj(CephContext *cct);
  ~RGWSI_Bucket_SObj();

  RGWSI_Bucket_BE_Handler& get_ep_be_handler() override {
    return ep_be_handler;
  }

  RGWSI_BucketInstance_BE_Handler& get_bi_be_handler() override {
    return bi_be_handler;
  }

  void init(RGWSI_Zone *_zone_svc,
            RGWSI_SysObj *_sysobj_svc,
	    RGWSI_SysObj_Cache *_cache_svc,
            RGWSI_BucketIndex *_bi,
            RGWSI_Meta *_meta_svc,
            RGWSI_MetaBackend *_meta_be_svc,
	    RGWSI_SyncModules *_sync_modules_svc,
	    RGWSI_Bucket_Sync *_bucket_sync_svc);


  int read_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                  const std::string& key,
                                  RGWBucketEntryPoint *entry_point,
                                  RGWObjVersionTracker *objv_tracker,
                                  real_time *pmtime,
                                  std::map<std::string, bufferlist> *pattrs,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp,
                                  rgw_cache_entry_info *cache_info = nullptr,
                                  boost::optional<obj_version> refresh_version = boost::none) override;

  int store_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                   const std::string& key,
                                   RGWBucketEntryPoint& info,
                                   bool exclusive,
                                   real_time mtime,
                                   const std::map<std::string, bufferlist> *pattrs,
                                   RGWObjVersionTracker *objv_tracker,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp) override;

  int remove_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                    const std::string& key,
                                    RGWObjVersionTracker *objv_tracker,
                                    optional_yield y,
                                    const DoutPrefixProvider *dpp) override;

  int read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                const std::string& key,
                                RGWBucketInfo *info,
                                real_time *pmtime,
                                std::map<std::string, bufferlist> *pattrs,
                                optional_yield y,
                                const DoutPrefixProvider *dpp,
                                rgw_cache_entry_info *cache_info = nullptr,
                                boost::optional<obj_version> refresh_version = boost::none) override;

  int read_bucket_info(RGWSI_Bucket_X_Ctx& ep_ctx,
                       const rgw_bucket& bucket,
                       RGWBucketInfo *info,
                       real_time *pmtime,
                       std::map<std::string, bufferlist> *pattrs,
                       boost::optional<obj_version> refresh_version,
                       optional_yield y,
                       const DoutPrefixProvider *dpp) override;

  int store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                 const std::string& key,
                                 RGWBucketInfo& info,
                                 std::optional<RGWBucketInfo *> orig_info, /* nullopt: orig_info was not fetched,
                                                                              nullptr: orig_info was not found (new bucket instance */
                                 bool exclusive,
                                 real_time mtime,
                                 const std::map<std::string, bufferlist> *pattrs,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp) override;

  int remove_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                  const std::string& key,
                                  const RGWBucketInfo& bucket_info,
                                  RGWObjVersionTracker *objv_tracker,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp) override;

  int read_bucket_stats(RGWSI_Bucket_X_Ctx& ctx,
                        const rgw_bucket& bucket,
                        RGWBucketEnt *ent,
                        optional_yield y,
                        const DoutPrefixProvider *dpp) override;

  int read_buckets_stats(RGWSI_Bucket_X_Ctx& ctx,
                         std::map<std::string, RGWBucketEnt>& m,
                         optional_yield y,
                         const DoutPrefixProvider *dpp) override;
};

