
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
#include "svc_bucket_types.h"
#include "svc_bucket_sync.h"

class RGWSI_Zone;
class RGWSI_SysObj_Cache;
class RGWSI_Bucket_SObj;

template <class T>
class RGWChainedCacheImpl;

class RGWBucketSyncPolicyHandler;

class RGWSI_Bucket_Sync_SObj : public RGWSI_Bucket_Sync
{
  struct bucket_sync_policy_cache_entry {
    std::shared_ptr<RGWBucketSyncPolicyHandler> handler;
  };

  using RGWChainedCacheImpl_bucket_sync_policy_cache_entry = RGWChainedCacheImpl<bucket_sync_policy_cache_entry>;
  unique_ptr<RGWChainedCacheImpl_bucket_sync_policy_cache_entry> sync_policy_cache;

  int do_start() override;

public:
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
    RGWSI_Bucket_SObj *bucket_sobj{nullptr};
  } svc;

  RGWSI_Bucket_Sync_SObj(CephContext *cct) : RGWSI_Bucket_Sync(cct) {}
  ~RGWSI_Bucket_Sync_SObj();

  void init(RGWSI_Zone *_zone_svc,
            RGWSI_SysObj_Cache *_cache_svc,
            RGWSI_Bucket_SObj *_bucket_sobj_svc);


  int get_policy_handler(RGWSI_Bucket_BI_Ctx& ctx,
                         const rgw_bucket& bucket,
                         std::shared_ptr<RGWBucketSyncPolicyHandler> *handler,
                         optional_yield y) override;
};

