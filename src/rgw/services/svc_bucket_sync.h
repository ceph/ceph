
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
#include "svc_bucket_types.h"

class RGWBucketSyncPolicyHandler;
using RGWBucketSyncPolicyHandlerRef = std::shared_ptr<RGWBucketSyncPolicyHandler>;

class RGWSI_Zone;
class RGWSI_SysObj_Cache;
class RGWSI_Bucket_SObj;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_Bucket_Sync_HintIndexManager;

struct rgw_sync_bucket_entity;


class RGWSI_Bucket_Sync : public RGWServiceInstance
{
  struct bucket_sync_policy_cache_entry {
    std::shared_ptr<RGWBucketSyncPolicyHandler> handler;
  };

  std::unique_ptr<RGWChainedCacheImpl<bucket_sync_policy_cache_entry> > sync_policy_cache;

  std::unique_ptr<RGWSI_Bucket_Sync_HintIndexManager> hint_index_mgr;

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;

  struct optional_zone_bucket {
    std::optional<rgw_zone_id> zone;
    std::optional<rgw_bucket> bucket;

    optional_zone_bucket(const std::optional<rgw_zone_id>& zone,
                         const std::optional<rgw_bucket>& bucket)
      : zone(zone), bucket(bucket) {}

    bool operator <(const optional_zone_bucket& ozb) const {
      if (zone < ozb.zone) {
        return true;
      }
      if (zone > ozb.zone) {
        return false;
      }
      return bucket < ozb.bucket;
    }
  };

  void get_hint_entities(const std::set<rgw_zone_id>& zone_names,
                         const std::set<rgw_bucket>& buckets,
                         std::set<rgw_sync_bucket_entity> *hint_entities,
                         optional_yield y, const DoutPrefixProvider *);
  int resolve_policy_hints(rgw_sync_bucket_entity& self_entity,
                           RGWBucketSyncPolicyHandlerRef& handler,
                           RGWBucketSyncPolicyHandlerRef& zone_policy_handler,
                           std::map<optional_zone_bucket, RGWBucketSyncPolicyHandlerRef>& temp_map,
                           optional_yield y,
                           const DoutPrefixProvider *dpp);
  int do_get_policy_handler(std::optional<rgw_zone_id> zone,
                            std::optional<rgw_bucket> _bucket,
                            std::map<optional_zone_bucket, RGWBucketSyncPolicyHandlerRef>& temp_map,
                            RGWBucketSyncPolicyHandlerRef *handler,
                            optional_yield y,
                            const DoutPrefixProvider *dpp);

public:

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
  } svc;

  RGWBucketCtl *bucketctl{nullptr};
  void set_bucketctl(RGWBucketCtl* _bucketctl) {
    bucketctl = _bucketctl;
  }


  RGWSI_Bucket_Sync(CephContext* cct);
  ~RGWSI_Bucket_Sync();

  void init(RGWSI_Zone *_zone_svc,
            RGWSI_SysObj *_sysobj_svc,
            RGWSI_SysObj_Cache *_cache_svc);

  int get_policy_handler(std::optional<rgw_zone_id> zone,
                         std::optional<rgw_bucket> bucket,
                         RGWBucketSyncPolicyHandlerRef *handler,
                         optional_yield y,
                         const DoutPrefixProvider *dpp);

  int handle_bi_update(const DoutPrefixProvider *dpp,
                       RGWBucketInfo& bucket_info,
                       RGWBucketInfo *orig_bucket_info,
                       optional_yield y);
  int handle_bi_removal(const DoutPrefixProvider *dpp,
                        const RGWBucketInfo& bucket_info,
                        optional_yield y);

  int get_bucket_sync_hints(const DoutPrefixProvider *dpp,
                            const rgw_bucket& bucket,
                            std::set<rgw_bucket> *sources,
                            std::set<rgw_bucket> *dests,
                            optional_yield y);
};


