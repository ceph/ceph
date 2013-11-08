// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "include/utime.h"
#include "common/lru_map.h"
#include "common/RefCountedObj.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_quota.h"

#define dout_subsys ceph_subsys_rgw


struct RGWQuotaBucketStats {
  RGWBucketStats stats;
  utime_t expiration;
  utime_t async_refresh_time;
};

class RGWBucketStatsCache {
  RGWRados *store;
  lru_map<rgw_bucket, RGWQuotaBucketStats> stats_map;
  RefCountedWaitObject *async_refcount;

  int fetch_bucket_totals(rgw_bucket& bucket, RGWBucketStats& stats);

public:
  RGWBucketStatsCache(RGWRados *_store) : store(_store), stats_map(store->ctx()->_conf->rgw_bucket_quota_cache_size) {
    async_refcount = new RefCountedWaitObject;
  }
  ~RGWBucketStatsCache() {
    async_refcount->put_wait(); /* wait for all pending async requests to complete */
  }

  int get_bucket_stats(rgw_bucket& bucket, RGWBucketStats& stats, RGWQuotaInfo& quota);
  void adjust_bucket_stats(rgw_bucket& bucket, int objs_delta, uint64_t added_bytes, uint64_t removed_bytes);

  bool can_use_cached_stats(RGWQuotaInfo& quota, RGWBucketStats& stats);

  void set_stats(rgw_bucket& bucket, RGWQuotaBucketStats& qs, RGWBucketStats& stats);
  int async_refresh(rgw_bucket& bucket, RGWQuotaBucketStats& qs);
  void async_refresh_response(rgw_bucket& bucket, RGWBucketStats& stats);
};

bool RGWBucketStatsCache::can_use_cached_stats(RGWQuotaInfo& quota, RGWBucketStats& cached_stats)
{
  if (quota.max_size_kb >= 0) {
    if (quota.max_size_soft_threshold < 0) {
      quota.max_size_soft_threshold = quota.max_size_kb * store->ctx()->_conf->rgw_bucket_quota_soft_threshold;
    }

    if (cached_stats.num_kb_rounded >= (uint64_t)quota.max_size_soft_threshold) {
      ldout(store->ctx(), 20) << "quota: can't use cached stats, exceeded soft threshold (size): "
        << cached_stats.num_kb_rounded << " >= " << quota.max_size_soft_threshold << dendl;
      return false;
    }
  }

  if (quota.max_objects >= 0) {
    if (quota.max_objs_soft_threshold < 0) {
      quota.max_objs_soft_threshold = quota.max_objects * store->ctx()->_conf->rgw_bucket_quota_soft_threshold;
    }

    if (cached_stats.num_objects >= (uint64_t)quota.max_objs_soft_threshold) {
      ldout(store->ctx(), 20) << "quota: can't use cached stats, exceeded soft threshold (num objs): "
        << cached_stats.num_objects << " >= " << quota.max_objs_soft_threshold << dendl;
      return false;
    }
  }

  return true;
}

int RGWBucketStatsCache::fetch_bucket_totals(rgw_bucket& bucket, RGWBucketStats& stats)
{
  RGWBucketInfo bucket_info;

  uint64_t bucket_ver;
  uint64_t master_ver;

  map<RGWObjCategory, RGWBucketStats> bucket_stats;
  int r = store->get_bucket_stats(bucket, &bucket_ver, &master_ver, bucket_stats, NULL);
  if (r < 0) {
    ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket.name << dendl;
    return r;
  }

  stats = RGWBucketStats();

  map<RGWObjCategory, RGWBucketStats>::iterator iter;
  for (iter = bucket_stats.begin(); iter != bucket_stats.end(); ++iter) {
    RGWBucketStats& s = iter->second;
    stats.num_kb += s.num_kb;
    stats.num_kb_rounded += s.num_kb_rounded;
    stats.num_objects += s.num_objects;
  }

  return 0;
}

class AsyncRefreshHandler : public RGWGetBucketStats_CB {
  RGWRados *store;
  RGWBucketStatsCache *cache;
public:
  AsyncRefreshHandler(RGWRados *_store, RGWBucketStatsCache *_cache, rgw_bucket& _bucket) : RGWGetBucketStats_CB(_bucket), store(_store), cache(_cache) {}

  int init_fetch();

  void handle_response(int r);
};


int AsyncRefreshHandler::init_fetch()
{
  ldout(store->ctx(), 20) << "initiating async quota refresh for bucket=" << bucket << dendl;
  int r = store->get_bucket_stats_async(bucket, this);
  if (r < 0) {
    ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket.name << dendl;

    /* get_bucket_stats_async() dropped our reference already */
    return r;
  }

  return 0;
}

void AsyncRefreshHandler::handle_response(int r)
{
  if (r < 0) {
    ldout(store->ctx(), 20) << "AsyncRefreshHandler::handle_response() r=" << r << dendl;
    return; /* nothing to do here */
  }

  RGWBucketStats bs;

  map<RGWObjCategory, RGWBucketStats>::iterator iter;
  for (iter = stats->begin(); iter != stats->end(); ++iter) {
    RGWBucketStats& s = iter->second;
    bs.num_kb += s.num_kb;
    bs.num_kb_rounded += s.num_kb_rounded;
    bs.num_objects += s.num_objects;
  }

  cache->async_refresh_response(bucket, bs);
}

class RGWBucketStatsAsyncTestSet : public lru_map<rgw_bucket, RGWQuotaBucketStats>::UpdateContext {
  int objs_delta;
  uint64_t added_bytes;
  uint64_t removed_bytes;
public:
  RGWBucketStatsAsyncTestSet() {}
  bool update(RGWQuotaBucketStats *entry) {
    if (entry->async_refresh_time.sec() == 0)
      return false;

    entry->async_refresh_time = utime_t(0, 0);

    return true;
  }
};

int RGWBucketStatsCache::async_refresh(rgw_bucket& bucket, RGWQuotaBucketStats& qs)
{
  /* protect against multiple updates */
  RGWBucketStatsAsyncTestSet test_update;
  if (!stats_map.find_and_update(bucket, NULL, &test_update)) {
    /* most likely we just raced with another update */
    return 0;
  }

  async_refcount->get();

  AsyncRefreshHandler *handler = new AsyncRefreshHandler(store, this, bucket);

  int ret = handler->init_fetch();
  if (ret < 0) {
    async_refcount->put();
    handler->put();
    return ret;
  }

  return 0;
}

void RGWBucketStatsCache::async_refresh_response(rgw_bucket& bucket, RGWBucketStats& stats)
{
  ldout(store->ctx(), 20) << "async stats refresh response for bucket=" << bucket << dendl;

  RGWQuotaBucketStats qs;

  stats_map.find(bucket, qs);

  set_stats(bucket, qs, stats);

  async_refcount->put();
}

void RGWBucketStatsCache::set_stats(rgw_bucket& bucket, RGWQuotaBucketStats& qs, RGWBucketStats& stats)
{
  qs.stats = stats;
  qs.expiration = ceph_clock_now(store->ctx());
  qs.async_refresh_time = qs.expiration;
  qs.expiration += store->ctx()->_conf->rgw_bucket_quota_ttl;
  qs.async_refresh_time += store->ctx()->_conf->rgw_bucket_quota_ttl / 2;

  stats_map.add(bucket, qs);
}

int RGWBucketStatsCache::get_bucket_stats(rgw_bucket& bucket, RGWBucketStats& stats, RGWQuotaInfo& quota) {
  RGWQuotaBucketStats qs;
  utime_t now = ceph_clock_now(store->ctx());
  if (stats_map.find(bucket, qs)) {
    if (qs.async_refresh_time.sec() > 0 && now >= qs.async_refresh_time) {
      int r = async_refresh(bucket, qs);
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: quota async refresh returned ret=" << r << dendl;

        /* continue processing, might be a transient error, async refresh is just optimization */
      }
    }

    if (can_use_cached_stats(quota, qs.stats) && qs.expiration > ceph_clock_now(store->ctx())) {
      stats = qs.stats;
      return 0;
    }
  }

  int ret = fetch_bucket_totals(bucket, stats);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  set_stats(bucket, qs, stats);

  return 0;
}


class RGWBucketStatsUpdate : public lru_map<rgw_bucket, RGWQuotaBucketStats>::UpdateContext {
  int objs_delta;
  uint64_t added_bytes;
  uint64_t removed_bytes;
public:
  RGWBucketStatsUpdate(int _objs_delta, uint64_t _added_bytes, uint64_t _removed_bytes) : 
                    objs_delta(_objs_delta), added_bytes(_added_bytes), removed_bytes(_removed_bytes) {}
  bool update(RGWQuotaBucketStats *entry) {
    uint64_t rounded_kb_added = rgw_rounded_kb(added_bytes);
    uint64_t rounded_kb_removed = rgw_rounded_kb(removed_bytes);

    entry->stats.num_kb_rounded += (rounded_kb_added - rounded_kb_removed);
    entry->stats.num_kb += (added_bytes - removed_bytes) / 1024;
    entry->stats.num_objects += objs_delta;

    return true;
  }
};


void RGWBucketStatsCache::adjust_bucket_stats(rgw_bucket& bucket, int objs_delta, uint64_t added_bytes, uint64_t removed_bytes)
{
  RGWBucketStatsUpdate update(objs_delta, added_bytes, removed_bytes);
  stats_map.find_and_update(bucket, NULL, &update);
}


class RGWQuotaHandlerImpl : public RGWQuotaHandler {
  RGWRados *store;
  RGWBucketStatsCache stats_cache;
public:
  RGWQuotaHandlerImpl(RGWRados *_store) : store(_store), stats_cache(_store) {}
  virtual int check_quota(rgw_bucket& bucket, RGWQuotaInfo& bucket_quota,
			  uint64_t num_objs, uint64_t size) {
    uint64_t size_kb = rgw_rounded_kb(size);
    if (!bucket_quota.enabled) {
      return 0;
    }

    RGWBucketStats stats;

    int ret = stats_cache.get_bucket_stats(bucket, stats, bucket_quota);
    if (ret < 0)
      return ret;

    ldout(store->ctx(), 20) << "bucket quota: max_objects=" << bucket_quota.max_objects
                            << " max_size_kb=" << bucket_quota.max_size_kb << dendl;

    if (bucket_quota.max_objects >= 0 &&
        stats.num_objects + num_objs > (uint64_t)bucket_quota.max_objects) {
      ldout(store->ctx(), 10) << "quota exceeded: stats.num_objects=" << stats.num_objects
                              << " bucket_quota.max_objects=" << bucket_quota.max_objects << dendl;

      return -ERR_QUOTA_EXCEEDED;
    }
    if (bucket_quota.max_size_kb >= 0 &&
               stats.num_kb_rounded + size_kb > (uint64_t)bucket_quota.max_size_kb) {
      ldout(store->ctx(), 10) << "quota exceeded: stats.num_kb_rounded=" << stats.num_kb_rounded << " size_kb=" << size_kb
                              << " bucket_quota.max_size_kb=" << bucket_quota.max_size_kb << dendl;
      return -ERR_QUOTA_EXCEEDED;
    }

    return 0;
  }

  virtual void update_stats(rgw_bucket& bucket, int obj_delta, uint64_t added_bytes, uint64_t removed_bytes) {
    stats_cache.adjust_bucket_stats(bucket, obj_delta, added_bytes, removed_bytes);
  };
};


RGWQuotaHandler *RGWQuotaHandler::generate_handler(RGWRados *store)
{
  return new RGWQuotaHandlerImpl(store);
};

void RGWQuotaHandler::free_handler(RGWQuotaHandler *handler)
{
  delete handler;
}
