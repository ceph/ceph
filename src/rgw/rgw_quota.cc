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


struct RGWQuotaCacheStats {
  RGWStorageStats stats;
  utime_t expiration;
  utime_t async_refresh_time;
};

template<class T>
class RGWQuotaCache {
protected:
  RGWRados *store;
  lru_map<T, RGWQuotaCacheStats> stats_map;
  RefCountedWaitObject *async_refcount;

  class StatsAsyncTestSet : public lru_map<rgw_bucket, RGWQuotaCacheStats>::UpdateContext {
    int objs_delta;
    uint64_t added_bytes;
    uint64_t removed_bytes;
  public:
    StatsAsyncTestSet() {}
    bool update(RGWQuotaCacheStats *entry) {
      if (entry->async_refresh_time.sec() == 0)
        return false;

      entry->async_refresh_time = utime_t(0, 0);

      return true;
    }
  };

  virtual int fetch_stats_from_storage(const string& user, rgw_bucket& bucket, RGWStorageStats& stats) = 0;

  virtual bool map_find(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs) = 0;
  virtual bool map_find_and_update(const string& user, rgw_bucket& bucket, lru_map<rgw_bucket, RGWQuotaCacheStats>::UpdateContext *ctx) = 0;
  virtual void map_add(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs) = 0;
public:
  RGWQuotaCache(RGWRados *_store, int size) : store(_store), stats_map(size) {
    async_refcount = new RefCountedWaitObject;
  }
  virtual ~RGWQuotaCache() {
    async_refcount->put_wait(); /* wait for all pending async requests to complete */
  }

  int get_stats(const string& user, rgw_bucket& bucket, RGWStorageStats& stats, RGWQuotaInfo& quota);
  void adjust_stats(const string& user, rgw_bucket& bucket, int objs_delta, uint64_t added_bytes, uint64_t removed_bytes);

  bool can_use_cached_stats(RGWQuotaInfo& quota, RGWStorageStats& stats);

  void set_stats(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs, RGWStorageStats& stats);
  int async_refresh(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs);
  void async_refresh_response(const string& user, rgw_bucket& bucket, RGWStorageStats& stats);

  class AsyncRefreshHandler {
  protected:
    RGWRados *store;
    RGWQuotaCache<T> *cache;
  public:
    AsyncRefreshHandler(RGWRados *_store, RGWQuotaCache<T> *_cache) : store(_store), cache(_cache) {}
    virtual ~AsyncRefreshHandler() {}

    virtual int init_fetch() = 0;
    virtual void drop_reference() = 0;
  };

  virtual AsyncRefreshHandler *allocate_refresh_handler(const string& user, rgw_bucket& bucket) = 0;
};

template<class T>
bool RGWQuotaCache<T>::can_use_cached_stats(RGWQuotaInfo& quota, RGWStorageStats& cached_stats)
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

template<class T>
int RGWQuotaCache<T>::async_refresh(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs)
{
  /* protect against multiple updates */
  StatsAsyncTestSet test_update;
  if (!map_find_and_update(user, bucket, &test_update)) {
    /* most likely we just raced with another update */
    return 0;
  }

  async_refcount->get();


  AsyncRefreshHandler *handler = allocate_refresh_handler(user, bucket);

  int ret = handler->init_fetch();
  if (ret < 0) {
    async_refcount->put();
    handler->drop_reference();
    return ret;
  }

  return 0;
}

template<class T>
void RGWQuotaCache<T>::async_refresh_response(const string& user, rgw_bucket& bucket, RGWStorageStats& stats)
{
  ldout(store->ctx(), 20) << "async stats refresh response for bucket=" << bucket << dendl;

  RGWQuotaCacheStats qs;

  map_find(user, bucket, qs);

  set_stats(user, bucket, qs, stats);

  async_refcount->put();
}

template<class T>
void RGWQuotaCache<T>::set_stats(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs, RGWStorageStats& stats)
{
  qs.stats = stats;
  qs.expiration = ceph_clock_now(store->ctx());
  qs.async_refresh_time = qs.expiration;
  qs.expiration += store->ctx()->_conf->rgw_bucket_quota_ttl;
  qs.async_refresh_time += store->ctx()->_conf->rgw_bucket_quota_ttl / 2;

  map_add(user, bucket, qs);
}

template<class T>
int RGWQuotaCache<T>::get_stats(const string& user, rgw_bucket& bucket, RGWStorageStats& stats, RGWQuotaInfo& quota) {
  RGWQuotaCacheStats qs;
  utime_t now = ceph_clock_now(store->ctx());
  if (map_find(user, bucket, qs)) {
    if (qs.async_refresh_time.sec() > 0 && now >= qs.async_refresh_time) {
      int r = async_refresh(user, bucket, qs);
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

  int ret = fetch_stats_from_storage(user, bucket, stats);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  set_stats(user, bucket, qs, stats);

  return 0;
}


class RGWQuotaStatsUpdate : public lru_map<rgw_bucket, RGWQuotaCacheStats>::UpdateContext {
  int objs_delta;
  uint64_t added_bytes;
  uint64_t removed_bytes;
public:
  RGWQuotaStatsUpdate(int _objs_delta, uint64_t _added_bytes, uint64_t _removed_bytes) : 
                    objs_delta(_objs_delta), added_bytes(_added_bytes), removed_bytes(_removed_bytes) {}
  bool update(RGWQuotaCacheStats *entry) {
    uint64_t rounded_kb_added = rgw_rounded_kb(added_bytes);
    uint64_t rounded_kb_removed = rgw_rounded_kb(removed_bytes);

    entry->stats.num_kb_rounded += (rounded_kb_added - rounded_kb_removed);
    entry->stats.num_kb += (added_bytes - removed_bytes) / 1024;
    entry->stats.num_objects += objs_delta;

    return true;
  }
};


template<class T>
void RGWQuotaCache<T>::adjust_stats(const string& user, rgw_bucket& bucket, int objs_delta,
                                 uint64_t added_bytes, uint64_t removed_bytes)
{
  RGWQuotaStatsUpdate update(objs_delta, added_bytes, removed_bytes);
  map_find_and_update(user, bucket, &update);
}

class BucketAsyncRefreshHandler : public RGWQuotaCache<rgw_bucket>::AsyncRefreshHandler,
                                  public RGWGetBucketStats_CB {
  string user;
public:
  BucketAsyncRefreshHandler(RGWRados *_store, RGWQuotaCache<rgw_bucket> *_cache,
                            const string& _user, rgw_bucket& _bucket) :
                                      RGWQuotaCache<rgw_bucket>::AsyncRefreshHandler(_store, _cache),
                                      RGWGetBucketStats_CB(_bucket), user(_user) {}

  void drop_reference() { put(); }
  void handle_response(int r);
  int init_fetch();
};

int BucketAsyncRefreshHandler::init_fetch()
{
  ldout(store->ctx(), 20) << "initiating async quota refresh for bucket=" << bucket << dendl;
  map<RGWObjCategory, RGWStorageStats> bucket_stats;
  int r = store->get_bucket_stats_async(bucket, this);
  if (r < 0) {
    ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket.name << dendl;

    /* get_bucket_stats_async() dropped our reference already */
    return r;
  }

  return 0;
}

void BucketAsyncRefreshHandler::handle_response(int r)
{
  if (r < 0) {
    ldout(store->ctx(), 20) << "AsyncRefreshHandler::handle_response() r=" << r << dendl;
    return; /* nothing to do here */
  }

  RGWStorageStats bs;

  map<RGWObjCategory, RGWStorageStats>::iterator iter;
  for (iter = stats->begin(); iter != stats->end(); ++iter) {
    RGWStorageStats& s = iter->second;
    bs.num_kb += s.num_kb;
    bs.num_kb_rounded += s.num_kb_rounded;
    bs.num_objects += s.num_objects;
  }

  cache->async_refresh_response(user, bucket, bs);
}

class RGWBucketStatsCache : public RGWQuotaCache<rgw_bucket> {
protected:
  bool map_find(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs) {
    return stats_map.find(bucket, qs);
  }

  bool map_find_and_update(const string& user, rgw_bucket& bucket, lru_map<rgw_bucket, RGWQuotaCacheStats>::UpdateContext *ctx) {
    return stats_map.find_and_update(bucket, NULL, ctx);
  }

  void map_add(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs) {
    stats_map.add(bucket, qs);
  }

  int fetch_stats_from_storage(const string& user, rgw_bucket& bucket, RGWStorageStats& stats);

public:
  RGWBucketStatsCache(RGWRados *_store) : RGWQuotaCache(store, store->ctx()->_conf->rgw_bucket_quota_cache_size) {
  }

  AsyncRefreshHandler *allocate_refresh_handler(const string& user, rgw_bucket& bucket) {
    return new BucketAsyncRefreshHandler(store, this, user, bucket);
  }
};

int RGWBucketStatsCache::fetch_stats_from_storage(const string& user, rgw_bucket& bucket, RGWStorageStats& stats)
{
  RGWBucketInfo bucket_info;

  uint64_t bucket_ver;
  uint64_t master_ver;

  map<RGWObjCategory, RGWStorageStats> bucket_stats;
  int r = store->get_bucket_stats(bucket, &bucket_ver, &master_ver, bucket_stats, NULL);
  if (r < 0) {
    ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket.name << dendl;
    return r;
  }

  stats = RGWStorageStats();

  map<RGWObjCategory, RGWStorageStats>::iterator iter;
  for (iter = bucket_stats.begin(); iter != bucket_stats.end(); ++iter) {
    RGWStorageStats& s = iter->second;
    stats.num_kb += s.num_kb;
    stats.num_kb_rounded += s.num_kb_rounded;
    stats.num_objects += s.num_objects;
  }

  return 0;
}

class UserAsyncRefreshHandler : public RGWQuotaCache<rgw_bucket>::AsyncRefreshHandler,
                                public RGWGetUserStats_CB {
  rgw_bucket bucket;
public:
  UserAsyncRefreshHandler(RGWRados *_store, RGWQuotaCache<rgw_bucket> *_cache,
                          const string& _user, rgw_bucket& _bucket) :
                          RGWQuotaCache<rgw_bucket>::AsyncRefreshHandler(_store, _cache),
                          RGWGetUserStats_CB(_user),
                          bucket(_bucket) {}

  void drop_reference() { put(); }
  int init_fetch();
  void handle_response(int r);
};

int UserAsyncRefreshHandler::init_fetch()
{
  ldout(store->ctx(), 20) << "initiating async quota refresh for user=" << user << dendl;
  int r = store->get_user_stats_async(user, this);
  if (r < 0) {
    ldout(store->ctx(), 0) << "could not get bucket info for user=" << user << dendl;

    /* get_bucket_stats_async() dropped our reference already */
    return r;
  }

  return 0;
}

void UserAsyncRefreshHandler::handle_response(int r)
{
  if (r < 0) {
    ldout(store->ctx(), 20) << "AsyncRefreshHandler::handle_response() r=" << r << dendl;
    return; /* nothing to do here */
  }

  cache->async_refresh_response(user, bucket, stats);
}

class RGWUserStatsCache : public RGWQuotaCache<rgw_bucket> {
protected:
  bool map_find(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs) {
    return stats_map.find(bucket, qs);
  }

  bool map_find_and_update(const string& user, rgw_bucket& bucket, lru_map<rgw_bucket, RGWQuotaCacheStats>::UpdateContext *ctx) {
    return stats_map.find_and_update(bucket, NULL, ctx);
  }

  void map_add(const string& user, rgw_bucket& bucket, RGWQuotaCacheStats& qs) {
    stats_map.add(bucket, qs);
  }

  int fetch_stats_from_storage(const string& user, rgw_bucket& bucket, RGWStorageStats& stats);

public:
  RGWUserStatsCache(RGWRados *_store) : RGWQuotaCache(store, store->ctx()->_conf->rgw_bucket_quota_cache_size) {
  }

  AsyncRefreshHandler *allocate_refresh_handler(const string& user, rgw_bucket& bucket) {
    return new UserAsyncRefreshHandler(store, this, user, bucket);
  }
};

int RGWUserStatsCache::fetch_stats_from_storage(const string& user, rgw_bucket& bucket, RGWStorageStats& stats)
{
  int r = store->get_user_stats(user, stats);
  if (r < 0) {
    ldout(store->ctx(), 0) << "could not get user stats for user=" << user << dendl;
    return r;
  }

  return 0;
}


class RGWQuotaHandlerImpl : public RGWQuotaHandler {
  RGWRados *store;
  RGWBucketStatsCache stats_cache;
public:
  RGWQuotaHandlerImpl(RGWRados *_store) : store(_store), stats_cache(_store) {}
  virtual int check_quota(const string& user, rgw_bucket& bucket, RGWQuotaInfo& bucket_quota,
			  uint64_t num_objs, uint64_t size) {
    uint64_t size_kb = rgw_rounded_kb(size);
    if (!bucket_quota.enabled) {
      return 0;
    }

    RGWStorageStats stats;

    int ret = stats_cache.get_stats(user, bucket, stats, bucket_quota);
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

  virtual void update_stats(const string& user, rgw_bucket& bucket, int obj_delta, uint64_t added_bytes, uint64_t removed_bytes) {
    stats_cache.adjust_stats(user, bucket, obj_delta, added_bytes, removed_bytes);
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
