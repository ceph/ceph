// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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


#include "include/function2.hpp"
#include "include/utime.h"
#include "common/lru_map.h"
#include "common/RefCountedObj.h"
#include "common/Thread.h"
#include "common/ceph_mutex.h"

#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_quota.h"
#include "rgw_bucket.h"
#include "rgw_user.h"

#include "services/svc_sys_obj.h"
#include "services/svc_meta.h"

#include <atomic>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

struct RGWQuotaCacheStats {
  RGWStorageStats stats;
  utime_t expiration;
  utime_t async_refresh_time;
};

template<class T>
class RGWQuotaCache {
protected:
  rgw::sal::Driver* driver;
  lru_map<T, RGWQuotaCacheStats> stats_map;
  RefCountedWaitObject *async_refcount;

  class StatsAsyncTestSet : public lru_map<T, RGWQuotaCacheStats>::UpdateContext {
    int objs_delta;
    uint64_t added_bytes;
    uint64_t removed_bytes;
  public:
    StatsAsyncTestSet() : objs_delta(0), added_bytes(0), removed_bytes(0) {}
    bool update(RGWQuotaCacheStats *entry) override {
      if (entry->async_refresh_time.sec() == 0)
        return false;

      entry->async_refresh_time = utime_t(0, 0);

      return true;
    }
  };

  virtual int fetch_stats_from_storage(const rgw_owner& owner, const rgw_bucket& bucket, RGWStorageStats& stats, optional_yield y, const DoutPrefixProvider *dpp) = 0;

  virtual bool map_find(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs) = 0;

  virtual bool map_find_and_update(const rgw_owner& owner, const rgw_bucket& bucket, typename lru_map<T, RGWQuotaCacheStats>::UpdateContext *ctx) = 0;
  virtual void map_add(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs) = 0;

  virtual void data_modified(const rgw_owner& owner, const rgw_bucket& bucket) {}
public:
  RGWQuotaCache(rgw::sal::Driver* _driver, int size) : driver(_driver), stats_map(size) {
    async_refcount = new RefCountedWaitObject;
  }
  virtual ~RGWQuotaCache() {
    async_refcount->put_wait(); /* wait for all pending async requests to complete */
  }

  int get_stats(const rgw_owner& owner, const rgw_bucket& bucket, RGWStorageStats& stats, optional_yield y,
                const DoutPrefixProvider* dpp);
  void adjust_stats(const rgw_owner& owner, rgw_bucket& bucket, int objs_delta, uint64_t added_bytes, uint64_t removed_bytes);

  void set_stats(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs, const RGWStorageStats& stats);
  int async_refresh(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs);
  void async_refresh_response(const rgw_owner& owner, rgw_bucket& bucket, const RGWStorageStats& stats);
  void async_refresh_fail(const rgw_owner& owner, rgw_bucket& bucket);

  /// start an async refresh that will eventually call async_refresh_response or
  /// async_refresh_fail. hold a reference to the waiter until completion
  virtual int init_refresh(const rgw_owner& owner, const rgw_bucket& bucket,
                           boost::intrusive_ptr<RefCountedWaitObject> waiter) = 0;
};

template<class T>
int RGWQuotaCache<T>::async_refresh(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs)
{
  /* protect against multiple updates */
  StatsAsyncTestSet test_update;
  if (!map_find_and_update(owner, bucket, &test_update)) {
    /* most likely we just raced with another update */
    return 0;
  }

  return init_refresh(owner, bucket, async_refcount);
}

template<class T>
void RGWQuotaCache<T>::async_refresh_fail(const rgw_owner& owner, rgw_bucket& bucket)
{
  ldout(driver->ctx(), 20) << "async stats refresh response for bucket=" << bucket << dendl;
}

template<class T>
void RGWQuotaCache<T>::async_refresh_response(const rgw_owner& owner, rgw_bucket& bucket, const RGWStorageStats& stats)
{
  ldout(driver->ctx(), 20) << "async stats refresh response for bucket=" << bucket << dendl;

  RGWQuotaCacheStats qs;

  map_find(owner, bucket, qs);

  set_stats(owner, bucket, qs, stats);
}

template<class T>
void RGWQuotaCache<T>::set_stats(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs, const RGWStorageStats& stats)
{
  qs.stats = stats;
  qs.expiration = ceph_clock_now();
  qs.async_refresh_time = qs.expiration;
  qs.expiration += driver->ctx()->_conf->rgw_bucket_quota_ttl;
  qs.async_refresh_time += driver->ctx()->_conf->rgw_bucket_quota_ttl / 2;

  map_add(owner, bucket, qs);
}

template<class T>
int RGWQuotaCache<T>::get_stats(const rgw_owner& owner, const rgw_bucket& bucket, RGWStorageStats& stats, optional_yield y, const DoutPrefixProvider* dpp) {
  RGWQuotaCacheStats qs;
  utime_t now = ceph_clock_now();
  if (map_find(owner, bucket, qs)) {
    if (qs.async_refresh_time.sec() > 0 && now >= qs.async_refresh_time) {
      int r = async_refresh(owner, bucket, qs);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: quota async refresh returned ret=" << r << dendl;

        /* continue processing, might be a transient error, async refresh is just optimization */
      }
    }

    if (qs.expiration > ceph_clock_now()) {
      stats = qs.stats;
      return 0;
    }
  }

  int ret = fetch_stats_from_storage(owner, bucket, stats, y, dpp);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  set_stats(owner, bucket, qs, stats);

  return 0;
}


template<class T>
class RGWQuotaStatsUpdate : public lru_map<T, RGWQuotaCacheStats>::UpdateContext {
  const int objs_delta;
  const uint64_t added_bytes;
  const uint64_t removed_bytes;
public:
  RGWQuotaStatsUpdate(const int objs_delta,
                      const uint64_t added_bytes,
                      const uint64_t removed_bytes)
    : objs_delta(objs_delta),
      added_bytes(added_bytes),
      removed_bytes(removed_bytes) {
  }

  bool update(RGWQuotaCacheStats * const entry) override {
    const uint64_t rounded_added = rgw_rounded_objsize(added_bytes);
    const uint64_t rounded_removed = rgw_rounded_objsize(removed_bytes);

    if (((int64_t)(entry->stats.size + added_bytes - removed_bytes)) >= 0) {
      entry->stats.size += added_bytes - removed_bytes;
    } else {
      entry->stats.size = 0;
    }

    if (((int64_t)(entry->stats.size_rounded + rounded_added - rounded_removed)) >= 0) {
      entry->stats.size_rounded += rounded_added - rounded_removed;
    } else {
      entry->stats.size_rounded = 0;
    }

    if (((int64_t)(entry->stats.num_objects + objs_delta)) >= 0) {
      entry->stats.num_objects += objs_delta;
    } else {
      entry->stats.num_objects = 0;
    }

    return true;
  }
};


template<class T>
void RGWQuotaCache<T>::adjust_stats(const rgw_owner& owner, rgw_bucket& bucket, int objs_delta,
                                 uint64_t added_bytes, uint64_t removed_bytes)
{
  RGWQuotaStatsUpdate<T> update(objs_delta, added_bytes, removed_bytes);
  map_find_and_update(owner, bucket, &update);

  data_modified(owner, bucket);
}

class RGWBucketStatsCache : public RGWQuotaCache<rgw_bucket> {
protected:
  bool map_find(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs) override {
    return stats_map.find(bucket, qs);
  }

  bool map_find_and_update(const rgw_owner& owner, const rgw_bucket& bucket, lru_map<rgw_bucket, RGWQuotaCacheStats>::UpdateContext *ctx) override {
    return stats_map.find_and_update(bucket, NULL, ctx);
  }

  void map_add(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs) override {
    stats_map.add(bucket, qs);
  }

  int fetch_stats_from_storage(const rgw_owner& owner, const rgw_bucket& bucket, RGWStorageStats& stats, optional_yield y, const DoutPrefixProvider *dpp) override;

public:
  explicit RGWBucketStatsCache(rgw::sal::Driver* _driver) : RGWQuotaCache<rgw_bucket>(_driver, _driver->ctx()->_conf->rgw_bucket_quota_cache_size) {
  }

  int init_refresh(const rgw_owner& owner, const rgw_bucket& bucket,
                   boost::intrusive_ptr<RefCountedWaitObject> waiter) override;
};

int RGWBucketStatsCache::fetch_stats_from_storage(const rgw_owner& owner, const rgw_bucket& _b, RGWStorageStats& stats, optional_yield y, const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;

  int r = driver->load_bucket(dpp, _b, &bucket, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "could not get bucket info for bucket=" << _b << " r=" << r << dendl;
    return r;
  }

  stats = RGWStorageStats();

  const auto& index = bucket->get_info().get_current_index();
  if (is_layout_indexless(index)) {
    return 0;
  }

  string bucket_ver;
  string master_ver;

  map<RGWObjCategory, RGWStorageStats> bucket_stats;
  r = bucket->read_stats(dpp, index, RGW_NO_SHARD, &bucket_ver,
			 &master_ver, bucket_stats, nullptr);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "could not get bucket stats for bucket="
                           << _b.name << dendl;
    return r;
  }

  for (const auto& pair : bucket_stats) {
    const RGWStorageStats& s = pair.second;

    stats.size += s.size;
    stats.size_rounded += s.size_rounded;
    stats.num_objects += s.num_objects;
  }

  return 0;
}

class BucketAsyncRefreshHandler : public rgw::sal::ReadStatsCB {
  RGWBucketStatsCache* cache;
  boost::intrusive_ptr<RefCountedWaitObject> waiter;
  rgw_owner owner;
  rgw_bucket bucket;
public:
  BucketAsyncRefreshHandler(RGWBucketStatsCache* cache,
                            boost::intrusive_ptr<RefCountedWaitObject> waiter,
                            const rgw_owner& owner, const rgw_bucket& bucket)
    : cache(cache), waiter(std::move(waiter)), owner(owner), bucket(bucket) {}

  void handle_response(int r, const RGWStorageStats& stats) override {
    if (r < 0) {
      cache->async_refresh_fail(owner, bucket);
      return;
    }

    cache->async_refresh_response(owner, bucket, stats);
  }
};


int RGWBucketStatsCache::init_refresh(const rgw_owner& owner, const rgw_bucket& bucket,
                                     boost::intrusive_ptr<RefCountedWaitObject> waiter)
{
  std::unique_ptr<rgw::sal::Bucket> rbucket;

  const DoutPrefix dp(driver->ctx(), dout_subsys, "rgw bucket async refresh handler: ");
  int r = driver->load_bucket(&dp, bucket, &rbucket, null_yield);
  if (r < 0) {
    ldpp_dout(&dp, 0) << "could not get bucket info for bucket=" << bucket << " r=" << r << dendl;
    return r;
  }

  ldpp_dout(&dp, 20) << "initiating async quota refresh for bucket=" << bucket << dendl;

  const auto& index = rbucket->get_info().get_current_index();
  if (is_layout_indexless(index)) {
    return 0;
  }

  boost::intrusive_ptr handler = new BucketAsyncRefreshHandler(
      this, std::move(waiter), owner, bucket);

  r = rbucket->read_stats_async(&dp, index, RGW_NO_SHARD, std::move(handler));
  if (r < 0) {
    ldpp_dout(&dp, 0) << "could not get bucket stats for bucket=" << bucket.name << dendl;
    return r;
  }

  return 0;
}

class RGWOwnerStatsCache : public RGWQuotaCache<rgw_owner> {
  const DoutPrefixProvider *dpp;
  std::atomic<bool> down_flag = { false };
  ceph::shared_mutex mutex = ceph::make_shared_mutex("RGWOwnerStatsCache");
  map<rgw_bucket, rgw_owner> modified_buckets;

  /* thread, sync recent modified buckets info */
  class BucketsSyncThread : public Thread {
    CephContext *cct;
    RGWOwnerStatsCache *stats;

    ceph::mutex lock = ceph::make_mutex("RGWOwnerStatsCache::BucketsSyncThread");
    ceph::condition_variable cond;
  public:

    BucketsSyncThread(CephContext *_cct, RGWOwnerStatsCache *_s) : cct(_cct), stats(_s) {}

    void *entry() override {
      ldout(cct, 20) << "BucketsSyncThread: start" << dendl;

      // rgw_reshard_debug_interval is a DEV level configuration
      // option, so we can assume it won't change while the RGW server
      // is running, so we'll handle it once before we loop
      double sync_interval_factor = 1.0;
      const uint64_t debug_interval = cct->_conf->rgw_reshard_debug_interval;
      if (debug_interval >= 1) {
	  constexpr double secs_per_day = 60 * 60 * 24;
	  sync_interval_factor = debug_interval / secs_per_day;

	  ldout(cct, 0) << "DEBUG: since the rgw_reshard_debug_interval is set at " <<
	    debug_interval << " the rgw_user_quota_bucket_sync_interval will be "
	    "multiplied by a factor of " << sync_interval_factor << dendl;
      }

      do {
        map<rgw_bucket, rgw_owner> buckets;

        stats->swap_modified_buckets(buckets);

        for (const auto& [bucket, owner] : buckets) {
          ldout(cct, 20) << "BucketsSyncThread: sync owner=" << owner << " bucket=" << bucket << dendl;
          const DoutPrefix dp(cct, dout_subsys, "rgw bucket sync thread: ");
          int r = stats->sync_bucket(owner, bucket, null_yield, &dp);
          if (r < 0) {
            ldout(cct, 0) << "WARNING: sync_bucket() returned r=" << r << dendl;
          }
        }

        if (stats->going_down()) {
          break;
	}

	uint64_t wait_secs = cct->_conf->rgw_user_quota_bucket_sync_interval;
	wait_secs = std::max(uint64_t(1),
			     uint64_t(wait_secs * sync_interval_factor));

	// note: this will likely wait for the intended period of
	// time, but could wait for less
	std::unique_lock locker{lock};
	cond.wait_for(locker, std::chrono::seconds(wait_secs));
      } while (!stats->going_down());

      ldout(cct, 20) << "BucketsSyncThread: done" << dendl;

      return NULL;
    }

    void stop() {
      std::lock_guard l{lock};
      cond.notify_all();
    }
  };

  /*
   * thread, full sync all users stats periodically
   *
   * only sync non idle users or ones that never got synced before, this is needed so that
   * users that didn't have quota turned on before (or existed before the user objclass
   * tracked stats) need to get their backend stats up to date.
   */
  class OwnerSyncThread : public Thread {
    CephContext *cct;
    RGWOwnerStatsCache *stats;
    const std::string metadata_section;

    ceph::mutex lock = ceph::make_mutex("RGWOwnerStatsCache::OwnerSyncThread");
    ceph::condition_variable cond;
  public:

    OwnerSyncThread(CephContext *_cct, RGWOwnerStatsCache *_s,
                    const std::string& metadata_section)
      : cct(_cct), stats(_s), metadata_section(metadata_section)
    {}

    void *entry() override {
      ldout(cct, 20) << "OwnerSyncThread: start" << dendl;
      do {
        const DoutPrefix dp(cct, dout_subsys, "rgw user sync thread: ");
        int ret = stats->sync_all_owners(&dp, metadata_section);
        if (ret < 0) {
          ldout(cct, 5) << "ERROR: sync_all_owners() returned ret=" << ret << dendl;
        }

        if (stats->going_down())
          break;

	std::unique_lock l{lock};
        cond.wait_for(l, std::chrono::seconds(cct->_conf->rgw_user_quota_sync_interval));
      } while (!stats->going_down());
      ldout(cct, 20) << "OwnerSyncThread: done" << dendl;

      return NULL;
    }

    void stop() {
      std::lock_guard l{lock};
      cond.notify_all();
    }
  };

  // TODO: AccountSyncThread and sync_all_accounts()

  BucketsSyncThread* buckets_sync_thread = nullptr;
  OwnerSyncThread* user_sync_thread = nullptr;
  OwnerSyncThread* account_sync_thread = nullptr;
protected:
  bool map_find(const rgw_owner& owner,const rgw_bucket& bucket, RGWQuotaCacheStats& qs) override {
    return stats_map.find(owner, qs);
  }

  bool map_find_and_update(const rgw_owner& owner, const rgw_bucket& bucket, lru_map<rgw_owner, RGWQuotaCacheStats>::UpdateContext *ctx) override {
    return stats_map.find_and_update(owner, NULL, ctx);
  }

  void map_add(const rgw_owner& owner, const rgw_bucket& bucket, RGWQuotaCacheStats& qs) override {
    stats_map.add(owner, qs);
  }

  int fetch_stats_from_storage(const rgw_owner& owner, const rgw_bucket& bucket, RGWStorageStats& stats, optional_yield y, const DoutPrefixProvider *dpp) override;
  int sync_bucket(const rgw_owner& owner, const rgw_bucket& bucket, optional_yield y, const DoutPrefixProvider *dpp);
  int sync_owner(const DoutPrefixProvider *dpp, const rgw_owner& owner, optional_yield y);
  int sync_all_owners(const DoutPrefixProvider *dpp,
                      const std::string& metadata_section);

  void data_modified(const rgw_owner& owner, const rgw_bucket& bucket) override;

  void swap_modified_buckets(map<rgw_bucket, rgw_owner>& out) {
    std::unique_lock lock{mutex};
    modified_buckets.swap(out);
  }

  template<class T> /* easier doing it as a template, Thread doesn't have ->stop() */
  void stop_thread(T **pthr) {
    T *thread = *pthr;
    if (!thread)
      return;

    thread->stop();
    thread->join();
    delete thread;
    *pthr = NULL;
  }

public:
  RGWOwnerStatsCache(const DoutPrefixProvider *dpp, rgw::sal::Driver* _driver, bool quota_threads)
    : RGWQuotaCache<rgw_owner>(_driver, _driver->ctx()->_conf->rgw_bucket_quota_cache_size), dpp(dpp)
  {
    if (quota_threads) {
      buckets_sync_thread = new BucketsSyncThread(driver->ctx(), this);
      buckets_sync_thread->create("rgw_buck_st_syn");
      user_sync_thread = new OwnerSyncThread(driver->ctx(), this, "user");
      user_sync_thread->create("rgw_user_st_syn");
      account_sync_thread = new OwnerSyncThread(driver->ctx(), this, "account");
      account_sync_thread->create("rgw_acct_st_syn");
    }
  }
  ~RGWOwnerStatsCache() override {
    stop();
  }

  int init_refresh(const rgw_owner& owner, const rgw_bucket& bucket,
                   boost::intrusive_ptr<RefCountedWaitObject> waiter) override;

  bool going_down() {
    return down_flag;
  }

  void stop() {
    down_flag = true;
    {
      std::unique_lock lock{mutex};
      stop_thread(&buckets_sync_thread);
    }
    stop_thread(&user_sync_thread);
    stop_thread(&account_sync_thread);
  }
};

class OwnerAsyncRefreshHandler : public rgw::sal::ReadStatsCB {
  RGWOwnerStatsCache* cache;
  boost::intrusive_ptr<RefCountedWaitObject> waiter;
  rgw_bucket bucket;
  rgw_owner owner;
 public:
  OwnerAsyncRefreshHandler(RGWOwnerStatsCache* cache,
                           boost::intrusive_ptr<RefCountedWaitObject> waiter,
                           const rgw_owner& owner, const rgw_bucket& bucket)
      : cache(cache), waiter(std::move(waiter)), bucket(bucket), owner(owner)
  {}

  void handle_response(int r, const RGWStorageStats& stats) override;
};

int RGWOwnerStatsCache::init_refresh(const rgw_owner& owner, const rgw_bucket& bucket,
                                     boost::intrusive_ptr<RefCountedWaitObject> waiter)
{
  boost::intrusive_ptr cb = new OwnerAsyncRefreshHandler(
      this, std::move(waiter), owner, bucket);

  ldpp_dout(dpp, 20) << "initiating async quota refresh for owner=" << owner << dendl;

  int r = driver->load_stats_async(dpp, owner, std::move(cb));
  if (r < 0) {
    ldpp_dout(dpp, 0) << "could not read stats for owner=" << owner << dendl;
    return r;
  }

  return 0;
}

void OwnerAsyncRefreshHandler::handle_response(int r, const RGWStorageStats& stats)
{
  if (r < 0) {
    cache->async_refresh_fail(owner, bucket);
    return;
  }

  cache->async_refresh_response(owner, bucket, stats);
}

int RGWOwnerStatsCache::fetch_stats_from_storage(const rgw_owner& owner,
                                                 const rgw_bucket& bucket,
                                                 RGWStorageStats& stats,
                                                 optional_yield y,
                                                 const DoutPrefixProvider *dpp)
{
  ceph::real_time synced; // ignored
  ceph::real_time updated; // ignored
  int r = driver->load_stats(dpp, y, owner, stats, synced, updated);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "could not read stats for owner " << owner << dendl;
    return r;
  }

  return 0;
}

int RGWOwnerStatsCache::sync_bucket(const rgw_owner& owner, const rgw_bucket& b,
                                    optional_yield y, const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;

  int r = driver->load_bucket(dpp, b, &bucket, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "could not get bucket info for bucket=" << b << " r=" << r << dendl;
    return r;
  }

  RGWBucketEnt ent;
  r = bucket->sync_owner_stats(dpp, y, &ent);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: sync_owner_stats() for bucket=" << bucket << " returned " << r << dendl;
    return r;
  }

  return bucket->check_bucket_shards(dpp, ent.count, y);
}

// for account owners, we need to look up the tenant name by account id
static int get_owner_tenant(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            rgw::sal::Driver* driver,
                            const rgw_owner& owner,
                            std::string& tenant)
{
  return std::visit(fu2::overload(
      [&] (const rgw_user& user) {
        tenant = user.tenant;
        return 0;
      },
      [&] (const rgw_account_id& account) {
        RGWAccountInfo info;
        rgw::sal::Attrs attrs;
        RGWObjVersionTracker objv;
        int ret = driver->load_account_by_id(dpp, y, account, info, attrs, objv);
        if (ret >= 0) {
          tenant = std::move(info.tenant);
        }
        return ret;
      }), owner);
}

int RGWOwnerStatsCache::sync_owner(const DoutPrefixProvider *dpp,
                                   const rgw_owner& owner, optional_yield y)
{
  RGWStorageStats stats;
  ceph::real_time last_stats_sync;
  ceph::real_time last_stats_update;

  int ret = driver->load_stats(dpp, y, owner, stats, last_stats_sync, last_stats_update);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "ERROR: can't read owner stats: ret=" << ret << dendl;
    return ret;
  }

  if (!driver->ctx()->_conf->rgw_user_quota_sync_idle_users &&
      last_stats_update < last_stats_sync) {
    ldpp_dout(dpp, 20) << "owner is idle, not doing a full sync (owner=" << owner << ")" << dendl;
    return 0;
  }

  real_time when_need_full_sync = last_stats_sync;
  when_need_full_sync += make_timespan(driver->ctx()->_conf->rgw_user_quota_sync_wait_time);
  
  // check if enough time passed since last full sync
  if (when_need_full_sync > ceph::real_clock::now()) {
    return 0;
  }

  std::string tenant;
  ret = get_owner_tenant(dpp, y, driver, owner, tenant);
  if (ret < 0) {
    return ret;
  }

  ret = rgw_sync_all_stats(dpp, y, driver, owner, tenant);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed user stats sync, ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWOwnerStatsCache::sync_all_owners(const DoutPrefixProvider *dpp,
                                        const std::string& metadata_section)
{
  void *handle;
  int ret = driver->meta_list_keys_init(dpp, metadata_section, string(), &handle);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "ERROR: can't get key: ret=" << ret << dendl;
    return ret;
  }

  bool truncated;
  int max = 1000;

  do {
    list<string> keys;
    ret = driver->meta_list_keys_next(dpp, handle, max, keys, &truncated);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: lists_keys_next(): ret=" << ret << dendl;
      break;
    }
    for (list<string>::iterator iter = keys.begin();
         iter != keys.end() && !going_down(); 
         ++iter) {
      const rgw_owner owner = parse_owner(*iter);
      ldpp_dout(dpp, 20) << "RGWOwnerStatsCache: sync owner=" << owner << dendl;
      int r = sync_owner(dpp, owner, null_yield);
      if (r < 0) {
        ldpp_dout(dpp, 5) << "ERROR: sync_owner() failed, owner=" << owner
            << " ret=" << r << dendl;
        /* continuing to next owner */
        continue;
      }
    }
  } while (truncated);

  driver->meta_list_keys_complete(handle);
  return ret;
}

void RGWOwnerStatsCache::data_modified(const rgw_owner& owner, const rgw_bucket& bucket)
{
  /* racy, but it's ok */
  mutex.lock_shared();
  bool need_update = modified_buckets.find(bucket) == modified_buckets.end();
  mutex.unlock_shared();

  if (need_update) {
    std::unique_lock lock{mutex};
    modified_buckets[bucket] = owner;
  }
}


class RGWQuotaInfoApplier {
  /* NOTE: no non-static field allowed as instances are supposed to live in
   * the static memory only. */
protected:
  RGWQuotaInfoApplier() = default;

public:
  virtual ~RGWQuotaInfoApplier() {}

  virtual bool is_size_exceeded(const DoutPrefixProvider *dpp,
                                const char * const entity,
                                const RGWQuotaInfo& qinfo,
                                const RGWStorageStats& stats,
                                const uint64_t size) const = 0;

  virtual bool is_num_objs_exceeded(const DoutPrefixProvider *dpp,
                                    const char * const entity,
                                    const RGWQuotaInfo& qinfo,
                                    const RGWStorageStats& stats,
                                    const uint64_t num_objs) const = 0;

  static const RGWQuotaInfoApplier& get_instance(const RGWQuotaInfo& qinfo);
};

class RGWQuotaInfoDefApplier : public RGWQuotaInfoApplier {
public:
  bool is_size_exceeded(const DoutPrefixProvider *dpp, const char * const entity,
                                const RGWQuotaInfo& qinfo,
                                const RGWStorageStats& stats,
                                const uint64_t size) const override;

  bool is_num_objs_exceeded(const DoutPrefixProvider *dpp, const char * const entity,
                                    const RGWQuotaInfo& qinfo,
                                    const RGWStorageStats& stats,
                                    const uint64_t num_objs) const override;
};

class RGWQuotaInfoRawApplier : public RGWQuotaInfoApplier {
public:
  bool is_size_exceeded(const DoutPrefixProvider *dpp, const char * const entity,
                                const RGWQuotaInfo& qinfo,
                                const RGWStorageStats& stats,
                                const uint64_t size) const override;

  bool is_num_objs_exceeded(const DoutPrefixProvider *dpp, const char * const entity,
                                    const RGWQuotaInfo& qinfo,
                                    const RGWStorageStats& stats,
                                    const uint64_t num_objs) const override;
};


bool RGWQuotaInfoDefApplier::is_size_exceeded(const DoutPrefixProvider *dpp,
                                              const char * const entity,
                                              const RGWQuotaInfo& qinfo,
                                              const RGWStorageStats& stats,
                                              const uint64_t size) const
{
  if (qinfo.max_size < 0) {
    /* The limit is not enabled. */
    return false;
  }

  const uint64_t cur_size = stats.size_rounded;
  const uint64_t new_size = rgw_rounded_objsize(size);

  if (std::cmp_greater(cur_size + new_size, qinfo.max_size)) {
    ldpp_dout(dpp, 10) << "quota exceeded: stats.size_rounded=" << stats.size_rounded
             << " size=" << new_size << " "
             << entity << "_quota.max_size=" << qinfo.max_size << dendl;
    return true;
  }

  return false;
}

bool RGWQuotaInfoDefApplier::is_num_objs_exceeded(const DoutPrefixProvider *dpp,
                                                  const char * const entity,
                                                  const RGWQuotaInfo& qinfo,
                                                  const RGWStorageStats& stats,
                                                  const uint64_t num_objs) const
{
  if (qinfo.max_objects < 0) {
    /* The limit is not enabled. */
    return false;
  }

  if (std::cmp_greater(stats.num_objects + num_objs, qinfo.max_objects)) {
    ldpp_dout(dpp, 10) << "quota exceeded: stats.num_objects=" << stats.num_objects
             << " " << entity << "_quota.max_objects=" << qinfo.max_objects
             << dendl;
    return true;
  }

  return false;
}

bool RGWQuotaInfoRawApplier::is_size_exceeded(const DoutPrefixProvider *dpp,
                                              const char * const entity,
                                              const RGWQuotaInfo& qinfo,
                                              const RGWStorageStats& stats,
                                              const uint64_t size) const
{
  if (qinfo.max_size < 0) {
    /* The limit is not enabled. */
    return false;
  }

  const uint64_t cur_size = stats.size;

  if (std::cmp_greater(cur_size + size, qinfo.max_size)) {
    ldpp_dout(dpp, 10) << "quota exceeded: stats.size=" << stats.size
             << " size=" << size << " "
             << entity << "_quota.max_size=" << qinfo.max_size << dendl;
    return true;
  }

  return false;
}

bool RGWQuotaInfoRawApplier::is_num_objs_exceeded(const DoutPrefixProvider *dpp,
                                                  const char * const entity,
                                                  const RGWQuotaInfo& qinfo,
                                                  const RGWStorageStats& stats,
                                                  const uint64_t num_objs) const
{
  if (qinfo.max_objects < 0) {
    /* The limit is not enabled. */
    return false;
  }

  if (std::cmp_greater(stats.num_objects + num_objs, qinfo.max_objects)) {
    ldpp_dout(dpp, 10) << "quota exceeded: stats.num_objects=" << stats.num_objects
             << " " << entity << "_quota.max_objects=" << qinfo.max_objects
             << dendl;
    return true;
  }

  return false;
}

const RGWQuotaInfoApplier& RGWQuotaInfoApplier::get_instance(
  const RGWQuotaInfo& qinfo)
{
  static RGWQuotaInfoDefApplier default_qapplier;
  static RGWQuotaInfoRawApplier raw_qapplier;

  if (qinfo.check_on_raw) {
    return raw_qapplier;
  } else {
    return default_qapplier;
  }
}


class RGWQuotaHandlerImpl : public RGWQuotaHandler {
  rgw::sal::Driver* driver;
  RGWBucketStatsCache bucket_stats_cache;
  RGWOwnerStatsCache owner_stats_cache;

  int check_quota(const DoutPrefixProvider *dpp,
                  const char * const entity,
                  const RGWQuotaInfo& quota,
                  const RGWStorageStats& stats,
                  const uint64_t num_objs,
                  const uint64_t size) {
    if (!quota.enabled) {
      return 0;
    }

    const auto& quota_applier = RGWQuotaInfoApplier::get_instance(quota);

    ldpp_dout(dpp, 20) << entity
                            << " quota: max_objects=" << quota.max_objects
                            << " max_size=" << quota.max_size << dendl;


    if (quota_applier.is_num_objs_exceeded(dpp, entity, quota, stats, num_objs)) {
      return -ERR_QUOTA_EXCEEDED;
    }

    if (quota_applier.is_size_exceeded(dpp, entity, quota, stats, size)) {
      return -ERR_QUOTA_EXCEEDED;
    }

    ldpp_dout(dpp, 20) << entity << " quota OK:"
                            << " stats.num_objects=" << stats.num_objects
                            << " stats.size=" << stats.size << dendl;
    return 0;
  }
public:
  RGWQuotaHandlerImpl(const DoutPrefixProvider *dpp, rgw::sal::Driver* _driver, bool quota_threads) : driver(_driver),
                                    bucket_stats_cache(_driver),
                                    owner_stats_cache(dpp, _driver, quota_threads) {}

  int check_quota(const DoutPrefixProvider *dpp,
                  const rgw_owner& owner,
                  const rgw_bucket& bucket,
                  const RGWQuota& quota,
                  uint64_t num_objs,
                  uint64_t size, optional_yield y) override {

    if (!quota.bucket_quota.enabled && !quota.user_quota.enabled) {
      return 0;
    }

    /*
     * we need to fetch bucket stats if the user quota is enabled, because
     * the whole system relies on us periodically updating the user's bucket
     * stats in the user's header, this happens in get_stats() if we actually
     * fetch that info and not rely on cached data
     */

    const DoutPrefix dp(driver->ctx(), dout_subsys, "rgw quota handler: ");
    if (quota.bucket_quota.enabled) {
      RGWStorageStats bucket_stats;
      int ret = bucket_stats_cache.get_stats(owner, bucket, bucket_stats, y, &dp);
      if (ret < 0) {
        return ret;
      }
      ret = check_quota(dpp, "bucket", quota.bucket_quota, bucket_stats, num_objs, size);
      if (ret < 0) {
        return ret;
      }
    }

    if (quota.user_quota.enabled) {
      RGWStorageStats owner_stats;
      int ret = owner_stats_cache.get_stats(owner, bucket, owner_stats, y, &dp);
      if (ret < 0) {
        return ret;
      }
      ret = check_quota(dpp, "user", quota.user_quota, owner_stats, num_objs, size);
      if (ret < 0) {
        return ret;
      }
    }
    return 0;
  }

  void update_stats(const rgw_owner& owner, rgw_bucket& bucket, int obj_delta, uint64_t added_bytes, uint64_t removed_bytes) override {
    bucket_stats_cache.adjust_stats(owner, bucket, obj_delta, added_bytes, removed_bytes);
    owner_stats_cache.adjust_stats(owner, bucket, obj_delta, added_bytes, removed_bytes);
  }
}; // class RGWQuotaHandlerImpl


RGWQuotaHandler *RGWQuotaHandler::generate_handler(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, bool quota_threads)
{
  return new RGWQuotaHandlerImpl(dpp, driver, quota_threads);
}

void RGWQuotaHandler::free_handler(RGWQuotaHandler *handler)
{
  delete handler;
}


void rgw_apply_default_bucket_quota(RGWQuotaInfo& quota, const ConfigProxy& conf)
{
  if (conf->rgw_bucket_default_quota_max_objects >= 0) {
    quota.max_objects = conf->rgw_bucket_default_quota_max_objects;
    quota.enabled = true;
  }
  if (conf->rgw_bucket_default_quota_max_size >= 0) {
    quota.max_size = conf->rgw_bucket_default_quota_max_size;
    quota.enabled = true;
  }
}

void rgw_apply_default_user_quota(RGWQuotaInfo& quota, const ConfigProxy& conf)
{
  if (conf->rgw_user_default_quota_max_objects >= 0) {
    quota.max_objects = conf->rgw_user_default_quota_max_objects;
    quota.enabled = true;
  }
  if (conf->rgw_user_default_quota_max_size >= 0) {
    quota.max_size = conf->rgw_user_default_quota_max_size;
    quota.enabled = true;
  }
}

void rgw_apply_default_account_quota(RGWQuotaInfo& quota, const ConfigProxy& conf)
{
  if (conf->rgw_account_default_quota_max_objects >= 0) {
    quota.max_objects = conf->rgw_account_default_quota_max_objects;
    quota.enabled = true;
  }
  if (conf->rgw_account_default_quota_max_size >= 0) {
    quota.max_size = conf->rgw_account_default_quota_max_size;
    quota.enabled = true;
  }
}

void RGWQuotaInfo::dump(Formatter *f) const
{
  f->dump_bool("enabled", enabled);
  f->dump_bool("check_on_raw", check_on_raw);

  f->dump_int("max_size", max_size);
  f->dump_int("max_size_kb", rgw_rounded_kb(max_size));
  f->dump_int("max_objects", max_objects);
}

void RGWQuotaInfo::generate_test_instances(std::list<RGWQuotaInfo*>& o)
{
  o.push_back(new RGWQuotaInfo);
  o.push_back(new RGWQuotaInfo);
  o.back()->enabled = true;
  o.back()->check_on_raw = true;
  o.back()->max_size = 1024;
  o.back()->max_objects = 1;
}

void RGWQuotaInfo::decode_json(JSONObj *obj)
{
  if (false == JSONDecoder::decode_json("max_size", max_size, obj)) {
    /* We're parsing an older version of the struct. */
    int64_t max_size_kb = 0;

    JSONDecoder::decode_json("max_size_kb", max_size_kb, obj);
    max_size = max_size_kb * 1024;
  }
  JSONDecoder::decode_json("max_objects", max_objects, obj);

  JSONDecoder::decode_json("check_on_raw", check_on_raw, obj);
  JSONDecoder::decode_json("enabled", enabled, obj);
}

