
#include "include/utime.h"
#include "common/lru_map.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_quota.h"

#define dout_subsys ceph_subsys_rgw


struct RGWQuotaBucketStats {
  RGWBucketStats stats;
  utime_t expiration;
};

class RGWBucketStatsCache {
  RGWRados *store;
  lru_map<rgw_bucket, RGWQuotaBucketStats> stats_map;

  int fetch_bucket_totals(rgw_bucket& bucket, RGWBucketStats& stats);

public:
#warning FIXME configurable stats_map size
  RGWBucketStatsCache(RGWRados *_store) : store(_store), stats_map(10000) {}

  int get_bucket_stats(rgw_bucket& bucket, RGWBucketStats& stats);
  void adjust_bucket_stats(rgw_bucket& bucket, int objs_delta, uint64_t added_bytes, uint64_t removed_bytes);
};

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

  map<RGWObjCategory, RGWBucketStats>::iterator iter;
  for (iter = bucket_stats.begin(); iter != bucket_stats.end(); ++iter) {
    RGWBucketStats& s = iter->second;
    stats.num_kb += s.num_kb;
    stats.num_kb_rounded += s.num_kb_rounded;
    stats.num_objects += s.num_objects;
  }

  return 0;
}

int RGWBucketStatsCache::get_bucket_stats(rgw_bucket& bucket, RGWBucketStats& stats) {
  RGWQuotaBucketStats qs;
  if (stats_map.find(bucket, qs)) {
    if (qs.expiration > ceph_clock_now(store->ctx())) {
      stats = qs.stats;
      return 0;
    }
  }

  int ret = fetch_bucket_totals(bucket, qs.stats);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  qs.expiration = ceph_clock_now(store->ctx());
  qs.expiration += store->ctx()->_conf->rgw_bucket_quota_ttl;

  stats_map.add(bucket, qs);

  return 0;
}


class RGWBucketStatsUpdate : public lru_map<rgw_bucket, RGWQuotaBucketStats>::UpdateContext {
  int objs_delta;
  uint64_t added_bytes;
  uint64_t removed_bytes;
public:
  RGWBucketStatsUpdate(int _objs_delta, uint64_t _added_bytes, uint64_t _removed_bytes) : 
                    objs_delta(_objs_delta), added_bytes(_added_bytes), removed_bytes(_removed_bytes) {}
  void update(RGWQuotaBucketStats& entry) {
    uint64_t rounded_kb_added = rgw_rounded_kb(added_bytes);
    uint64_t rounded_kb_removed = rgw_rounded_kb(removed_bytes);

    entry.stats.num_kb_rounded += (rounded_kb_added - rounded_kb_removed);
    entry.stats.num_kb += (added_bytes - removed_bytes) / 1024;
    entry.stats.num_objects += objs_delta;
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

    int ret = stats_cache.get_bucket_stats(bucket, stats);
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
