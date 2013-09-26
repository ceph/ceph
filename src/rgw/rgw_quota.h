#ifndef CEPH_RGW_QUOTA_H
#define CEPH_RGW_QUOTA_H


#include "include/utime.h"
#include "common/lru_map.h"

class RGWRados;

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
};




#endif
