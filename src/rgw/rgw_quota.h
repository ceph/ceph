#ifndef CEPH_RGW_QUOTA_H
#define CEPH_RGW_QUOTA_H


#include "include/utime.h"
#include "common/lru_map.h"

class RGWRados;

struct RGWQuotaBucketStats {
  RGWBucketStats stats;
  utime_t expiration;
};

struct RGWQuotaInfo {
  uint64_t max_kb;
  uint64_t max_objs;
  bool is_set;

  RGWQuotaInfo() : max_kb(0), max_objs(0), is_set(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(max_kb, bl);
    ::encode(max_objs, bl);
    ::encode(is_set, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(max_kb, bl);
    ::decode(max_objs, bl);
    ::decode(is_set, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(RGWQuotaInfo)

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



#endif
