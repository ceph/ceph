

#ifndef CEPH_RGW_QUOTA_H
#define CEPH_RGW_QUOTA_H

#include "include/utime.h"
#include "common/config_fwd.h"
#include "common/lru_map.h"

#include "rgw/rgw_quota_types.h"
#include "common/async/yield_context.h"
#include "rgw_sal_fwd.h"

struct rgw_bucket;

class RGWQuotaHandler {
public:
  RGWQuotaHandler() {}
  virtual ~RGWQuotaHandler() {
  }
  virtual int check_quota(const DoutPrefixProvider *dpp, const rgw_user& bucket_owner, rgw_bucket& bucket,
                          RGWQuota& quota,
			  uint64_t num_objs, uint64_t size, optional_yield y) = 0;

  virtual void check_bucket_shards(const DoutPrefixProvider *dpp, uint64_t max_objs_per_shard,
                                   uint64_t num_shards, uint64_t num_objs, bool is_multisite,
                                   bool& need_resharding, uint32_t *suggested_num_shards) = 0;

  virtual void update_stats(const rgw_user& bucket_owner, rgw_bucket& bucket, int obj_delta, uint64_t added_bytes, uint64_t removed_bytes) = 0;

  static RGWQuotaHandler *generate_handler(const DoutPrefixProvider *dpp, rgw::sal::Store* store, bool quota_threads);
  static void free_handler(RGWQuotaHandler *handler);
};

// apply default quotas from configuration
void rgw_apply_default_bucket_quota(RGWQuotaInfo& quota, const ConfigProxy& conf);
void rgw_apply_default_user_quota(RGWQuotaInfo& quota, const ConfigProxy& conf);

#endif
