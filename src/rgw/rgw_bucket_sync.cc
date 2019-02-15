

#include "rgw_common.h"
#include "rgw_bucket_sync.h"



void RGWBucketSyncPolicy::post_init()
{
  source_zones.clear();
  for (auto& t : targets) {
    for (auto& r : t.second.rules) {
      source_zones.insert(r.zone_id);
    }
  }
}

