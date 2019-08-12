

#include "rgw_common.h"
#include "rgw_bucket_sync.h"
#include "rgw_data_sync.h"
#include "rgw_zone.h"

#include "services/svc_zone.h"


#if 0
void RGWBucketSyncPolicyInfo::post_init()
{
  if (pipes) {
    return;
  }

  for (auto& p : *pipes) {
    auto& pipe = p.second;

    source_zones.insert(pipe.source.zone_id());
  }
}
#endif

int RGWBucketSyncPolicyHandler::init()
{
  const auto& zone_id = zone_svc->get_zone().id;

  if (!bucket_info.sync_policy) {
    return 0;
  }

  auto& sync_policy = *bucket_info.sync_policy;

  for (auto& entry : sync_policy.entries) {
    if (!entry.bucket ||
        !(*entry.bucket == bucket_info.bucket)) {
      continue;
    }


  }

  source_zones.clear();

#warning FIXME
#if 0
  if (!sync_policy ||
      !sync_policy->pipes) {
    return 0;
  }

  for (auto& p : *sync_policy->pipes) {
    auto& pipe = p.second;

    if (pipe.target.zone_id == zone_id) {
      source_zones.insert(pipe.source.zone_id());
    }
  }
#endif

  return 0;
}

#if 0
vector<rgw_bucket_sync_pipe> rgw_bucket_sync_target_info::build_pipes(const rgw_bucket& source_bs)
{
  vector<rgw_bucket_sync_pipe> pipes;

  for (auto t : targets) {
    rgw_bucket_sync_pipe pipe;
    pipe.source_bs = source_bs;
    pipe.source_prefix = t.source_prefix;
    pipe.dest_prefix = t.dest_prefix;
    pipes.push_back(std::move(pipe));
  }
  return pipes;
}
#endif
