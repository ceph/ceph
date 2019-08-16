

#include "rgw_common.h"
#include "rgw_bucket_sync.h"
#include "rgw_data_sync.h"
#include "rgw_zone.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

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

  if (sync_policy.targets) {
    for (auto& target : *sync_policy.targets) {
      if (!(target.bucket || *target.bucket == bucket_info.bucket)) {
        continue;
      }

      if (!(target.type.empty() ||
            target.type == "rgw")) {
        ldout(zone_svc->ctx(), 20) << "unsuppported sync target: " << target.type << dendl;
        continue;
      }

      if (target.zones.find("*") == target.zones.end() &&
          target.zones.find(zone_id) == target.zones.end()) {
        continue;
      }

      /* populate trivial peers */
      for (auto& rule : target.flow_rules) {
        set<string> source_zones;
        set<string> target_zones;
        rule.get_zone_peers(zone_id, &source_zones, &target_zones);

        for (auto& sz : source_zones) {
          peer_info sinfo;
          sinfo.bucket = bucket_info.bucket;
          sources[sz].insert(sinfo);
        }

        for (auto& tz : target_zones) {
          peer_info tinfo;
          tinfo.bucket = bucket_info.bucket;
          targets[tz].insert(tinfo);
        }
      }

      /* non trivial sources */
      for (auto& source : target.sources) {
        if (!source.bucket ||
            *source.bucket == bucket_info.bucket) {
          if ((source.type.empty() || source.type == "rgw") &&
              source.zone &&
              source.bucket) {
            peer_info sinfo;
            sinfo.type = source.type;
            sinfo.bucket = *source.bucket;
            sources[*source.zone].insert(sinfo);
          }
        }
      }
    }
  }

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
