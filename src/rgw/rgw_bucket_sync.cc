

#include "rgw_common.h"
#include "rgw_bucket_sync.h"
#include "rgw_data_sync.h"
#include "rgw_zone.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw


static std::vector<rgw_sync_bucket_pipe> filter_relevant_pipes(const std::vector<rgw_sync_bucket_pipes>& pipes,
                                                               const string& source_zone,
                                                               const string& dest_zone)
{
  std::vector<rgw_sync_bucket_pipe> relevant_pipes;
  for (auto& p : pipes) {
    if (p.source.match_zone(source_zone) &&
        p.dest.match_zone(dest_zone)) {
      for (auto pipe : p.expand()) {
        pipe.source.apply_zone(source_zone);
        pipe.dest.apply_zone(dest_zone);
        relevant_pipes.push_back(pipe);
      }
    }
  }

  return std::move(relevant_pipes);
}

static bool is_wildcard_bucket(const rgw_bucket& bucket)
{
  return bucket.name.empty();
}

void rgw_sync_group_pipe_map::dump(ceph::Formatter *f) const
{
  encode_json("zone", zone, f);
  encode_json("buckets", rgw_sync_bucket_entities::bucket_key(bucket), f);
  encode_json("sources", sources, f);
  encode_json("dests", dests, f);
}

ostream& operator<<(ostream& os, const rgw_sync_bucket_entity& e) {
  os << "{b=" << rgw_sync_bucket_entities::bucket_key(e.bucket) << ",z=" << e.zone.value_or("") << "}";
  return os;
}

ostream& operator<<(ostream& os, const rgw_sync_bucket_pipe& pipe) {
  os << "{s=" << pipe.source << ",d=" << pipe.dest << "}";
  return os;
}

ostream& operator<<(ostream& os, const rgw_sync_bucket_entities& e) {
  os << "{b=" << rgw_sync_bucket_entities::bucket_key(e.bucket) << ",z=" << e.zones.value_or(std::set<string>()) << "}";
  return os;
}

ostream& operator<<(ostream& os, const rgw_sync_bucket_pipes& pipe) {
  os << "{id=" << pipe.id << ",s=" << pipe.source << ",d=" << pipe.dest << "}";
  return os;
}


template <typename CB1, typename CB2>
void rgw_sync_group_pipe_map::try_add_to_pipe_map(const string& source_zone,
                                                  const string& dest_zone,
                                                  const std::vector<rgw_sync_bucket_pipes>& pipes,
                                                  zb_pipe_map_t *pipe_map,
                                                  CB1 filter_cb,
                                                  CB2 call_filter_cb)
{
  if (!filter_cb(source_zone, nullopt, dest_zone, nullopt)) {
    return;
  }
  auto relevant_pipes = filter_relevant_pipes(pipes, source_zone, dest_zone);

  for (auto& pipe : relevant_pipes) {
    rgw_sync_bucket_entity zb;
    if (!call_filter_cb(pipe, &zb)) {
      continue;
    }
    pipe_map->insert(make_pair(zb, pipe));
  }
}
          
template <typename CB>
void rgw_sync_group_pipe_map::try_add_source(const string& source_zone,
                  const string& dest_zone,
                  const std::vector<rgw_sync_bucket_pipes>& pipes,
                  CB filter_cb)
{
  return try_add_to_pipe_map(source_zone, dest_zone, pipes,
                             &sources,
                             filter_cb,
                             [&](const rgw_sync_bucket_pipe& pipe, rgw_sync_bucket_entity *zb) {
                             *zb = rgw_sync_bucket_entity{source_zone, pipe.source.get_bucket()};
                             return filter_cb(source_zone, zb->bucket, dest_zone, pipe.dest.get_bucket());
                             });
}

template <typename CB>
void rgw_sync_group_pipe_map::try_add_dest(const string& source_zone,
                                           const string& dest_zone,
                                           const std::vector<rgw_sync_bucket_pipes>& pipes,
                                           CB filter_cb)
{
  return try_add_to_pipe_map(source_zone, dest_zone, pipes,
                             &dests,
                             filter_cb,
                             [&](const rgw_sync_bucket_pipe& pipe, rgw_sync_bucket_entity *zb) {
                             *zb = rgw_sync_bucket_entity{dest_zone, pipe.dest.get_bucket()};
                             return filter_cb(source_zone, pipe.source.get_bucket(), dest_zone, zb->bucket);
                             });
}

using zb_pipe_map_t = rgw_sync_group_pipe_map::zb_pipe_map_t;

pair<zb_pipe_map_t::const_iterator, zb_pipe_map_t::const_iterator> rgw_sync_group_pipe_map::find_pipes(const zb_pipe_map_t& m,
                                                                                                       const string& zone,
                                                                                                       std::optional<rgw_bucket> b) const
{
  if (!b) {
    return m.equal_range(rgw_sync_bucket_entity{zone, rgw_bucket()});
  }

  auto zb = rgw_sync_bucket_entity{zone, *b};

  auto range = m.equal_range(zb);
  if (range.first == range.second &&
      !is_wildcard_bucket(*b)) {
    /* couldn't find the specific bucket, try to find by wildcard */
    zb.bucket = rgw_bucket();
    range = m.equal_range(zb);
  }

  return range;
}


template <typename CB>
void rgw_sync_group_pipe_map::init(const string& _zone,
                                   std::optional<rgw_bucket> _bucket,
                                   const rgw_sync_policy_group& group,
                                   CB filter_cb) {
  zone = _zone;
  bucket = _bucket;

  rgw_sync_bucket_entity zb(zone, bucket);

  status = group.status;

  std::vector<rgw_sync_bucket_pipes> zone_pipes;

  /* only look at pipes that touch the specific zone and bucket */
  for (auto& pipe : group.pipes) {
    if (pipe.contains_zone_bucket(zone, bucket)) {
      zone_pipes.push_back(pipe);
    }
  }

  if (group.data_flow.empty()) {
    return;
  }

  auto& flow = group.data_flow;

  /* symmetrical */
  if (flow.symmetrical) {
    for (auto& symmetrical_group : *flow.symmetrical) {
      if (symmetrical_group.zones.find(zone) != symmetrical_group.zones.end()) {
        for (auto& z : symmetrical_group.zones) {
          if (z != zone) {
            try_add_source(z, zone, zone_pipes, filter_cb);
            try_add_dest(zone, z, zone_pipes, filter_cb);
          }
        }
      }
    }
  }

  /* directional */
  if (flow.directional) {
    for (auto& rule : *flow.directional) {
      if (rule.source_zone == zone) {
        try_add_dest(zone, rule.dest_zone, zone_pipes, filter_cb);
      } else if (rule.dest_zone == zone) {
        try_add_source(rule.source_zone, zone, zone_pipes, filter_cb);
      }
    }
  }
}

/*
 * find all relevant pipes in our zone that match {dest_bucket} <- {source_zone, source_bucket}
 */
vector<rgw_sync_bucket_pipe> rgw_sync_group_pipe_map::find_source_pipes(const string& source_zone,
                                                                        std::optional<rgw_bucket> source_bucket,
                                                                        std::optional<rgw_bucket> dest_bucket) const {
  vector<rgw_sync_bucket_pipe> result;

  auto range = find_pipes(sources, source_zone, source_bucket);

  for (auto iter = range.first; iter != range.second; ++iter) {
    auto pipe = iter->second;
    if (pipe.dest.match_bucket(dest_bucket)) {
      result.push_back(pipe);
    }
  }
  return std::move(result);
}

/*
 * find all relevant pipes in other zones that pull from a specific
 * source bucket in out zone {source_bucket} -> {dest_zone, dest_bucket}
 */
vector<rgw_sync_bucket_pipe> rgw_sync_group_pipe_map::find_dest_pipes(std::optional<rgw_bucket> source_bucket,
                                                                      const string& dest_zone,
                                                                      std::optional<rgw_bucket> dest_bucket) const {
  vector<rgw_sync_bucket_pipe> result;

  auto range = find_pipes(dests, dest_zone, dest_bucket);

  for (auto iter = range.first; iter != range.second; ++iter) {
    auto pipe = iter->second;
    if (pipe.source.match_bucket(source_bucket)) {
      result.push_back(pipe);
    }
  }

  return std::move(result);
}

/*
 * find all relevant pipes from {source_zone, source_bucket} -> {dest_zone, dest_bucket}
 */
vector<rgw_sync_bucket_pipe> rgw_sync_group_pipe_map::find_pipes(const string& source_zone,
                                                                 std::optional<rgw_bucket> source_bucket,
                                                                 const string& dest_zone,
                                                                 std::optional<rgw_bucket> dest_bucket) const {
  if (dest_zone == zone) {
    return find_source_pipes(source_zone, source_bucket, dest_bucket);
  }

  if (source_zone == zone) {
    return find_dest_pipes(source_bucket, dest_zone, dest_bucket);
  }

  return vector<rgw_sync_bucket_pipe>();
}

void RGWBucketSyncFlowManager::pipe_flow::dump(ceph::Formatter *f) const
{
  encode_json("pipes", pipes, f);
}

bool RGWBucketSyncFlowManager::allowed_data_flow(const string& source_zone,
                                                 std::optional<rgw_bucket> source_bucket,
                                                 const string& dest_zone,
                                                 std::optional<rgw_bucket> dest_bucket,
                                                 bool check_activated) const
{
  bool found = false;
  bool found_activated = false;

  for (auto m : flow_groups) {
    auto& fm = m.second;
    auto pipes = fm.find_pipes(source_zone, source_bucket,
                               dest_zone, dest_bucket);

    bool is_found = !pipes.empty();

    if (is_found) {
      switch (fm.status) {
        case rgw_sync_policy_group::Status::FORBIDDEN:
          return false;
        case rgw_sync_policy_group::Status::ENABLED:
          found = true;
          found_activated = true;
          break;
        case rgw_sync_policy_group::Status::ALLOWED:
          found = true;
          break;
        default:
          break; /* unknown -- ignore */
      }
    }
  }

  if (check_activated && found_activated) {
    return true;
  }

  return found;
}

/*
 * find all the matching flows om a flow map for a specific bucket
 */
RGWBucketSyncFlowManager::flow_map_t::iterator RGWBucketSyncFlowManager::find_bucket_flow(RGWBucketSyncFlowManager::flow_map_t& m, std::optional<rgw_bucket> bucket) {
  if (bucket) {
    auto iter = m.find(*bucket);

    if (iter != m.end()) {
      return iter;
    }
  }

  return m.find(rgw_bucket());
}

void RGWBucketSyncFlowManager::init(const rgw_sync_policy_info& sync_policy) {
  for (auto& item : sync_policy.groups) {
    auto& group = item.second;
    auto& flow_group_map = flow_groups[group.id];

    flow_group_map.init(zone_name, bucket, group,
                        [&](const string& source_zone,
                            std::optional<rgw_bucket> source_bucket,
                            const string& dest_zone,
                            std::optional<rgw_bucket> dest_bucket) {
                        if (!parent) {
                          return true;
                        }
                        return parent->allowed_data_flow(source_zone,
                                                         source_bucket,
                                                         dest_zone,
                                                         dest_bucket,
                                                         false); /* just check that it's not disabled */
                        });
  }
}

void RGWBucketSyncFlowManager::reflect(std::optional<rgw_bucket> effective_bucket,
                                       flow_map_t *flow_by_source,
                                       flow_map_t *flow_by_dest)

{
  rgw_sync_bucket_entity entity;
  entity.zone = zone_name;
  entity.bucket = effective_bucket.value_or(rgw_bucket());

  if (parent) {
    parent->reflect(effective_bucket, flow_by_source, flow_by_dest);
  }

  for (auto& item : flow_groups) {
    auto& flow_group_map = item.second;
    for (auto& entry : flow_group_map.sources) {
      rgw_sync_bucket_pipe pipe = entry.second;
      if (!pipe.dest.match_bucket(effective_bucket)) {
        continue;
      }

      pipe.source.apply_bucket(effective_bucket);
      pipe.dest.apply_bucket(effective_bucket);

      auto& by_source = (*flow_by_source)[pipe.source.get_bucket()];
      by_source.pipes.insert(pipe);
    }

    for (auto& entry : flow_group_map.dests) {
      rgw_sync_bucket_pipe pipe = entry.second;

      if (!pipe.source.match_bucket(effective_bucket)) {
        continue;
      }

      pipe.source.apply_bucket(effective_bucket);
      pipe.dest.apply_bucket(effective_bucket);

      auto& by_dest = (*flow_by_dest)[pipe.dest.get_bucket()];
      by_dest.pipes.insert(pipe);
    }
  }
}


RGWBucketSyncFlowManager::RGWBucketSyncFlowManager(const string& _zone_name,
                                                   std::optional<rgw_bucket> _bucket,
                                                   RGWBucketSyncFlowManager *_parent) : zone_name(_zone_name),
                                                                                        bucket(_bucket),
                                                                                        parent(_parent) {}


int RGWBucketSyncPolicyHandler::init()
{
#warning FIXME
#if 0
  const auto& zone_id = zone_svc->get_zone().id;
  auto& zg = zone_svc->get_zonegroup();

  if (!bucket_info.sync_policy) {
    return 0;
  }

  auto& sync_policy = *bucket_info.sync_policy;

  if (sync_policy.dests) {
    for (auto& dest : *sync_policy.dests) {
      if (!(dest.bucket || *dest.bucket == bucket_info.bucket)) {
        continue;
      }

      if (dest.zones.find("*") == dest.zones.end() &&
          dest.zones.find(zone_id) == dest.zones.end()) {
        continue;
      }

      if (dest.flow_rules) {
        /* populate trivial peers */
        for (auto& rule : *dest.flow_rules) {
          set<string> source_zones;
          set<string> dest_zones;
          rule.get_zone_peers(zone_id, &source_zones, &dest_zones);

          for (auto& sz : source_zones) {
            peer_info sinfo;
            sinfo.bucket = bucket_info.bucket;
            sources[sz].insert(sinfo);
          }

          for (auto& tz : dest_zones) {
            peer_info tinfo;
            tinfo.bucket = bucket_info.bucket;
            dests[tz].insert(tinfo);
          }
        }
      }

      /* non trivial sources */
      for (auto& source : dest.sources) {
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
#endif

  return 0;
}

bool RGWBucketSyncPolicyHandler::bucket_exports_data() const
{
  if (bucket_is_sync_source()) {
    return true;
  }

  return (zone_svc->need_to_log_data() &&
          bucket_info.datasync_flag_enabled());
}

bool RGWBucketSyncPolicyHandler::bucket_imports_data() const
{
  return bucket_is_sync_target();
}
