

#include "rgw_common.h"
#include "rgw_bucket_sync.h"
#include "rgw_data_sync.h"
#include "rgw_zone.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw


static std::vector<rgw_sync_bucket_pipe> filter_relevant_pipes(const std::vector<rgw_sync_bucket_pipe>& pipes,
                                                               const string& source_zone,
                                                               const string& target_zone)
{
  std::vector<rgw_sync_bucket_pipe> relevant_pipes;
  for (auto& pipe : relevant_pipes) {
    if (pipe.source.match_zone(source_zone)) {
      relevant_pipes.push_back(pipe);
    }
    if (pipe.target.match_zone(target_zone)) {
      relevant_pipes.push_back(pipe);
    }
  }

  return std::move(relevant_pipes);
}

static bool is_wildcard_bucket(const rgw_bucket& bucket)
{
  return bucket.name.empty();
}

struct group_pipe_map {
  string zone;
  std::optional<rgw_bucket> bucket;

  rgw_sync_policy_group::Status status{rgw_sync_policy_group::Status::FORBIDDEN};

  struct zone_bucket {
    string zone; /* zone name */
    rgw_bucket bucket; /* bucket, if empty then wildcard */

    bool operator<(const zone_bucket& zb) const {
      if (zone < zb.zone) {
        return true;
      }
      if (zone > zb.zone) {
        return false;
      }
      return (bucket < zb.bucket);
    }
  };

  using zb_pipe_map_t = std::multimap<zone_bucket, rgw_sync_bucket_pipe>;

  zb_pipe_map_t sources; /* all the pipes where zone is pulling from, by source_zone, s */
  zb_pipe_map_t targets; /* all the pipes that pull from zone */


  template <typename CB1, typename CB2>
  void try_add_to_pipe_map(const string& source_zone,
                           const string& target_zone,
                           const std::vector<rgw_sync_bucket_pipe>& pipes,
                           zb_pipe_map_t *pipe_map,
                           CB1 filter_cb,
                           CB2 call_filter_cb) {
    if (!filter_cb(source_zone, nullopt, target_zone, nullopt)) {
      return;
    }
    auto relevant_pipes = filter_relevant_pipes(pipes, source_zone, target_zone);

    for (auto& pipe : relevant_pipes) {
      zone_bucket zb;
      if (!call_filter_cb(pipe, &zb)) {
        continue;
      }
      pipe_map->insert(make_pair(zb, pipe));
    }
  }
          
  template <typename CB>
  void try_add_source(const string& source_zone,
                  const string& target_zone,
                  const std::vector<rgw_sync_bucket_pipe>& pipes,
                  CB filter_cb)
  {
    return try_add_to_pipe_map(source_zone, target_zone, pipes,
                               &sources,
                               filter_cb,
                               [&](const rgw_sync_bucket_pipe& pipe, zone_bucket *zb) {
        *zb = zone_bucket{source_zone, pipe.source.get_bucket()};
        return filter_cb(source_zone, zb->bucket, target_zone, pipe.target.get_bucket());
      });
  }
          
  template <typename CB>
  void try_add_target(const string& source_zone,
                  const string& target_zone,
                  const std::vector<rgw_sync_bucket_pipe>& pipes,
                  CB filter_cb)
  {
    return try_add_to_pipe_map(source_zone, target_zone, pipes,
                               &targets,
                               filter_cb,
                               [&](const rgw_sync_bucket_pipe& pipe, zone_bucket *zb) {
        *zb = zone_bucket{target_zone, pipe.target.get_bucket()};
        return filter_cb(source_zone, pipe.source.get_bucket(), target_zone, zb->bucket);
      });
  }
          
  pair<zb_pipe_map_t::const_iterator, zb_pipe_map_t::const_iterator> find_pipes(const zb_pipe_map_t& m,
                                                                                const string& zone,
                                                                                std::optional<rgw_bucket> b) {
    if (!b) {
      return m.equal_range(zone_bucket{zone, rgw_bucket()});
    }

    auto zb = zone_bucket{zone, *bucket};

    auto range = m.equal_range(zb);
    if (range.first == range.second &&
        !is_wildcard_bucket(*bucket)) {
      /* couldn't find the specific bucket, try to find by wildcard */
      zb.bucket = rgw_bucket();
      range = m.equal_range(zb);
    }

    return range;
  }


  template <typename CB>
  void init(const string& _zone,
            std::optional<rgw_bucket> _bucket,
            const rgw_sync_policy_group& group,
            CB filter_cb) {
    zone = _zone;
    bucket = _bucket;

    status = group.status;

    auto& pipes = group.pipes;

    std::vector<rgw_sync_bucket_pipe> zone_pipes;

    /* only look at pipes that touch the specific zone and bucket */
    if (pipes) {
      for (auto& pipe : *pipes) {
        if (pipe.contains_zone(zone) &&
            pipe.contains_bucket(bucket)) {
          zone_pipes.push_back(pipe);
        }
      }
    }

    if (!group.data_flow) {
      return;
    }

    auto& flow = *group.data_flow;

    /* symmetrical */
    if (flow.symmetrical) {
      for (auto& symmetrical_group : *flow.symmetrical) {
        if (symmetrical_group.zones.find(zone) != symmetrical_group.zones.end()) {
          for (auto& z : symmetrical_group.zones) {
            if (z != zone) {
              try_add_source(z, zone, zone_pipes, filter_cb);
              try_add_target(zone, z, zone_pipes, filter_cb);
            }
          }
        }
      }
    }

    /* directional */
    if (flow.directional) {
      for (auto& rule : *flow.directional) {
        if (rule.source_zone == zone) {
          try_add_target(zone, rule.target_zone, zone_pipes, filter_cb);
        } else if (rule.target_zone == zone) {
          try_add_source(rule.source_zone, zone, zone_pipes, filter_cb);
        }
      }
    }
  }

  /*
   * find all relevant pipes in our zone that match {target_bucket} <- {source_zone, source_bucket}
   */
  vector<rgw_sync_bucket_pipe> find_source_pipes(const string& source_zone,
                                                 std::optional<rgw_bucket> source_bucket,
                                                 std::optional<rgw_bucket> target_bucket) {
    vector<rgw_sync_bucket_pipe> result;

    auto range = find_pipes(sources, source_zone, source_bucket);

    for (auto iter = range.first; iter != range.second; ++iter) {
      auto pipe = iter->second;
      if (pipe.target.match_bucket(target_bucket)) {
        result.push_back(pipe);
      }
    }
    return std::move(result);
  }

  /*
   * find all relevant pipes in other zones that pull from a specific
   * source bucket in out zone {source_bucket} -> {target_zone, target_bucket}
   */
  vector<rgw_sync_bucket_pipe> find_target_pipes(std::optional<rgw_bucket> source_bucket,
                                                 const string& target_zone,
                                                 std::optional<rgw_bucket> target_bucket) {
    vector<rgw_sync_bucket_pipe> result;

    auto range = find_pipes(targets, target_zone, target_bucket);

    for (auto iter = range.first; iter != range.second; ++iter) {
      auto pipe = iter->second;
      if (pipe.source.match_bucket(source_bucket)) {
        result.push_back(pipe);
      }
    }

    return std::move(result);
  }

  /*
   * find all relevant pipes from {source_zone, source_bucket} -> {target_zone, target_bucket}
   */
  vector<rgw_sync_bucket_pipe> find_pipes(const string& source_zone,
                                          std::optional<rgw_bucket> source_bucket,
                                          const string& target_zone,
                                          std::optional<rgw_bucket> target_bucket) {
    if (target_zone == zone) {
      return find_source_pipes(source_zone, source_bucket, target_bucket);
    }

    if (source_zone == zone) {
      return find_target_pipes(source_bucket, target_zone, target_bucket);
    }

    return vector<rgw_sync_bucket_pipe>();
  }
};


class RGWBucketSyncFlowManager {
  RGWSI_Zone *zone_svc;
  std::optional<rgw_bucket> bucket;

  RGWBucketSyncFlowManager *parent{nullptr};

  map<string, group_pipe_map> flow_groups;

  struct pipe_flow {
    vector<group_pipe_map *> flow_groups;
    vector<rgw_sync_bucket_pipe> pipe;
  };

  bool allowed_data_flow(const string& source_zone,
                         std::optional<rgw_bucket> source_bucket,
                         const string& target_zone,
                         std::optional<rgw_bucket> target_bucket,
                         bool check_activated) {
    bool found = false;
    bool found_activated = false;

    for (auto m : flow_groups) {
      auto& fm = m.second;
      auto pipes = fm.find_pipes(source_zone, source_bucket,
                                 target_zone, target_bucket);

      bool is_found = !pipes.empty();

      if (is_found) {
        switch (fm.status) {
        case rgw_sync_policy_group::Status::FORBIDDEN:
          return false;
        case rgw_sync_policy_group::Status::ENABLED:
          found = true;
          found_activated = true;
          /* fall through */
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

  using flow_map_t = map<rgw_bucket, pipe_flow>;

  flow_map_t flow_by_source;
  flow_map_t flow_by_target;


  /*
   * find all the matching flows om a flow map for a specific bucket
   */
  flow_map_t::iterator find_bucket_flow(flow_map_t& m, std::optional<rgw_bucket> bucket) {
    if (bucket) {
      auto iter = m.find(*bucket);

      if (iter != m.end()) {
        return iter;
      }
    }

    return m.find(rgw_bucket());
  }


  void update_flow_maps(const rgw_sync_bucket_pipe& pipe,
                        group_pipe_map *flow_group) {
    auto source_bucket = pipe.source.get_bucket();
    auto target_bucket = pipe.target.get_bucket();

    if (!bucket ||
        *bucket != source_bucket) {
      auto& by_source = flow_by_source[source_bucket];
      by_source.flow_groups.push_back(flow_group);
      by_source.pipe.push_back(pipe);
    }

    if (!bucket ||
        *bucket != target_bucket) {
      auto& by_target = flow_by_target[target_bucket];
      by_target.flow_groups.push_back(flow_group);
      by_target.pipe.push_back(pipe);
    }
  }

#warning FIXME
#if 0
  bool allowed_sync_flow(std::optional<rgw_bucket> bucket,
                         const string& source,
                         const string& target) {
    auto& zone = zone_svc->zone_name();

    if (source == zone) {
      for (auto& item : range) {
        auto& pf = range.second;

        auto& pipe = pf.pipe;
      }
    }
  }
#endif



public:

  RGWBucketSyncFlowManager(RGWSI_Zone *_zone_svc,
                           std::optional<rgw_bucket> _bucket,
                           RGWBucketSyncFlowManager *_parent) : zone_svc(_zone_svc),
                                                                bucket(_bucket),
                                                                parent(_parent) {}

  void init(const rgw_sync_policy_info& sync_policy) {
    for (auto& item : sync_policy.groups) {
      auto& group = item.second;
      auto& flow_group_map = flow_groups[group.id];

      flow_group_map.init(zone_svc->zone_name(), bucket, group,
                          [&](const string& source_zone,
                              std::optional<rgw_bucket> source_bucket,
                              const string& target_zone,
                              std::optional<rgw_bucket> target_bucket) {
                            if (!parent) {
                              return true;
                            }
                            return parent->allowed_data_flow(source_zone,
                                                             source_bucket,
                                                             target_zone,
                                                             target_bucket,
                                                             false); /* just check that it's not disabled */
                          });

      if (group.pipes) {
        for (auto& pipe : *group.pipes) {
          if (!pipe.contains_bucket(bucket)) {
            continue;
          }

          update_flow_maps(pipe, &flow_group_map);
        }
      } else {
        update_flow_maps(rgw_sync_bucket_pipe(), &flow_group_map);
      }
    }
  }

#warning implement me
#if 0
  bool get_bucket_sources(const rgw_bucket& bucket,
                          std::vector<rgw_sync_bucket_entity> *sources) {
    auto iter = find_bucket_flow(flow_by_target, bucket);
    if (iter == flow_by_target.end()) {
      if (!parent) {
        return false;
      }

      return parent->get_bucket_sources(bucket, sources);
    }

    auto& pipe_flow = iter->second;


  }
#endif
};


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

  if (sync_policy.targets) {
    for (auto& target : *sync_policy.targets) {
      if (!(target.bucket || *target.bucket == bucket_info.bucket)) {
        continue;
      }

      if (target.zones.find("*") == target.zones.end() &&
          target.zones.find(zone_id) == target.zones.end()) {
        continue;
      }

      if (target.flow_rules) {
        /* populate trivial peers */
        for (auto& rule : *target.flow_rules) {
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
