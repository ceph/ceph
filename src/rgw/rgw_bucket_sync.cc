

#include "rgw_common.h"
#include "rgw_bucket_sync.h"
#include "rgw_data_sync.h"
#include "rgw_zone.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw


ostream& operator<<(ostream& os, const rgw_sync_bucket_entity& e) {
  os << "{b=" << rgw_sync_bucket_entities::bucket_key(e.bucket) << ",z=" << e.zone.value_or("") << ",az=" << (int)e.all_zones << "}";
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
                                   rgw_sync_data_flow_group *_default_flow,
                                   std::set<std::string> *_pall_zones,
                                   CB filter_cb) {
  zone = _zone;
  bucket = _bucket;
  default_flow = _default_flow;
  pall_zones = _pall_zones;

  rgw_sync_bucket_entity zb(zone, bucket);

  status = group.status;

  std::vector<rgw_sync_bucket_pipes> zone_pipes;

  /* only look at pipes that touch the specific zone and bucket */
  for (auto& pipe : group.pipes) {
    if (pipe.contains_zone_bucket(zone, bucket)) {
      zone_pipes.push_back(pipe);
    }
  }

  const rgw_sync_data_flow_group *pflow;

  if (!group.data_flow.empty()) {
    pflow = &group.data_flow;
  } else {
    if (!default_flow) {
      return;
    }
    pflow = default_flow;
  }

  auto& flow = *pflow;

  pall_zones->insert(zone);

  /* symmetrical */
  if (flow.symmetrical) {
    for (auto& symmetrical_group : *flow.symmetrical) {
      if (symmetrical_group.zones.find(zone) != symmetrical_group.zones.end()) {
        for (auto& z : symmetrical_group.zones) {
          if (z != zone) {
            pall_zones->insert(z);
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
        pall_zones->insert(rule.dest_zone);
        try_add_dest(zone, rule.dest_zone, zone_pipes, filter_cb);
      } else if (rule.dest_zone == zone) {
        pall_zones->insert(rule.source_zone);
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

void RGWBucketSyncFlowManager::pipe_set::dump(ceph::Formatter *f) const
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

void RGWBucketSyncFlowManager::init(const rgw_sync_policy_info& sync_policy) {
  std::optional<rgw_sync_data_flow_group> default_flow;
  if (parent) {
    default_flow.emplace();
    default_flow->init_default(parent->all_zones);
  }

  for (auto& item : sync_policy.groups) {
    auto& group = item.second;
    auto& flow_group_map = flow_groups[group.id];

    flow_group_map.init(zone_name, bucket, group,
                        (default_flow ? &(*default_flow) : nullptr),
                        &all_zones,
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
                                       RGWBucketSyncFlowManager::pipe_set *source_pipes,
                                       RGWBucketSyncFlowManager::pipe_set *dest_pipes) const

{
  rgw_sync_bucket_entity entity;
  entity.zone = zone_name;
  entity.bucket = effective_bucket.value_or(rgw_bucket());

  if (parent) {
    parent->reflect(effective_bucket, source_pipes, dest_pipes);
  }

  for (auto& item : flow_groups) {
    auto& flow_group_map = item.second;

    /* only return enabled groups */
    if (flow_group_map.status != rgw_sync_policy_group::Status::ENABLED) {
      continue;
    }

    for (auto& entry : flow_group_map.sources) {
      rgw_sync_bucket_pipe pipe = entry.second;
      if (!pipe.dest.match_bucket(effective_bucket)) {
        continue;
      }

      pipe.source.apply_bucket(effective_bucket);
      pipe.dest.apply_bucket(effective_bucket);

      source_pipes->pipes.insert(pipe);
    }

    for (auto& entry : flow_group_map.dests) {
      rgw_sync_bucket_pipe pipe = entry.second;

      if (!pipe.source.match_bucket(effective_bucket)) {
        continue;
      }

      pipe.source.apply_bucket(effective_bucket);
      pipe.dest.apply_bucket(effective_bucket);

      dest_pipes->pipes.insert(pipe);
    }
  }
}


RGWBucketSyncFlowManager::RGWBucketSyncFlowManager(const string& _zone_name,
                                                   std::optional<rgw_bucket> _bucket,
                                                   const RGWBucketSyncFlowManager *_parent) : zone_name(_zone_name),
                                                                                              bucket(_bucket),
                                                                                              parent(_parent) {}


void RGWSyncPolicyCompat::convert_old_sync_config(RGWSI_Zone *zone_svc,
                                                  RGWSI_SyncModules *sync_modules_svc,
                                                  rgw_sync_policy_info *ppolicy)
{
  bool found = false;

  rgw_sync_policy_info policy;

  auto& group = policy.groups["default"];
  auto& zonegroup = zone_svc->get_zonegroup();

  for (const auto& ziter1 : zonegroup.zones) {
    const string& id1 = ziter1.first;
    const RGWZone& z1 = ziter1.second;

    for (const auto& ziter2 : zonegroup.zones) {
      const string& id2 = ziter2.first;
      const RGWZone& z2 = ziter2.second;

      if (id1 == id2) {
        continue;
      }

      if (z1.syncs_from(z2.name)) {
        found = true;
        rgw_sync_directional_rule *rule;
        group.data_flow.find_directional(z2.name, z1.name, true, &rule);
      }
    }
  }

  if (!found) { /* nothing syncs */
    return;
  }

  rgw_sync_bucket_pipes pipes;
  pipes.id = "all";
  pipes.source.all_zones = true;
  pipes.dest.all_zones = true;

  group.pipes.emplace_back(std::move(pipes));


  group.status = rgw_sync_policy_group::Status::ENABLED;

  *ppolicy = std::move(policy);
}

RGWBucketSyncPolicyHandler::RGWBucketSyncPolicyHandler(RGWSI_Zone *_zone_svc,
                                                       RGWSI_SyncModules *sync_modules_svc,
                                                       std::optional<string> effective_zone) : zone_svc(_zone_svc) {
  zone_name = effective_zone.value_or(zone_svc->zone_name());
  flow_mgr.reset(new RGWBucketSyncFlowManager(zone_name,
                                              nullopt,
                                              nullptr));
  sync_policy = zone_svc->get_zonegroup().sync_policy;

  if (sync_policy.empty()) {
    RGWSyncPolicyCompat::convert_old_sync_config(zone_svc, sync_modules_svc, &sync_policy);
  }

  init();
}

RGWBucketSyncPolicyHandler::RGWBucketSyncPolicyHandler(const RGWBucketSyncPolicyHandler *_parent,
                                                       const RGWBucketInfo& _bucket_info) : parent(_parent),
                                                                                            bucket_info(_bucket_info) {
  if (_bucket_info.sync_policy) {
    sync_policy = *_bucket_info.sync_policy;
  }
  bucket = _bucket_info.bucket;
  zone_svc = parent->zone_svc;
  flow_mgr.reset(new RGWBucketSyncFlowManager(parent->zone_name,
                                              _bucket_info.bucket,
                                              parent->flow_mgr.get()));
  init();
}

RGWBucketSyncPolicyHandler::RGWBucketSyncPolicyHandler(const RGWBucketSyncPolicyHandler *_parent,
                                                       const rgw_bucket& _bucket,
                                                       std::optional<rgw_sync_policy_info> _sync_policy) : parent(_parent) {
  if (_sync_policy) {
    sync_policy = *_sync_policy;
  }
  bucket = _bucket;
  zone_svc = parent->zone_svc;
  flow_mgr.reset(new RGWBucketSyncFlowManager(parent->zone_name,
                                              _bucket,
                                              parent->flow_mgr.get()));
  init();
}

RGWBucketSyncPolicyHandler *RGWBucketSyncPolicyHandler::alloc_child(const RGWBucketInfo& bucket_info) const
{
  return new RGWBucketSyncPolicyHandler(this, bucket_info);
}

RGWBucketSyncPolicyHandler *RGWBucketSyncPolicyHandler::alloc_child(const rgw_bucket& bucket,
                                                                    std::optional<rgw_sync_policy_info> sync_policy) const
{
  return new RGWBucketSyncPolicyHandler(this, bucket, sync_policy);
}

void RGWBucketSyncPolicyHandler::init()
{
  flow_mgr->init(sync_policy);

  flow_mgr->reflect(bucket, &sources_by_name, &targets_by_name);

  /* convert to zone ids */

  for (auto& pipe : sources_by_name.pipes) {
    if (!pipe.source.zone) {
      continue;
    }
    source_zones.insert(*pipe.source.zone);
    rgw_sync_bucket_pipe new_pipe = pipe;
    string zone_id;

    if (zone_svc->find_zone_id_by_name(*pipe.source.zone, &zone_id)) {
      new_pipe.source.zone = zone_id;
    }
    sources[*new_pipe.source.zone].pipes.insert(new_pipe);
  }
  for (auto& pipe : targets_by_name.pipes) {
    if (!pipe.dest.zone) {
      continue;
    }
    target_zones.insert(*pipe.dest.zone);
    rgw_sync_bucket_pipe new_pipe = pipe;
    string zone_id;
    if (zone_svc->find_zone_id_by_name(*pipe.dest.zone, &zone_id)) {
      new_pipe.dest.zone = zone_id;
    }
    targets[*new_pipe.dest.zone].pipes.insert(new_pipe);
  }
}

bool RGWBucketSyncPolicyHandler::bucket_exports_data() const
{
  if (!bucket) {
    return false;
  }

  if (bucket_is_sync_source()) {
    return true;
  }

  return (zone_svc->need_to_log_data() &&
          bucket_info->datasync_flag_enabled());
}

bool RGWBucketSyncPolicyHandler::bucket_imports_data() const
{
  return bucket_is_sync_target();
}

