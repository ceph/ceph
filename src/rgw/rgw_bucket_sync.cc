

#include "rgw_common.h"
#include "rgw_bucket_sync.h"
#include "rgw_data_sync.h"
#include "rgw_zone.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw


void rgw_sync_bucket_entities::add_zones(const std::vector<string>& new_zones) {
  for (auto& z : new_zones) {
    if (z == "*") {
      all_zones = true;
      zones.reset();
      return;
    }

    if (!zones) {
      zones.emplace();
    }

    zones->insert(z);
  }
}

void rgw_sync_bucket_entities::remove_zones(const std::vector<string>& rm_zones) {
  all_zones = false;

  if (!zones) {
    return;
  }

  for (auto& z : rm_zones) {
    zones->erase(z);
  }
}

static void set_bucket_field(std::optional<string> source, string *field) {
  if (!source) {
    return;
  }
  if (source == "*") {
    field->clear();
    return;
  }
  *field = *source;
}

void rgw_sync_bucket_entities::set_bucket(std::optional<string> tenant,
                std::optional<string> bucket_name,
                std::optional<string> bucket_id)
{
  if ((!bucket) && (tenant || bucket_name || bucket_id)) {
    bucket.emplace();
  }

  set_bucket_field(tenant, &bucket->tenant);
  set_bucket_field(bucket_name, &bucket->name);
  set_bucket_field(bucket_id, &bucket->bucket_id);

  if (bucket->tenant.empty() &&
      bucket->name.empty() &&
      bucket->bucket_id.empty()) {
    bucket.reset();
  }
}

void rgw_sync_bucket_entities::remove_bucket(std::optional<string> tenant,
                                           std::optional<string> bucket_name,
                                           std::optional<string> bucket_id)
{
  if (!bucket) {
    return;
  }

  if (tenant) {
    bucket->tenant.clear();
  }
  if (bucket_name) {
    bucket->name.clear();
  }
  if (bucket_id) {
    bucket->bucket_id.clear();
  }

  if (bucket->tenant.empty() &&
      bucket->name.empty() &&
      bucket->bucket_id.empty()) {
    bucket.reset();
  }
}


string rgw_sync_bucket_entities::bucket_key(std::optional<rgw_bucket> b)
{
  if (!b) {
    return string("*");
  }

  rgw_bucket _b = *b;

  if (_b.name.empty()) {
    _b.name = "*";
  }

  return _b.get_key();
}


bool rgw_sync_data_flow_group::find_symmetrical(const string& flow_id, bool create, rgw_sync_symmetric_group **flow_group)
{
  if (!symmetrical) {
    if (!create) {
      return false;
    }
    symmetrical.emplace();
  }

  for (auto& group : *symmetrical) {
    if (flow_id == group.id) {
      *flow_group = &group;
      return true;
    }
  }

  if (!create) {
    return false;
  }

  auto& group = symmetrical->emplace_back();
  *flow_group = &group;
  (*flow_group)->id = flow_id;
  return true;
}

void rgw_sync_data_flow_group::remove_symmetrical(const string& flow_id, std::optional<std::vector<string> > zones)
{
  if (!symmetrical) {
    return;
  }

  auto& groups = *symmetrical;

  auto iter = groups.begin();

  for (; iter != groups.end(); ++iter) {
    if (iter->id == flow_id) {
      if (!zones) {
        groups.erase(iter);
        return;
      }
      break;
    }
  }

  if (iter == groups.end()) {
    return;
  }

  auto& flow_group = *iter;

  for (auto& z : *zones) {
    flow_group.zones.erase(z);
  }

  if (flow_group.zones.empty()) {
    groups.erase(iter);
  }
}

bool rgw_sync_data_flow_group::find_directional(const string& source_zone, const string& dest_zone, bool create, rgw_sync_directional_rule **flow_group)
{
  if (!directional) {
    if (!create) {
      return false;
    }
    directional.emplace();
  }

  for (auto& rule : *directional) {
    if (source_zone == rule.source_zone &&
        dest_zone == rule.dest_zone) {
      *flow_group = &rule;
      return true;
    }
  }

  if (!create) {
    return false;
  }

  auto& rule = directional->emplace_back();
  *flow_group = &rule;

  rule.source_zone = source_zone;
  rule.dest_zone = dest_zone;

  return true;
}

void rgw_sync_data_flow_group::remove_directional(const string& source_zone, const string& dest_zone)
{
  if (!directional) {
    return;
  }

  for (auto iter = directional->begin(); iter != directional->end(); ++iter) {
    auto& rule = *iter;
    if (source_zone == rule.source_zone &&
        dest_zone == rule.dest_zone) {
      directional->erase(iter);
      return;
    }
  }
}

bool rgw_sync_policy_group::find_pipe(const string& pipe_id, bool create, rgw_sync_bucket_pipes **pipe)
{
  for (auto& p : pipes) {
    if (pipe_id == p.id) {
      *pipe = &p;
      return true;
    }
  }

  if (!create) {
    return false;
  }

  auto& p = pipes.emplace_back();
  *pipe = &p;
  p.id = pipe_id;

  return true;
}

void rgw_sync_policy_group::remove_pipe(const string& pipe_id)
{
  for (auto iter = pipes.begin(); iter != pipes.end(); ++iter) {
    if (pipe_id == iter->id) {
      pipes.erase(iter);
      return;
    }
  }
}

static std::vector<rgw_sync_bucket_pipes> filter_relevant_pipes(const std::vector<rgw_sync_bucket_pipes>& pipes,
                                                               const string& source_zone,
                                                               const string& dest_zone)
{
  std::vector<rgw_sync_bucket_pipes> relevant_pipes;
  for (auto& pipe : pipes) {
    if (pipe.source.match_zone(source_zone) &&
        pipe.dest.match_zone(dest_zone)) {
      relevant_pipes.push_back(pipe);
    }
  }

  return std::move(relevant_pipes);
}

static bool is_wildcard_bucket(const rgw_bucket& bucket)
{
  return bucket.name.empty();
}

void rgw_sync_group_pipe_map::zone_bucket::dump(ceph::Formatter *f) const
{
  encode_json("zone", zone, f);
  encode_json("buckets", rgw_sync_bucket_entities::bucket_key(bucket), f);
}

void rgw_sync_group_pipe_map::dump(ceph::Formatter *f) const
{
  encode_json("zone", zone, f);
  encode_json("buckets", rgw_sync_bucket_entities::bucket_key(bucket), f);
  encode_json("sources", sources, f);
  encode_json("dests", dests, f);
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
    zone_bucket zb;
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
                             [&](const rgw_sync_bucket_pipes& pipe, zone_bucket *zb) {
                             *zb = zone_bucket{source_zone, pipe.source.get_bucket()};
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
                             [&](const rgw_sync_bucket_pipes& pipe, zone_bucket *zb) {
                             *zb = zone_bucket{dest_zone, pipe.dest.get_bucket()};
                             return filter_cb(source_zone, pipe.source.get_bucket(), dest_zone, zb->bucket);
                             });
}

using zb_pipe_map_t = rgw_sync_group_pipe_map::zb_pipe_map_t;

pair<zb_pipe_map_t::const_iterator, zb_pipe_map_t::const_iterator> rgw_sync_group_pipe_map::find_pipes(const zb_pipe_map_t& m,
                                                                                                       const string& zone,
                                                                                                       std::optional<rgw_bucket> b)
{
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
void rgw_sync_group_pipe_map::init(const string& _zone,
                                   std::optional<rgw_bucket> _bucket,
                                   const rgw_sync_policy_group& group,
                                   CB filter_cb) {
  zone = _zone;
  bucket = _bucket;

  zone_bucket zb(zone, bucket);

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
vector<rgw_sync_bucket_pipes> rgw_sync_group_pipe_map::find_source_pipes(const string& source_zone,
                                                                        std::optional<rgw_bucket> source_bucket,
                                                                        std::optional<rgw_bucket> dest_bucket) {
  vector<rgw_sync_bucket_pipes> result;

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
vector<rgw_sync_bucket_pipes> rgw_sync_group_pipe_map::find_dest_pipes(std::optional<rgw_bucket> source_bucket,
                                                                      const string& dest_zone,
                                                                      std::optional<rgw_bucket> dest_bucket) {
  vector<rgw_sync_bucket_pipes> result;

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
vector<rgw_sync_bucket_pipes> rgw_sync_group_pipe_map::find_pipes(const string& source_zone,
                                                                 std::optional<rgw_bucket> source_bucket,
                                                                 const string& dest_zone,
                                                                 std::optional<rgw_bucket> dest_bucket) {
  if (dest_zone == zone) {
    return find_source_pipes(source_zone, source_bucket, dest_bucket);
  }

  if (source_zone == zone) {
    return find_dest_pipes(source_bucket, dest_zone, dest_bucket);
  }

  return vector<rgw_sync_bucket_pipes>();
}

void RGWBucketSyncFlowManager::pipe_flow::dump(ceph::Formatter *f) const
{
#warning cleanup
#if 0
  {
    Formatter::ArraySection os(*f, "flow_groups");
    for (auto& g : flow_groups) {
      encode_json("group", *g, f);
    }
  }
#endif

  encode_json("pipe", pipe, f);
}

bool RGWBucketSyncFlowManager::allowed_data_flow(const string& source_zone,
                                                 std::optional<rgw_bucket> source_bucket,
                                                 const string& dest_zone,
                                                 std::optional<rgw_bucket> dest_bucket,
                                                 bool check_activated) {
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


void RGWBucketSyncFlowManager::update_flow_maps(const rgw_sync_bucket_pipes& pipe) {
  auto source_bucket = pipe.source.get_bucket();
  auto dest_bucket = pipe.dest.get_bucket();

  if (pipe.match_dest(zone_name, bucket)) { /* we're the dest */
    auto& by_source = flow_by_source[source_bucket];
    by_source.pipe.push_back(pipe);
  }

  if (pipe.match_source(zone_name, bucket)) { /* we're the source */
    auto& by_dest = flow_by_dest[dest_bucket];
    by_dest.pipe.push_back(pipe);
  }

#if 0
  if (!bucket ||
      *bucket != source_bucket) {
    auto& by_source = flow_by_source[source_bucket];
    by_source.flow_groups.push_back(flow_group);
    by_source.pipe.push_back(pipe);
  }

  if (!bucket ||
      *bucket != dest_bucket) {
    auto& by_dest = flow_by_dest[dest_bucket];
    by_dest.flow_groups.push_back(flow_group);
    by_dest.pipe.push_back(pipe);
  }
#endif
}

void RGWBucketSyncFlowManager::init(const rgw_sync_policy_info& sync_policy) {
    rgw_sync_bucket_entities entity;
    entity.zones = std::set<string>( { zone_name } );
    entity.bucket =  bucket;

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

    for (auto& entry : flow_group_map.sources) {
      rgw_sync_bucket_pipes pipe;
      rgw_sync_bucket_entities source;
      pipe.source.zones = std::set<string>( { entry.first.zone } );
      pipe.source.bucket = entry.first.bucket;
      pipe.dest = entity;

      auto& by_source = flow_by_source[pipe.source.get_bucket()];
      by_source.pipe.push_back(pipe);
    }

    for (auto& entry : flow_group_map.dests) {
      rgw_sync_bucket_pipes pipe;
      rgw_sync_bucket_entities dest;
      pipe.dest.zones = std::set<string>( { entry.first.zone } );
      pipe.dest.bucket = entry.first.bucket;
      pipe.source = entity;

      auto& by_dest = flow_by_source[pipe.dest.get_bucket()];
      by_dest.pipe.push_back(pipe);
    }
  }

#if 0
    if (!group.pipes.empty()) {
      for (auto& pipe : group.pipes) {
        if (!pipe.contains_zone_bucket(zone_name, bucket)) {
          continue;
        }

        update_flow_maps(pipe, flow_group_map);
      }
    } else {
      update_flow_maps(rgw_sync_bucket_pipes(), flow_group_map);
    }
  }
#endif
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
