// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_common.h"
#include "rgw_bucket_sync.h"
#include "rgw_data_sync.h"
#include "rgw_zone.h"

#include "services/svc_zone.h"
#include "services/svc_bucket_sync.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

ostream& operator<<(ostream& os, const rgw_sync_bucket_entity& e) {
  os << "{b=" << rgw_sync_bucket_entities::bucket_key(e.bucket) << ",z=" << e.zone.value_or(rgw_zone_id()) << ",az=" << (int)e.all_zones << "}";
  return os;
}

ostream& operator<<(ostream& os, const rgw_sync_bucket_pipe& pipe) {
  os << "{s=" << pipe.source << ",d=" << pipe.dest << "}";
  return os;
}

ostream& operator<<(ostream& os, const rgw_sync_bucket_entities& e) {
  os << "{b=" << rgw_sync_bucket_entities::bucket_key(e.bucket) << ",z=" << e.zones.value_or(std::set<rgw_zone_id>()) << "}";
  return os;
}

ostream& operator<<(ostream& os, const rgw_sync_bucket_pipes& pipe) {
  os << "{id=" << pipe.id << ",s=" << pipe.source << ",d=" << pipe.dest << "}";
  return os;
}

static std::vector<rgw_sync_bucket_pipe> filter_relevant_pipes(const std::vector<rgw_sync_bucket_pipes>& pipes,
                                                               const rgw_zone_id& source_zone,
                                                               const rgw_zone_id& dest_zone)
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

  return relevant_pipes;
}

static bool is_wildcard_bucket(const rgw_bucket& bucket)
{
  return bucket.name.empty();
}

void rgw_sync_group_pipe_map::dump(ceph::Formatter *f) const
{
  encode_json("zone", zone.id, f);
  encode_json("buckets", rgw_sync_bucket_entities::bucket_key(bucket), f);
  encode_json("sources", sources, f);
  encode_json("dests", dests, f);
}


template <typename CB1, typename CB2>
void rgw_sync_group_pipe_map::try_add_to_pipe_map(const rgw_zone_id& source_zone,
                                                  const rgw_zone_id& dest_zone,
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
void rgw_sync_group_pipe_map::try_add_source(const rgw_zone_id& source_zone,
                  const rgw_zone_id& dest_zone,
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
void rgw_sync_group_pipe_map::try_add_dest(const rgw_zone_id& source_zone,
                                           const rgw_zone_id& dest_zone,
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
                                                                                                       const rgw_zone_id& zone,
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
void rgw_sync_group_pipe_map::init(const DoutPrefixProvider *dpp,
                                   CephContext *cct,
                                   const rgw_zone_id& _zone,
                                   std::optional<rgw_bucket> _bucket,
                                   const rgw_sync_policy_group& group,
                                   rgw_sync_data_flow_group *_default_flow,
                                   std::set<rgw_zone_id> *_pall_zones,
                                   CB filter_cb) {
  zone = _zone;
  bucket = _bucket;
  default_flow = _default_flow;
  pall_zones = _pall_zones;

  rgw_sync_bucket_entity zb(zone, bucket);

  status = group.status;

  std::vector<rgw_sync_bucket_pipes> zone_pipes;

  string bucket_key = (bucket ? bucket->get_key() : "*");

  /* only look at pipes that touch the specific zone and bucket */
  for (auto& pipe : group.pipes) {
    if (pipe.contains_zone_bucket(zone, bucket)) {
      ldpp_dout(dpp, 20) << __func__ << "(): pipe_map (zone=" << zone << " bucket=" << bucket_key << "): adding potential pipe: " << pipe << dendl;
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
  for (auto& symmetrical_group : flow.symmetrical) {
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

  /* directional */
  for (auto& rule : flow.directional) {
    if (rule.source_zone == zone) {
      pall_zones->insert(rule.dest_zone);
      try_add_dest(zone, rule.dest_zone, zone_pipes, filter_cb);
    } else if (rule.dest_zone == zone) {
      pall_zones->insert(rule.source_zone);
      try_add_source(rule.source_zone, zone, zone_pipes, filter_cb);
    }
  }
}

/*
 * find all relevant pipes in our zone that match {dest_bucket} <- {source_zone, source_bucket}
 */
vector<rgw_sync_bucket_pipe> rgw_sync_group_pipe_map::find_source_pipes(const rgw_zone_id& source_zone,
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
  return result;
}

/*
 * find all relevant pipes in other zones that pull from a specific
 * source bucket in out zone {source_bucket} -> {dest_zone, dest_bucket}
 */
vector<rgw_sync_bucket_pipe> rgw_sync_group_pipe_map::find_dest_pipes(std::optional<rgw_bucket> source_bucket,
                                                                      const rgw_zone_id& dest_zone,
                                                                      std::optional<rgw_bucket> dest_bucket) const {
  vector<rgw_sync_bucket_pipe> result;

  auto range = find_pipes(dests, dest_zone, dest_bucket);

  for (auto iter = range.first; iter != range.second; ++iter) {
    auto pipe = iter->second;
    if (pipe.source.match_bucket(source_bucket)) {
      result.push_back(pipe);
    }
  }

  return result;
}

/*
 * find all relevant pipes from {source_zone, source_bucket} -> {dest_zone, dest_bucket}
 */
vector<rgw_sync_bucket_pipe> rgw_sync_group_pipe_map::find_pipes(const rgw_zone_id& source_zone,
                                                                 std::optional<rgw_bucket> source_bucket,
                                                                 const rgw_zone_id& dest_zone,
                                                                 std::optional<rgw_bucket> dest_bucket) const {
  if (dest_zone == zone) {
    return find_source_pipes(source_zone, source_bucket, dest_bucket);
  }

  if (source_zone == zone) {
    return find_dest_pipes(source_bucket, dest_zone, dest_bucket);
  }

  return vector<rgw_sync_bucket_pipe>();
}

void RGWBucketSyncFlowManager::pipe_rules::insert(const rgw_sync_bucket_pipe& pipe)
{
  pipes.push_back(pipe);

  auto ppipe = &pipes.back();
  auto prefix = ppipe->params.source.filter.prefix.value_or(string());

  prefix_refs.insert(make_pair(prefix, ppipe));

  for (auto& t : ppipe->params.source.filter.tags) {
    string tag = t.key + "=" + t.value;
    auto titer = tag_refs.find(tag);
    if (titer != tag_refs.end() &&
        ppipe->params.priority > titer->second->params.priority) {
      titer->second = ppipe;
    } else {
      tag_refs[tag] = ppipe;
    }
  }
}

bool RGWBucketSyncFlowManager::pipe_rules::find_basic_info_without_tags(const rgw_obj_key& key,
                                                                        std::optional<rgw_user> *user,
                                                                        std::optional<rgw_user> *acl_translation_owner,
                                                                        std::optional<string> *storage_class,
                                                                        rgw_sync_pipe_params::Mode *mode,
                                                                        bool *need_more_info) const
{
  std::optional<string> owner;

  *need_more_info = false;

  if (prefix_refs.empty()) {
    return false;
  }

  auto end = prefix_refs.upper_bound(key.name);
  auto iter = end;
  if (iter != prefix_refs.begin()) {
    --iter;
  }
  if (iter == prefix_refs.end()) {
    return false;
  }

  if (iter != prefix_refs.begin()) {
    iter = prefix_refs.find(iter->first); /* prefix_refs is multimap, find first element
                                             holding that key */
  }

  std::vector<decltype(iter)> iters;

  std::optional<int> priority;

  for (; iter != end; ++iter) {
    auto& prefix = iter->first;
    if (!boost::starts_with(key.name, prefix)) {
      continue;
    }

    auto& rule_params = iter->second->params;
    auto& filter = rule_params.source.filter;

    if (rule_params.priority > priority) {
      priority = rule_params.priority;

      if (!filter.has_tags()) {
        iters.clear();
      }
      iters.push_back(iter);

      *need_more_info = filter.has_tags(); /* if highest priority filter has tags, then
                                              we can't be sure if it would be used.
                                              We need to first read the info from the source object */
    }
  }

  if (iters.empty()) {
    return false;
  }

  std::optional<rgw_user> _user;
  std::optional<rgw_sync_pipe_acl_translation> _acl_translation;
  std::optional<string> _storage_class;
  rgw_sync_pipe_params::Mode _mode{rgw_sync_pipe_params::Mode::MODE_SYSTEM};

  // make sure all params are the same by saving the first one
  // encountered and comparing all subsequent to it
  bool first_iter = true;
  for (auto& iter : iters) {
    const rgw_sync_pipe_params& rule_params = iter->second->params;
    if (first_iter) {
      _user = rule_params.user;
      _acl_translation = rule_params.dest.acl_translation;
      _storage_class = rule_params.dest.storage_class;
      _mode = rule_params.mode;
      first_iter = false;
    } else {
      // note: three of these == operators are comparing std::optional
      // against std::optional; as one would expect they are equal a)
      // if both do not contain values or b) if both do and those
      // contained values are the same
      const bool conflict =
	!(_user == rule_params.user &&
	  _acl_translation == rule_params.dest.acl_translation &&
	  _storage_class == rule_params.dest.storage_class &&
	  _mode == rule_params.mode);
      if (conflict) {
	*need_more_info = true;
	return false;
      }
    }
  }

  *user = _user;
  if (_acl_translation) {
    *acl_translation_owner = _acl_translation->owner;
  }
  *storage_class = _storage_class;
  *mode = _mode;

  return true;
}

bool RGWBucketSyncFlowManager::pipe_rules::find_obj_params(const rgw_obj_key& key,
                                                           const RGWObjTags::tag_map_t& tags,
                                                           rgw_sync_pipe_params *params) const
{
  if (prefix_refs.empty()) {
    return false;
  }

  auto iter = prefix_refs.upper_bound(key.name);
  if (iter != prefix_refs.begin()) {
    --iter;
  }
  if (iter == prefix_refs.end()) {
    return false;
  }

  auto end = prefix_refs.upper_bound(key.name);
  auto max = end;

  std::optional<int> priority;

  for (; iter != end; ++iter) {
    /* NOTE: this is not the most efficient way to do it,
     * a trie data structure would be better
     */
    auto& prefix = iter->first;
    if (!boost::starts_with(key.name, prefix)) {
      continue;
    }

    auto& rule_params = iter->second->params;
    auto& filter = rule_params.source.filter;

    if (!filter.check_tags(tags)) {
      continue;
    }

    if (rule_params.priority > priority) {
      priority = rule_params.priority;
      max = iter;
    }
  }

  if (max == end) {
    return false;
  }

  *params = max->second->params;
  return true;
}

/*
 * return either the current prefix for s, or the next one if s is not within a prefix
 */

RGWBucketSyncFlowManager::pipe_rules::prefix_map_t::const_iterator RGWBucketSyncFlowManager::pipe_rules::prefix_search(const std::string& s) const
{
  if (prefix_refs.empty()) {
    return prefix_refs.end();
  }
  auto next = prefix_refs.upper_bound(s);
  auto iter = next;
  if (iter != prefix_refs.begin()) {
    --iter;
  }
  if (!boost::starts_with(s, iter->first)) {
    return next;
  }

  return iter;
}

void RGWBucketSyncFlowManager::pipe_set::insert(const rgw_sync_bucket_pipe& pipe) {
  /* Ensure this pipe doesn't match with any disabled pipes */ 
  for (auto p: disabled_pipe_map) {
    if (p.second.source.match(pipe.source) && p.second.dest.match(pipe.dest)) {
      return;
    }
  }
  pipe_map.insert(make_pair(pipe.id, pipe));

  auto& rules_ref = rules[endpoints_pair(pipe)];

  if (!rules_ref) {
    rules_ref = make_shared<RGWBucketSyncFlowManager::pipe_rules>();
  }

  rules_ref->insert(pipe);

  pipe_handler h(rules_ref, pipe);

  handlers.insert(h);
}

void RGWBucketSyncFlowManager::pipe_set::remove_all() {
  pipe_map.clear();
  disabled_pipe_map.clear();
  rules.clear();
  handlers.clear();
}

void RGWBucketSyncFlowManager::pipe_set::disable(const rgw_sync_bucket_pipe& pipe) {
  /* This pipe is disabled. Add it to disabled pipes & remove any
   * matching pipes already inserted
   */
  disabled_pipe_map.insert(make_pair(pipe.id, pipe));
  for (auto iter_p = pipe_map.begin(); iter_p != pipe_map.end(); ) {
    auto p = iter_p++;
    if (p->second.source.match(pipe.source) && p->second.dest.match(pipe.dest)) {
      auto& rules_ref = rules[endpoints_pair(p->second)];
      if (rules_ref) {
        pipe_handler h(rules_ref, p->second);
        handlers.erase(h);
      }
      rules.erase(endpoints_pair(p->second));
      pipe_map.erase(p);
    }
  }
}

void RGWBucketSyncFlowManager::pipe_set::dump(ceph::Formatter *f) const
{
  encode_json("pipes", pipe_map, f);
}

bool RGWBucketSyncFlowManager::allowed_data_flow(const rgw_zone_id& source_zone,
                                                 std::optional<rgw_bucket> source_bucket,
                                                 const rgw_zone_id& dest_zone,
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

void RGWBucketSyncFlowManager::init(const DoutPrefixProvider *dpp, const rgw_sync_policy_info& sync_policy) {
  std::optional<rgw_sync_data_flow_group> default_flow;
  if (parent) {
    default_flow.emplace();
    default_flow->init_default(parent->all_zones);
  }

  for (auto& item : sync_policy.groups) {
    auto& group = item.second;
    auto& flow_group_map = flow_groups[group.id];

    flow_group_map.init(dpp, cct, zone_id, bucket, group,
                        (default_flow ? &(*default_flow) : nullptr),
                        &all_zones,
                        [&](const rgw_zone_id& source_zone,
                            std::optional<rgw_bucket> source_bucket,
                            const rgw_zone_id& dest_zone,
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

/*
* These are the semantics to be followed while resolving the policy
* conflicts -
*
* ==================================================
* zonegroup               bucket          Result
* ==================================================
* enabled                 enabled         enabled
*                         allowed         enabled
*                         forbidden       disabled
* allowed                 enabled         enabled
*                         allowed         disabled
*                         forbidden       disabled
* forbidden               enabled         disabled
*                         allowed         disabled
*                         forbidden       disabled
*
* In case multiple group policies are set to reflect for any sync pair
* (<source-zone,source-bucket>, <dest-zone,dest-bucket>), the following
* rules are applied in the order-
* 1) Even if one policy status is FORBIDDEN, the sync will be disabled
* 2) Atleast one policy should be	ENABLED	for the	sync to	be allowed.
*
*/
void RGWBucketSyncFlowManager::reflect(const DoutPrefixProvider *dpp,
                                       std::optional<rgw_bucket> effective_bucket,
                                       RGWBucketSyncFlowManager::pipe_set *source_pipes,
                                       RGWBucketSyncFlowManager::pipe_set *dest_pipes,
                                       bool only_enabled) const

{
  string effective_bucket_key;
  bool is_forbidden = false;
  if (effective_bucket) {
    effective_bucket_key = effective_bucket->get_key();
  }
  if (parent) {
    parent->reflect(dpp, effective_bucket, source_pipes, dest_pipes, only_enabled);
  }

  for (auto& item : flow_groups) {
    auto& flow_group_map = item.second;
    is_forbidden = false;

    if (flow_group_map.status == rgw_sync_policy_group::Status::FORBIDDEN) {
      /* FORBIDDEN takes precedence over all the other rules.
       * Remove any other pipes which may allow access.
       */
      is_forbidden = true;
    } else if (flow_group_map.status != rgw_sync_policy_group::Status::ENABLED &&
        (only_enabled || flow_group_map.status != rgw_sync_policy_group::Status::ALLOWED)) {
      /* only return enabled groups */
      continue;
    }

    for (auto& entry : flow_group_map.sources) {
      rgw_sync_bucket_pipe pipe = entry.second;
      if (!pipe.dest.match_bucket(effective_bucket)) {
        continue;
      }

      pipe.source.apply_bucket(effective_bucket);
      pipe.dest.apply_bucket(effective_bucket);

      if (is_forbidden) {
        ldpp_dout(dpp, 20) << __func__ << "(): flow manager (bucket=" << effective_bucket_key << "): removing source pipe: " << pipe << dendl;
        source_pipes->disable(pipe);
      } else {
        ldpp_dout(dpp, 20) << __func__ << "(): flow manager (bucket=" << effective_bucket_key << "): adding source pipe: " << pipe << dendl;
        source_pipes->insert(pipe);
      }
    }

    for (auto& entry : flow_group_map.dests) {
      rgw_sync_bucket_pipe pipe = entry.second;

      if (!pipe.source.match_bucket(effective_bucket)) {
        continue;
      }

      pipe.source.apply_bucket(effective_bucket);
      pipe.dest.apply_bucket(effective_bucket);

      if (is_forbidden) {
        ldpp_dout(dpp, 20) << __func__ << "(): flow manager (bucket=" << effective_bucket_key << "): removing dest pipe: " << pipe << dendl;
        dest_pipes->disable(pipe);
      } else {
        ldpp_dout(dpp, 20) << __func__ << "(): flow manager (bucket=" << effective_bucket_key << "): adding dest pipe: " << pipe << dendl;
        dest_pipes->insert(pipe);
      }
    }
  }
}


RGWBucketSyncFlowManager::RGWBucketSyncFlowManager(CephContext *_cct,
                                                   const rgw_zone_id& _zone_id,
                                                   std::optional<rgw_bucket> _bucket,
                                                   const RGWBucketSyncFlowManager *_parent) : cct(_cct),
                                                                                              zone_id(_zone_id),
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
    auto& id1 = ziter1.first;
    const RGWZone& z1 = ziter1.second;

    for (const auto& ziter2 : zonegroup.zones) {
      auto& id2 = ziter2.first;
      const RGWZone& z2 = ziter2.second;

      if (id1 == id2) {
        continue;
      }

      if (z1.syncs_from(z2.name)) {
        found = true;
        rgw_sync_directional_rule *rule;
        group.data_flow.find_or_create_directional(id2,
                                                   id1,
                                                   &rule);
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
						       RGWSI_Bucket_Sync *_bucket_sync_svc,
                                                       std::optional<rgw_zone_id> effective_zone) : zone_svc(_zone_svc) ,
                                                                                                    bucket_sync_svc(_bucket_sync_svc) {
  zone_id = effective_zone.value_or(zone_svc->zone_id());
  flow_mgr.reset(new RGWBucketSyncFlowManager(zone_svc->ctx(),
                                              zone_id,
                                              nullopt,
                                              nullptr));
  sync_policy = zone_svc->get_zonegroup().sync_policy;

  if (sync_policy.empty()) {
    RGWSyncPolicyCompat::convert_old_sync_config(zone_svc, sync_modules_svc, &sync_policy);
    legacy_config = true;
  }
}

RGWBucketSyncPolicyHandler::RGWBucketSyncPolicyHandler(const RGWBucketSyncPolicyHandler *_parent,
                                                       const RGWBucketInfo& _bucket_info,
                                                       map<string, bufferlist>&& _bucket_attrs) : parent(_parent),
                                                                                                       bucket_info(_bucket_info),
                                                                                                       bucket_attrs(std::move(_bucket_attrs)) {
  if (_bucket_info.sync_policy) {
    sync_policy = *_bucket_info.sync_policy;

    for (auto& entry : sync_policy.groups) {
      for (auto& pipe : entry.second.pipes) {
        if (pipe.params.mode == rgw_sync_pipe_params::MODE_USER &&
            pipe.params.user.empty()) {
          pipe.params.user = _bucket_info.owner;
        }
      }
    }
  }
  legacy_config = parent->legacy_config;
  bucket = _bucket_info.bucket;
  zone_svc = parent->zone_svc;
  bucket_sync_svc = parent->bucket_sync_svc;
  flow_mgr.reset(new RGWBucketSyncFlowManager(zone_svc->ctx(),
                                              parent->zone_id,
                                              _bucket_info.bucket,
                                              parent->flow_mgr.get()));
}

RGWBucketSyncPolicyHandler::RGWBucketSyncPolicyHandler(const RGWBucketSyncPolicyHandler *_parent,
                                                       const rgw_bucket& _bucket,
                                                       std::optional<rgw_sync_policy_info> _sync_policy) : parent(_parent) {
  if (_sync_policy) {
    sync_policy = *_sync_policy;
  }
  legacy_config = parent->legacy_config;
  bucket = _bucket;
  zone_svc = parent->zone_svc;
  bucket_sync_svc = parent->bucket_sync_svc;
  flow_mgr.reset(new RGWBucketSyncFlowManager(zone_svc->ctx(),
                                              parent->zone_id,
                                              _bucket,
                                              parent->flow_mgr.get()));
}

RGWBucketSyncPolicyHandler *RGWBucketSyncPolicyHandler::alloc_child(const RGWBucketInfo& bucket_info,
                                                                    map<string, bufferlist>&& bucket_attrs) const
{
  return new RGWBucketSyncPolicyHandler(this, bucket_info, std::move(bucket_attrs));
}

RGWBucketSyncPolicyHandler *RGWBucketSyncPolicyHandler::alloc_child(const rgw_bucket& bucket,
                                                                    std::optional<rgw_sync_policy_info> sync_policy) const
{
  return new RGWBucketSyncPolicyHandler(this, bucket, sync_policy);
}

int RGWBucketSyncPolicyHandler::init(const DoutPrefixProvider *dpp, optional_yield y)
{
  int r = bucket_sync_svc->get_bucket_sync_hints(dpp, bucket.value_or(rgw_bucket()),
						 &source_hints,
						 &target_hints,
						 y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize bucket sync policy handler: get_bucket_sync_hints() on bucket="
      << bucket << " returned r=" << r << dendl;
    return r;
  }

  flow_mgr->init(dpp, sync_policy);

  reflect(dpp, &source_pipes,
          &target_pipes,
          &sources,
          &targets,
          &source_zones,
          &target_zones,
          true);

  return 0;
}

void RGWBucketSyncPolicyHandler::reflect(const DoutPrefixProvider *dpp, RGWBucketSyncFlowManager::pipe_set *psource_pipes,
                                         RGWBucketSyncFlowManager::pipe_set *ptarget_pipes,
                                         map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set> *psources,
                                         map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set> *ptargets,
                                         std::set<rgw_zone_id> *psource_zones,
                                         std::set<rgw_zone_id> *ptarget_zones,
                                         bool only_enabled) const
{
  RGWBucketSyncFlowManager::pipe_set _source_pipes;
  RGWBucketSyncFlowManager::pipe_set _target_pipes;
  map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set> _sources;
  map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set> _targets;
  std::set<rgw_zone_id> _source_zones;
  std::set<rgw_zone_id> _target_zones;

  flow_mgr->reflect(dpp, bucket, &_source_pipes, &_target_pipes, only_enabled);

  for (auto& entry : _source_pipes.pipe_map) {
    auto& pipe = entry.second;
    if (!pipe.source.zone) {
      continue;
    }
    _source_zones.insert(*pipe.source.zone);
    _sources[*pipe.source.zone].insert(pipe);
  }

  for (auto& entry : _target_pipes.pipe_map) {
    auto& pipe = entry.second;
    if (!pipe.dest.zone) {
      continue;
    }
    _target_zones.insert(*pipe.dest.zone);
    _targets[*pipe.dest.zone].insert(pipe);
  }

  if (psource_pipes) {
    *psource_pipes = std::move(_source_pipes);
  }
  if (ptarget_pipes) {
    *ptarget_pipes = std::move(_target_pipes);
  }
  if (psources) {
    *psources = std::move(_sources);
  }
  if (ptargets) {
    *ptargets = std::move(_targets);
  }
  if (psource_zones) {
    *psource_zones = std::move(_source_zones);
  }
  if (ptarget_zones) {
    *ptarget_zones = std::move(_target_zones);
  }
}

multimap<rgw_zone_id, rgw_sync_bucket_pipe> RGWBucketSyncPolicyHandler::get_all_sources() const
{
  multimap<rgw_zone_id, rgw_sync_bucket_pipe> m;

  for (auto& source_entry : sources) {
    auto& zone_id = source_entry.first;

    auto& pipes = source_entry.second.pipe_map;

    for (auto& entry : pipes) {
      auto& pipe = entry.second;
      m.insert(make_pair(zone_id, pipe));
    }
  }

  for (auto& pipe : resolved_sources) {
    if (!pipe.source.zone) {
      continue;
    }

    m.insert(make_pair(*pipe.source.zone, pipe));
  }

  return m;
}

multimap<rgw_zone_id, rgw_sync_bucket_pipe> RGWBucketSyncPolicyHandler::get_all_dests() const
{
  multimap<rgw_zone_id, rgw_sync_bucket_pipe> m;

  for (auto& dest_entry : targets) {
    auto& zone_id = dest_entry.first;

    auto& pipes = dest_entry.second.pipe_map;

    for (auto& entry : pipes) {
      auto& pipe = entry.second;
      m.insert(make_pair(zone_id, pipe));
    }
  }

  for (auto& pipe : resolved_dests) {
    if (!pipe.dest.zone) {
      continue;
    }

    m.insert(make_pair(*pipe.dest.zone, pipe));
  }

  return m;
}

multimap<rgw_zone_id, rgw_sync_bucket_pipe> RGWBucketSyncPolicyHandler::get_all_dests_in_zone(const rgw_zone_id& zone_id) const
{
  multimap<rgw_zone_id, rgw_sync_bucket_pipe> m;

  auto iter = targets.find(zone_id);
  if (iter != targets.end()) {
    auto& pipes = iter->second.pipe_map;

    for (auto& entry : pipes) {
      auto& pipe = entry.second;
      m.insert(make_pair(zone_id, pipe));
    }
  }

  for (auto& pipe : resolved_dests) {
    if (!pipe.dest.zone ||
        *pipe.dest.zone != zone_id) {
      continue;
    }

    m.insert(make_pair(*pipe.dest.zone, pipe));
  }

  return m;
}

void RGWBucketSyncPolicyHandler::get_pipes(std::set<rgw_sync_bucket_pipe> *_sources, std::set<rgw_sync_bucket_pipe> *_targets,
                                           std::optional<rgw_sync_bucket_entity> filter_peer) { /* return raw pipes */
  for (auto& entry : source_pipes.pipe_map) {
    auto& source_pipe = entry.second;
    if (!filter_peer ||
        source_pipe.source.match(*filter_peer)) {
      _sources->insert(source_pipe);
    }
  }

  for (auto& entry : target_pipes.pipe_map) {
    auto& target_pipe = entry.second;
    if (!filter_peer ||
        target_pipe.dest.match(*filter_peer)) {
      _targets->insert(target_pipe);
    }
  }
}

bool RGWBucketSyncPolicyHandler::bucket_exports_data() const
{
  if (!bucket) {
    return false;
  }

  if (!zone_svc->sync_module_exports_data()) {
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

