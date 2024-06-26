
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_common.h"
#include "rgw_sync_policy.h"

class RGWSI_Zone;
class RGWSI_SyncModules;
class RGWSI_Bucket_Sync;

struct rgw_sync_group_pipe_map;
struct rgw_sync_bucket_pipes;
struct rgw_sync_policy_info;

struct rgw_sync_group_pipe_map {
  rgw_zone_id zone;
  std::optional<rgw_bucket> bucket;

  rgw_sync_policy_group::Status status{rgw_sync_policy_group::Status::UNKNOWN};

  using zb_pipe_map_t = std::multimap<rgw_sync_bucket_entity, rgw_sync_bucket_pipe>;

  zb_pipe_map_t sources; /* all the pipes where zone is pulling from */
  zb_pipe_map_t dests; /* all the pipes that pull from zone */

  std::set<rgw_zone_id> *pall_zones{nullptr};
  rgw_sync_data_flow_group *default_flow{nullptr}; /* flow to use if policy doesn't define it,
                                                      used in the case of bucket sync policy, not at the
                                                      zonegroup level */

  void dump(ceph::Formatter *f) const;

  template <typename CB1, typename CB2>
  void try_add_to_pipe_map(const rgw_zone_id& source_zone,
                           const rgw_zone_id& dest_zone,
                           const std::vector<rgw_sync_bucket_pipes>& pipes,
                           zb_pipe_map_t *pipe_map,
                           CB1 filter_cb,
                           CB2 call_filter_cb);
          
  template <typename CB>
  void try_add_source(const rgw_zone_id& source_zone,
                      const rgw_zone_id& dest_zone,
                      const std::vector<rgw_sync_bucket_pipes>& pipes,
                      CB filter_cb);
          
  template <typename CB>
  void try_add_dest(const rgw_zone_id& source_zone,
                  const rgw_zone_id& dest_zone,
                  const std::vector<rgw_sync_bucket_pipes>& pipes,
                  CB filter_cb);
          
  std::pair<zb_pipe_map_t::const_iterator, zb_pipe_map_t::const_iterator> find_pipes(const zb_pipe_map_t& m,
                                                                                const rgw_zone_id& zone,
                                                                                std::optional<rgw_bucket> b) const;

  template <typename CB>
  void init(const DoutPrefixProvider *dpp, CephContext *cct,
            const rgw_zone_id& _zone,
            std::optional<rgw_bucket> _bucket,
            const rgw_sync_policy_group& group,
            rgw_sync_data_flow_group *_default_flow,
            std::set<rgw_zone_id> *_pall_zones,
            CB filter_cb);

  /*
   * find all relevant pipes in our zone that match {dest_bucket} <- {source_zone, source_bucket}
   */
  std::vector<rgw_sync_bucket_pipe> find_source_pipes(const rgw_zone_id& source_zone,
                                                 std::optional<rgw_bucket> source_bucket,
                                                 std::optional<rgw_bucket> dest_bucket) const;

  /*
   * find all relevant pipes in other zones that pull from a specific
   * source bucket in out zone {source_bucket} -> {dest_zone, dest_bucket}
   */
  std::vector<rgw_sync_bucket_pipe> find_dest_pipes(std::optional<rgw_bucket> source_bucket,
                                               const rgw_zone_id& dest_zone,
                                               std::optional<rgw_bucket> dest_bucket) const;

  /*
   * find all relevant pipes from {source_zone, source_bucket} -> {dest_zone, dest_bucket}
   */
  std::vector<rgw_sync_bucket_pipe> find_pipes(const rgw_zone_id& source_zone,
                                          std::optional<rgw_bucket> source_bucket,
                                          const rgw_zone_id& dest_zone,
                                          std::optional<rgw_bucket> dest_bucket) const;
};

class RGWSyncPolicyCompat {
public:
  static void convert_old_sync_config(RGWSI_Zone *zone_svc,
                                      RGWSI_SyncModules *sync_modules_svc,
                                      rgw_sync_policy_info *ppolicy);
};

class RGWBucketSyncFlowManager {
  friend class RGWBucketSyncPolicyHandler;
public:
  struct endpoints_pair {
    rgw_sync_bucket_entity source;
    rgw_sync_bucket_entity dest;

    endpoints_pair() {}
    endpoints_pair(const rgw_sync_bucket_pipe& pipe) {
      source = pipe.source;
      dest = pipe.dest;
    }

    bool operator<(const endpoints_pair& e) const {
      if (source < e.source) {
        return true;
      }
      if (e.source < source) {
        return false;
      }
      return (dest < e.dest);
    }
  };

  /*
   * pipe_rules: deal with a set of pipes that have common endpoints_pair
   */
  class pipe_rules {
    std::list<rgw_sync_bucket_pipe> pipes;

  public:
    using prefix_map_t = std::multimap<std::string, rgw_sync_bucket_pipe *>;

    std::map<std::string, rgw_sync_bucket_pipe *> tag_refs;
    prefix_map_t prefix_refs;

    void insert(const rgw_sync_bucket_pipe& pipe);

    bool find_basic_info_without_tags(const rgw_obj_key& key,
                                      std::optional<rgw_user> *user,
                                      std::optional<rgw_user> *acl_translation,
                                      std::optional<std::string> *storage_class,
                                      rgw_sync_pipe_params::Mode *mode,
                                      bool *need_more_info) const;
    bool find_obj_params(const rgw_obj_key& key, 
                         const RGWObjTags::tag_map_t& tags,
                         rgw_sync_pipe_params *params) const;

    void scan_prefixes(std::vector<std::string> *prefixes) const;

    prefix_map_t::const_iterator prefix_begin() const {
      return prefix_refs.begin();
    }
    prefix_map_t::const_iterator prefix_search(const std::string& s) const;
    prefix_map_t::const_iterator prefix_end() const {
      return prefix_refs.end();
    }
  };

  using pipe_rules_ref = std::shared_ptr<pipe_rules>;

  /*
   * pipe_handler: extends endpoints_rule to point at the corresponding rules handler
   */
  struct pipe_handler : public endpoints_pair {
    pipe_rules_ref rules;

    pipe_handler() {}
    pipe_handler(pipe_rules_ref& _rules,
                 const rgw_sync_bucket_pipe& _pipe) : endpoints_pair(_pipe),
                                                      rules(_rules) {}
    bool specific() const {
      return source.specific() && dest.specific();
    }
    
    bool find_basic_info_without_tags(const rgw_obj_key& key,
                                      std::optional<rgw_user> *user,
                                      std::optional<rgw_user> *acl_translation,
                                      std::optional<std::string> *storage_class,
                                      rgw_sync_pipe_params::Mode *mode,
                                      bool *need_more_info) const {
      if (!rules) {
        return false;
      }
      return rules->find_basic_info_without_tags(key, user, acl_translation, storage_class, mode, need_more_info);
    }

    bool find_obj_params(const rgw_obj_key& key,
                         const RGWObjTags::tag_map_t& tags,
                         rgw_sync_pipe_params *params) const {
      if (!rules) {
        return false;
      }
      return rules->find_obj_params(key, tags, params);
    }
  };

  struct pipe_set {
    std::map<endpoints_pair, pipe_rules_ref> rules;
    std::multimap<std::string, rgw_sync_bucket_pipe> pipe_map;
    std::multimap<std::string, rgw_sync_bucket_pipe> disabled_pipe_map;

    std::set<pipe_handler> handlers;

    using iterator = std::set<pipe_handler>::iterator;

    void clear() {
      rules.clear();
      pipe_map.clear();
      disabled_pipe_map.clear();
      handlers.clear();
    }

    void insert(const rgw_sync_bucket_pipe& pipe);
    void remove_all();
    void disable(const rgw_sync_bucket_pipe& pipe);

    iterator begin() const {
      return handlers.begin();
    }

    iterator end() const {
      return handlers.end();
    }

    void dump(ceph::Formatter *f) const;
  };

private:

  CephContext *cct;

  rgw_zone_id zone_id;
  std::optional<rgw_bucket> bucket;

  const RGWBucketSyncFlowManager *parent{nullptr};

  std::map<std::string, rgw_sync_group_pipe_map> flow_groups;

  std::set<rgw_zone_id> all_zones;

  bool allowed_data_flow(const rgw_zone_id& source_zone,
                         std::optional<rgw_bucket> source_bucket,
                         const rgw_zone_id& dest_zone,
                         std::optional<rgw_bucket> dest_bucket,
                         bool check_activated) const;

  /*
   * find all the matching flows om a flow map for a specific bucket
   */
  void update_flow_maps(const rgw_sync_bucket_pipes& pipe);

  void init(const DoutPrefixProvider *dpp, const rgw_sync_policy_info& sync_policy);

public:

  RGWBucketSyncFlowManager(CephContext *_cct,
                           const rgw_zone_id& _zone_id,
                           std::optional<rgw_bucket> _bucket,
                           const RGWBucketSyncFlowManager *_parent);

  void reflect(const DoutPrefixProvider *dpp, std::optional<rgw_bucket> effective_bucket,
               pipe_set *flow_by_source,
               pipe_set *flow_by_dest,  
               bool only_enabled) const;

};

static inline std::ostream& operator<<(std::ostream& os, const RGWBucketSyncFlowManager::endpoints_pair& e) {
  os << e.dest << " -> " << e.source;
  return os;
}

class RGWBucketSyncPolicyHandler {
  bool legacy_config{false};
  const RGWBucketSyncPolicyHandler *parent{nullptr};
  RGWSI_Zone *zone_svc;
  RGWSI_Bucket_Sync *bucket_sync_svc;
  rgw_zone_id zone_id;
  std::optional<RGWBucketInfo> bucket_info;
  std::optional<std::map<std::string, bufferlist> > bucket_attrs;
  std::optional<rgw_bucket> bucket;
  std::unique_ptr<RGWBucketSyncFlowManager> flow_mgr;
  rgw_sync_policy_info sync_policy;

  RGWBucketSyncFlowManager::pipe_set source_pipes;
  RGWBucketSyncFlowManager::pipe_set target_pipes;

  std::map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set> sources; /* source pipes by source zone id */
  std::map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set> targets; /* target pipes by target zone id */

  std::set<rgw_zone_id> source_zones;
  std::set<rgw_zone_id> target_zones;

  std::set<rgw_bucket> source_hints;
  std::set<rgw_bucket> target_hints;
  std::set<rgw_sync_bucket_pipe> resolved_sources;
  std::set<rgw_sync_bucket_pipe> resolved_dests;


  bool bucket_is_sync_source() const {
    return !targets.empty() || !resolved_dests.empty();
  }

  bool bucket_is_sync_target() const {
    return !sources.empty() || !resolved_sources.empty();
  }

  RGWBucketSyncPolicyHandler(const RGWBucketSyncPolicyHandler *_parent,
                             const RGWBucketInfo& _bucket_info,
                             std::map<std::string, bufferlist>&& _bucket_attrs);

  RGWBucketSyncPolicyHandler(const RGWBucketSyncPolicyHandler *_parent,
                             const rgw_bucket& _bucket,
                             std::optional<rgw_sync_policy_info> _sync_policy);
public:
  RGWBucketSyncPolicyHandler(RGWSI_Zone *_zone_svc,
                             RGWSI_SyncModules *sync_modules_svc,
			     RGWSI_Bucket_Sync *bucket_sync_svc,
                             std::optional<rgw_zone_id> effective_zone = std::nullopt);

  RGWBucketSyncPolicyHandler *alloc_child(const RGWBucketInfo& bucket_info,
                                          std::map<std::string, bufferlist>&& bucket_attrs) const;
  RGWBucketSyncPolicyHandler *alloc_child(const rgw_bucket& bucket,
                                          std::optional<rgw_sync_policy_info> sync_policy) const;

  int init(const DoutPrefixProvider *dpp, optional_yield y);

  void reflect(const DoutPrefixProvider *dpp, RGWBucketSyncFlowManager::pipe_set *psource_pipes,
               RGWBucketSyncFlowManager::pipe_set *ptarget_pipes,
               std::map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set> *psources,
               std::map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set> *ptargets,
               std::set<rgw_zone_id> *psource_zones,
               std::set<rgw_zone_id> *ptarget_zones,
               bool only_enabled) const;

  void set_resolved_hints(std::set<rgw_sync_bucket_pipe>&& _resolved_sources,
                          std::set<rgw_sync_bucket_pipe>&& _resolved_dests) {
    resolved_sources = std::move(_resolved_sources);
    resolved_dests = std::move(_resolved_dests);
  }

  const std::set<rgw_sync_bucket_pipe>& get_resolved_source_hints() {
    return resolved_sources;
  }

  const std::set<rgw_sync_bucket_pipe>& get_resolved_dest_hints() {
    return resolved_dests;
  }

  const std::set<rgw_zone_id>& get_source_zones() const {
    return source_zones;
  }

  const std::set<rgw_zone_id>& get_target_zones() const {
    return target_zones;
  }

  const  std::map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set>& get_sources() {
    return sources;
  }

  std::multimap<rgw_zone_id, rgw_sync_bucket_pipe> get_all_sources() const;
  std::multimap<rgw_zone_id, rgw_sync_bucket_pipe> get_all_dests() const;
  std::multimap<rgw_zone_id, rgw_sync_bucket_pipe> get_all_dests_in_zone(const rgw_zone_id& zone_id) const;

  const std::map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set>& get_targets() {
    return targets;
  }

  const std::optional<RGWBucketInfo>& get_bucket_info() const {
    return bucket_info;
  }

  const std::optional<std::map<std::string, bufferlist> >& get_bucket_attrs() const {
    return bucket_attrs;
  }

  void get_pipes(RGWBucketSyncFlowManager::pipe_set **_sources, RGWBucketSyncFlowManager::pipe_set **_targets) { /* return raw pipes (with zone name) */
    *_sources = &source_pipes;
    *_targets = &target_pipes;
  }
  void get_pipes(std::set<rgw_sync_bucket_pipe> *sources, std::set<rgw_sync_bucket_pipe> *targets,
                 std::optional<rgw_sync_bucket_entity> filter_peer);

  const std::set<rgw_bucket>& get_source_hints() const {
    return source_hints;
  }

  const std::set<rgw_bucket>& get_target_hints() const {
    return target_hints;
  }

  bool bucket_exports_data() const;
  bool bucket_imports_data() const;

  const rgw_sync_policy_info& get_sync_policy() const {
    return sync_policy;
  }

  bool is_legacy_config() const {
    return legacy_config;
  }
};

