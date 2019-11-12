
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
  string zone;
  std::optional<rgw_bucket> bucket;

  rgw_sync_policy_group::Status status{rgw_sync_policy_group::Status::FORBIDDEN};

  using zb_pipe_map_t = std::multimap<rgw_sync_bucket_entity, rgw_sync_bucket_pipe>;

  zb_pipe_map_t sources; /* all the pipes where zone is pulling from */
  zb_pipe_map_t dests; /* all the pipes that pull from zone */

  std::set<string> *pall_zones{nullptr};
  rgw_sync_data_flow_group *default_flow{nullptr}; /* flow to use if policy doesn't define it,
                                                      used in the case of bucket sync policy, not at the
                                                      zonegroup level */

  void dump(ceph::Formatter *f) const;

  template <typename CB1, typename CB2>
  void try_add_to_pipe_map(const string& source_zone,
                           const string& dest_zone,
                           const std::vector<rgw_sync_bucket_pipes>& pipes,
                           zb_pipe_map_t *pipe_map,
                           CB1 filter_cb,
                           CB2 call_filter_cb);
          
  template <typename CB>
  void try_add_source(const string& source_zone,
                      const string& dest_zone,
                      const std::vector<rgw_sync_bucket_pipes>& pipes,
                      CB filter_cb);
          
  template <typename CB>
  void try_add_dest(const string& source_zone,
                  const string& dest_zone,
                  const std::vector<rgw_sync_bucket_pipes>& pipes,
                  CB filter_cb);
          
  pair<zb_pipe_map_t::const_iterator, zb_pipe_map_t::const_iterator> find_pipes(const zb_pipe_map_t& m,
                                                                                const string& zone,
                                                                                std::optional<rgw_bucket> b) const;

  template <typename CB>
  void init(const string& _zone,
            std::optional<rgw_bucket> _bucket,
            const rgw_sync_policy_group& group,
            rgw_sync_data_flow_group *_default_flow,
            std::set<std::string> *_pall_zones,
            CB filter_cb);

  /*
   * find all relevant pipes in our zone that match {dest_bucket} <- {source_zone, source_bucket}
   */
  vector<rgw_sync_bucket_pipe> find_source_pipes(const string& source_zone,
                                                 std::optional<rgw_bucket> source_bucket,
                                                 std::optional<rgw_bucket> dest_bucket) const;

  /*
   * find all relevant pipes in other zones that pull from a specific
   * source bucket in out zone {source_bucket} -> {dest_zone, dest_bucket}
   */
  vector<rgw_sync_bucket_pipe> find_dest_pipes(std::optional<rgw_bucket> source_bucket,
                                               const string& dest_zone,
                                               std::optional<rgw_bucket> dest_bucket) const;

  /*
   * find all relevant pipes from {source_zone, source_bucket} -> {dest_zone, dest_bucket}
   */
  vector<rgw_sync_bucket_pipe> find_pipes(const string& source_zone,
                                          std::optional<rgw_bucket> source_bucket,
                                          const string& dest_zone,
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

  class pipe_rules {
    void resolve_prefix(rgw_sync_bucket_pipe *ppipe);

  public:
    std::map<string, rgw_sync_bucket_pipe> pipe_map; /* id to pipe */

    std::multimap<size_t, rgw_sync_bucket_pipe *> prefix_by_size;

    map<rgw_sync_pipe_filter_tag, rgw_sync_bucket_pipe *> tag_refs;
    map<string, rgw_sync_bucket_pipe *> prefix_refs;

    void insert(const rgw_sync_bucket_pipe& pipe);

    void finish_init();
  };

  struct pipe_set {
    std::map<endpoints_pair, pipe_rules> rules;
    std::set<rgw_sync_bucket_pipe> pipes;

    using iterator = std::set<rgw_sync_bucket_pipe>::iterator;

    void clear() {
      pipes.clear();
    }

    void insert(const rgw_sync_bucket_pipe& pipe) {
      pipes.insert(pipe);
    }

    iterator begin() {
      return pipes.begin();
    }

    iterator end() {
      return pipes.end();
    }

    void dump(ceph::Formatter *f) const;
  };

private:

  string zone_name;
  std::optional<rgw_bucket> bucket;

  const RGWBucketSyncFlowManager *parent{nullptr};

  map<string, rgw_sync_group_pipe_map> flow_groups;

  std::set<std::string> all_zones;

  bool allowed_data_flow(const string& source_zone,
                         std::optional<rgw_bucket> source_bucket,
                         const string& dest_zone,
                         std::optional<rgw_bucket> dest_bucket,
                         bool check_activated) const;

  /*
   * find all the matching flows om a flow map for a specific bucket
   */
  void update_flow_maps(const rgw_sync_bucket_pipes& pipe);

  void init(const rgw_sync_policy_info& sync_policy);

public:

  RGWBucketSyncFlowManager(const string& _zone_name,
                           std::optional<rgw_bucket> _bucket,
                           const RGWBucketSyncFlowManager *_parent);

  void reflect(std::optional<rgw_bucket> effective_bucket,
               pipe_set *flow_by_source,
               pipe_set *flow_by_dest,  
               bool only_enabled) const;

};

class RGWBucketSyncPolicyHandler {
  const RGWBucketSyncPolicyHandler *parent{nullptr};
  RGWSI_Zone *zone_svc;
  RGWSI_Bucket_Sync *bucket_sync_svc;
  string zone_name;
  std::optional<RGWBucketInfo> bucket_info;
  std::optional<rgw_bucket> bucket;
  std::unique_ptr<RGWBucketSyncFlowManager> flow_mgr;
  rgw_sync_policy_info sync_policy;

  RGWBucketSyncFlowManager::pipe_set sources_by_name;
  RGWBucketSyncFlowManager::pipe_set targets_by_name;

  map<string, RGWBucketSyncFlowManager::pipe_set> sources; /* source pipes by source zone id */
  map<string, RGWBucketSyncFlowManager::pipe_set> targets; /* target pipes by target zone id */

  std::set<string> source_zones; /* source zones by name */
  std::set<string> target_zones; /* target zones by name */

  std::set<rgw_bucket> source_hints;
  std::set<rgw_bucket> target_hints;

  bool bucket_is_sync_source() const {
    return !targets.empty();
  }

  bool bucket_is_sync_target() const {
    return !sources.empty();
  }

  RGWBucketSyncPolicyHandler(const RGWBucketSyncPolicyHandler *_parent,
                             const RGWBucketInfo& _bucket_info);

  RGWBucketSyncPolicyHandler(const RGWBucketSyncPolicyHandler *_parent,
                             const rgw_bucket& _bucket,
                             std::optional<rgw_sync_policy_info> _sync_policy);
public:
  RGWBucketSyncPolicyHandler(RGWSI_Zone *_zone_svc,
                             RGWSI_SyncModules *sync_modules_svc,
			     RGWSI_Bucket_Sync *bucket_sync_svc,
                             std::optional<string> effective_zone = std::nullopt);

  RGWBucketSyncPolicyHandler *alloc_child(const RGWBucketInfo& bucket_info) const;
  RGWBucketSyncPolicyHandler *alloc_child(const rgw_bucket& bucket,
                                          std::optional<rgw_sync_policy_info> sync_policy) const;

  int init(optional_yield y);

  void reflect(RGWBucketSyncFlowManager::pipe_set *psources_by_name,
               RGWBucketSyncFlowManager::pipe_set *ptargets_by_name,
               map<string, RGWBucketSyncFlowManager::pipe_set> *psources,
               map<string, RGWBucketSyncFlowManager::pipe_set> *ptargets,
               std::set<string> *psource_zones,
               std::set<string> *ptarget_zones,
               bool only_enabled) const;

  const std::set<string>& get_source_zones() const {
    return source_zones;
  }

  const std::set<string>& get_target_zones() const {
    return target_zones;
  }

  const  map<string, RGWBucketSyncFlowManager::pipe_set>& get_sources() {
    return sources;
  }

  const  map<string, RGWBucketSyncFlowManager::pipe_set>& get_targets() {
    return targets;
  }

  const std::optional<RGWBucketInfo>& get_bucket_info() const {
    return bucket_info;
  }

  void get_pipes(RGWBucketSyncFlowManager::pipe_set **sources, RGWBucketSyncFlowManager::pipe_set **targets) { /* return raw pipes (with zone name) */
    *sources = &sources_by_name;
    *targets = &targets_by_name;
  }
  void get_pipes(RGWBucketSyncFlowManager::pipe_set *sources, RGWBucketSyncFlowManager::pipe_set *targets,
		 std::optional<rgw_sync_bucket_entity> filter_peer);

  const std::set<rgw_bucket>& get_source_hints() const {
    return source_hints;
  }

  const std::set<rgw_bucket>& get_target_hints() const {
    return target_hints;
  }

  bool bucket_exports_data() const;
  bool bucket_imports_data() const;
};

