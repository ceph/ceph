
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
public:
  struct pipe_set {
    std::set<rgw_sync_bucket_pipe> pipes;

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

public:

  RGWBucketSyncFlowManager(const string& _zone_name,
                           std::optional<rgw_bucket> _bucket,
                           const RGWBucketSyncFlowManager *_parent);

  void init(const rgw_sync_policy_info& sync_policy);
  void reflect(std::optional<rgw_bucket> effective_bucket,
               pipe_set *flow_by_source,
               pipe_set *flow_by_dest) const;

};

class RGWBucketSyncPolicyHandler {
  RGWSI_Zone *zone_svc;
  RGWBucketInfo bucket_info;
  std::unique_ptr<RGWBucketSyncFlowManager> flow_mgr;

  map<string, RGWBucketSyncFlowManager::pipe_set> sources; /* source pipes by source zone id */
  map<string, RGWBucketSyncFlowManager::pipe_set> targets; /* target pipes by target zone id */

  bool bucket_is_sync_source() const {
    return !targets.empty();
  }

  bool bucket_is_sync_target() const {
    return !sources.empty();
  }

public:
  RGWBucketSyncPolicyHandler(RGWSI_Zone *_zone_svc,
                             RGWBucketInfo& _bucket_info);
  int init();

  const  map<string, RGWBucketSyncFlowManager::pipe_set>& get_sources() {
    return sources;
  }

  const RGWBucketInfo& get_bucket_info() const {
    return bucket_info;
  }

  bool bucket_exports_data() const;
  bool bucket_imports_data() const;
};

