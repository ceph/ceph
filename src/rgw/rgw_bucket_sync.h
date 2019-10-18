
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

class RGWSI_Zone;
struct group_pipe_map;
struct rgw_sync_bucket_pipe;;
struct rgw_sync_policy_info;

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
                         const string& dest_zone,
                         std::optional<rgw_bucket> dest_bucket,
                         bool check_activated);

  using flow_map_t = map<rgw_bucket, pipe_flow>;

  flow_map_t flow_by_source;
  flow_map_t flow_by_dest;

  /*
   * find all the matching flows om a flow map for a specific bucket
   */
  flow_map_t::iterator find_bucket_flow(flow_map_t& m, std::optional<rgw_bucket> bucket);

  void update_flow_maps(const rgw_sync_bucket_pipe& pipe,
                        group_pipe_map *flow_group);

public:

  RGWBucketSyncFlowManager(RGWSI_Zone *_zone_svc,
                           std::optional<rgw_bucket> _bucket,
                           RGWBucketSyncFlowManager *_parent);

  void init(const rgw_sync_policy_info& sync_policy);
};

class RGWBucketSyncPolicyHandler {
  RGWSI_Zone *zone_svc;
  RGWBucketInfo bucket_info;

  std::set<string> source_zones;

public:
  struct peer_info {
    std::string type;
    rgw_bucket bucket;
    /* need to have config for other type of sources */

    bool operator<(const peer_info& si) const {
      if (type == si.type) {
        return (bucket < si.bucket);
      }
      return (type < si.type);
    }

    bool is_rgw() const {
      return (type.empty() || type == "rgw");
    }

    string get_type() const {
      if (!type.empty()) {
        return type;
      }
      return "rgw";
    }

    void dump(Formatter *f) const;
  };

private:
  std::map<string, std::set<peer_info> > sources; /* peers by zone */
  std::map<string, std::set<peer_info> > targets; /* peers by zone */

public:
  RGWBucketSyncPolicyHandler(RGWSI_Zone *_zone_svc,
                             RGWBucketInfo& _bucket_info) : zone_svc(_zone_svc),
                                                            bucket_info(_bucket_info) {}
  int init();

  std::map<string, std::set<peer_info> >& get_sources() {
    return sources;
  }

  const RGWBucketInfo& get_bucket_info() const {
    return bucket_info;
  }

  bool zone_is_source(const string& zone_id) const {
    return sources.find(zone_id) != sources.end();
  }

  bool bucket_is_sync_source() const {
    return !targets.empty();
  }

  bool bucket_is_sync_target() const {
    return !sources.empty();
  }

  bool bucket_exports_data() const;
  bool bucket_imports_data() const;
};

inline ostream& operator<<(ostream& out, const RGWBucketSyncPolicyHandler::peer_info& pi) {
  return out << pi.bucket << " (" << pi.get_type() << ")";
}
