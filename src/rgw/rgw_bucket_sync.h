
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
struct rgw_sync_group_pipe_map;
struct rgw_sync_bucket_pipes;
struct rgw_sync_policy_info;

struct rgw_sync_group_pipe_map {
  string zone;
  std::optional<rgw_bucket> bucket;

  rgw_sync_policy_group::Status status{rgw_sync_policy_group::Status::FORBIDDEN};

  struct zone_bucket {
    string zone; /* zone name */
    rgw_bucket bucket; /* bucket, if empty then wildcard */

    zone_bucket() {}
    zone_bucket(const string& _zone,
                std::optional<rgw_bucket> _bucket) : zone(_zone),
                                                     bucket(_bucket.value_or(rgw_bucket())) {}


    bool operator<(const zone_bucket& zb) const {
      if (zone < zb.zone) {
        return true;
      }
      if (zone > zb.zone) {
        return false;
      }
      return (bucket < zb.bucket);
    }
    void dump(ceph::Formatter *f) const;
  };

  using zb_pipe_map_t = std::multimap<zone_bucket, rgw_sync_bucket_pipes>;

  zb_pipe_map_t sources; /* all the pipes where zone is pulling from */
  zb_pipe_map_t dests; /* all the pipes that pull from zone */

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
                                                                                std::optional<rgw_bucket> b);

  template <typename CB>
  void init(const string& _zone,
            std::optional<rgw_bucket> _bucket,
            const rgw_sync_policy_group& group,
            CB filter_cb);

  /*
   * find all relevant pipes in our zone that match {dest_bucket} <- {source_zone, source_bucket}
   */
  vector<rgw_sync_bucket_pipes> find_source_pipes(const string& source_zone,
                                                 std::optional<rgw_bucket> source_bucket,
                                                 std::optional<rgw_bucket> dest_bucket);

  /*
   * find all relevant pipes in other zones that pull from a specific
   * source bucket in out zone {source_bucket} -> {dest_zone, dest_bucket}
   */
  vector<rgw_sync_bucket_pipes> find_dest_pipes(std::optional<rgw_bucket> source_bucket,
                                                 const string& dest_zone,
                                                 std::optional<rgw_bucket> dest_bucket);

  /*
   * find all relevant pipes from {source_zone, source_bucket} -> {dest_zone, dest_bucket}
   */
  vector<rgw_sync_bucket_pipes> find_pipes(const string& source_zone,
                                          std::optional<rgw_bucket> source_bucket,
                                          const string& dest_zone,
                                          std::optional<rgw_bucket> dest_bucket);
};

class RGWBucketSyncFlowManager {
public:
  struct pipe_flow {
    std::vector<rgw_sync_bucket_pipe> pipe;

    void dump(ceph::Formatter *f) const;
  };

  using flow_map_t = map<rgw_bucket, pipe_flow>;

private:

  string zone_name;
  std::optional<rgw_bucket> bucket;

  RGWBucketSyncFlowManager *parent{nullptr};

  map<string, rgw_sync_group_pipe_map> flow_groups;

  bool allowed_data_flow(const string& source_zone,
                         std::optional<rgw_bucket> source_bucket,
                         const string& dest_zone,
                         std::optional<rgw_bucket> dest_bucket,
                         bool check_activated);

  flow_map_t flow_by_source;
  flow_map_t flow_by_dest;

  /*
   * find all the matching flows om a flow map for a specific bucket
   */
  flow_map_t::iterator find_bucket_flow(flow_map_t& m, std::optional<rgw_bucket> bucket);

  void update_flow_maps(const rgw_sync_bucket_pipes& pipe);

public:

  RGWBucketSyncFlowManager(const string& _zone_name,
                           std::optional<rgw_bucket> _bucket,
                           RGWBucketSyncFlowManager *_parent);

  void init(const rgw_sync_policy_info& sync_policy);

  const flow_map_t& get_sources() {
    return flow_by_source;
  }
  const flow_map_t& get_dests() {
    return flow_by_dest;
  }
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
