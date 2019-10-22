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

#if 0
struct rgw_sync_flow_directional_rule {
  string source_zone;
  string target_zone;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source_zone, bl);
    encode(target_zone, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source_zone, bl);
    decode(target_zone, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_flow_directional_rule)

struct rgw_sync_flow_rule {
  string id;
  std::optional<rgw_sync_flow_directional_rule> directional;
  std::optional<std::set<string> > symmetrical;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(directional, bl);
    encode(symmetrical, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(directional, bl);
    decode(symmetrical, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  void get_zone_peers(const string& zone_id, std::set<string> *sources, std::set<string> *targets) const;
};
WRITE_CLASS_ENCODER(rgw_sync_flow_rule)

struct rgw_sync_source {
  string id;
  string type;
  std::optional<string> zone;
  std::optional<rgw_bucket> bucket;
  /* FIXME: config */

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(type, bl);
    encode(zone, bl);
    encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(type, bl);
    decode(zone, bl);
    decode(bucket, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_source)

struct rgw_sync_target {
  string id;
  string type;
  std::optional<std::vector<rgw_sync_flow_rule> > flow_rules; /* flow rules for trivial sources,
                                                                if set then needs to be a subset of higher level rules */
  std::set<string> zones;  /* target zones. Can be wildcard */
  /* FIXME: add config */

  std::vector<rgw_sync_source> sources; /* non-trivial sources */
  std::optional<rgw_bucket> bucket; /* can be explicit, or not set. If not set then depending
                                       on the context */
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(type, bl);
    encode(flow_rules, bl);
    encode(zones, bl);
    encode(sources, bl);
    encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(type, bl);
    decode(flow_rules, bl);
    decode(zones, bl);
    decode(sources, bl);
    decode(bucket, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_target)


struct rgw_sync_policy_info {
  std::optional<std::vector<rgw_sync_flow_rule> > flow_rules;
  std::optional<std::vector<rgw_sync_source> > sources;
  std::optional<std::vector<rgw_sync_target> > targets;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(flow_rules, bl);
    encode(sources, bl);
    encode(targets, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(flow_rules, bl);
    decode(sources, bl);
    decode(targets, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool empty() const {
    return (!flow_rules || flow_rules->empty()) &&
           (!targets || targets->empty());
  }

};
WRITE_CLASS_ENCODER(rgw_sync_policy_info)

#endif

struct rgw_sync_symmetric_group {
  string id;
  std::set<string> zones;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(zones, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(zones, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_symmetric_group)

struct rgw_sync_directional_rule {
  string source_zone;
  string dest_zone;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source_zone, bl);
    encode(dest_zone, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source_zone, bl);
    decode(dest_zone, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_directional_rule)

struct rgw_sync_bucket_entity {
  std::optional<string> zone; /* define specific zones */
  std::optional<rgw_bucket> bucket; /* define specific bucket */

  static bool match_str(const string& s1, const string& s2) { /* empty string is wildcard */
    return (s1.empty() ||
            s2.empty() ||
            s1 == s2);
  }

  bool all_zones{false};

  rgw_sync_bucket_entity() {}
  rgw_sync_bucket_entity(const string& _zone,
                         std::optional<rgw_bucket> _bucket) : zone(_zone),
                                                              bucket(_bucket.value_or(rgw_bucket())) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(all_zones, bl);
    encode(zone, bl);
    encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(all_zones, bl);
    decode(zone, bl);
    decode(bucket, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  rgw_bucket get_bucket() const {
    return bucket.value_or(rgw_bucket());
  }

  string bucket_key() const;

  bool match_zone(const string& z) const {
    if (all_zones) {
      return true;
    }
    if (!zone) {
      return false;
    }

    return (*zone == z);
  }

  void apply_zone(const string& z) {
    all_zones = false;
    zone = z;
  }

  bool match_bucket(std::optional<rgw_bucket> b) const {
    if (!b) {
      return true;
    }

    if (!bucket) {
      return true;
    }

    return (match_str(bucket->tenant, b->tenant) &&
            match_str(bucket->name, b->name) &&
            match_str(bucket->bucket_id, b->bucket_id));
  }

  const bool operator<(const rgw_sync_bucket_entity& e) const {
    if (all_zones != e.all_zones) {
      if (zone < e.zone) {
        return true;
      }
      if (zone > e.zone) {
        return false;
      }
    }
    return (bucket < e.bucket);
  }

  void apply_bucket(std::optional<rgw_bucket> _b);
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_entity)

struct rgw_sync_bucket_pipe {
public:
  rgw_sync_bucket_entity source;
  rgw_sync_bucket_entity dest;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source, bl);
    encode(dest, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source, bl);
    decode(dest, bl);
    DECODE_FINISH(bl);
  }

  const bool operator<(const rgw_sync_bucket_pipe& p) const {
    if (source < p.source) {
      return true;
    }
    if (p.source < source) {
      return false;
    }
    return (dest < p.dest);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_pipe)

struct rgw_sync_bucket_entities {
private:
  bool match_str(const string& s1, const string& s2) const { /* empty string is wildcard */
    return (s1.empty() ||
            s2.empty() ||
            s1 == s2);
  }

public:
  std::optional<rgw_bucket> bucket; /* define specific bucket */
  std::optional<std::set<string> > zones; /* define specific zones, if not set then all zones */

  bool all_zones{false};


  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    encode(zones, bl);
    encode(all_zones, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket, bl);
    decode(zones, bl);
    decode(all_zones, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool match_bucket(std::optional<rgw_bucket> b) const {
    if (!b) {
      return true;
    }

    if (!bucket) {
      return true;
    }

    return (match_str(bucket->tenant, b->tenant) &&
            match_str(bucket->name, b->name) &&
            match_str(bucket->bucket_id, b->bucket_id));
  }

  void add_zones(const std::vector<string>& new_zones);
  void remove_zones(const std::vector<string>& rm_zones);
  void set_bucket(std::optional<string> tenant,
                  std::optional<string> bucket_name,
                  std::optional<string> bucket_id);
  void remove_bucket(std::optional<string> tenant,
                     std::optional<string> bucket_name,
                     std::optional<string> bucket_id);

  bool match_zone(const string& zone) const {
    if (all_zones) {
      return true;
    } else if (!zones) {
      return false;
    }

    return (zones->find(zone) != zones->end());
  }

  std::vector<rgw_sync_bucket_entity> expand() const;

  rgw_bucket get_bucket() const {
    return bucket.value_or(rgw_bucket());
  }

  static string bucket_key(std::optional<rgw_bucket> b);
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_entities)

struct rgw_sync_bucket_pipes {
private:
  void symmetrical_copy_if_empty(string& s1, string& s2) const {
    if (s1.empty()) {
      s1 = s2;
    } else if (s2.empty()) {
      s2 = s1;
    }
  }

public:
  string id;
  rgw_sync_bucket_entities source;
  rgw_sync_bucket_entities dest;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(source, bl);
    encode(dest, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(source, bl);
    decode(dest, bl);
    DECODE_FINISH(bl);
  }

  bool contains_bucket(std::optional<rgw_bucket> b) const {
    return (source.match_bucket(b) || dest.match_bucket(b));
  }
  bool contains_zone(const string& zone) const {
    return (source.match_zone(zone) || dest.match_zone(zone));
  }

  bool match_source(const string& zone, std::optional<rgw_bucket> b) const {
    return (source.match_zone(zone) && source.match_bucket(b));
  }

  bool match_dest(const string& zone, std::optional<rgw_bucket> b) const {
    return (dest.match_zone(zone) && dest.match_bucket(b));
  }

  bool contains_zone_bucket(const string& zone, std::optional<rgw_bucket> b) const {
    return (match_source(zone, b) || match_dest(zone, b));
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  std::vector<rgw_sync_bucket_pipe> expand() const;
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_pipes)

/*
 * define data flow between zones. Symmetrical: zones sync from each other.
 * Directional: one zone fetches data from another.
 */
struct rgw_sync_data_flow_group {
  std::optional<std::vector<rgw_sync_symmetric_group> > symmetrical;
  std::optional<std::vector<rgw_sync_directional_rule> > directional;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(symmetrical, bl);
    encode(directional, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(symmetrical, bl);
    decode(directional, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool empty() const {
    return ((!symmetrical || symmetrical->empty()) &&
            (!directional || directional->empty()));
  }

  bool find_symmetrical(const string& flow_id, bool create, rgw_sync_symmetric_group **flow_group);
  void remove_symmetrical(const string& flow_id, std::optional<std::vector<string> > zones);
  bool find_directional(const string& source_zone, const string& dest_zone, bool create, rgw_sync_directional_rule **flow_group);
  void remove_directional(const string& source_zone, const string& dest_zone);
};
WRITE_CLASS_ENCODER(rgw_sync_data_flow_group)


struct rgw_sync_policy_group {
  string id;

  rgw_sync_data_flow_group data_flow; /* override data flow, howver, will not be able to
                                                        add new flows that don't exist at higher level */
  std::vector<rgw_sync_bucket_pipes> pipes; /* if not defined then applies to all
                                                              buckets (DR sync) */

  enum Status {
    UNKNOWN     = 0,  /* ? */
    FORBIDDEN   = 1,  /* sync not allowed */
    ALLOWED     = 2,  /* sync allowed */
    ENABLED     = 3,  /* sync should happen */
  } status;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(data_flow, bl);
    encode(pipes, bl);
    encode((uint32_t)status, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(data_flow, bl);
    decode(pipes, bl);
    uint32_t s;
    decode(s, bl);
    status = (Status)s;
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool set_status(const string& s) {
    if (s == "forbidden") {
      status = rgw_sync_policy_group::Status::FORBIDDEN;
    } else if (s == "allowed") {
      status = rgw_sync_policy_group::Status::ALLOWED;
    } else if (s == "enabled") {
      status = rgw_sync_policy_group::Status::ENABLED;
    } else {
      status = rgw_sync_policy_group::Status::UNKNOWN;
      return false;
    }

    return true;
  }

  bool find_pipe(const string& pipe_id, bool create, rgw_sync_bucket_pipes **pipe);
  void remove_pipe(const string& pipe_id);
};
WRITE_CLASS_ENCODER(rgw_sync_policy_group)

struct rgw_sync_policy_info {
  std::map<string, rgw_sync_policy_group> groups;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(groups, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(groups, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool empty() const {
    return groups.empty();
  }
};
WRITE_CLASS_ENCODER(rgw_sync_policy_info)


