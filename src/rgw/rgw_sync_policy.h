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
  std::set<string> zones;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(zones, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(zones, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_symmetric_group)

struct rgw_sync_directional_rule {
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
WRITE_CLASS_ENCODER(rgw_sync_directional_rule)

struct rgw_sync_bucket_entity {
private:
  bool match_str(const string& s1, const string& s2) const { /* empty string is wildcard */
    return (s1.empty() ||
            s2.empty() ||
            s1 == s2);
  }

public:
  std::optional<rgw_bucket> bucket; /* define specific bucket */
  std::optional<std::set<string> > zones; /* define specific zones, if not set then all zones */

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    encode(zones, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket, bl);
    decode(zones, bl);
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

  bool match_zone(const string& zone) const {
    if (!zones) { /* all zones */
      return true;
    }

    return (zones->find(zone) != zones->end());
  }

  rgw_bucket get_bucket() const {
    return bucket.value_or(rgw_bucket());
  }
    
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_entity)

struct rgw_sync_bucket_pipe {
private:
  void symmetrical_copy_if_empty(string& s1, string& s2) const {
    if (s1.empty()) {
      s1 = s2;
    } else if (s2.empty()) {
      s2 = s1;
    }
  }

public:
  rgw_sync_bucket_entity source;
  rgw_sync_bucket_entity target;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source, bl);
    encode(target, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source, bl);
    decode(target, bl);
    DECODE_FINISH(bl);
  }

  bool contains_bucket(std::optional<rgw_bucket> b) const {
    return (source.match_bucket(b) || target.match_bucket(b));
  }
  bool contains_zone(const string& zone) const {
    return (source.match_zone(zone) || target.match_zone(zone));
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  void get_bucket_pair(rgw_bucket *source_bucket,
                       rgw_bucket *target_bucket) const {
    *source_bucket = source.get_bucket();
    *target_bucket = target.get_bucket();

    symmetrical_copy_if_empty(source_bucket->tenant, target_bucket->tenant);
    symmetrical_copy_if_empty(source_bucket->name, target_bucket->name);
    if (source_bucket->name == target_bucket->name) { /* doesn't make sense to copy bucket id if not same bucket name */
      symmetrical_copy_if_empty(source_bucket->bucket_id, target_bucket->bucket_id);
    }
  }
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_pipe)

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
};
WRITE_CLASS_ENCODER(rgw_sync_data_flow_group)


struct rgw_sync_policy_group {
  string id;

  std::optional<rgw_sync_data_flow_group> data_flow; /* override data flow, howver, will not be able to
                                                        add new flows that don't exist at higher level */
  std::optional<std::vector<rgw_sync_bucket_pipe> > pipes; /* if not defined then applies to all
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


