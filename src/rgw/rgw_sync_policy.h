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
  bool wildcard{false};
  std::optional<rgw_bucket> bucket;
  std::optional<std::set<string> > zones;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(wildcard, bl);
    encode(bucket, bl);
    encode(zones, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(wildcard, bl);
    decode(bucket, bl);
    decode(zones, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_entity)

struct rgw_sync_bucket_pipe {
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

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_pipe)



struct rgw_sync_policy_info {
  /* zone data flow */
  std::optional<std::vector<rgw_sync_symmetric_group> > symmetrical;
  std::optional<std::vector<rgw_sync_directional_rule> > directional;

  std::optional<std::vector<rgw_sync_bucket_pipe> > pipes;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(symmetrical, bl);
    encode(directional, bl);
    encode(pipes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(symmetrical, bl);
    decode(directional, bl);
    decode(pipes, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  template <class O>
  bool empty_opt(O& o) const {
    return (!o || o->empty());
  }

  bool empty() const {
    return (empty_opt(symmetrical) &&
            empty_opt(directional) &&
            empty_opt(pipes));
  }

};
WRITE_CLASS_ENCODER(rgw_sync_policy_info)


