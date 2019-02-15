
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

class JSONObj;

class RGWBucketSyncPolicy {
public:
  struct target;

private:
  rgw_bucket bucket; /* source bucket */
  std::map<string, target> targets; /* map: target zone_id -> target rules */

  /* in-memory only */
  std::set<string> source_zones;

  void post_init();

public:

  struct rule {
    std::string zone_id;
    std::string dest_bucket;
    std::string source_obj_prefix;
    std::string dest_obj_prefix;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(zone_id, bl);
      encode(dest_bucket, bl);
      encode(source_obj_prefix, bl);
      encode(dest_obj_prefix, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(zone_id, bl);
      decode(dest_bucket, bl);
      decode(source_obj_prefix, bl);
      decode(dest_obj_prefix, bl);
      DECODE_FINISH(bl);
    }

    void dump(ceph::Formatter *f) const;
    void decode_json(JSONObj *obj);
  };

  struct target {
    std::string target_zone_id;
    std::vector<rule> rules;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(target_zone_id, bl);
      encode(rules, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(target_zone_id, bl);
      decode(rules, bl);
      DECODE_FINISH(bl);
    }

    void dump(ceph::Formatter *f) const;
    void decode_json(JSONObj *obj);
  };

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    encode(targets, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket, bl);
    decode(targets, bl);
    post_init();
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool empty() const {
    return targets.empty();
  }

  bool zone_is_source(const string& zone_id) const {
    return source_zones.find(zone_id) != source_zones.end();
  }
};
WRITE_CLASS_ENCODER(RGWBucketSyncPolicy::rule)
WRITE_CLASS_ENCODER(RGWBucketSyncPolicy::target)
WRITE_CLASS_ENCODER(RGWBucketSyncPolicy)
