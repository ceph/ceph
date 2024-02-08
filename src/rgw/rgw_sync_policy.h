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

#include "rgw_basic_types.h"
#include "rgw_tag.h"


struct rgw_sync_symmetric_group {
  std::string id;
  std::set<rgw_zone_id> zones;

  rgw_sync_symmetric_group() {}
  rgw_sync_symmetric_group(const std::string& _id,
                           const std::set<rgw_zone_id> _zones) : id(_id), zones(_zones) {}


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
  rgw_zone_id source_zone;
  rgw_zone_id dest_zone;

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
  std::optional<rgw_zone_id> zone; /* define specific zones */
  std::optional<rgw_bucket> bucket; /* define specific bucket */

  static bool match_str(const std::string& s1, const std::string& s2) { /* empty std::string is wildcard */
    return (s1.empty() ||
            s2.empty() ||
            s1 == s2);
  }

  bool all_zones{false};

  rgw_sync_bucket_entity() {}
  rgw_sync_bucket_entity(const rgw_zone_id& _zone,
                         std::optional<rgw_bucket> _bucket) : zone(_zone),
                                                              bucket(_bucket.value_or(rgw_bucket())) {}

  bool specific() const {
    return zone && bucket;
  }

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

  std::string bucket_key() const;

  bool match_zone(const rgw_zone_id& z) const {
    if (all_zones) {
      return true;
    }
    if (!zone) {
      return false;
    }

    return (*zone == z);
  }

  void apply_zone(const rgw_zone_id& z) {
    all_zones = false;
    zone = z;
  }

  static bool match_bucket_id(const std::string& bid1, const std::string& bid2) {
    return (bid1.empty() || bid2.empty() || (bid1 == bid2));
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
            match_bucket_id(bucket->bucket_id, b->bucket_id));
  }

  bool match(const rgw_sync_bucket_entity& entity) const {
    if (!entity.zone) {
      return match_bucket(entity.bucket);
    }
    return (match_zone(*entity.zone) && match_bucket(entity.bucket));
  }

  const bool operator<(const rgw_sync_bucket_entity& e) const {
    if (all_zones && !e.all_zones) {
      return false;
    }
    if (!all_zones && e.all_zones) {
      return true;
    }
    if (zone < e.zone) {
      return true;
    }
    if (e.zone < zone) {
      return false;
    }
    return (bucket < e.bucket);
  }

  void apply_bucket(std::optional<rgw_bucket> _b);
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_entity)

struct rgw_sync_pipe_filter_tag {
  std::string key;
  std::string value;

  rgw_sync_pipe_filter_tag() {}
  rgw_sync_pipe_filter_tag(const std::string& s) {
    from_str(s);
  }
  rgw_sync_pipe_filter_tag(const std::string& _key,
                           const std::string& _value) : key(_key),
                                                   value(_value) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(value, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    decode(value, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool from_str(const std::string& s);

  bool operator<(const rgw_sync_pipe_filter_tag& t) const {
    if (key < t.key) {
      return true;
    }
    if (t.key < key) {
      return false;
    }
    return (value < t.value);
  }

  bool operator==(const std::string& s) const;
};
WRITE_CLASS_ENCODER(rgw_sync_pipe_filter_tag)

struct rgw_sync_pipe_filter {
  std::optional<std::string> prefix;
  std::set<rgw_sync_pipe_filter_tag> tags;

  void set_prefix(std::optional<std::string> opt_prefix,
                  bool prefix_rm);
  void set_tags(std::list<std::string>& tags_add,
                std::list<std::string>& tags_rm);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool is_subset_of(const rgw_sync_pipe_filter& f) const;

  bool has_tags() const;
  bool check_tag(const std::string& s) const;
  bool check_tag(const std::string& k, const std::string& v) const;
  bool check_tags(const std::vector<std::string>& tags) const;
  bool check_tags(const RGWObjTags::tag_map_t& tags) const;
};
WRITE_CLASS_ENCODER(rgw_sync_pipe_filter)

struct rgw_sync_pipe_acl_translation {
  rgw_user owner;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(owner, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(owner, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool operator==(const rgw_sync_pipe_acl_translation& aclt) const {
    return (owner == aclt.owner);
  }
};
WRITE_CLASS_ENCODER(rgw_sync_pipe_acl_translation)

struct rgw_sync_pipe_source_params {
  rgw_sync_pipe_filter filter;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(filter, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(filter, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_pipe_source_params)

struct rgw_sync_pipe_dest_params {
  std::optional<rgw_sync_pipe_acl_translation> acl_translation;
  std::optional<std::string> storage_class;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(acl_translation, bl);
    encode(storage_class, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(acl_translation, bl);
    decode(storage_class, bl);
    DECODE_FINISH(bl);
  }

  void set_storage_class(const std::string& sc) {
    storage_class = sc;
  }

  void set_owner(const rgw_user& owner) {
    if (owner.empty()){
      acl_translation.reset();
    } else {
      acl_translation.emplace();
      acl_translation->owner = owner;
    }
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool operator==(const rgw_sync_pipe_dest_params& rhs) const {
    return (acl_translation == rhs.acl_translation &&
	    storage_class == rhs.storage_class);
  }
};
WRITE_CLASS_ENCODER(rgw_sync_pipe_dest_params)

struct rgw_sync_pipe_params {
  rgw_sync_pipe_source_params source;
  rgw_sync_pipe_dest_params dest;
  enum Mode {
    MODE_SYSTEM = 0,
    MODE_USER = 1,
  } mode{MODE_SYSTEM};
  int32_t priority{0};
  rgw_user user;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source, bl);
    encode(dest, bl);
    encode(priority, bl);
    encode((uint8_t)mode, bl);
    encode(user, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source, bl);
    decode(dest, bl);
    decode(priority, bl);
    uint8_t m;
    decode(m, bl);
    mode = (Mode)m;
    decode(user, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_sync_pipe_params)

struct rgw_sync_bucket_pipe {
  std::string id;
  rgw_sync_bucket_entity source;
  rgw_sync_bucket_entity dest;

  rgw_sync_pipe_params params;

  bool specific() const {
    return source.specific() && dest.specific();
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(source, bl);
    encode(dest, bl);
    encode(params, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(source, bl);
    decode(dest, bl);
    decode(params, bl);
    DECODE_FINISH(bl);
  }

  const bool operator<(const rgw_sync_bucket_pipe& p) const {
    if (id < p.id) {
      return true;
    }
    if (id >p.id) {
      return false;
    }
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
  std::optional<rgw_bucket> bucket; /* define specific bucket */
  std::optional<std::set<rgw_zone_id> > zones; /* define specific zones, if not set then all zones */

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

    return (rgw_sync_bucket_entity::match_str(bucket->tenant, b->tenant) &&
            rgw_sync_bucket_entity::match_str(bucket->name, b->name) &&
            rgw_sync_bucket_entity::match_str(bucket->bucket_id, b->bucket_id));
  }

  void add_zones(const std::vector<rgw_zone_id>& new_zones);
  void remove_zones(const std::vector<rgw_zone_id>& rm_zones);
  void set_bucket(std::optional<std::string> tenant,
                  std::optional<std::string> bucket_name,
                  std::optional<std::string> bucket_id);
  void remove_bucket(std::optional<std::string> tenant,
                     std::optional<std::string> bucket_name,
                     std::optional<std::string> bucket_id);

  bool match_zone(const rgw_zone_id& zone) const {
    if (!zones) {
      if (all_zones) {
	return true;
      }
      return false;
    }

    return (zones->find(zone) != zones->end());
  }

  std::vector<rgw_sync_bucket_entity> expand() const;

  rgw_bucket get_bucket() const {
    return bucket.value_or(rgw_bucket());
  }

  static std::string bucket_key(std::optional<rgw_bucket> b);

  void set_all_zones(bool state) {
    all_zones = state;
    if (all_zones) {
      zones.reset();
    }
  }
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_entities)

struct rgw_sync_bucket_pipes {
  std::string id;
  rgw_sync_bucket_entities source;
  rgw_sync_bucket_entities dest;

  rgw_sync_pipe_params params;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(source, bl);
    encode(dest, bl);
    encode(params, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(source, bl);
    decode(dest, bl);
    decode(params, bl);
    DECODE_FINISH(bl);
  }

  bool match_source(const rgw_zone_id& zone, std::optional<rgw_bucket> b) const {
    return (source.match_zone(zone) && source.match_bucket(b));
  }

  bool match_dest(const rgw_zone_id& zone, std::optional<rgw_bucket> b) const {
    return (dest.match_zone(zone) && dest.match_bucket(b));
  }

  bool contains_zone_bucket(const rgw_zone_id& zone, std::optional<rgw_bucket> b) const {
    return (match_source(zone, b) || match_dest(zone, b));
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);

  std::vector<rgw_sync_bucket_pipe> expand() const;

  void get_potential_related_buckets(const rgw_bucket& bucket,
                                     std::set<rgw_bucket> *sources,
                                     std::set<rgw_bucket> *dests) const;
};
WRITE_CLASS_ENCODER(rgw_sync_bucket_pipes)

std::ostream& operator<<(std::ostream& os, const rgw_sync_bucket_entity& e);
std::ostream& operator<<(std::ostream& os, const rgw_sync_bucket_pipe& pipe);
std::ostream& operator<<(std::ostream& os, const rgw_sync_bucket_entities& e);
std::ostream& operator<<(std::ostream& os, const rgw_sync_bucket_pipes& pipe);

/*
 * define data flow between zones. Symmetrical: zones sync from each other.
 * Directional: one zone fetches data from another.
 */
struct rgw_sync_data_flow_group {
  std::vector<rgw_sync_symmetric_group> symmetrical;
  std::vector<rgw_sync_directional_rule> directional;

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
    return (symmetrical.empty() && directional.empty());
  }

  bool find_or_create_symmetrical(const std::string& flow_id, rgw_sync_symmetric_group **flow_group);
  void remove_symmetrical(const std::string& flow_id, std::optional<std::vector<rgw_zone_id> > zones);
  bool find_or_create_directional(const rgw_zone_id& source_zone, const rgw_zone_id& dest_zone, rgw_sync_directional_rule **flow_group);
  void remove_directional(const rgw_zone_id& source_zone, const rgw_zone_id& dest_zone);

  void init_default(const std::set<rgw_zone_id>& zones);
};
WRITE_CLASS_ENCODER(rgw_sync_data_flow_group)


struct rgw_sync_policy_group {
  std::string id;

  rgw_sync_data_flow_group data_flow; /* override data flow, however, will not be able to
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

  bool set_status(const std::string& s) {
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

  bool find_pipe(const std::string& pipe_id, bool create, rgw_sync_bucket_pipes **pipe);
  void remove_pipe(const std::string& pipe_id);

  void get_potential_related_buckets(const rgw_bucket& bucket,
                                     std::set<rgw_bucket> *sources,
                                     std::set<rgw_bucket> *dests) const;

};
WRITE_CLASS_ENCODER(rgw_sync_policy_group)

struct rgw_sync_policy_info {
  std::map<std::string, rgw_sync_policy_group> groups;

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
  static void generate_test_instances(std::list<rgw_sync_policy_info*>& o);
  void decode_json(JSONObj *obj);

  bool empty() const {
    return groups.empty();
  }

  void get_potential_related_buckets(const rgw_bucket& bucket,
                                     std::set<rgw_bucket> *sources,
                                     std::set<rgw_bucket> *dests) const;
};
WRITE_CLASS_ENCODER(rgw_sync_policy_info)


