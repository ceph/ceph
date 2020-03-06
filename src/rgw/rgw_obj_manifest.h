// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_common.h"
#include "rgw_compression_types.h"

class RGWSI_Zone;
struct RGWZoneGroup;
struct RGWZoneParams;
class RGWRados;

class rgw_obj_select {
  rgw_placement_rule placement_rule;
  rgw_obj obj;
  rgw_raw_obj raw_obj;
  bool is_raw;

public:
  rgw_obj_select() : is_raw(false) {}
  explicit rgw_obj_select(const rgw_obj& _obj) : obj(_obj), is_raw(false) {}
  explicit rgw_obj_select(const rgw_raw_obj& _raw_obj) : raw_obj(_raw_obj), is_raw(true) {}
  rgw_obj_select(const rgw_obj_select& rhs) {
    placement_rule = rhs.placement_rule;
    is_raw = rhs.is_raw;
    if (is_raw) {
      raw_obj = rhs.raw_obj;
    } else {
      obj = rhs.obj;
    }
  }

  rgw_raw_obj get_raw_obj(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params) const;
  rgw_raw_obj get_raw_obj(RGWRados *store) const;

  rgw_obj_select& operator=(const rgw_obj& rhs) {
    obj = rhs;
    is_raw = false;
    return *this;
  }

  rgw_obj_select& operator=(const rgw_raw_obj& rhs) {
    raw_obj = rhs;
    is_raw = true;
    return *this;
  }

  void set_placement_rule(const rgw_placement_rule& rule) {
    placement_rule = rule;
  }
  void dump(Formatter *f) const;
};

struct RGWObjManifestPart {
  rgw_obj loc;   /* the object where the data is located */
  uint64_t loc_ofs;  /* the offset at that object where the data is located */
  uint64_t size;     /* the part size */

  RGWObjManifestPart() : loc_ofs(0), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(loc, bl);
    encode(loc_ofs, bl);
    encode(size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(2, 2, 2, bl);
     decode(loc, bl);
     decode(loc_ofs, bl);
     decode(size, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWObjManifestPart*>& o);
};
WRITE_CLASS_ENCODER(RGWObjManifestPart)

/*
 The manifest defines a set of rules for structuring the object parts.
 There are a few terms to note:
     - head: the head part of the object, which is the part that contains
       the first chunk of data. An object might not have a head (as in the
       case of multipart-part objects).
     - stripe: data portion of a single rgw object that resides on a single
       rados object.
     - part: a collection of stripes that make a contiguous part of an
       object. A regular object will only have one part (although might have
       many stripes), a multipart object might have many parts. Each part
       has a fixed stripe size, although the last stripe of a part might
       be smaller than that. Consecutive parts may be merged if their stripe
       value is the same.
*/

struct RGWObjManifestRule {
  uint32_t start_part_num;
  uint64_t start_ofs;
  uint64_t part_size; /* each part size, 0 if there's no part size, meaning it's unlimited */
  uint64_t stripe_max_size; /* underlying obj max size */
  string override_prefix;

  RGWObjManifestRule() : start_part_num(0), start_ofs(0), part_size(0), stripe_max_size(0) {}
  RGWObjManifestRule(uint32_t _start_part_num, uint64_t _start_ofs, uint64_t _part_size, uint64_t _stripe_max_size) :
                       start_part_num(_start_part_num), start_ofs(_start_ofs), part_size(_part_size), stripe_max_size(_stripe_max_size) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(start_part_num, bl);
    encode(start_ofs, bl);
    encode(part_size, bl);
    encode(stripe_max_size, bl);
    encode(override_prefix, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(start_part_num, bl);
    decode(start_ofs, bl);
    decode(part_size, bl);
    decode(stripe_max_size, bl);
    if (struct_v >= 2)
      decode(override_prefix, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWObjManifestRule)

class RGWObjManifest {
protected:
  bool explicit_objs{false}; /* really old manifest? */
  map<uint64_t, RGWObjManifestPart> objs;

  uint64_t obj_size{0};

  rgw_obj obj;
  uint64_t head_size{0};
  rgw_placement_rule head_placement_rule;

  uint64_t max_head_size{0};
  string prefix;
  rgw_bucket_placement tail_placement; /* might be different than the original bucket,
                                       as object might have been copied across pools */
  map<uint64_t, RGWObjManifestRule> rules;

  string tail_instance; /* tail object's instance */

  void convert_to_explicit(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params);
  int append_explicit(RGWObjManifest& m, const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params);
  void append_rules(RGWObjManifest& m, map<uint64_t, RGWObjManifestRule>::iterator& iter, string *override_prefix);

  void update_iterators() {
    begin_iter.seek(0);
    end_iter.seek(obj_size);
  }
public:

  RGWObjManifest() : begin_iter(this), end_iter(this) {}
  RGWObjManifest(const RGWObjManifest& rhs) {
    *this = rhs;
  }
  RGWObjManifest& operator=(const RGWObjManifest& rhs) {
    explicit_objs = rhs.explicit_objs;
    objs = rhs.objs;
    obj_size = rhs.obj_size;
    obj = rhs.obj;
    head_size = rhs.head_size;
    max_head_size = rhs.max_head_size;
    prefix = rhs.prefix;
    tail_placement = rhs.tail_placement;
    rules = rhs.rules;
    tail_instance = rhs.tail_instance;

    begin_iter.set_manifest(this);
    end_iter.set_manifest(this);

    begin_iter.seek(rhs.begin_iter.get_ofs());
    end_iter.seek(rhs.end_iter.get_ofs());

    return *this;
  }

  map<uint64_t, RGWObjManifestPart>& get_explicit_objs() {
    return objs;
  }


  void set_explicit(uint64_t _size, map<uint64_t, RGWObjManifestPart>& _objs) {
    explicit_objs = true;
    objs.swap(_objs);
    set_obj_size(_size);
  }

  void get_implicit_location(uint64_t cur_part_id, uint64_t cur_stripe, uint64_t ofs, string *override_prefix, rgw_obj_select *location);

  void set_trivial_rule(uint64_t tail_ofs, uint64_t stripe_max_size) {
    RGWObjManifestRule rule(0, tail_ofs, 0, stripe_max_size);
    rules[0] = rule;
    max_head_size = tail_ofs;
  }

  void set_multipart_part_rule(uint64_t stripe_max_size, uint64_t part_num) {
    RGWObjManifestRule rule(0, 0, 0, stripe_max_size);
    rule.start_part_num = part_num;
    rules[0] = rule;
    max_head_size = 0;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(7, 6, bl);
    encode(obj_size, bl);
    encode(objs, bl);
    encode(explicit_objs, bl);
    encode(obj, bl);
    encode(head_size, bl);
    encode(max_head_size, bl);
    encode(prefix, bl);
    encode(rules, bl);
    bool encode_tail_bucket = !(tail_placement.bucket == obj.bucket);
    encode(encode_tail_bucket, bl);
    if (encode_tail_bucket) {
      encode(tail_placement.bucket, bl);
    }
    bool encode_tail_instance = (tail_instance != obj.key.instance);
    encode(encode_tail_instance, bl);
    if (encode_tail_instance) {
      encode(tail_instance, bl);
    }
    encode(head_placement_rule, bl);
    encode(tail_placement.placement_rule, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN_32(7, 2, 2, bl);
    decode(obj_size, bl);
    decode(objs, bl);
    if (struct_v >= 3) {
      decode(explicit_objs, bl);
      decode(obj, bl);
      decode(head_size, bl);
      decode(max_head_size, bl);
      decode(prefix, bl);
      decode(rules, bl);
    } else {
      explicit_objs = true;
      if (!objs.empty()) {
        map<uint64_t, RGWObjManifestPart>::iterator iter = objs.begin();
        obj = iter->second.loc;
        head_size = iter->second.size;
        max_head_size = head_size;
      }
    }

    if (explicit_objs && head_size > 0 && !objs.empty()) {
      /* patch up manifest due to issue 16435:
       * the first object in the explicit objs list might not be the one we need to access, use the
       * head object instead if set. This would happen if we had an old object that was created
       * when the explicit objs manifest was around, and it got copied.
       */
      rgw_obj& obj_0 = objs[0].loc;
      if (!obj_0.get_oid().empty() && obj_0.key.ns.empty()) {
        objs[0].loc = obj;
        objs[0].size = head_size;
      }
    }

    if (struct_v >= 4) {
      if (struct_v < 6) {
        decode(tail_placement.bucket, bl);
      } else {
        bool need_to_decode;
        decode(need_to_decode, bl);
        if (need_to_decode) {
          decode(tail_placement.bucket, bl);
        } else {
          tail_placement.bucket = obj.bucket;
        }
      }
    }

    if (struct_v >= 5) {
      if (struct_v < 6) {
        decode(tail_instance, bl);
      } else {
        bool need_to_decode;
        decode(need_to_decode, bl);
        if (need_to_decode) {
          decode(tail_instance, bl);
        } else {
          tail_instance = obj.key.instance;
        }
      }
    } else { // old object created before 'tail_instance' field added to manifest
      tail_instance = obj.key.instance;
    }

    if (struct_v >= 7) {
      decode(head_placement_rule, bl);
      decode(tail_placement.placement_rule, bl);
    }

    update_iterators();
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWObjManifest*>& o);

  int append(RGWObjManifest& m, const RGWZoneGroup& zonegroup,
             const RGWZoneParams& zone_params);
  int append(RGWObjManifest& m, RGWSI_Zone *zone_svc);

  bool get_rule(uint64_t ofs, RGWObjManifestRule *rule);

  bool empty() {
    if (explicit_objs)
      return objs.empty();
    return rules.empty();
  }

  bool has_explicit_objs() {
    return explicit_objs;
  }

  bool has_tail() {
    if (explicit_objs) {
      if (objs.size() == 1) {
        map<uint64_t, RGWObjManifestPart>::iterator iter = objs.begin();
        rgw_obj& o = iter->second.loc;
        return !(obj == o);
      }
      return (objs.size() >= 2);
    }
    return (obj_size > head_size);
  }

  void set_head(const rgw_placement_rule& placement_rule, const rgw_obj& _o, uint64_t _s) {
    head_placement_rule = placement_rule;
    obj = _o;
    head_size = _s;

    if (explicit_objs && head_size > 0) {
      objs[0].loc = obj;
      objs[0].size = head_size;
    }
  }

  const rgw_obj& get_obj() {
    return obj;
  }

  void set_tail_placement(const rgw_placement_rule& placement_rule, const rgw_bucket& _b) {
    tail_placement.placement_rule = placement_rule;
    tail_placement.bucket = _b;
  }

  const rgw_bucket_placement& get_tail_placement() {
    return tail_placement;
  }

  const rgw_placement_rule& get_head_placement_rule() {
    return head_placement_rule;
  }

  void set_prefix(const string& _p) {
    prefix = _p;
  }

  const string& get_prefix() {
    return prefix;
  }

  void set_tail_instance(const string& _ti) {
    tail_instance = _ti;
  }

  const string& get_tail_instance() {
    return tail_instance;
  }

  void set_head_size(uint64_t _s) {
    head_size = _s;
  }

  void set_obj_size(uint64_t s) {
    obj_size = s;

    update_iterators();
  }

  uint64_t get_obj_size() {
    return obj_size;
  }

  uint64_t get_head_size() {
    return head_size;
  }

  uint64_t get_max_head_size() {
    return max_head_size;
  }

  class obj_iterator {
    RGWObjManifest *manifest = nullptr;
    uint64_t part_ofs = 0;   /* where current part starts */
    uint64_t stripe_ofs = 0; /* where current stripe starts */
    uint64_t ofs = 0;        /* current position within the object */
    uint64_t stripe_size = 0;      /* current part size */

    int cur_part_id = 0;
    int cur_stripe = 0;
    string cur_override_prefix;

    rgw_obj_select location;

    map<uint64_t, RGWObjManifestRule>::iterator rule_iter;
    map<uint64_t, RGWObjManifestRule>::iterator next_rule_iter;

    map<uint64_t, RGWObjManifestPart>::iterator explicit_iter;

    void update_explicit_pos();


  protected:

    void set_manifest(RGWObjManifest *m) {
      manifest = m;
    }

  public:
    obj_iterator() = default;
    explicit obj_iterator(RGWObjManifest *_m)
      : obj_iterator(_m, 0)
    {}
    obj_iterator(RGWObjManifest *_m, uint64_t _ofs) : manifest(_m) {
      seek(_ofs);
    }
    void seek(uint64_t ofs);

    void operator++();
    bool operator==(const obj_iterator& rhs) const {
      return (ofs == rhs.ofs);
    }
    bool operator!=(const obj_iterator& rhs) const {
      return (ofs != rhs.ofs);
    }
    const rgw_obj_select& get_location() {
      return location;
    }

    int get_cur_part_id() const {
      return cur_part_id;
    }

    /* start of current stripe */
    uint64_t get_stripe_ofs() {
      if (manifest->explicit_objs) {
        return explicit_iter->first;
      }
      return stripe_ofs;
    }

    /* current ofs relative to start of rgw object */
    uint64_t get_ofs() const {
      return ofs;
    }

    /* stripe number */
    int get_cur_stripe() const {
      return cur_stripe;
    }

    /* current stripe size */
    uint64_t get_stripe_size() {
      if (manifest->explicit_objs) {
        return explicit_iter->second.size;
      }
      return stripe_size;
    }

    /* offset where data starts within current stripe */
    uint64_t location_ofs() {
      if (manifest->explicit_objs) {
        return explicit_iter->second.loc_ofs;
      }
      return 0; /* all stripes start at zero offset */
    }

    void update_location();

    friend class RGWObjManifest;
    void dump(Formatter *f) const;
  }; // class obj_iterator

  const obj_iterator& obj_begin();
  const obj_iterator& obj_end();
  obj_iterator obj_find(uint64_t ofs);

  obj_iterator begin_iter;
  obj_iterator end_iter;

  /*
   * simple object generator. Using a simple single rule manifest.
   */
  class generator {
    RGWObjManifest *manifest;
    uint64_t last_ofs;
    uint64_t cur_part_ofs;
    int cur_part_id;
    int cur_stripe;
    uint64_t cur_stripe_size;
    string cur_oid;
    
    string oid_prefix;

    rgw_obj_select cur_obj;

    RGWObjManifestRule rule;

  public:
    generator() : manifest(NULL), last_ofs(0), cur_part_ofs(0), cur_part_id(0), 
		  cur_stripe(0), cur_stripe_size(0) {}
    int create_begin(CephContext *cct, RGWObjManifest *manifest,
                     const rgw_placement_rule& head_placement_rule,
                     const rgw_placement_rule *tail_placement_rule,
                     const rgw_bucket& bucket,
                     const rgw_obj& obj);

    int create_next(uint64_t ofs);

    rgw_raw_obj get_cur_obj(RGWZoneGroup& zonegroup, RGWZoneParams& zone_params) { return cur_obj.get_raw_obj(zonegroup, zone_params); }
    rgw_raw_obj get_cur_obj(RGWRados *store) const { return cur_obj.get_raw_obj(store); }

    /* total max size of current stripe (including head obj) */
    uint64_t cur_stripe_max_size() const {
      return cur_stripe_size;
    }
  };
};
WRITE_CLASS_ENCODER(RGWObjManifest)
