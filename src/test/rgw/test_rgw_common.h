// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include <iostream>
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_rados.h"
#include "rgw/rgw_zone.h"

#ifndef CEPH_TEST_RGW_COMMON_H
#define CEPH_TEST_RGW_COMMON_H

struct old_rgw_bucket {
  std::string tenant;
  std::string name;
  std::string data_pool;
  std::string data_extra_pool; /* if not set, then we should use data_pool instead */
  std::string index_pool;
  std::string marker;
  std::string bucket_id;

  std::string oid; /*
                    * runtime in-memory only info. If not empty, points to the bucket instance object
                    */

  old_rgw_bucket() { }
  // cppcheck-suppress noExplicitConstructor
  old_rgw_bucket(const std::string& s) : name(s) {
    data_pool = index_pool = s;
    marker = "";
  }
  explicit old_rgw_bucket(const char *n) : name(n) {
    data_pool = index_pool = n;
    marker = "";
  }
  old_rgw_bucket(const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id, const char *h) :
    tenant(t), name(n), data_pool(dp), index_pool(ip), marker(m), bucket_id(id) {}

  void encode(bufferlist& bl) const {
     ENCODE_START(8, 3, bl);
    encode(name, bl);
    encode(data_pool, bl);
    encode(marker, bl);
    encode(bucket_id, bl);
    encode(index_pool, bl);
    encode(data_extra_pool, bl);
    encode(tenant, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(8, 3, 3, bl);
    decode(name, bl);
    decode(data_pool, bl);
    if (struct_v >= 2) {
      decode(marker, bl);
      if (struct_v <= 3) {
        uint64_t id;
        decode(id, bl);
        char buf[16];
        snprintf(buf, sizeof(buf), "%llu", (long long)id);
        bucket_id = buf;
      } else {
        decode(bucket_id, bl);
      }
    }
    if (struct_v >= 5) {
      decode(index_pool, bl);
    } else {
      index_pool = data_pool;
    }
    if (struct_v >= 7) {
      decode(data_extra_pool, bl);
    }
    if (struct_v >= 8) {
      decode(tenant, bl);
    }
    DECODE_FINISH(bl);
  }

  // format a key for the bucket/instance. pass delim=0 to skip a field
  std::string get_key(char tenant_delim = '/',
                      char id_delim = ':') const;

  const std::string& get_data_extra_pool() {
    if (data_extra_pool.empty()) {
      return data_pool;
    }
    return data_extra_pool;
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<old_rgw_bucket*>& o);

  bool operator<(const old_rgw_bucket& b) const {
    return name.compare(b.name) < 0;
  }
};
WRITE_CLASS_ENCODER(old_rgw_bucket)

class old_rgw_obj {
  std::string orig_obj;
  std::string loc;
  std::string object;
  std::string instance;
public:
  const std::string& get_object() const { return object; }
  const std::string& get_orig_obj() const { return orig_obj; }
  const std::string& get_loc() const { return loc; }
  const std::string& get_instance() const { return instance; }
  old_rgw_bucket bucket;
  std::string ns;

  bool in_extra_data; /* in-memory only member, does not serialize */

  // Represents the hash index source for this object once it is set (non-empty)
  std::string index_hash_source;

  old_rgw_obj() : in_extra_data(false) {}
  old_rgw_obj(old_rgw_bucket& b, const std::string& o) : in_extra_data(false) {
    init(b, o);
  }
  old_rgw_obj(old_rgw_bucket& b, const rgw_obj_key& k) : in_extra_data(false) {
    from_index_key(b, k);
  }
  void init(old_rgw_bucket& b, const std::string& o) {
    bucket = b;
    set_obj(o);
    reset_loc();
  }
  void init_ns(old_rgw_bucket& b, const std::string& o, const std::string& n) {
    bucket = b;
    set_ns(n);
    set_obj(o);
    reset_loc();
  }
  int set_ns(const char *n) {
    if (!n)
      return -EINVAL;
    std::string ns_str(n);
    return set_ns(ns_str);
  }
  int set_ns(const std::string& n) {
    if (n[0] == '_')
      return -EINVAL;
    ns = n;
    set_obj(orig_obj);
    return 0;
  }
  int set_instance(const std::string& i) {
    if (i[0] == '_')
      return -EINVAL;
    instance = i;
    set_obj(orig_obj);
    return 0;
  }

  int clear_instance() {
    return set_instance(string());
  }

  void set_loc(const std::string& k) {
    loc = k;
  }

  void reset_loc() {
    loc.clear();
    /*
     * For backward compatibility. Older versions used to have object locator on all objects,
     * however, the orig_obj was the effective object locator. This had the same effect as not
     * having object locator at all for most objects but the ones that started with underscore as
     * these were escaped.
     */
    if (orig_obj[0] == '_' && ns.empty()) {
      loc = orig_obj;
    }
  }

  bool have_null_instance() {
    return instance == "null";
  }

  bool have_instance() {
    return !instance.empty();
  }

  bool need_to_encode_instance() {
    return have_instance() && !have_null_instance();
  }

  void set_obj(const std::string& o) {
    object.reserve(128);

    orig_obj = o;
    if (ns.empty() && !need_to_encode_instance()) {
      if (o.empty()) {
        return;
      }
      if (o.size() < 1 || o[0] != '_') {
        object = o;
        return;
      }
      object = "_";
      object.append(o);
    } else {
      object = "_";
      object.append(ns);
      if (need_to_encode_instance()) {
        object.append(string(":") + instance);
      }
      object.append("_");
      object.append(o);
    }
    reset_loc();
  }

  /*
   * get the object's key name as being referred to by the bucket index.
   */
  std::string get_index_key_name() const {
    if (ns.empty()) {
      if (orig_obj.size() < 1 || orig_obj[0] != '_') {
        return orig_obj;
      }
      return std::string("_") + orig_obj;
    };

    char buf[ns.size() + 16];
    snprintf(buf, sizeof(buf), "_%s_", ns.c_str());
    return std::string(buf) + orig_obj;
  };

  void from_index_key(old_rgw_bucket& b, const rgw_obj_key& key) {
    if (key.name[0] != '_') {
      init(b, key.name);
      set_instance(key.instance);
      return;
    }
    if (key.name[1] == '_') {
      init(b, key.name.substr(1));
      set_instance(key.instance);
      return;
    }
    ssize_t pos = key.name.find('_', 1);
    if (pos < 0) {
      /* shouldn't happen, just use key */
      init(b, key.name);
      set_instance(key.instance);
      return;
    }

    init_ns(b, key.name.substr(pos + 1), key.name.substr(1, pos -1));
    set_instance(key.instance);
  }

  void get_index_key(rgw_obj_key *key) const {
    key->name = get_index_key_name();
    key->instance = instance;
  }

  static void parse_ns_field(string& ns, std::string& instance) {
    int pos = ns.find(':');
    if (pos >= 0) {
      instance = ns.substr(pos + 1);
      ns = ns.substr(0, pos);
    } else {
      instance.clear();
    }
  }

  std::string& get_hash_object() {
    return index_hash_source.empty() ? orig_obj : index_hash_source;
  }
  /**
   * Translate a namespace-mangled object name to the user-facing name
   * existing in the given namespace.
   *
   * If the object is part of the given namespace, it returns true
   * and cuts down the name to the unmangled version. If it is not
   * part of the given namespace, it returns false.
   */
  static bool translate_raw_obj_to_obj_in_ns(string& obj, std::string& instance, std::string& ns) {
    if (obj[0] != '_') {
      if (ns.empty()) {
        return true;
      }
      return false;
    }

    std::string obj_ns;
    bool ret = parse_raw_oid(obj, &obj, &instance, &obj_ns);
    if (!ret) {
      return ret;
    }

    return (ns == obj_ns);
  }

  static bool parse_raw_oid(const std::string& oid, std::string *obj_name, std::string *obj_instance, std::string *obj_ns) {
    obj_instance->clear();
    obj_ns->clear();
    if (oid[0] != '_') {
      *obj_name = oid;
      return true;
    }

    if (oid.size() >= 2 && oid[1] == '_') {
      *obj_name = oid.substr(1);
      return true;
    }

    if (oid[0] != '_' || oid.size() < 3) // for namespace, min size would be 3: _x_
      return false;

    int pos = oid.find('_', 1);
    if (pos <= 1) // if it starts with __, it's not in our namespace
      return false;

    *obj_ns = oid.substr(1, pos - 1);
    parse_ns_field(*obj_ns, *obj_instance);

    *obj_name = oid.substr(pos + 1);
    return true;
  }

  /**
   * Given a mangled object name and an empty namespace string, this
   * function extracts the namespace into the string and sets the object
   * name to be the unmangled version.
   *
   * It returns true after successfully doing so, or
   * false if it fails.
   */
  static bool strip_namespace_from_object(string& obj, std::string& ns, std::string& instance) {
    ns.clear();
    instance.clear();
    if (obj[0] != '_') {
      return true;
    }

    size_t pos = obj.find('_', 1);
    if (pos == std::string::npos) {
      return false;
    }

    if (obj[1] == '_') {
      obj = obj.substr(1);
      return true;
    }

    size_t period_pos = obj.find('.');
    if (period_pos < pos) {
      return false;
    }

    ns = obj.substr(1, pos-1);
    obj = obj.substr(pos+1, std::string::npos);

    parse_ns_field(ns, instance);
    return true;
  }

  void set_in_extra_data(bool val) {
    in_extra_data = val;
  }

  bool is_in_extra_data() const {
    return in_extra_data;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 3, bl);
    encode(bucket.name, bl);
    encode(loc, bl);
    encode(ns, bl);
    encode(object, bl);
    encode(bucket, bl);
    encode(instance, bl);
    if (!ns.empty() || !instance.empty()) {
      encode(orig_obj, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    decode(bucket.name, bl);
    decode(loc, bl);
    decode(ns, bl);
    decode(object, bl);
    if (struct_v >= 2)
      decode(bucket, bl);
    if (struct_v >= 4)
      decode(instance, bl);
    if (ns.empty() && instance.empty()) {
      if (object[0] != '_') {
        orig_obj = object;
      } else {
	orig_obj = object.substr(1);
      }
    } else {
      if (struct_v >= 5) {
        decode(orig_obj, bl);
      } else {
        ssize_t pos = object.find('_', 1);
        if (pos < 0) {
          throw buffer::error();
        }
        orig_obj = object.substr(pos);
      }
    }
    DECODE_FINISH(bl);
  }

  bool operator==(const old_rgw_obj& o) const {
    return (object.compare(o.object) == 0) &&
           (bucket.name.compare(o.bucket.name) == 0) &&
           (ns.compare(o.ns) == 0) &&
           (instance.compare(o.instance) == 0);
  }
  bool operator<(const old_rgw_obj& o) const {
    int r = bucket.name.compare(o.bucket.name);
    if (r == 0) {
      r = bucket.bucket_id.compare(o.bucket.bucket_id);
      if (r == 0) {
        r = object.compare(o.object);
        if (r == 0) {
          r = ns.compare(o.ns);
          if (r == 0) {
            r = instance.compare(o.instance);
          }
        }
      }
    }

    return (r < 0);
  }
};
WRITE_CLASS_ENCODER(old_rgw_obj)

static inline void prepend_old_bucket_marker(const old_rgw_bucket& bucket, const string& orig_oid, string& oid)
{
  if (bucket.marker.empty() || orig_oid.empty()) {
    oid = orig_oid;
  } else {
    oid = bucket.marker;
    oid.append("_");
    oid.append(orig_oid);
  }
}

void test_rgw_init_env(RGWZoneGroup *zonegroup, RGWZoneParams *zone_params);

struct test_rgw_env {
  RGWZoneGroup zonegroup;
  RGWZoneParams zone_params;
  rgw_data_placement_target default_placement;

  test_rgw_env() {
    test_rgw_init_env(&zonegroup, &zone_params);
    default_placement.data_pool = rgw_pool(zone_params.placement_pools[zonegroup.default_placement].data_pool);
    default_placement.data_extra_pool =  rgw_pool(zone_params.placement_pools[zonegroup.default_placement].data_extra_pool);
  }

  rgw_data_placement_target get_placement(const std::string& placement_id) {
    const RGWZonePlacementInfo& pi = zone_params.placement_pools[placement_id];
    rgw_data_placement_target pt;
    pt.index_pool = pi.index_pool;
    pt.data_pool = pi.data_pool;
    pt.data_extra_pool = pi.data_extra_pool;
    return pt;
  }

  rgw_raw_obj get_raw(const rgw_obj& obj) {
    rgw_obj_select s(obj);
    return s.get_raw_obj(zonegroup, zone_params);
  }

  rgw_raw_obj get_raw(const rgw_obj_select& os) {
    return os.get_raw_obj(zonegroup, zone_params);
  }
};

void test_rgw_add_placement(RGWZoneGroup *zonegroup, RGWZoneParams *zone_params, const std::string& name, bool is_default);
void test_rgw_populate_explicit_placement_bucket(rgw_bucket *b, const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id);
void test_rgw_populate_old_bucket(old_rgw_bucket *b, const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id);

std::string test_rgw_get_obj_oid(const rgw_obj& obj);
void test_rgw_init_explicit_placement_bucket(rgw_bucket *bucket, const char *name);
void test_rgw_init_old_bucket(old_rgw_bucket *bucket, const char *name);
void test_rgw_populate_bucket(rgw_bucket *b, const char *t, const char *n, const char *m, const char *id);
void test_rgw_init_bucket(rgw_bucket *bucket, const char *name);
rgw_obj test_rgw_create_obj(const rgw_bucket& bucket, const std::string& name, const std::string& instance, const std::string& ns);

#endif

