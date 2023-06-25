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

/* N.B., this header defines fundamental serialized types.  Do not
 * include files which can only be compiled in radosgw or OSD
 * contexts (e.g., rgw_sal.h, rgw_common.h) */

#pragma once

#include <fmt/format.h>

#include "rgw_pool_types.h"
#include "rgw_bucket_types.h"
#include "rgw_user_types.h"

#include "common/dout.h"
#include "common/Formatter.h"

struct rgw_obj_index_key { // cls_rgw_obj_key now aliases this type
  std::string name;
  std::string instance;

  rgw_obj_index_key() {}
  rgw_obj_index_key(const std::string &_name) : name(_name) {}
  rgw_obj_index_key(const std::string& n, const std::string& i) : name(n), instance(i) {}

  std::string to_string() const {
    return fmt::format("{}({})", name, instance);
  }

  bool empty() const {
    return name.empty();
  }

  void set(const std::string& _name) {
    name = _name;
    instance.clear();
  }

  bool operator==(const rgw_obj_index_key& k) const {
    return (name.compare(k.name) == 0) &&
           (instance.compare(k.instance) == 0);
  }

  bool operator!=(const rgw_obj_index_key& k) const {
    return (name.compare(k.name) != 0) ||
           (instance.compare(k.instance) != 0);
  }

  bool operator<(const rgw_obj_index_key& k) const {
    int r = name.compare(k.name);
    if (r == 0) {
      r = instance.compare(k.instance);
    }
    return (r < 0);
  }

  bool operator<=(const rgw_obj_index_key& k) const {
    return !(k < *this);
  }

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(instance, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    decode(instance, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_string("name", name);
    f->dump_string("instance", instance);
  }
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_obj_index_key*>& ls) {
    ls.push_back(new rgw_obj_index_key);
    ls.push_back(new rgw_obj_index_key);
    ls.back()->name = "name";
    ls.back()->instance = "instance";
  }

  size_t estimate_encoded_size() const {
    constexpr size_t start_overhead = sizeof(__u8) + sizeof(__u8) + sizeof(ceph_le32); // version and length prefix
    constexpr size_t string_overhead = sizeof(__u32); // strings are encoded with 32-bit length prefix
    return start_overhead +
        string_overhead + name.size() +
        string_overhead + instance.size();
  }
};
WRITE_CLASS_ENCODER(rgw_obj_index_key)

struct rgw_obj_key {
  std::string name;
  std::string instance;
  std::string ns;

  rgw_obj_key() {}

  // cppcheck-suppress noExplicitConstructor
  rgw_obj_key(const std::string& n) : name(n) {}
  rgw_obj_key(const std::string& n, const std::string& i) : name(n), instance(i) {}
  rgw_obj_key(const std::string& n, const std::string& i, const std::string& _ns) : name(n), instance(i), ns(_ns) {}

  rgw_obj_key(const rgw_obj_index_key& k) {
    parse_index_key(k.name, &name, &ns);
    instance = k.instance;
  }

  static void parse_index_key(const std::string& key, std::string *name, std::string *ns) {
    if (key[0] != '_') {
      *name = key;
      ns->clear();
      return;
    }
    if (key[1] == '_') {
      *name = key.substr(1);
      ns->clear();
      return;
    }
    ssize_t pos = key.find('_', 1);
    if (pos < 0) {
      /* shouldn't happen, just use key */
      *name = key;
      ns->clear();
      return;
    }

    *name = key.substr(pos + 1);
    *ns = key.substr(1, pos -1);
  }

  void set(const std::string& n) {
    name = n;
    instance.clear();
    ns.clear();
  }

  void set(const std::string& n, const std::string& i) {
    name = n;
    instance = i;
    ns.clear();
  }

  void set(const std::string& n, const std::string& i, const std::string& _ns) {
    name = n;
    instance = i;
    ns = _ns;
  }

  bool set(const rgw_obj_index_key& index_key) {
    if (!parse_raw_oid(index_key.name, this)) {
      return false;
    }
    instance = index_key.instance;
    return true;
  }

  void set_instance(const std::string& i) {
    instance = i;
  }

  const std::string& get_instance() const {
    return instance;
  }

  void set_ns(const std::string& _ns) {
    ns = _ns;
  }

  const std::string& get_ns() const {
    return ns;
  }

  std::string get_index_key_name() const {
    if (ns.empty()) {
      if (name.size() < 1 || name[0] != '_') {
        return name;
      }
      return std::string("_") + name;
    };

    char buf[ns.size() + 16];
    snprintf(buf, sizeof(buf), "_%s_", ns.c_str());
    return std::string(buf) + name;
  };

  void get_index_key(rgw_obj_index_key* key) const {
    key->name = get_index_key_name();
    key->instance = instance;
  }

  std::string get_loc() const {
    /*
     * For backward compatibility. Older versions used to have object locator on all objects,
     * however, the name was the effective object locator. This had the same effect as not
     * having object locator at all for most objects but the ones that started with underscore as
     * these were escaped.
     */
    if (name[0] == '_' && ns.empty()) {
      return name;
    }

    return {};
  }

  bool empty() const {
    return name.empty();
  }

  bool have_null_instance() const {
    return instance == "null";
  }

  bool have_instance() const {
    return !instance.empty();
  }

  bool need_to_encode_instance() const {
    return have_instance() && !have_null_instance();
  }

  std::string get_oid() const {
    if (ns.empty() && !need_to_encode_instance()) {
      if (name.size() < 1 || name[0] != '_') {
        return name;
      }
      return std::string("_") + name;
    }

    std::string oid = "_";
    oid.append(ns);
    if (need_to_encode_instance()) {
      oid.append(std::string(":") + instance);
    }
    oid.append("_");
    oid.append(name);
    return oid;
  }

  bool operator==(const rgw_obj_key& k) const {
    return (name.compare(k.name) == 0) &&
           (instance.compare(k.instance) == 0);
  }

  bool operator<(const rgw_obj_key& k) const {
    int r = name.compare(k.name);
    if (r == 0) {
      r = instance.compare(k.instance);
    }
    return (r < 0);
  }

  bool operator<=(const rgw_obj_key& k) const {
    return !(k < *this);
  }

  static void parse_ns_field(std::string& ns, std::string& instance) {
    int pos = ns.find(':');
    if (pos >= 0) {
      instance = ns.substr(pos + 1);
      ns = ns.substr(0, pos);
    } else {
      instance.clear();
    }
  }

  // takes an oid and parses out the namespace (ns), name, and
  // instance
  static bool parse_raw_oid(const std::string& oid, rgw_obj_key *key) {
    key->instance.clear();
    key->ns.clear();
    if (oid[0] != '_') {
      key->name = oid;
      return true;
    }

    if (oid.size() >= 2 && oid[1] == '_') {
      key->name = oid.substr(1);
      return true;
    }

    if (oid.size() < 3) // for namespace, min size would be 3: _x_
      return false;

    size_t pos = oid.find('_', 2); // oid must match ^_[^_].+$
    if (pos == std::string::npos)
      return false;

    key->ns = oid.substr(1, pos - 1);
    parse_ns_field(key->ns, key->instance);

    key->name = oid.substr(pos + 1);
    return true;
  }

  /**
   * Translate a namespace-mangled object name to the user-facing name
   * existing in the given namespace.
   *
   * If the object is part of the given namespace, it returns true
   * and cuts down the name to the unmangled version. If it is not
   * part of the given namespace, it returns false.
   */
  static bool oid_to_key_in_ns(const std::string& oid, rgw_obj_key *key, const std::string& ns) {
    bool ret = parse_raw_oid(oid, key);
    if (!ret) {
      return ret;
    }

    return (ns == key->ns);
  }

  /**
   * Given a mangled object name and an empty namespace std::string, this
   * function extracts the namespace into the std::string and sets the object
   * name to be the unmangled version.
   *
   * It returns true after successfully doing so, or
   * false if it fails.
   */
  static bool strip_namespace_from_name(std::string& name, std::string& ns, std::string& instance) {
    ns.clear();
    instance.clear();
    if (name[0] != '_') {
      return true;
    }

    size_t pos = name.find('_', 1);
    if (pos == std::string::npos) {
      return false;
    }

    if (name[1] == '_') {
      name = name.substr(1);
      return true;
    }

    size_t period_pos = name.find('.');
    if (period_pos < pos) {
      return false;
    }

    ns = name.substr(1, pos-1);
    name = name.substr(pos+1, std::string::npos);

    parse_ns_field(ns, instance);
    return true;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(name, bl);
    encode(instance, bl);
    encode(ns, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(name, bl);
    decode(instance, bl);
    if (struct_v >= 2) {
      decode(ns, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_obj_key)

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<rgw_obj_key> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const rgw_obj_key& key, FormatContext& ctx) const {
    if (key.instance.empty()) {
      return formatter<std::string_view>::format(key.name, ctx);
    } else {
      return fmt::format_to(ctx.out(), "{}[{}]", key.name, key.instance);
    }
  }
};
#endif

inline std::ostream& operator<<(std::ostream& out, const rgw_obj_key &key) {
#if FMT_VERSION >= 90000
  return out << fmt::format("{}", key);
#else
  if (key.instance.empty()) {
    return out << fmt::format("{}", key.name);
  } else {
    return out << fmt::format("{}[{}]", key.name, key.instance);
  }
#endif
}

struct rgw_raw_obj {
  rgw_pool pool;
  std::string oid;
  std::string loc;

  rgw_raw_obj() {}
  rgw_raw_obj(const rgw_pool& _pool, const std::string& _oid) {
    init(_pool, _oid);
  }
  rgw_raw_obj(const rgw_pool& _pool, const std::string& _oid, const std::string& _loc) : loc(_loc) {
    init(_pool, _oid);
  }

  void init(const rgw_pool& _pool, const std::string& _oid) {
    pool = _pool;
    oid = _oid;
  }

  bool empty() const {
    return oid.empty();
  }

  void encode(bufferlist& bl) const {
     ENCODE_START(6, 6, bl);
    encode(pool, bl);
    encode(oid, bl);
    encode(loc, bl);
    ENCODE_FINISH(bl);
  }

  void decode_from_rgw_obj(bufferlist::const_iterator& bl);

  void decode(bufferlist::const_iterator& bl) {
    unsigned ofs = bl.get_off();
    DECODE_START(6, bl);
    if (struct_v < 6) {
      /*
       * this object was encoded as rgw_obj, prior to rgw_raw_obj been split out of it,
       * let's decode it as rgw_obj and convert it
       */
      bl.seek(ofs);
      decode_from_rgw_obj(bl);
      return;
    }
    decode(pool, bl);
    decode(oid, bl);
    decode(loc, bl);
    DECODE_FINISH(bl);
  }

  bool operator<(const rgw_raw_obj& o) const {
    int r = pool.compare(o.pool);
    if (r == 0) {
      r = oid.compare(o.oid);
      if (r == 0) {
        r = loc.compare(o.loc);
      }
    }
    return (r < 0);
  }

  bool operator==(const rgw_raw_obj& o) const {
    return (pool == o.pool && oid == o.oid && loc == o.loc);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<rgw_raw_obj*>& o);
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_raw_obj)

inline std::ostream& operator<<(std::ostream& out, const rgw_raw_obj& o) {
  out << o.pool << ":" << o.oid;
  return out;
}

struct rgw_obj {
  rgw_bucket bucket;
  rgw_obj_key key;

  bool in_extra_data{false}; /* in-memory only member, does not serialize */

  // Represents the hash index source for this object once it is set (non-empty)
  std::string index_hash_source;

  rgw_obj() {}
  rgw_obj(const rgw_bucket& b, const std::string& name) : bucket(b), key(name) {}
  rgw_obj(const rgw_bucket& b, const rgw_obj_key& k) : bucket(b), key(k) {}
  rgw_obj(const rgw_bucket& b, const rgw_obj_index_key& k) : bucket(b), key(k) {}

  void init(const rgw_bucket& b, const rgw_obj_key& k) {
    bucket = b;
    key = k;
  }

  void init(const rgw_bucket& b, const std::string& name) {
    bucket = b;
    key.set(name);
  }

  void init(const rgw_bucket& b, const std::string& name, const std::string& i, const std::string& n) {
    bucket = b;
    key.set(name, i, n);
  }

  void init_ns(const rgw_bucket& b, const std::string& name, const std::string& n) {
    bucket = b;
    key.name = name;
    key.instance.clear();
    key.ns = n;
  }

  bool empty() const {
    return key.empty();
  }

  void set_key(const rgw_obj_key& k) {
    key = k;
  }

  std::string get_oid() const {
    return key.get_oid();
  }

  const std::string& get_hash_object() const {
    return index_hash_source.empty() ? key.name : index_hash_source;
  }

  void set_in_extra_data(bool val) {
    in_extra_data = val;
  }

  bool is_in_extra_data() const {
    return in_extra_data;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(6, 6, bl);
    encode(bucket, bl);
    encode(key.ns, bl);
    encode(key.name, bl);
    encode(key.instance, bl);
//    encode(placement_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
    if (struct_v < 6) {
      std::string s;
      decode(bucket.name, bl); /* bucket.name */
      decode(s, bl); /* loc */
      decode(key.ns, bl);
      decode(key.name, bl);
      if (struct_v >= 2)
        decode(bucket, bl);
      if (struct_v >= 4)
        decode(key.instance, bl);
      if (key.ns.empty() && key.instance.empty()) {
        if (key.name[0] == '_') {
          key.name = key.name.substr(1);
        }
      } else {
        if (struct_v >= 5) {
          decode(key.name, bl);
        } else {
          ssize_t pos = key.name.find('_', 1);
          if (pos < 0) {
            throw buffer::malformed_input();
          }
          key.name = key.name.substr(pos + 1);
        }
      }
    } else {
      decode(bucket, bl);
      decode(key.ns, bl);
      decode(key.name, bl);
      decode(key.instance, bl);
//      decode(placement_id, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<rgw_obj*>& o);

  bool operator==(const rgw_obj& o) const {
    return (key == o.key) &&
           (bucket == o.bucket);
  }
  bool operator<(const rgw_obj& o) const {
    int r = key.name.compare(o.key.name);
    if (r == 0) {
      r = bucket.bucket_id.compare(o.bucket.bucket_id); /* not comparing bucket.name, if bucket_id is equal so will be bucket.name */
      if (r == 0) {
        r = key.ns.compare(o.key.ns);
        if (r == 0) {
          r = key.instance.compare(o.key.instance);
        }
      }
    }

    return (r < 0);
  }

  const rgw_pool& get_explicit_data_pool() {
    if (!in_extra_data || bucket.explicit_placement.data_extra_pool.empty()) {
      return bucket.explicit_placement.data_pool;
    }
    return bucket.explicit_placement.data_extra_pool;
  }
};
WRITE_CLASS_ENCODER(rgw_obj)
