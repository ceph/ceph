// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_OBJECT_H
#define CEPH_OBJECT_H

#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <iosfwd>
#include <string>
#include <string>
#include <string_view>

#include "include/rados.h"
#include "include/unordered_map.h"
#include "common/Formatter.h"

#include "hash.h"
#include "encoding.h"
#include "ceph_hash.h"

struct object_t {
  std::string name;

  object_t() {}
  // cppcheck-suppress noExplicitConstructor
  object_t(const char *s) : name(s) {}
  // cppcheck-suppress noExplicitConstructor
  object_t(const std::string& s) : name(s) {}
  object_t(std::string&& s) : name(std::move(s)) {}
  object_t(std::string_view s) : name(s) {}

  auto operator<=>(const object_t&) const noexcept = default;

  void swap(object_t& o) {
    name.swap(o.name);
  }
  void clear() {
    name.clear();
  }

  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    encode(name, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    decode(name, bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_string("name", name);
  }

  static void generate_test_instances(std::list<object_t*>& o) {
    o.push_back(new object_t);
    o.push_back(new object_t("myobject"));
  }
};
WRITE_CLASS_ENCODER(object_t)

inline std::ostream& operator<<(std::ostream& out, const object_t& o) {
  return out << o.name;
}

namespace std {
template<> struct hash<object_t> {
  size_t operator()(const object_t& r) const {
    //static hash<string> H;
    //return H(r.name);
    return ceph_str_hash_linux(r.name.c_str(), r.name.length());
  }
};
} // namespace std


struct file_object_t {
  uint64_t ino, bno;
  mutable char buf[34];

  file_object_t(uint64_t i=0, uint64_t b=0) : ino(i), bno(b) {
    buf[0] = 0;
  }
  
  const char *c_str() const {
    if (!buf[0])
      snprintf(buf, sizeof(buf), "%llx.%08llx", (long long unsigned)ino, (long long unsigned)bno);
    return buf;
  }

  operator object_t() {
    return object_t(c_str());
  }
};


// ---------------------------
// snaps

struct snapid_t {
  uint64_t val;
  // cppcheck-suppress noExplicitConstructor
  snapid_t(uint64_t v=0) : val(v) {}
  snapid_t operator+=(snapid_t o) { val += o.val; return *this; }
  snapid_t operator++() { ++val; return *this; }
  operator uint64_t() const { return val; }
};

inline void encode(snapid_t i, ceph::buffer::list &bl) {
  using ceph::encode;
  encode(i.val, bl);
}
inline void decode(snapid_t &i, ceph::buffer::list::const_iterator &p) {
  using ceph::decode;
  decode(i.val, p);
}

template<>
struct denc_traits<snapid_t> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = true;
  static void bound_encode(const snapid_t& o, size_t& p) {
    denc(o.val, p);
  }
  static void encode(const snapid_t &o, ceph::buffer::list::contiguous_appender& p) {
    denc(o.val, p);
  }
  static void decode(snapid_t& o, ceph::buffer::ptr::const_iterator &p) {
    denc(o.val, p);
  }
};

inline std::ostream& operator<<(std::ostream& out, const snapid_t& s) {
  if (s == CEPH_NOSNAP)
    return out << "head";
  else if (s == CEPH_SNAPDIR)
    return out << "snapdir";
  else
    return out << std::hex << s.val << std::dec;
}


struct sobject_t {
  object_t oid;
  snapid_t snap;

  sobject_t() : snap(0) {}
  sobject_t(object_t o, snapid_t s) : oid(o), snap(s) {}

  auto operator<=>(const sobject_t&) const noexcept = default;

  void swap(sobject_t& o) {
    oid.swap(o.oid);
    snapid_t t = snap;
    snap = o.snap;
    o.snap = t;
  }

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(oid, bl);
    encode(snap, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(oid, bl);
    decode(snap, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("oid") << oid;
    f->dump_stream("snap") << snap;
  }
  static void generate_test_instances(std::list<sobject_t*>& o) {
    o.push_back(new sobject_t);
    o.push_back(new sobject_t(object_t("myobject"), 123));
  }
};
WRITE_CLASS_ENCODER(sobject_t)

inline std::ostream& operator<<(std::ostream& out, const sobject_t &o) {
  return out << o.oid << "/" << o.snap;
}
namespace std {
template<> struct hash<sobject_t> {
  size_t operator()(const sobject_t &r) const {
    static hash<object_t> H;
    static rjhash<uint64_t> I;
    return H(r.oid) ^ I(r.snap);
  }
};
} // namespace std

#endif
