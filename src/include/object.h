// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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
#include <iosfwd>
#include <list>
#include <string>
#include <string>
#include <string_view>

#include "encoding.h"
#include "hash.h"
#include "ceph_hash.h"

namespace ceph { class Formatter; }

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

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);

  void dump(ceph::Formatter *f) const;

  static std::list<object_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(object_t)

std::ostream& operator<<(std::ostream& out, const object_t& o);

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
  
  const char *c_str() const;

  operator object_t() {
    return object_t(c_str());
  }
};

#endif
