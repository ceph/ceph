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

#ifndef CEPH_SOBJECT_H
#define CEPH_SOBJECT_H

#include <iosfwd>
#include <list>

#include "include/hash.h"
#include "include/encoding.h"
#include "include/object.h"
#include "include/snapid.h"

namespace ceph { class Formatter; }

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
  void dump(ceph::Formatter *f) const;
  static std::list<sobject_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(sobject_t)

std::ostream& operator<<(std::ostream& out, const sobject_t &o);

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
