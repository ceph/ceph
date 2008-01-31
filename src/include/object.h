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

#ifndef __OBJECT_H
#define __OBJECT_H

#include <stdint.h>

#include <iostream>
#include <iomanip>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "hash.h"

typedef uint32_t objectrev_t;

struct object_t {
  static const uint32_t MAXREV = 0xffffffffU;

  uint64_t ino;    // "file" identifier
  uint32_t bno;    // "block" in that "file"
  objectrev_t rev; // revision.  normally ctime (as epoch).

  object_t() : ino(0), bno(0), rev(0) {}
  object_t(uint64_t i, uint32_t b) : ino(i), bno(b), rev(0) {}
  object_t(uint64_t i, uint32_t b, uint32_t r) : ino(i), bno(b), rev(r) {}

  // IMPORTANT: make this match struct ceph_object ****
  object_t(const ceph_object& co) {
    ino = le64_to_cpu(co.ino);
    bno = le32_to_cpu(co.bno);
    rev = le32_to_cpu(co.rev);
  }  
  operator ceph_object() {
    ceph_object oid;
    oid.ino = cpu_to_le64(ino);
    oid.bno = cpu_to_le32(bno);
    oid.rev = cpu_to_le32(rev);
    return oid;
  }
} __attribute__ ((packed));

inline bool operator==(const object_t l, const object_t r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}
inline bool operator!=(const object_t l, const object_t r) {
  return memcmp(&l, &r, sizeof(l)) != 0;
}
inline bool operator>(const object_t l, const object_t r) {
  return memcmp(&l, &r, sizeof(l)) > 0;
}
inline bool operator<(const object_t l, const object_t r) {
  return memcmp(&l, &r, sizeof(l)) < 0;
}
inline bool operator>=(const object_t l, const object_t r) { 
  return memcmp(&l, &r, sizeof(l)) >= 0;
}
inline bool operator<=(const object_t l, const object_t r) {
  return memcmp(&l, &r, sizeof(l)) <= 0;
}
inline ostream& operator<<(ostream& out, const object_t o) {
  out << hex << o.ino << '.';
  out.setf(ios::right);
  out.fill('0');
  out << setw(8) << o.bno << dec;
  out.unsetf(ios::right);
  if (o.rev) 
    out << '.' << o.rev;
  return out;
}

namespace __gnu_cxx {
  template<> struct hash<object_t> {
    size_t operator()(const object_t &r) const { 
      static rjhash<uint64_t> H;
      static rjhash<uint32_t> I;
      return H(r.ino) ^ I(r.bno) ^ I(r.rev);
    }
  };

}
#endif
