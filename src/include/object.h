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

#include "encoding.h"


struct object_t {
  union {
    __u8 raw[20];
    struct {
      uint64_t ino;    // "file" identifier
      uint32_t bno;    // "block" in that "file"
      uint64_t snap;   // snap revision.
    } __attribute__ ((packed));
  } __attribute__ ((packed));

  object_t() : ino(0), bno(0), snap(0) {}
  object_t(uint64_t i, uint32_t b) : ino(i), bno(b), snap(0) {}
  object_t(uint64_t i, uint32_t b, uint64_t r) : ino(i), bno(b), snap(r) {}

  // IMPORTANT: make this match struct ceph_object ****
  object_t(const ceph_object& co) {
    ino = co.ino;
    bno = co.bno;
    snap = co.snap;
  }  
  operator ceph_object() {
    ceph_object oid;
    oid.ino = ino;
    oid.bno = bno;
    oid.snap = snap;
    return oid;
  }
  void encode(bufferlist &bl) const {
    ::encode(ino, bl);
    ::encode(bno, bl);
    ::encode(snap, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u64 i, r;
    __u32 b;
    ::decode(i, bl);
    ::decode(b, bl);
    ::decode(r, bl);
    ino = i;
    bno = b;
    snap = r;
  }
} __attribute__ ((packed));
WRITE_CLASS_ENCODER(object_t)

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
  out << hex;
  out << o.ino << '.';
  out.setf(ios::right);
  out.fill('0');
  out << setw(8) << o.bno;
  out.unsetf(ios::right);
  if (o.snap) {
    if (o.snap == CEPH_NOSNAP)
      out << ".head";
    else
      out << '.' << o.snap;
  }
  out << dec;
  return out;
}

namespace __gnu_cxx {
  template<> struct hash<object_t> {
    size_t operator()(const object_t &r) const { 
      static rjhash<uint64_t> H;
      static rjhash<uint32_t> I;
      return H(r.ino) ^ I(r.bno) ^ H(r.snap);
    }
  };
}


struct coll_t {
  __u64 high;
  __u64 low;

  coll_t(__u64 h=0, __u64 l=0) : high(h), low(l) {}
  
  void encode(bufferlist& bl) const {
    ::encode(high, bl);
    ::encode(low, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u64 h, l;
    ::decode(h, bl);
    ::decode(l, bl);
    high = h;
    low = l;
  }
} __attribute__ ((packed));
WRITE_CLASS_ENCODER(coll_t)

inline ostream& operator<<(ostream& out, const coll_t& c) {
  return out << hex << c.high << '.' << c.low << dec;
}

inline bool operator<(const coll_t& l, const coll_t& r) {
  return l.high < r.high || (l.high == r.high && l.low < r.low);
}
inline bool operator<=(const coll_t& l, const coll_t& r) {
  return l.high < r.high || (l.high == r.high && l.low <= r.low);
}
inline bool operator==(const coll_t& l, const coll_t& r) {
  return l.high == r.high && l.low == r.low;
}
inline bool operator!=(const coll_t& l, const coll_t& r) {
  return l.high != r.high || l.low != r.low;
}
inline bool operator>(const coll_t& l, const coll_t& r) {
  return l.high > r.high || (l.high == r.high && l.low > r.low);
}
inline bool operator>=(const coll_t& l, const coll_t& r) {
  return l.high > r.high || (l.high == r.high && l.low >= r.low);
}


namespace __gnu_cxx {
  template<> struct hash<coll_t> {
    size_t operator()(const coll_t &c) const { 
      static rjhash<uint64_t> H;
      return H(c.high) ^ H(c.low);
    }
  };
}


#endif
