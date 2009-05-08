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
      uint64_t pad;
    } __attribute__ ((packed));
  } __attribute__ ((packed));

  object_t() : ino(0), bno(0), pad(0) {}
  object_t(uint64_t i, uint32_t b) : ino(i), bno(b), pad(0) {}

  // IMPORTANT: make this match struct ceph_object ****
  object_t(const ceph_object& co) {
    ino = co.ino;
    bno = co.bno;
    pad = co.pad;
  }  
  operator ceph_object() {
    ceph_object oid;
    oid.ino = ino;
    oid.bno = bno;
    oid.pad = pad;
    return oid;
  }
  void encode(bufferlist &bl) const {
    ::encode(ino, bl);
    ::encode(bno, bl);
    ::encode(pad, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u64 i, r;
    __u32 b;
    ::decode(i, bl);
    ::decode(b, bl);
    ::decode(r, bl);
    ino = i;
    bno = b;
    pad = r;
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
  out << dec;
  return out;
}

namespace __gnu_cxx {
  template<> struct hash<object_t> {
    size_t operator()(const object_t &r) const { 
      static rjhash<uint64_t> H;
      static rjhash<uint32_t> I;
      return H(r.ino) ^ I(r.bno) ^ H(r.pad);
    }
  };
}


// ---------------------------
// snaps

struct snapid_t {
  __u64 val;
  snapid_t(__u64 v=0) : val(v) {}
  snapid_t operator+=(snapid_t o) { val += o.val; return *this; }
  snapid_t operator++() { ++val; return *this; }
  operator __u64() const { return val; }  
};

inline void encode(snapid_t i, bufferlist &bl) { encode(i.val, bl); }
inline void decode(snapid_t &i, bufferlist::iterator &p) { decode(i.val, p); }

inline ostream& operator<<(ostream& out, snapid_t s) {
  if (s == CEPH_NOSNAP)
    return out << "head";
  else if (s == CEPH_SNAPDIR)
    return out << "snapdir";
  else
    return out << hex << s.val << dec;
}


struct sobject_t {
  object_t oid;
  snapid_t snap;

  sobject_t() : snap(0) {}
  sobject_t(object_t o, snapid_t s) : oid(o), snap(s) {}

  void encode(bufferlist& bl) const {
    ::encode(oid, bl);
    ::encode(snap, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(oid, bl);
    ::decode(snap, bl);
  }
};
WRITE_CLASS_ENCODER(sobject_t)

inline bool operator==(const sobject_t l, const sobject_t r) {
  return l.oid == r.oid && l.snap == r.snap;
}
inline bool operator!=(const sobject_t l, const sobject_t r) {
  return l.oid != r.oid || l.snap != r.snap;
}
inline bool operator>(const sobject_t l, const sobject_t r) {
  return l.oid > r.oid || (l.oid == r.oid && l.snap > r.snap);
}
inline bool operator<(const sobject_t l, const sobject_t r) {
  return l.oid < r.oid || (l.oid == r.oid && l.snap < r.snap);
}
inline bool operator>=(const sobject_t l, const sobject_t r) { 
  return l.oid > r.oid || (l.oid == r.oid && l.snap >= r.snap);
}
inline bool operator<=(const sobject_t l, const sobject_t r) {
  return l.oid < r.oid || (l.oid == r.oid && l.snap <= r.snap);
}
inline ostream& operator<<(ostream& out, const sobject_t o) {
  return out << o.oid << "/" << o.snap;
}

namespace __gnu_cxx {
  template<> struct hash<sobject_t> {
    size_t operator()(const sobject_t &r) const { 
      static hash<object_t> H;
      static rjhash<uint64_t> I;
      return H(r.oid) ^ I(r.snap);
    }
  };
}

// ---------------------------

typedef sobject_t pobject_t;

struct coll_t {
  __u64 pgid;
  snapid_t snap;

  coll_t(__u64 p=0, snapid_t s=0) : pgid(p), snap(s) {}
  
  void encode(bufferlist& bl) const {
    ::encode(pgid, bl);
    ::encode(snap, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(pgid, bl);
    ::decode(snap, bl);
  }
};
WRITE_CLASS_ENCODER(coll_t)

inline ostream& operator<<(ostream& out, const coll_t& c) {
  return out << hex << c.pgid << '.' << c.snap << dec;
}

inline bool operator<(const coll_t& l, const coll_t& r) {
  return l.pgid < r.pgid || (l.pgid == r.pgid && l.snap < r.snap);
}
inline bool operator<=(const coll_t& l, const coll_t& r) {
  return l.pgid < r.pgid || (l.pgid == r.pgid && l.snap <= r.snap);
}
inline bool operator==(const coll_t& l, const coll_t& r) {
  return l.pgid == r.pgid && l.snap == r.snap;
}
inline bool operator!=(const coll_t& l, const coll_t& r) {
  return l.pgid != r.pgid || l.snap != r.snap;
}
inline bool operator>(const coll_t& l, const coll_t& r) {
  return l.pgid > r.pgid || (l.pgid == r.pgid && l.snap > r.snap);
}
inline bool operator>=(const coll_t& l, const coll_t& r) {
  return l.pgid > r.pgid || (l.pgid == r.pgid && l.snap >= r.snap);
}

namespace __gnu_cxx {
  template<> struct hash<coll_t> {
    size_t operator()(const coll_t &c) const { 
      static rjhash<uint32_t> H;
      static rjhash<uint64_t> I;
      return H(c.pgid) ^ I(c.snap);
    }
  };
}


#endif
