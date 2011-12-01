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

#include <stdint.h>
#include <stdio.h>

#include <iostream>
#include <iomanip>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "hash.h"
#include "encoding.h"
#include "ceph_hash.h"

/* Maximum supported object name length for Ceph, in bytes.
 *
 * This comes directly out of the ext3/ext4/btrfs limits. We will issue a
 * nasty warning message in the (unlikely) event that you are not using one of
 * those filesystems, and your filesystem can't handle the full 255 characters.
 */
#define MAX_CEPH_OBJECT_NAME_LEN 4096

struct object_t {
  string name;

  object_t() {}
  object_t(const char *s) : name(s) {}
  object_t(const string& s) : name(s) {}
  void swap(object_t& o) {
    name.swap(o.name);
  }
  
  void encode(bufferlist &bl) const {
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(name, bl);
  }
};
WRITE_CLASS_ENCODER(object_t)

inline bool operator==(const object_t& l, const object_t& r) {
  return l.name == r.name;
}
inline bool operator!=(const object_t& l, const object_t& r) {
  return l.name != r.name;
}
inline bool operator>(const object_t& l, const object_t& r) {
  return l.name > r.name;
}
inline bool operator<(const object_t& l, const object_t& r) {
  return l.name < r.name;
}
inline bool operator>=(const object_t& l, const object_t& r) { 
  return l.name >= r.name;
}
inline bool operator<=(const object_t& l, const object_t& r) {
  return l.name <= r.name;
}
inline ostream& operator<<(ostream& out, const object_t& o) {
  return out << o.name;
}

namespace __gnu_cxx {
  template<> struct hash<object_t> {
    size_t operator()(const object_t& r) const { 
      //static hash<string> H;
      //return H(r.name);
      return ceph_str_hash_linux(r.name.c_str(), r.name.length());
    }
  };
}


struct file_object_t {
  uint64_t ino, bno;
  mutable char buf[33];

  file_object_t(uint64_t i=0, uint64_t b=0) : ino(i), bno(b) {
    buf[0] = 0;
  }
  
  const char *c_str() const {
    if (!buf[0])
      sprintf(buf, "%llx.%08llx", (long long unsigned)ino, (long long unsigned)bno);
    return buf;
  }

  operator object_t() {
    return object_t(c_str());
  }
};


// a locator constrains the placement of an object.  mainly, which pool
// does it go in.
struct object_locator_t {
  int64_t pool;
  int32_t preferred;
  string key;

  explicit object_locator_t() : pool(-1), preferred(-1) {}

  explicit object_locator_t(int64_t po) : pool(po), preferred(-1) {}

  explicit object_locator_t(int64_t po, int pre) : pool(po), preferred(pre) {}

  int get_pool() const {
    return pool;
  }
  int get_preferred() const {
    return preferred;
  }

  void clear() {
    pool = -1;
    preferred = -1;
    key = "";
  }

  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(pool, bl);
    ::encode(preferred, bl);
    ::encode(key, bl);
  }
  void decode(bufferlist::iterator& p) {
    __u8 struct_v;
    ::decode(struct_v, p);
    if (struct_v < 2) {
      int32_t op;
      ::decode(op, p);
      pool = op;
      int16_t pref;
      ::decode(pref, p);
      preferred = pref;
    } else {
      ::decode(pool, p);
      ::decode(preferred, p);
    }
    ::decode(key, p);
  }
};
WRITE_CLASS_ENCODER(object_locator_t)

inline bool operator==(const object_locator_t& l, const object_locator_t& r) {
  return l.pool == r.pool && l.preferred == r.preferred && l.key == r.key;
}
inline bool operator!=(const object_locator_t& l, const object_locator_t& r) {
  return !(l == r);
}

inline ostream& operator<<(ostream& out, const object_locator_t& loc)
{
  out << "@" << loc.pool;
  if (loc.preferred >= 0)
    out << "p" << loc.preferred;
  if (loc.key.length())
    out << ":" << loc.key;
  return out;
}


// ---------------------------
// snaps

struct snapid_t {
  uint64_t val;
  snapid_t(uint64_t v=0) : val(v) {}
  snapid_t operator+=(snapid_t o) { val += o.val; return *this; }
  snapid_t operator++() { ++val; return *this; }
  operator uint64_t() const { return val; }  
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

  void swap(sobject_t& o) {
    oid.swap(o.oid);
    snapid_t t = snap;
    snap = o.snap;
    o.snap = t;
  }

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

inline bool operator==(const sobject_t &l, const sobject_t &r) {
  return l.oid == r.oid && l.snap == r.snap;
}
inline bool operator!=(const sobject_t &l, const sobject_t &r) {
  return l.oid != r.oid || l.snap != r.snap;
}
inline bool operator>(const sobject_t &l, const sobject_t &r) {
  return l.oid > r.oid || (l.oid == r.oid && l.snap > r.snap);
}
inline bool operator<(const sobject_t &l, const sobject_t &r) {
  return l.oid < r.oid || (l.oid == r.oid && l.snap < r.snap);
}
inline bool operator>=(const sobject_t &l, const sobject_t &r) {
  return l.oid > r.oid || (l.oid == r.oid && l.snap >= r.snap);
}
inline bool operator<=(const sobject_t &l, const sobject_t &r) {
  return l.oid < r.oid || (l.oid == r.oid && l.snap <= r.snap);
}
inline ostream& operator<<(ostream& out, const sobject_t &o) {
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

typedef uint32_t filestore_hobject_key_t;
struct hobject_t {
  object_t oid;
  snapid_t snap;
  uint32_t hash;
  bool max;

private:
  string key;

public:
  const string &get_key() const {
    return key;
  }
  
  hobject_t() : snap(0), hash(0), max(false) {}
  hobject_t(object_t oid, const string& key, snapid_t snap, uint64_t hash) : 
    oid(oid), snap(snap), hash(hash), max(false),
    key(oid.name == key ? string() : key) {}

  hobject_t(const sobject_t &soid, const string &key, uint32_t hash) : 
    oid(soid.oid), snap(soid.snap), hash(hash), max(false),
    key(soid.oid.name == key ? string() : key) {}

  // maximum sorted value.
  static hobject_t get_max() {
    hobject_t h;
    h.max = true;
    return h;
  }
  bool is_max() const {
    return max;
  }

  filestore_hobject_key_t get_filestore_key() const {
    uint32_t retval = hash;
    // reverse nibbles
    retval = ((retval & 0x0f0f0f0f) << 4) | ((retval & 0xf0f0f0f0) >> 4);
    retval = ((retval & 0x00ff00ff) << 8) | ((retval & 0xff00ff00) >> 8);
    retval = ((retval & 0x0000ffff) << 16) | ((retval & 0xffff0000) >> 16);
    return retval;
  }

  /* Do not use when a particular hash function is needed */
  explicit hobject_t(const sobject_t &o) :
    oid(o.oid), snap(o.snap) {
    hash = __gnu_cxx::hash<sobject_t>()(o);
  }

  void swap(hobject_t &o) {
    hobject_t temp(o);
    o.oid = oid;
    o.key = key;
    o.snap = snap;
    o.hash = hash;
    oid = temp.oid;
    key = temp.key;
    snap = temp.snap;
    hash = temp.hash;
  }

  void encode(bufferlist& bl) const {
    __u8 version = 1;
    ::encode(version, bl);
    ::encode(key, bl);
    ::encode(oid, bl);
    ::encode(snap, bl);
    ::encode(hash, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 version;
    ::decode(version, bl);
    if (version >= 1)
      ::decode(key, bl);
    ::decode(oid, bl);
    ::decode(snap, bl);
    ::decode(hash, bl);
  }
};
WRITE_CLASS_ENCODER(hobject_t)

namespace __gnu_cxx {
  template<> struct hash<hobject_t> {
    size_t operator()(const hobject_t &r) const {
      static hash<object_t> H;
      static rjhash<uint64_t> I;
      return H(r.oid) ^ I(r.snap);
    }
  };
}

inline ostream& operator<<(ostream& out, const hobject_t& o)
{
  if (o.is_max())
    return out << "MAX";
  out << o.oid << "/" << o.snap << "/" << std::hex << o.hash << std::dec;
  if (o.get_key().length())
    out << "@" << o.get_key();
  return out;
}

// sort hobject_t's by <get_filestore_key,name,snapid>
inline bool operator==(const hobject_t &l, const hobject_t &r) {
  return l.oid == r.oid && l.snap == r.snap && l.hash == r.hash && l.max == r.max;
}
inline bool operator!=(const hobject_t &l, const hobject_t &r) {
  return l.oid != r.oid || l.snap != r.snap || l.hash != r.hash || l.max != r.max;
}
inline bool operator>(const hobject_t &l, const hobject_t &r) {
  return l.max > r.max ||
    (l.max == r.max && (l.get_filestore_key() > r.get_filestore_key() ||
			(l.get_filestore_key() == r.get_filestore_key() && (l.oid > r.oid || 
					      (l.oid == r.oid && l.snap > r.snap)))));
}
inline bool operator<(const hobject_t &l, const hobject_t &r) {
  return l.max < r.max ||
    (l.max == r.max && (l.get_filestore_key() < r.get_filestore_key() ||
			(l.get_filestore_key() == r.get_filestore_key() && (l.oid < r.oid ||
					      (l.oid == r.oid && l.snap < r.snap)))));
}
inline bool operator>=(const hobject_t &l, const hobject_t &r) {
  return l.max > r.max ||
    (l.max == r.max && (l.get_filestore_key() > r.get_filestore_key() ||
			(l.get_filestore_key() == r.get_filestore_key() && (l.oid > r.oid ||
					      (l.oid == r.oid && l.snap >= r.snap)))));
}
inline bool operator<=(const hobject_t &l, const hobject_t &r) {
  return l.max < r.max ||
    (l.max == r.max && (l.get_filestore_key() < r.get_filestore_key() ||
			(l.get_filestore_key() == r.get_filestore_key() && (l.oid < r.oid ||
					      (l.oid == r.oid && l.snap <= r.snap)))));
}


// ---------------------------

#endif
